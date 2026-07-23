//! The General's command-and-control directives: `crew redirect`, `crew belay`, and
//! `crew command`.
//!
//! These let the General steer a running agent without tearing the crew down (issue
//! #38). Each posts a high-priority message to a role's direct channel, from the
//! General; the broker delivers it on the role's self-filtered inbox stream (issue
//! #10), and the role honors it at its next tool boundary (its briefing tells it so).
//! A `redirect` steers a role without stopping it; a `belay` halts its current work and
//! re-tasks it. Delivery is the same whether the role is mid-turn or idle: the message
//! lands on the inbox, never by killing the process.
//!
//! `crew command` is the **direct override** (issue #42): the General orders a specialist
//! itself, bypassing the commander's fan-out, and the commander is informed rather than
//! bypassed silently, so the chain of command stays intact. The default (brief the
//! commander) is unchanged; the override is explicit.
//!
//! All post as the General, so unlike the agent shim (`src/shim.rs`) they need no role
//! card: the broker address comes from `--broker`, else the `CREW_BROKER_*` environment.

use crew_substrate::broker::Config as BrokerConfig;
use crew_substrate::core::{BrokerEndpoint, Channel};
use eyre::{eyre, Result, WrapErr};
use serde_json::json;

/// Steers `role` mid-task without stopping it: post the General's `redirect`.
///
/// # Errors
/// Returns an error if `role` is not a plain role name, the broker configuration is
/// invalid, or the broker cannot be reached or rejects the message.
pub fn redirect(broker: Option<&str>, role: &str, message: &str) -> Result<()> {
    direct(broker, role, "redirect", message)
}

/// Halts `role`'s current work and re-tasks it: post the General's `belay`.
///
/// # Errors
/// Returns an error if `role` is not a plain role name, the broker configuration is
/// invalid, or the broker cannot be reached or rejects the message.
pub fn belay(broker: Option<&str>, role: &str, order: &str) -> Result<()> {
    direct(broker, role, "belay", order)
}

/// Commands `role` directly, bypassing the commander's fan-out, and informs the commander
/// so the override is visible rather than silent (issue #42).
///
/// The General overrides the commander to order a specialist itself: it posts an `order`
/// from the General to the role's `@role` channel (`title`, plus `scope` and `acceptance`
/// when given), and then a note to the commander's feed announcing the direct order, so a
/// reassignment or a direct task is never bypassing the commander behind its back. Ordering
/// the commander itself carries no notice (it is the addressee). The default, briefing the
/// commander, is unchanged; this is the deliberate override.
///
/// # Errors
/// Returns an error if `role` (or the commander) is not a plain role name, the broker
/// configuration is invalid, or the broker cannot be reached or rejects a message.
pub fn command(
    broker: Option<&str>,
    role: &str,
    order: &str,
    scope: Option<&str>,
    acceptance: Option<&str>,
    commander: Option<&str>,
) -> Result<()> {
    let role = plain_role(role);
    let target = role_channel(&role, "command")?;
    let commander = commander
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map_or_else(|| "commander".to_owned(), plain_role);
    let base = resolve_base(broker)?;

    // The direct order to the specialist: the General taking the commander's ordering role.
    let order_payload = json!({
        "from": { "kind": "general" },
        "kind": "order",
        "title": order,
        "scope": scope.unwrap_or_default(),
        "owned_paths": [],
        "acceptance": acceptance.unwrap_or_default(),
    });
    post_message(&base, target.name().as_str(), &order_payload, "order")?;

    // Inform the commander, unless it is the one being commanded.
    if role.eq_ignore_ascii_case(&commander) {
        println!("ordered {role} directly");
        return Ok(());
    }
    let commander_channel = role_channel(&commander, "inform")?;
    let notice = json!({
        "from": { "kind": "general" },
        "kind": "note",
        "body": commander_notice(&role, order),
    });
    post_message(
        &base,
        commander_channel.name().as_str(),
        &notice,
        "commander notice",
    )?;
    println!("ordered {role} directly; informed {commander}");
    Ok(())
}

/// Posts a General directive (`kind`, a `redirect` or `belay`) to `role`'s direct
/// channel with `body`, printing a short confirmation.
fn direct(broker: Option<&str>, role: &str, kind: &str, body: &str) -> Result<()> {
    let role = plain_role(role);
    let target = role_channel(&role, "steer")?;
    let base = resolve_base(broker)?;
    let payload = json!({ "from": { "kind": "general" }, "kind": kind, "body": body });
    post_message(&base, target.name().as_str(), &payload, kind)?;
    println!("{kind} sent to {role}");
    Ok(())
}

/// Strips a leading `@` and surrounding whitespace from a role name.
fn plain_role(role: &str) -> String {
    role.trim().trim_start_matches('@').to_owned()
}

/// The direct `@role` channel for `role`, or an error naming what the caller wanted to do.
fn role_channel(role: &str, verb: &str) -> Result<Channel> {
    Channel::parse(&format!("@{role}"))
        .filter(|channel| matches!(channel, Channel::Direct(_)))
        .ok_or_else(|| eyre!("`{role}` is not a role to {verb}; name a single specialist"))
}

/// The note that informs the commander of a direct order, so the override is not silent.
fn commander_notice(role: &str, order: &str) -> String {
    format!(
        "Direct order from the General to {role}: {order}. You are informed, not bypassed: \
         adjust your plan around it."
    )
}

/// Posts `payload` to `channel`'s message endpoint, surfacing a broker refusal as `what`.
fn post_message(base: &str, channel: &str, payload: &serde_json::Value, what: &str) -> Result<()> {
    let url = format!("{base}/channels/{channel}/messages");
    match ureq::post(&url)
        .set("content-type", "application/json")
        .send_string(&payload.to_string())
    {
        Ok(_) => Ok(()),
        // The broker answered with a typed 4xx/5xx; surface its reason.
        Err(ureq::Error::Status(code, response)) => {
            let reason = broker_error(response).unwrap_or_else(|| format!("HTTP {code}"));
            Err(eyre!("the broker rejected the {what}: {reason}"))
        }
        // A transport error means the broker is unreachable.
        Err(err) => Err(err)
            .wrap_err_with(|| format!("could not reach the broker at {base}; is `crewd` running?")),
    }
}

/// The broker base URL: the `--broker` value if given, else the broker's environment.
///
/// # Errors
/// Returns an error if `CREW_BROKER_HOST` or `CREW_BROKER_PORT` is set but invalid.
fn resolve_base(flag: Option<&str>) -> Result<String> {
    if let Some(url) = flag {
        return Ok(normalize_base(url));
    }
    let config = BrokerConfig::from_env().wrap_err("could not read the broker configuration")?;
    Ok(BrokerEndpoint::new(config.host.to_string(), config.port).base_url())
}

/// Normalizes a `--broker` value: default the scheme to `http`, drop a trailing slash.
fn normalize_base(url: &str) -> String {
    let url = url.trim().trim_end_matches('/');
    if url.starts_with("http://") || url.starts_with("https://") {
        url.to_owned()
    } else {
        format!("http://{url}")
    }
}

/// The `{ "error": ... }` message from a broker error response, if any.
fn broker_error(response: ureq::Response) -> Option<String> {
    let text = response.into_string().ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    value.get("error")?.as_str().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::{commander_notice, normalize_base, plain_role, role_channel};

    #[test]
    fn normalize_base_defaults_the_scheme_and_trims() {
        assert_eq!(normalize_base("localhost:2739/"), "http://localhost:2739");
        assert_eq!(
            normalize_base("http://127.0.0.1:2739"),
            "http://127.0.0.1:2739"
        );
    }

    #[test]
    fn plain_role_strips_the_at_and_whitespace() {
        assert_eq!(plain_role(" @backend "), "backend");
        assert_eq!(plain_role("frontend"), "frontend");
    }

    #[test]
    fn a_role_resolves_to_its_direct_channel_and_a_bad_one_errors() {
        assert_eq!(
            role_channel("backend", "command").unwrap().name().as_str(),
            "@backend"
        );
        assert!(
            role_channel("frontend+backend", "command").is_err(),
            "a pair is not a single role to command",
        );
    }

    #[test]
    fn the_commander_notice_names_the_role_and_the_order_and_says_it_is_informed() {
        let notice = commander_notice("backend", "switch to the login bug");
        assert!(notice.contains("backend") && notice.contains("switch to the login bug"));
        assert!(
            notice.contains("informed, not bypassed"),
            "the commander is informed, not bypassed: {notice}",
        );
    }
}
