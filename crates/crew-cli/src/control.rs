//! The General's operator-facing sends: the free-form `crew brief`, and the
//! steering directives `crew redirect`, `crew belay`, and `crew command`.
//!
//! `crew brief` is the General's plain send (issue #118): a free-form `note` to
//! the commander by default, a named role, or a channel (`all-units` or a
//! pair). It is the counterpart to the agent shim's `crew send`
//! (`src/shim.rs`), but posts as the General rather than as an agent's role, so
//! it is how the General sets the unit to work or broadcasts to all-units.
//!
//! The directives let the General steer a running agent without tearing the
//! crew down (issue #38). Each posts a high-priority message to a role's direct
//! channel, from the General; the broker delivers it on the role's
//! self-filtered inbox stream (issue #10), and the role honors it at its next
//! tool boundary (its briefing tells it so). A `redirect` steers a role without
//! stopping it; a `belay` halts its current work and re-tasks it. Delivery is
//! the same whether the role is mid-turn or idle: the message lands on the
//! inbox, never by killing the process.
//!
//! `crew command` is the **direct override** (issue #42): the General orders a
//! specialist itself, bypassing the commander's fan-out, and the commander is
//! informed rather than bypassed silently, so the chain of command stays
//! intact. The default (brief the commander) is unchanged; the override is
//! explicit.
//!
//! `crew approve` and `crew approvals` are the rules-of-engagement gate (issue
//! #39): a role blocks on `crew_request_approval` before a risky action, and the
//! General lists the pending requests and grants or denies each, so the role
//! proceeds only on a grant.
//!
//! All post as the General, so unlike the agent shim they need no role card:
//! the broker address comes from `--broker`, else the `CREW_BROKER_*`
//! environment.

use crew_substrate::{
    broker::Config as BrokerConfig,
    core::{BrokerEndpoint, Channel, RoleId},
};
use eyre::{eyre, Result, WrapErr};
use serde_json::json;

/// Briefs the crew as the General: post a free-form `note` to the commander by
/// default, a named role, or a channel (issue #118).
///
/// This is the General's plain send, distinct from the agent shim's `crew send`
/// (which posts as an agent's role and needs a role card): it posts as the
/// General, so it needs only the broker address. The target follows the crew's
/// one addressing rule ([`Channel::resolve`]): `to` a role wins, else `channel`
/// a name (`all-units` or a pair like `a+b`), else the commander. This is both
/// the default brief that sets the unit to work and the all-units broadcast.
///
/// # Errors
/// Returns an error if the target is not routable, the broker configuration is
/// invalid, or the broker cannot be reached or rejects the message.
pub fn brief(
    broker: Option<&str>,
    to: Option<&str>,
    channel: Option<&str>,
    commander: Option<&str>,
    body: &str,
) -> Result<()> {
    let target = brief_target(to, channel, commander)?;
    let base = resolve_base(broker)?;
    let payload = json!({ "from": { "kind": "general" }, "kind": "note", "body": body });
    post_message(&base, target.name().as_str(), &payload, "brief")?;
    println!("briefed {}", target.name());
    Ok(())
}

/// Resolves a brief's target: `to` a role, else `channel` a name, else the
/// commander, applying the crew's one addressing rule ([`Channel::resolve`]).
fn brief_target(
    to: Option<&str>,
    channel: Option<&str>,
    commander: Option<&str>,
) -> Result<Channel> {
    let commander = RoleId::new(default_commander(commander));
    Channel::resolve(to, channel, &commander).ok_or_else(|| {
        eyre!("that is not a routable target; name a role, `all-units`, or a pair like `a+b`")
    })
}

/// Steers `role` mid-task without stopping it: post the General's `redirect`.
///
/// # Errors
/// Returns an error if `role` is not a plain role name, the broker
/// configuration is invalid, or the broker cannot be reached or rejects the
/// message.
pub fn redirect(broker: Option<&str>, role: &str, message: &str) -> Result<()> {
    direct(broker, role, "redirect", message)
}

/// Halts `role`'s current work and re-tasks it: post the General's `belay`.
///
/// # Errors
/// Returns an error if `role` is not a plain role name, the broker
/// configuration is invalid, or the broker cannot be reached or rejects the
/// message.
pub fn belay(broker: Option<&str>, role: &str, order: &str) -> Result<()> {
    direct(broker, role, "belay", order)
}

/// Commands `role` directly, bypassing the commander's fan-out, and informs the
/// commander so the override is visible rather than silent (issue #42).
///
/// The General overrides the commander to order a specialist itself: it posts
/// an `order` from the General to the role's `@role` channel (`title`, plus
/// `scope` and `acceptance` when given), and then a note to the commander's
/// feed announcing the direct order, so a reassignment or a direct task is
/// never bypassing the commander behind its back. Ordering the commander itself
/// carries no notice (it is the addressee). The default, briefing the
/// commander, is unchanged; this is the deliberate override.
///
/// # Errors
/// Returns an error if `role` (or the commander) is not a plain role name, the
/// broker configuration is invalid, or the broker cannot be reached or rejects
/// a message.
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
    let commander = default_commander(commander);
    let base = resolve_base(broker)?;

    // The direct order to the specialist: the General taking the commander's
    // ordering role.
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

/// Grants or denies a role's rules-of-engagement approval request (issue #39).
///
/// The General answers a `crew_request_approval` the role is blocked on: it
/// looks the request up by id to find the role that made it, then posts an
/// `approval_decision` back to that role. The waiting role unblocks and
/// proceeds only on a grant; a `reason` rides along for either.
///
/// # Errors
/// Returns an error if no pending request has the id, the broker configuration
/// is invalid, or the broker cannot be reached or rejects the decision.
pub fn approve(
    broker: Option<&str>,
    request_id: &str,
    deny: bool,
    reason: Option<&str>,
) -> Result<()> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(eyre!(
            "name the approval request id to answer (see `crew approvals`)"
        ));
    }
    let base = resolve_base(broker)?;
    let requester = find_request_sender(&base, request_id)?;
    let target = role_channel(&requester, "answer")?;
    let payload = json!({
        "from": { "kind": "general" },
        "kind": "approval_decision",
        "in_reply_to": request_id,
        "granted": !deny,
        "body": reason.unwrap_or_default(),
    });
    post_message(&base, target.name().as_str(), &payload, "approval decision")?;
    println!(
        "{} {requester}'s request {request_id}",
        if deny { "denied" } else { "approved" },
    );
    Ok(())
}

/// Lists the approval requests still awaiting a decision (issue #39).
///
/// A request is pending until an `approval_decision` names it, so the General
/// sees what to answer and each request's id.
///
/// # Errors
/// Returns an error if the broker configuration is invalid or the broker cannot
/// be reached.
pub fn approvals(broker: Option<&str>) -> Result<()> {
    let base = resolve_base(broker)?;
    let messages = fetch_messages(&base)?;

    let decided: std::collections::HashSet<&str> = messages
        .iter()
        .filter(|event| event["kind"]["data"]["kind"] == "approval_decision")
        .filter_map(|event| event["kind"]["data"]["in_reply_to"].as_str())
        .collect();
    let pending: Vec<&serde_json::Value> = messages
        .iter()
        .filter(|event| event["kind"]["data"]["kind"] == "approval_request")
        .filter(|event| !decided.contains(event["kind"]["data"]["id"].as_str().unwrap_or_default()))
        .collect();

    if pending.is_empty() {
        println!("No pending approval requests.");
        return Ok(());
    }
    println!("{} pending approval request(s):", pending.len());
    for request in pending {
        let data = &request["kind"]["data"];
        let id = data["id"].as_str().unwrap_or("?");
        let role = request["from"]["id"].as_str().unwrap_or("?");
        let action = data["action"].as_str().unwrap_or("?");
        let detail = data["body"].as_str().unwrap_or_default();
        println!("  {id}  {role} wants to {action}: {detail}");
        println!("      approve: crew approve {id}   deny: crew approve {id} --deny");
    }
    Ok(())
}

/// The role that posted the approval request `request_id`, so its decision
/// routes back to it.
fn find_request_sender(base: &str, request_id: &str) -> Result<String> {
    let request = fetch_messages(base)?
        .into_iter()
        .find(|event| {
            event["kind"]["data"]["kind"] == "approval_request"
                && event["kind"]["data"]["id"].as_str() == Some(request_id)
        })
        .ok_or_else(|| eyre!("no approval request has id `{request_id}`; see `crew approvals`"))?;
    request["from"]["id"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| eyre!("the approval request `{request_id}` carries no sender role"))
}

/// Fetches the broker's message events, for finding approval requests and
/// decisions.
fn fetch_messages(base: &str) -> Result<Vec<serde_json::Value>> {
    let url = format!("{base}/history?kind=message");
    let text = ureq::get(&url)
        .call()
        .map_err(|err| eyre!("could not reach the broker at {base}; is `crewd` running? {err}"))?
        .into_string()
        .wrap_err("could not read the broker response")?;
    let history: serde_json::Value =
        serde_json::from_str(&text).wrap_err("could not parse the broker response")?;
    match history["events"].as_array() {
        Some(events) => Ok(events.clone()),
        None => Ok(Vec::new()),
    }
}

/// Posts a General directive (`kind`, a `redirect` or `belay`) to `role`'s
/// direct channel with `body`, printing a short confirmation.
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

/// The commander to address by default: the given name if any, else the
/// conventional `commander`, with a leading `@` and whitespace stripped.
///
/// The General has no role card to name the crew's commander, so this matches
/// [`RoleCard`](crew_substrate::core::RoleCard)'s own default when a card omits
/// it.
fn default_commander(name: Option<&str>) -> String {
    name.map(str::trim)
        .filter(|name| !name.is_empty())
        .map_or_else(|| "commander".to_owned(), plain_role)
}

/// The direct `@role` channel for `role`, or an error naming what the caller
/// wanted to do.
fn role_channel(role: &str, verb: &str) -> Result<Channel> {
    Channel::parse(&format!("@{role}"))
        .filter(|channel| matches!(channel, Channel::Direct(_)))
        .ok_or_else(|| eyre!("`{role}` is not a role to {verb}; name a single specialist"))
}

/// The note that informs the commander of a direct order, so the override is
/// not silent.
fn commander_notice(role: &str, order: &str) -> String {
    format!(
        "Direct order from the General to {role}: {order}. You are informed, not bypassed: \
         adjust your plan around it."
    )
}

/// Posts `payload` to `channel`'s message endpoint, surfacing a broker refusal
/// as `what`.
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

/// The broker base URL: the `--broker` value if given, else the broker's
/// environment.
///
/// # Errors
/// Returns an error if `CREW_BROKER_HOST` or `CREW_BROKER_PORT` is set but
/// invalid.
fn resolve_base(flag: Option<&str>) -> Result<String> {
    if let Some(url) = flag {
        return Ok(normalize_base(url));
    }
    let config = BrokerConfig::from_env().wrap_err("could not read the broker configuration")?;
    Ok(BrokerEndpoint::new(config.host.to_string(), config.port).base_url())
}

/// Normalizes a `--broker` value: default the scheme to `http`, drop a trailing
/// slash.
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
    use super::{
        brief_target, commander_notice, default_commander, normalize_base, plain_role, role_channel,
    };

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
    fn default_commander_falls_back_to_commander_and_strips_the_at() {
        assert_eq!(default_commander(None), "commander");
        assert_eq!(default_commander(Some("  ")), "commander");
        assert_eq!(default_commander(Some(" @lead ")), "lead");
    }

    #[test]
    fn a_brief_defaults_to_the_commander_and_resolves_channels() {
        // No target: the free-form brief reaches the commander (issue #118).
        assert_eq!(
            brief_target(None, None, None).unwrap().name().as_str(),
            "@commander",
        );
        // A custom commander name is honored for the default brief.
        assert_eq!(
            brief_target(None, None, Some("lead"))
                .unwrap()
                .name()
                .as_str(),
            "@lead",
        );
        // A named role wins over the default.
        assert_eq!(
            brief_target(Some("backend"), None, None)
                .unwrap()
                .name()
                .as_str(),
            "@backend",
        );
        // A channel name broadcasts.
        assert_eq!(
            brief_target(None, Some("all-units"), None)
                .unwrap()
                .name()
                .as_str(),
            "all-units",
        );
        // An unroutable target is an error, not a silent misfire.
        assert!(
            brief_target(None, Some("nonsense"), None).is_err(),
            "an unrecognized channel does not resolve",
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
