//! The General's command-and-control directives: `crew redirect` and `crew belay`.
//!
//! These let the General steer a running agent without tearing the crew down (issue
//! #38). Each posts a high-priority message to a role's direct channel, from the
//! General; the broker delivers it on the role's self-filtered inbox stream (issue
//! #10), and the role honors it at its next tool boundary (its briefing tells it so).
//! A `redirect` steers a role without stopping it; a `belay` halts its current work and
//! re-tasks it. Delivery is the same whether the role is mid-turn or idle: the message
//! lands on the inbox, never by killing the process.
//!
//! Both post as the General, so unlike the agent shim (`src/shim.rs`) they need no role
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

/// Posts a General directive (`kind`, a `redirect` or `belay`) to `role`'s direct
/// channel with `body`, printing a short confirmation.
fn direct(broker: Option<&str>, role: &str, kind: &str, body: &str) -> Result<()> {
    let target = Channel::parse(&format!("@{}", role.trim().trim_start_matches('@')))
        .filter(|channel| matches!(channel, Channel::Direct(_)))
        .ok_or_else(|| eyre!("`{role}` is not a role to steer; name a single specialist"))?;
    let base = resolve_base(broker)?;
    let url = format!("{base}/channels/{}/messages", target.name().as_str());
    let payload = json!({ "from": { "kind": "general" }, "kind": kind, "body": body });

    match ureq::post(&url)
        .set("content-type", "application/json")
        .send_string(&payload.to_string())
    {
        Ok(_) => {
            println!("{kind} sent to {role}");
            Ok(())
        }
        // The broker answered with a typed 4xx/5xx; surface its reason.
        Err(ureq::Error::Status(code, response)) => {
            let reason = broker_error(response).unwrap_or_else(|| format!("HTTP {code}"));
            Err(eyre!("the broker rejected the {kind}: {reason}"))
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
    use super::normalize_base;

    #[test]
    fn normalize_base_defaults_the_scheme_and_trims() {
        assert_eq!(normalize_base("localhost:2739/"), "http://localhost:2739");
        assert_eq!(
            normalize_base("http://127.0.0.1:2739"),
            "http://127.0.0.1:2739"
        );
    }
}
