//! `crew pause`, `crew resume`, and `crew standdown`: the General's brake and kill
//! switch (issue #41).
//!
//! Each posts to the broker's control endpoints as the operator, so the pause state is
//! recorded on the roster and the stream and every role honors it: a paused role, or a
//! stood-down crew, pulls no new work (its role card says so). `pause` / `resume` take
//! an optional role: with one they gate that role, without one the whole crew.
//! `standdown` halts every role at once and preserves the durable state, so the crew is
//! recoverable. The broker address comes from `--broker`, else the `CREW_BROKER_*`
//! environment.

use crew_substrate::broker::Config as BrokerConfig;
use crew_substrate::core::BrokerEndpoint;
use eyre::{eyre, Result, WrapErr};
use serde_json::json;

/// Pauses one role, or the whole crew when `role` is `None`.
///
/// # Errors
/// Returns an error if the broker cannot be reached, or rejects the request (for
/// example a named role that is not registered).
pub fn pause(broker: Option<&str>, role: Option<&str>) -> Result<()> {
    post(broker, "/pause", role)?;
    println!("{}", outcome("Paused", role));
    Ok(())
}

/// Resumes one role, or the whole crew when `role` is `None`.
///
/// # Errors
/// Returns an error if the broker cannot be reached, or rejects the request.
pub fn resume(broker: Option<&str>, role: Option<&str>) -> Result<()> {
    post(broker, "/resume", role)?;
    println!("{}", outcome("Resumed", role));
    Ok(())
}

/// Stands the whole crew down: halt every role now, preserving the state.
///
/// # Errors
/// Returns an error if the broker cannot be reached or rejects the request.
pub fn standdown(broker: Option<&str>) -> Result<()> {
    post(broker, "/standdown", None)?;
    println!(
        "Stood the crew down: every role halts and the state is preserved. Resume with \
         `crew resume`."
    );
    Ok(())
}

/// A confirmation line: `<verb> backend`, or `<verb> the crew` crew-wide.
fn outcome(verb: &str, role: Option<&str>) -> String {
    match role {
        Some(role) => format!("{verb} {role}."),
        None => format!("{verb} the crew."),
    }
}

/// Posts a control action to the broker, with an optional target role in the body.
fn post(broker: Option<&str>, path: &str, role: Option<&str>) -> Result<()> {
    let base = resolve_base(broker)?;
    let url = format!("{base}{path}");
    let body = match role {
        Some(role) => json!({ "role": role }),
        None => json!({}),
    };
    match ureq::post(&url)
        .set("content-type", "application/json")
        .send_string(&body.to_string())
    {
        Ok(_) => Ok(()),
        // The broker answered with a typed 4xx/5xx; surface its reason.
        Err(ureq::Error::Status(code, response)) => {
            let reason = broker_error(response).unwrap_or_else(|| format!("HTTP {code}"));
            Err(eyre!("the broker rejected the request: {reason}"))
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
    use super::{normalize_base, outcome};

    #[test]
    fn outcome_names_the_role_or_the_crew() {
        assert_eq!(outcome("Paused", Some("backend")), "Paused backend.");
        assert_eq!(outcome("Resumed", None), "Resumed the crew.");
    }

    #[test]
    fn normalize_base_defaults_the_scheme_and_trims() {
        assert_eq!(normalize_base("localhost:2739/"), "http://localhost:2739");
        assert_eq!(
            normalize_base("http://127.0.0.1:2739"),
            "http://127.0.0.1:2739"
        );
    }
}
