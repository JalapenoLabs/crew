//! `crew pause`, `crew resume`, and `crew standdown`: the General's brake and
//! kill switch (issue #41).
//!
//! Each posts to the broker's control endpoints as the operator, so the pause
//! state is recorded on the roster and the stream and every role honors it: a
//! paused role, or a stood-down crew, pulls no new work (its role card says
//! so). `pause` / `resume` take an optional role: with one they gate that role,
//! without one the whole crew. `standdown` halts every role at once and
//! preserves the durable state, so the crew is recoverable. The broker address
//! comes from `--broker`, else the `CREW_BROKER_*` environment.

use eyre::{eyre, Result, WrapErr};
use serde_json::json;

use crate::broker_base::{broker_error, resolve_base};

/// Pauses one role, or the whole crew when `role` is `None`.
///
/// # Errors
/// Returns an error if the broker cannot be reached, or rejects the request
/// (for example a named role that is not registered).
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

/// Posts a control action to the broker, with an optional target role in the
/// body.
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

#[cfg(test)]
mod tests {
    use super::outcome;

    #[test]
    fn outcome_names_the_role_or_the_crew() {
        assert_eq!(outcome("Paused", Some("backend")), "Paused backend.");
        assert_eq!(outcome("Resumed", None), "Resumed the crew.");
    }
}
