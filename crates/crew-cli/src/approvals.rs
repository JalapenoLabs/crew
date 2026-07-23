//! `crew approvals` / `crew approve` / `crew deny`: answer approval requests (issue #40).
//!
//! The General's front-end to the approval gate (issue #39). `crew approvals` lists the
//! requests waiting on a decision (`GET /approvals`), and `crew approve <id>` /
//! `crew deny <id> "reason"` resolve one (`POST /approvals/{id}/decision`), so the blocked
//! role proceeds or abandons the action. A native notification (`crew notify`) tells the
//! General a request is pending, so the crew is not blocked unknowingly. The broker address
//! comes from `--broker`, else the `CREW_BROKER_*` environment.

use std::fmt::Write as _;

use eyre::{eyre, Result, WrapErr};
use serde_json::{json, Value};

use crate::broker::resolve_base;

/// Prints the pending approval requests: who is waiting on your sign-off, and for what.
///
/// # Errors
/// Returns an error if the broker configuration is invalid, or the broker cannot be reached
/// or returns a malformed response.
pub fn list(broker: Option<&str>) -> Result<()> {
    let base = resolve_base(broker)?;
    let url = format!("{base}/approvals");
    let text = ureq::get(&url)
        .call()
        .map_err(|err| unreachable_broker(&base, &err))?
        .into_string()
        .wrap_err("could not read the approval gate")?;
    let view: Value = serde_json::from_str(&text).wrap_err("the approval gate was malformed")?;
    println!("{}", render_list(&view));
    Ok(())
}

/// Approves a pending request, so the blocked role proceeds.
///
/// # Errors
/// Returns an error if the broker configuration is invalid, the request is unknown or
/// already decided, or the broker cannot be reached.
pub fn approve(broker: Option<&str>, id: &str) -> Result<()> {
    decide(broker, id, true, "")?;
    println!("Approved {id}. The role may proceed.");
    Ok(())
}

/// Denies a pending request with a reason, so the blocked role abandons the action.
///
/// # Errors
/// Returns an error if the reason is empty, the broker configuration is invalid, the
/// request is unknown or already decided, or the broker cannot be reached.
pub fn deny(broker: Option<&str>, id: &str, reason: &str) -> Result<()> {
    if reason.trim().is_empty() {
        return Err(eyre!(
            "a denial needs a reason, so the role learns why: crew deny <id> \"the reason\""
        ));
    }
    decide(broker, id, false, reason)?;
    println!("Denied {id}: {reason}. The role will abandon the action.");
    Ok(())
}

/// Posts a decision on a request, surfacing the broker's refusal as a readable error.
fn decide(broker: Option<&str>, id: &str, approve: bool, reason: &str) -> Result<()> {
    let base = resolve_base(broker)?;
    let url = format!("{base}/approvals/{id}/decision");
    let payload = json!({ "approve": approve, "reason": reason });
    ureq::post(&url)
        .set("content-type", "application/json")
        .send_string(&payload.to_string())
        .map_err(|err| {
            let ureq::Error::Status(_, response) = err else {
                return unreachable_broker(&base, &err);
            };
            let message = response
                .into_string()
                .ok()
                .and_then(|body| {
                    serde_json::from_str::<Value>(&body)
                        .ok()
                        .and_then(|value| value["error"].as_str().map(str::to_owned))
                })
                .unwrap_or_else(|| "the broker refused the decision".to_owned());
            eyre!("{message}")
        })?;
    Ok(())
}

/// The error for a broker that cannot be reached.
fn unreachable_broker(base: &str, err: &ureq::Error) -> eyre::Report {
    eyre!("could not reach the broker at {base}; is `crewd` running? ({err})")
}

/// Renders the approval gate for the operator: one line per pending request, oldest first.
fn render_list(view: &Value) -> String {
    let empty = Vec::new();
    let requests = view["requests"].as_array().unwrap_or(&empty);
    let pending: Vec<&Value> = requests
        .iter()
        .filter(|request| request["decision"] == "pending")
        .collect();
    if pending.is_empty() {
        return "No approvals are waiting.".to_owned();
    }
    let mut out = format!(
        "{} approval{} waiting:\n",
        pending.len(),
        if pending.len() == 1 { "" } else { "s" }
    );
    for request in pending {
        let id = request["id"].as_str().unwrap_or("?");
        let role = request["role"].as_str().unwrap_or("?");
        let action = request["action"].as_str().unwrap_or("?");
        let detail = request["detail"].as_str().unwrap_or("");
        if detail.is_empty() {
            let _ = writeln!(out, "  {id}  {role} -> {action}");
        } else {
            let _ = writeln!(out, "  {id}  {role} -> {action}: {detail}");
        }
    }
    out.push_str("Approve with `crew approve <id>`, or deny with `crew deny <id> \"reason\"`.");
    out
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::render_list;

    #[test]
    fn an_empty_gate_reads_as_nothing_waiting() {
        let line = render_list(&json!({ "requests": [] }));
        assert!(line.contains("No approvals are waiting"));
    }

    #[test]
    fn the_list_shows_pending_requests_and_hides_decided_ones() {
        let view = json!({ "requests": [
            { "id": "abc", "role": "backend", "action": "merge", "detail": "PR #42", "decision": "pending" },
            { "id": "def", "role": "frontend", "action": "push", "decision": "approved" },
        ]});
        let line = render_list(&view);
        assert!(
            line.contains("1 approval waiting"),
            "counts only pending: {line}"
        );
        assert!(line.contains("abc") && line.contains("backend -> merge: PR #42"));
        assert!(
            !line.contains("def"),
            "a decided request is not listed: {line}"
        );
        assert!(line.contains("crew approve") && line.contains("crew deny"));
    }
}
