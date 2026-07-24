//! Parsing each agent's `--output-format stream-json` into activity events
//! (issue #24).
//!
//! The supervisor spawns each agent as a headless `claude -p` process emitting
//! stream-json: one JSON object per stdout line, tagged by a `type` field (the
//! same shapes Seraphim parses). [`parse`] distills each line into zero or more
//! [`Activity`] items, mapping the shapes crew models and dropping the firehose
//! it does not (a session `init` is a turn start, the `result` line a turn end,
//! an assistant `tool_use` a tool call, and assistant `text` output). A line
//! the parser does not recognize becomes [`Activity::Other`] rather than an
//! error, so the log survives a schema drift across Claude Code versions.
//!
//! [`forward_activity`] is the runtime half: it reads the fleet's captured
//! output on a detached thread and records each parsed activity on the broker,
//! keyed by role, so a role's turns and tool calls appear on its per-agent
//! timeline and the aggregate stream. This is the per-agent activity log the
//! broker cannot see, since it happens inside the agent's process (see
//! `docs/observability.md`, two event sources).

use std::{sync::mpsc::Receiver, thread};

use crew_core::Activity;
use serde_json::Value;
use tracing::{event, Level};

use crate::{
    lifecycle::UsageRecorder,
    roster::RosterClient,
    spawn::{Captured, OutputStream},
};

/// Parses one stream-json line into the activity items crew models.
///
/// Returns a `Vec` because one assistant line can carry several content blocks
/// (text plus tool calls). A blank line yields nothing; a non-JSON line or an
/// unrecognized `type` yields a single [`Activity::Other`], so an unexpected
/// shape is surfaced, never a crash.
pub(crate) fn parse(line: &str) -> Vec<Activity> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
        // Stream-json rides stdout as one JSON object per line, so a non-JSON
        // stdout line is unexpected; surface it rather than swallow it.
        return vec![Activity::Other {
            raw: "non-json".to_owned(),
        }];
    };

    match value.get("type").and_then(Value::as_str) {
        // The session init is the turn's start. Claude Code emits other `system`
        // subtypes (`thinking_tokens`, `status`, ...) as live telemetry; dropping
        // them keeps the activity log to the turns, tools, and text crew models.
        Some("system") => match value.get("subtype").and_then(Value::as_str) {
            Some("init") => vec![Activity::TurnStarted],
            _ => Vec::new(),
        },
        Some("assistant") => parse_assistant(&value),
        // The terminal line of a turn.
        Some("result") => vec![Activity::TurnEnded],
        // Known shapes crew does not model as activity: tool results, the
        // partial-message usage firehose (the per-turn token feed comes from the
        // `result` line instead, issue #177), and rate-limit notices (whose
        // subscription-usage feed is issue #113). Drop them so the log stays clean.
        Some("user" | "stream_event" | "message_start" | "message_delta" | "rate_limit_event") => {
            Vec::new()
        }
        // An unrecognized top-level shape (a schema drift): keep it as `Other` so
        // it is visible on the stream instead of silently dropped.
        Some(other) => vec![Activity::Other {
            raw: other.to_owned(),
        }],
        None => vec![Activity::Other { raw: String::new() }],
    }
}

/// Expands an `assistant` line's content blocks into text and tool-call
/// activity.
fn parse_assistant(value: &Value) -> Vec<Activity> {
    let Some(blocks) = value.pointer("/message/content").and_then(Value::as_array) else {
        return vec![Activity::Other {
            raw: "assistant".to_owned(),
        }];
    };

    let mut activities = Vec::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(text) = block.get("text").and_then(Value::as_str) {
                    if !text.trim().is_empty() {
                        activities.push(Activity::Output {
                            text: text.to_owned(),
                        });
                    }
                }
            }
            Some("tool_use") => {
                let tool = block
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned();
                activities.push(Activity::ToolCall { tool });
            }
            // `thinking` / `redacted_thinking` and any other block are outside the
            // crew activity vocabulary; drop them.
            _ => {}
        }
    }
    activities
}

/// A turn's token-and-cost usage, parsed from a stream-json `result` line
/// (issue #177).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TurnUsage {
    /// The turn's total tokens: input, output, and cache read and creation
    /// summed.
    tokens: u64,
    /// The turn's cost in micro-USD (millionths of a dollar).
    cost_micro_usd: u64,
}

/// Extracts a turn's usage from a stream-json line, or `None` when it is not a
/// `result` or carries no spend to charge (issue #177).
///
/// Only the `result` line ends a turn with its final `usage` (the summed token
/// fields) and `total_cost_usd`; every other line carries no turn total. A
/// `result` with neither tokens nor cost yields `None`, so a zero charge is
/// never reported. A non-JSON or typeless line is not a result, so it too
/// yields `None`.
fn parse_usage(line: &str) -> Option<TurnUsage> {
    let value = serde_json::from_str::<Value>(line.trim()).ok()?;
    if value.get("type").and_then(Value::as_str) != Some("result") {
        return None;
    }
    let tokens = value.get("usage").map_or(0, sum_tokens);
    let cost_micro_usd = value
        .get("total_cost_usd")
        .and_then(Value::as_f64)
        .map_or(0, to_micro_usd);
    (tokens != 0 || cost_micro_usd != 0).then_some(TurnUsage {
        tokens,
        cost_micro_usd,
    })
}

/// Sums a `usage` object's token fields, each absent field counting as zero.
///
/// Input, output, and both cache token counts are the tokens the turn
/// processed, so the crew budget (a token ceiling, issue #54) charges their
/// sum.
fn sum_tokens(usage: &Value) -> u64 {
    [
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
    ]
    .iter()
    .filter_map(|field| usage.get(field).and_then(Value::as_u64))
    .fold(0, u64::saturating_add)
}

/// Converts a dollar cost to whole micro-USD (millionths of a dollar).
///
/// A non-finite or negative cost (never expected from the parser) is zero; the
/// saturating float-to-int cast clamps the rounded value into `u64`.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the guarded, saturating float-to-int cast clamps to 0..=u64::MAX micro-USD"
)]
fn to_micro_usd(dollars: f64) -> u64 {
    if !dollars.is_finite() || dollars <= 0.0 {
        return 0;
    }
    (dollars * 1_000_000.0).round() as u64
}

/// Reads the fleet's captured output and records each agent's parsed activity
/// on the broker, keyed by role (issue #24), charging each turn's usage against
/// the crew budget (issue #177).
///
/// Spawns a detached thread that drains the merged capture channel: for each
/// stdout line (stream-json is on stdout; stderr carries the briefing echo and
/// diagnostics), it parses the line, emits every resulting activity through
/// `roster`, and, when the line is a `result` carrying usage, charges the
/// turn's tokens and cost through `recorder`, so budget enforcement runs off
/// real spend rather than only when a caller pokes the seam. Both are
/// best-effort: a broker hiccup or a charge failure is logged, never fatal, so
/// a dropped event never takes an agent down. The thread ends when the channel
/// disconnects, which happens once every agent has stopped and its capture
/// readers have drained.
pub(crate) fn forward_activity(
    output: Receiver<Captured>,
    roster: RosterClient,
    recorder: UsageRecorder,
) {
    thread::spawn(move || {
        for captured in output {
            if captured.stream != OutputStream::Stdout {
                continue;
            }
            for activity in parse(&captured.line) {
                if let Err(err) = roster.emit_activity(&captured.role, &activity) {
                    event!(
                        name: "supervisor.activity.report_failed",
                        Level::DEBUG,
                        crew.role = %captured.role,
                        error = %err,
                        "could not record activity for `{{crew.role}}`: {err}",
                    );
                }
            }
            // Charge the turn's usage against the crew budget (issue #177): the `result`
            // line carries the turn's final tokens and cost, so charging it here makes
            // budget enforcement (issue #54) live off real spend.
            if let Some(usage) = parse_usage(&captured.line) {
                if let Err(err) =
                    recorder.record_usage(&captured.role, usage.tokens, usage.cost_micro_usd)
                {
                    event!(
                        name: "supervisor.usage.charge_failed",
                        Level::WARN,
                        crew.role = %captured.role,
                        error = %err,
                        "could not charge usage for `{{crew.role}}`: {err}",
                    );
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use crew_core::Activity;

    use super::parse;

    #[test]
    fn a_session_init_is_a_turn_start() {
        let line = r#"{"type":"system","subtype":"init","session_id":"abc","model":"opus"}"#;
        assert_eq!(parse(line), vec![Activity::TurnStarted]);
    }

    #[test]
    fn other_system_subtypes_are_dropped() {
        let line = r#"{"type":"system","subtype":"status","session_id":"abc"}"#;
        assert!(
            parse(line).is_empty(),
            "live telemetry does not flood the log"
        );
    }

    #[test]
    fn a_result_is_a_turn_end() {
        let line = r#"{"type":"result","subtype":"success","result":"done","session_id":"s9"}"#;
        assert_eq!(parse(line), vec![Activity::TurnEnded]);
    }

    #[test]
    fn an_assistant_line_yields_text_and_tool_calls_in_order() {
        let line = r#"{"type":"assistant","session_id":"s1","message":{"content":[
            {"type":"text","text":"Looking into it"},
            {"type":"tool_use","name":"Read","input":{"path":"foo.rs"}}
        ]}}"#;
        assert_eq!(
            parse(line),
            vec![
                Activity::Output {
                    text: "Looking into it".to_owned(),
                },
                Activity::ToolCall {
                    tool: "Read".to_owned(),
                },
            ],
        );
    }

    #[test]
    fn thinking_and_empty_text_blocks_are_dropped() {
        let line = r#"{"type":"assistant","session_id":"s1","message":{"content":[
            {"type":"thinking","thinking":"Let me check the config."},
            {"type":"text","text":"   "},
            {"type":"tool_use","name":"Bash","input":{}}
        ]}}"#;
        assert_eq!(
            parse(line),
            vec![Activity::ToolCall {
                tool: "Bash".to_owned(),
            }],
            "only the tool call survives; thinking and blank text are dropped",
        );
    }

    #[test]
    fn a_tool_use_without_a_name_falls_back_rather_than_dropping() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"tool_use","input":{}}]}}"#;
        assert_eq!(
            parse(line),
            vec![Activity::ToolCall {
                tool: "unknown".to_owned(),
            }],
        );
    }

    #[test]
    fn the_usage_firehose_and_tool_results_are_dropped() {
        for line in [
            r#"{"type":"user","message":{"content":[{"type":"tool_result","content":"ok"}]}}"#,
            r#"{"type":"stream_event","event":{"type":"message_delta","usage":{"output_tokens":7}}}"#,
            r#"{"type":"rate_limit_event","rate_limit_info":{"status":"allowed"}}"#,
        ] {
            assert!(parse(line).is_empty(), "not activity: {line}");
        }
    }

    #[test]
    fn an_unknown_shape_becomes_other_rather_than_crashing() {
        // The acceptance: a schema drift is surfaced, never a panic.
        let unknown = parse(r#"{"type":"telepathy_event","payload":42}"#);
        assert_eq!(
            unknown,
            vec![Activity::Other {
                raw: "telepathy_event".to_owned(),
            }],
        );

        let non_json = parse("this is not json at all");
        assert_eq!(
            non_json,
            vec![Activity::Other {
                raw: "non-json".to_owned(),
            }],
        );

        // A typeless JSON object and a malformed assistant line are also Other.
        assert_eq!(
            parse(r#"{"no":"type"}"#),
            vec![Activity::Other { raw: String::new() }],
        );
        assert_eq!(
            parse(r#"{"type":"assistant","message":{}}"#),
            vec![Activity::Other {
                raw: "assistant".to_owned(),
            }],
        );
    }

    #[test]
    fn a_blank_line_yields_nothing() {
        assert!(parse("   ").is_empty());
        assert!(parse("").is_empty());
    }

    #[test]
    fn a_result_line_yields_its_summed_tokens_and_cost() {
        use super::{parse_usage, TurnUsage};

        // The `result` line carries the turn's final usage: every token field summed,
        // and the dollar cost rendered as whole micro-USD (issue #177).
        let line = r#"{"type":"result","subtype":"success","total_cost_usd":0.0123,
            "usage":{"input_tokens":200,"output_tokens":800,
                     "cache_creation_input_tokens":50,"cache_read_input_tokens":1000}}"#;
        assert_eq!(
            parse_usage(line),
            Some(TurnUsage {
                tokens: 2_050,
                cost_micro_usd: 12_300,
            }),
        );
    }

    #[test]
    fn a_result_with_partial_usage_sums_what_is_present() {
        use super::{parse_usage, TurnUsage};

        // Absent token fields count as zero, and a missing cost is zero.
        let line = r#"{"type":"result","usage":{"output_tokens":1500}}"#;
        assert_eq!(
            parse_usage(line),
            Some(TurnUsage {
                tokens: 1_500,
                cost_micro_usd: 0,
            }),
        );
    }

    #[test]
    fn non_result_lines_carry_no_usage() {
        use super::parse_usage;

        // Only the `result` line ends a turn, so nothing else charges spend, and a
        // non-JSON line is not a result either.
        for line in [
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}"#,
            r#"{"type":"system","subtype":"init"}"#,
            r#"{"type":"stream_event","event":{"usage":{"output_tokens":7}}}"#,
            "not json at all",
        ] {
            assert_eq!(parse_usage(line), None, "not a turn total: {line}");
        }
    }

    #[test]
    fn a_result_with_no_spend_is_ignored() {
        use super::parse_usage;

        // A result carrying neither tokens nor cost is not a zero charge to report.
        assert_eq!(
            parse_usage(r#"{"type":"result","subtype":"success"}"#),
            None
        );
        assert_eq!(
            parse_usage(r#"{"type":"result","usage":{},"total_cost_usd":0}"#),
            None,
        );
    }
}
