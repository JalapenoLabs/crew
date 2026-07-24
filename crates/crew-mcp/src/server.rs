//! The MCP server: a JSON-RPC 2.0 loop over stdio exposing the crew tools.
//!
//! Speaks the Model Context Protocol over newline-delimited stdio so a Claude
//! Code (or Codex) agent can call [`crew_send`], [`crew_inbox`], and
//! [`crew_roster`]. It handles the `initialize` handshake, `tools/list`, and
//! `tools/call`, dispatching a call to the [`crew_client::Broker`] client. The
//! tool docs are written for the agent to get the call right the first try.

use std::io::{BufRead, Write};

use crew_client::{
    BoardSnapshot, BriefingPacket, Broker, GateSnapshot, InboxItem, LedgerItem, RosterSnapshot,
    Standing,
};
use crew_core::LaneEnforcement;
use serde_json::{json, Value};

/// The MCP protocol version this server implements.
const PROTOCOL_VERSION: &str = "2024-11-05";

/// The server name reported in the `initialize` handshake.
const SERVER_NAME: &str = "crew-mcp";

/// The crew MCP server, driving one agent role's tools over stdio.
#[derive(Debug)]
pub struct Server {
    broker: Broker,
    /// The role's owned lane, for `crew_lane` (issue #46).
    owned_paths: Vec<String>,
    /// How the crew enforces this role's lane.
    lane_enforcement: LaneEnforcement,
}

impl Server {
    /// Builds a server that dispatches tool calls to `broker`, enforcing the
    /// role's lane (`owned_paths`, `lane_enforcement`) for `crew_lane`.
    #[must_use]
    pub fn new(
        broker: Broker,
        owned_paths: Vec<String>,
        lane_enforcement: LaneEnforcement,
    ) -> Self {
        Self {
            broker,
            owned_paths,
            lane_enforcement,
        }
    }

    /// Runs the stdio JSON-RPC loop, reading requests and writing responses,
    /// until the input ends.
    ///
    /// # Errors
    /// Returns an error only if reading the input or writing the output fails.
    pub fn serve(&mut self, input: impl BufRead, mut output: impl Write) -> std::io::Result<()> {
        for line in input.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if let Some(response) = self.handle_line(&line) {
                serde_json::to_writer(&mut output, &response)?;
                output.write_all(b"\n")?;
                output.flush()?;
            }
        }
        Ok(())
    }

    /// Handles one JSON-RPC line, returning a response for a request or `None`
    /// for a notification (which gets no reply).
    fn handle_line(&mut self, line: &str) -> Option<Value> {
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            return Some(rpc_error(&Value::Null, -32700, "parse error"));
        };
        match (
            message.get("method").and_then(Value::as_str),
            message.get("id"),
        ) {
            (Some(method), Some(id)) => {
                Some(self.handle_request(method, id, message.get("params")))
            }
            (None, Some(id)) => Some(rpc_error(id, -32600, "invalid request: no method")),
            // A notification (no id), such as `notifications/initialized`: no reply.
            (_, None) => None,
        }
    }

    /// Dispatches a request by method.
    fn handle_request(&mut self, method: &str, id: &Value, params: Option<&Value>) -> Value {
        match method {
            "initialize" => rpc_result(id, initialize(params)),
            "ping" => rpc_result(id, json!({})),
            "tools/list" => rpc_result(id, json!({ "tools": tool_catalog() })),
            "tools/call" => self.tools_call(id, params),
            other => rpc_error(id, -32601, &format!("method not found: {other}")),
        }
    }

    /// Runs a `tools/call`, mapping a tool error to an `isError` result (which
    /// the agent reads) rather than a protocol error.
    fn tools_call(&mut self, id: &Value, params: Option<&Value>) -> Value {
        let Some(name) = params.and_then(|p| p.get("name")).and_then(Value::as_str) else {
            return rpc_error(id, -32602, "invalid params: no tool name");
        };
        let empty = json!({});
        let arguments = params.and_then(|p| p.get("arguments")).unwrap_or(&empty);
        match self.call_tool(name, arguments) {
            Ok(text) => rpc_result(id, tool_content(&text, false)),
            Err(text) => rpc_result(id, tool_content(&text, true)),
        }
    }

    /// Executes a tool against the broker.
    #[expect(
        clippy::too_many_lines,
        reason = "a flat tool-dispatch match grows one self-contained arm per tool"
    )]
    fn call_tool(&mut self, name: &str, arguments: &Value) -> Result<String, String> {
        match name {
            "crew_send" => {
                let body =
                    str_arg(arguments, "body").ok_or("crew_send requires a non-empty `body`")?;
                self.broker.send(
                    str_arg(arguments, "to"),
                    str_arg(arguments, "channel"),
                    body,
                )
            }
            "crew_order" => {
                let to =
                    str_arg(arguments, "to").ok_or("crew_order requires a `to` role to order")?;
                let title = str_arg(arguments, "title")
                    .ok_or("crew_order requires a `title` for the task")?;
                self.broker.order(
                    to,
                    title,
                    str_arg(arguments, "scope").unwrap_or_default(),
                    &str_list_arg(arguments, "owned_paths"),
                    str_arg(arguments, "acceptance").unwrap_or_default(),
                    str_arg(arguments, "body").unwrap_or_default(),
                )
            }
            "crew_ask" => {
                let body =
                    str_arg(arguments, "body").ok_or("crew_ask requires a non-empty `body`")?;
                self.broker.ask(
                    str_arg(arguments, "to"),
                    str_arg(arguments, "channel"),
                    body,
                    &str_list_arg(arguments, "options"),
                )
            }
            "crew_answer" => {
                let body =
                    str_arg(arguments, "body").ok_or("crew_answer requires a non-empty `body`")?;
                let in_reply_to = str_arg(arguments, "in_reply_to").ok_or(
                    "crew_answer requires the `in_reply_to` id of the question it answers",
                )?;
                self.broker.answer(
                    str_arg(arguments, "to"),
                    str_arg(arguments, "channel"),
                    body,
                    in_reply_to,
                )
            }
            "crew_inbox" => Ok(render_inbox(&self.broker.inbox()?)),
            "crew_roster" => Ok(render_roster(&self.broker.roster()?)),
            "crew_lane" => {
                let path =
                    str_arg(arguments, "path").ok_or("crew_lane requires a `path` to check")?;
                self.broker
                    .check_lane(&self.owned_paths, self.lane_enforcement, path)
            }
            "crew_claim" => {
                let task =
                    str_arg(arguments, "task").ok_or("crew_claim requires a `task` to claim")?;
                self.broker.claim(
                    task,
                    str_arg(arguments, "state").unwrap_or("claimed"),
                    str_arg(arguments, "title").unwrap_or_default(),
                )
            }
            "crew_ledger" => Ok(render_ledger(&self.broker.ledger()?)),
            "crew_submit" => {
                let task =
                    str_arg(arguments, "task").ok_or("crew_submit requires a `task` to submit")?;
                self.broker.submit(
                    task,
                    str_arg(arguments, "acceptance").unwrap_or_default(),
                    str_arg(arguments, "to"),
                )
            }
            "crew_verdict" => {
                let task =
                    str_arg(arguments, "task").ok_or("crew_verdict requires a `task` to judge")?;
                let pass = bool_arg(arguments, "pass")
                    .ok_or("crew_verdict requires a boolean `pass` (true if it holds)")?;
                self.broker.verdict(
                    task,
                    pass,
                    str_arg(arguments, "failure").unwrap_or_default(),
                )
            }
            "crew_gate" => Ok(render_gate(&self.broker.gate()?)),
            "crew_complete" => self
                .broker
                .complete(str_arg(arguments, "summary").unwrap_or_default()),
            "crew_board" => Ok(render_board(
                &self.broker.board(str_arg(arguments, "section"))?,
            )),
            "crew_record" => {
                let key = str_arg(arguments, "key")
                    .ok_or("crew_record requires a `key` (the entry's topic)")?;
                if bool_arg(arguments, "retract").unwrap_or(false) {
                    self.broker.retract(key)
                } else {
                    let section = str_arg(arguments, "section").ok_or(
                        "crew_record requires a `section` (decision, interface, or gotcha) \
                         unless retracting",
                    )?;
                    let body = str_arg(arguments, "body")
                        .ok_or("crew_record requires a `body` (the content) unless retracting")?;
                    self.broker.record(key, section, body)
                }
            }
            "crew_briefing" => Ok(render_briefing(
                &self
                    .broker
                    .briefing(str_arg(arguments, "task"), usize_arg(arguments, "budget"))?,
            )),
            other => Err(format!("unknown tool `{other}`")),
        }
    }
}

/// The `initialize` result: echo the client's protocol version (or the default)
/// and advertise the tools capability.
fn initialize(params: Option<&Value>) -> Value {
    let version = params
        .and_then(|p| p.get("protocolVersion"))
        .and_then(Value::as_str)
        .unwrap_or(PROTOCOL_VERSION);
    json!({
        "protocolVersion": version,
        "capabilities": { "tools": {} },
        "serverInfo": { "name": SERVER_NAME, "version": env!("CARGO_PKG_VERSION") },
    })
}

/// The tool catalog for `tools/list`: messaging, coordination, work-ledger,
/// done-gate, board, briefing.
fn tool_catalog() -> Value {
    let mut tools = messaging_tools();
    tools.extend(coordination_tools());
    tools.extend(ledger_tools());
    tools.extend(done_gate_tools());
    tools.extend(board_tools());
    tools.extend(briefing_tools());
    Value::Array(tools)
}

/// The messaging tools: send a note, order a specialist, ask a question, and
/// answer one.
fn messaging_tools() -> Vec<Value> {
    vec![
        json!({
            "name": "crew_send",
            "description": "Send a message to a teammate or a channel, as your role. \
                By default it goes to the commander (the unit's router), so an unaddressed \
                note reaches the lead. Set `to` to direct-message one role (for example \
                `to: \"backend\"` reaches only backend). Set `channel` to post to a named \
                channel: `all-units` reaches the whole unit, and a pair like \
                `frontend+backend` reaches just those two. Give at most one of `to` or \
                `channel`; give neither to reach the commander. Use crew_order, not this, to \
                assign a scoped task. `body` is the message text.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "to": { "type": "string", "description": "A role to direct-message (its @role channel)." },
                    "channel": { "type": "string", "description": "A channel name: `all-units`, or a pair like `frontend+backend`." },
                    "body": { "type": "string", "description": "The message text (markdown)." }
                },
                "required": ["body"]
            }
        }),
        json!({
            "name": "crew_order",
            "description": "Issue an order: assign a scoped task to one specialist, as your \
                role. This is the commander's fan-out handle for decomposing the General's \
                brief into work. It direct-messages `to` an `order` the specialist can act \
                on: `title` names the task, `scope` says what is in and out, `owned_paths` \
                are the paths it owns while working, and `acceptance` is how it is judged \
                done. `body` adds any freeform detail. Use crew_send for a plain message.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "to": { "type": "string", "description": "The specialist role to order (its @role channel)." },
                    "title": { "type": "string", "description": "A short title for the task." },
                    "scope": { "type": "string", "description": "What is in and out of scope." },
                    "owned_paths": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "The paths the role owns while working the task."
                    },
                    "acceptance": { "type": "string", "description": "How the task is judged done." },
                    "body": { "type": "string", "description": "Optional freeform detail (markdown)." }
                },
                "required": ["to", "title"]
            }
        }),
        json!({
            "name": "crew_ask",
            "description": "Ask a typed question and wait on a decision, as your role. Use this, \
                not crew_send, when you need an answer before you can proceed: a question is a \
                first-class message the unit tracks, so an unanswered one surfaces a coordination \
                stall or deadlock instead of stalling silently. Target it like crew_send: `to` a \
                single role, `channel` for `all-units` or a pair, or neither to reach the \
                commander. `body` is the question; `options` optionally lists suggested answers.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "to": { "type": "string", "description": "A role to ask directly (its @role channel)." },
                    "channel": { "type": "string", "description": "A channel name: `all-units`, or a pair like `frontend+backend`." },
                    "body": { "type": "string", "description": "The question (markdown)." },
                    "options": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional suggested answers to choose from."
                    }
                },
                "required": ["body"]
            }
        }),
        json!({
            "name": "crew_answer",
            "description": "Answer a question a teammate asked, as your role, clearing the wait it \
                was blocked on. `in_reply_to` is the id of the question you are answering, shown in \
                your inbox as `[id ...]`; naming it threads the reply to its question. Target it \
                like crew_send: `to` the asker, or a `channel`. `body` is the answer.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "to": { "type": "string", "description": "The role to answer (usually the asker's @role channel)." },
                    "channel": { "type": "string", "description": "A channel name, if the question was asked on one." },
                    "body": { "type": "string", "description": "The answer (markdown)." },
                    "in_reply_to": { "type": "string", "description": "The id of the question being answered (from your inbox)." }
                },
                "required": ["body", "in_reply_to"]
            }
        }),
    ]
}

/// The coordination tools: read the inbox and roster, and check a lane.
fn coordination_tools() -> Vec<Value> {
    vec![
        json!({
            "name": "crew_inbox",
            "description": "Read the messages addressed to you since you last called this. \
                Returns messages on your direct `@role` channel, any pair channel you belong \
                to, and `all-units`, and never your own. Each carries an `[id ...]` you can pass \
                to crew_answer to reply to a question. Call it to catch up on what teammates \
                have sent you.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
        json!({
            "name": "crew_roster",
            "description": "List the unit's roles: each teammate, the paths (lanes) it owns, \
                and whether it is working, idle, stopped, or dead. Use it to see who is on the \
                team and what they own before sending or claiming work.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
        json!({
            "name": "crew_lane",
            "description": "Check whether a file `path` is inside your owned lane before you \
                edit it, so you do not wander into a teammate's lane. An in-lane path is yours: \
                proceed. An out-of-lane path is reported to the unit; do not edit it silently, \
                route the change through the commander (crew_send) instead. Under a blocking \
                policy the edit is refused. Call it before editing outside your obvious lane.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "The repo-relative file path you are about to edit." }
                },
                "required": ["path"]
            }
        }),
    ]
}

/// The work-ledger tools: claim a task or move a claim forward, and read the
/// ledger.
fn ledger_tools() -> Vec<Value> {
    vec![
        json!({
            "name": "crew_claim",
            "description": "Claim a piece of work before you start it, so no two roles touch \
                the same work. `task` is a stable key the crew agrees on (a path, a feature, an \
                order's title). If another role already holds it, this fails and names the \
                holder: coordinate, do not race. Call it again with `state` to move your claim \
                forward: `in_progress` when you start, `blocked` if you are stuck, and `done` \
                when you finish (which frees the task). `title` is an optional human label.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task": { "type": "string", "description": "The work item's key (a path, feature, or order title)." },
                    "state": {
                        "type": "string",
                        "enum": ["claimed", "in_progress", "blocked", "done"],
                        "description": "The state to move it to; defaults to `claimed`."
                    },
                    "title": { "type": "string", "description": "An optional short label for the ledger." }
                },
                "required": ["task"]
            }
        }),
        json!({
            "name": "crew_ledger",
            "description": "Read the work ledger: every claimed task, who owns it, and its \
                state. Check it before claiming, so you never grab work a teammate already holds.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
    ]
}

/// The adversarial done-gate tools: submit work, return a verdict, and read the
/// gate.
fn done_gate_tools() -> Vec<Value> {
    vec![
        json!({
            "name": "crew_submit",
            "description": "Submit your finished work for adversarial verification instead of \
                reporting it done yourself. Done means verified, not asserted: an independent \
                role tries to break it against the acceptance criteria before it counts as done. \
                `task` is the task title (match the order's title), `acceptance` restates the \
                criteria the work must meet, and `to` optionally names the reviewer to notify \
                (for example `to: \"qa\"`). This does not mark the task done; wait for the verdict, \
                and if it fails, fix the specific failure and resubmit.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task": { "type": "string", "description": "The task title, matching the order it came from." },
                    "acceptance": { "type": "string", "description": "The acceptance criteria the work claims to meet." },
                    "to": { "type": "string", "description": "An optional reviewer role to notify (for example `qa`)." }
                },
                "required": ["task"]
            }
        }),
        json!({
            "name": "crew_verdict",
            "description": "Return a verdict on a task another role submitted for verification. \
                You are the skeptic: actively try to break the work against its acceptance \
                criteria. Set `pass` true only if you could not break it, which marks the task \
                done. Set `pass` false and give the specific, actionable `failure` if you broke \
                it, which returns the work to its owner to fix. You cannot verify your own work: \
                an independent role must judge it. `task` is the task title.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task": { "type": "string", "description": "The task title under verification." },
                    "pass": { "type": "boolean", "description": "True if you could not break it (marks it done); false if you broke it." },
                    "failure": { "type": "string", "description": "The specific failure when `pass` is false, so the handback is actionable." }
                },
                "required": ["task", "pass"]
            }
        }),
        json!({
            "name": "crew_gate",
            "description": "Read the done-gate: every task under verification, who owns it, who \
                is verifying it, and whether it is submitted, passed, or failed. Use it to see \
                what is awaiting an independent verifier and what has been proven done.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
        json!({
            "name": "crew_complete",
            "description": "Report the whole mission gracefully complete, the crew's version of \
                signing off. Call this, typically as the commander, only when the work is truly \
                done (every task verified through the done-gate), so the General is pulled back \
                on a real finish. This is a graceful completion, not the emergency stand-down, \
                and it does not halt the crew; it announces the mission is done. Pass a short \
                `summary` of what shipped: it is rendered in the completion notification, so the \
                General reads the outcome at a glance.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "summary": { "type": "string", "description": "A short summary of what the mission shipped, shown in the completion notification." }
                }
            }
        }),
    ]
}

/// The situation-board tools: read the crew's durable memory, and record or
/// retract an entry.
fn board_tools() -> Vec<Value> {
    vec![
        json!({
            "name": "crew_board",
            "description": "Read the shared situation board: the crew's durable memory of agreed \
                interfaces, decisions and their rationale, and known gotchas. Check it before you \
                re-derive a settled decision or re-litigate a choice; it outlives the message \
                stream and survives a restart. Pass `section` to read just `decision`, \
                `interface`, or `gotcha`.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "section": { "type": "string", "enum": ["decision", "interface", "gotcha"], "description": "Read just this section; omit for the whole board." }
                }
            }
        }),
        json!({
            "name": "crew_record",
            "description": "Record a decision, an agreed interface, or a known gotcha on the shared \
                situation board, so the crew stops re-deriving it. `key` is a stable topic (for \
                example `auth-strategy`); recording the same key again updates the entry. \
                `section` is `decision`, `interface`, or `gotcha`, and `body` is the content (for a \
                decision, include the rationale). Set `retract: true` with just the `key` to remove \
                an entry the crew no longer holds. The commander curates the board.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "key": { "type": "string", "description": "The entry's stable topic key." },
                    "section": { "type": "string", "enum": ["decision", "interface", "gotcha"], "description": "The section; required unless retracting." },
                    "body": { "type": "string", "description": "The entry's content; required unless retracting." },
                    "retract": { "type": "boolean", "description": "Remove the entry named by `key` instead of recording one." }
                },
                "required": ["key"]
            }
        }),
    ]
}

/// The briefing tool: catch up with a bounded packet instead of the whole log.
fn briefing_tools() -> Vec<Value> {
    vec![json!({
        "name": "crew_briefing",
        "description": "Get your bounded briefing packet: the current decision board and a \
            rolling summary scoped to your lane (what has been said to you and about your \
            work), instead of reading the whole log. Call this first thing when you boot to \
            catch up in seconds. `budget` optionally caps the packet size in bytes; `task` \
            optionally narrows the summary to one task's id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "budget": { "type": "integer", "description": "Cap the packet size in bytes (defaults to a few KB)." },
                "task": { "type": "string", "description": "Narrow the summary to this task id, if you have one." }
            }
        }
    })]
}

/// A JSON-RPC success response, moving `result` into the envelope.
fn rpc_result(id: &Value, result: Value) -> Value {
    Value::Object(serde_json::Map::from_iter([
        ("jsonrpc".to_owned(), Value::from("2.0")),
        ("id".to_owned(), id.clone()),
        ("result".to_owned(), result),
    ]))
}

/// A JSON-RPC error response.
fn rpc_error(id: &Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

/// A `tools/call` result: one text block, flagged as an error or not.
fn tool_content(text: &str, is_error: bool) -> Value {
    json!({ "content": [{ "type": "text", "text": text }], "isError": is_error })
}

/// A trimmed, non-blank string argument, if present.
fn str_arg<'a>(arguments: &'a Value, key: &str) -> Option<&'a str> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// A boolean argument, if present and a JSON boolean.
fn bool_arg(arguments: &Value, key: &str) -> Option<bool> {
    arguments.get(key).and_then(Value::as_bool)
}

/// A non-negative integer argument as a `usize`, if present and in range.
fn usize_arg(arguments: &Value, key: &str) -> Option<usize> {
    arguments
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
}

/// A string-array argument as owned strings, dropping blanks; empty if absent.
fn str_list_arg(arguments: &Value, key: &str) -> Vec<String> {
    arguments
        .get(key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Renders the inbox items an agent reads.
fn render_inbox(items: &[InboxItem]) -> String {
    if items.is_empty() {
        return "No new messages.".to_owned();
    }
    let lines: Vec<String> = items
        .iter()
        .map(|item| {
            // Lead with the structured detail (an order's task); fall back to the body.
            let content = match (item.detail.as_str(), item.body.as_str()) {
                ("", body) => body.to_owned(),
                (detail, "") => detail.to_owned(),
                (detail, body) => format!("{detail}. {body}"),
            };
            // A redirect or belay is a General directive to honor at once, so flag it.
            let marker = if item.directive { "[honor now] " } else { "" };
            // Carry the message id so a reply can name it: crew_answer takes the id of
            // the question it answers.
            format!(
                "- {}{} on {} ({}) [id {}]: {}",
                marker, item.from, item.channel, item.kind, item.id, content
            )
        })
        .collect();
    format!("{} new message(s):\n{}", items.len(), lines.join("\n"))
}

/// Renders the work ledger an agent reads.
fn render_ledger(items: &[LedgerItem]) -> String {
    if items.is_empty() {
        return "The ledger is empty; no work is claimed.".to_owned();
    }
    let lines: Vec<String> = items
        .iter()
        .map(|item| {
            let title = if item.title.is_empty() {
                String::new()
            } else {
                format!(" ({})", item.title)
            };
            format!("- {} [{}] {}{}", item.task, item.state, item.owner, title)
        })
        .collect();
    format!("{} task(s):\n{}", items.len(), lines.join("\n"))
}

/// Renders the roster an agent reads.
fn render_roster(snapshot: &RosterSnapshot) -> String {
    if snapshot.roles.is_empty() {
        return "The roster is empty.".to_owned();
    }
    // The crew is gated whenever it is not running; a role is also gated on its
    // own.
    let crew_gated = snapshot.standing != Standing::Running;
    let lines: Vec<String> = snapshot
        .roles
        .iter()
        .map(|role| {
            let owns = if role.owned_paths.is_empty() {
                String::new()
            } else {
                format!(" owns {}", role.owned_paths.join(", "))
            };
            // Flag a role that must pull no new work: paused on its own, or crew-gated.
            let gated = if role.paused || crew_gated {
                " [PAUSED: pull no new work]"
            } else {
                ""
            };
            format!("- {} [{}]{}{}", role.role, role.liveness, owns, gated)
        })
        .collect();
    let header = match snapshot.standing {
        Standing::Running => format!("{} role(s):", snapshot.roles.len()),
        Standing::Paused => format!("{} role(s) (the crew is PAUSED):", snapshot.roles.len()),
        Standing::StoodDown => {
            format!("{} role(s) (the crew is STOOD DOWN):", snapshot.roles.len())
        }
    };
    format!("{header}\n{}", lines.join("\n"))
}

/// Renders the done-gate an agent reads: each task under verification and its
/// standing.
fn render_gate(snapshot: &GateSnapshot) -> String {
    if snapshot.tasks.is_empty() {
        return "The done-gate is empty; no task is under verification.".to_owned();
    }
    let lines: Vec<String> = snapshot
        .tasks
        .iter()
        .map(|task| {
            let verifier = task
                .verifier
                .as_deref()
                .map(|who| format!(" by {who}"))
                .unwrap_or_default();
            let detail = if task.detail.is_empty() {
                String::new()
            } else {
                format!(": {}", task.detail)
            };
            format!(
                "- {} owned by {} [{}{}]{}",
                task.task, task.owner, task.verdict, verifier, detail
            )
        })
        .collect();
    format!(
        "{} task(s) under the done-gate:\n{}",
        snapshot.tasks.len(),
        lines.join("\n")
    )
}

/// Renders the situation board an agent reads: each entry, its section, author,
/// and content.
fn render_board(snapshot: &BoardSnapshot) -> String {
    if snapshot.entries.is_empty() {
        return "The situation board is empty.".to_owned();
    }
    let lines: Vec<String> = snapshot
        .entries
        .iter()
        .map(|entry| {
            format!(
                "- [{}] {} (by {}): {}",
                entry.section, entry.key, entry.author, entry.body
            )
        })
        .collect();
    format!(
        "{} board entr{}:\n{}",
        snapshot.entries.len(),
        if snapshot.entries.len() == 1 {
            "y"
        } else {
            "ies"
        },
        lines.join("\n")
    )
}

/// Renders the briefing packet an agent reads on boot: the bounded text plus
/// its size.
fn render_briefing(packet: &BriefingPacket) -> String {
    let note = if packet.capped {
        format!(
            "\n\n[briefing capped to {} of {} bytes; call crew_board or crew_inbox for more]",
            packet.size, packet.budget
        )
    } else {
        format!("\n\n[briefing: {} of {} bytes]", packet.size, packet.budget)
    };
    format!("{}{note}", packet.text)
}

#[cfg(test)]
mod tests {
    use crew_client::Broker;
    use crew_core::RoleId;
    use serde_json::{json, Value};

    use super::Server;

    /// A server whose broker points nowhere; the protocol paths under test
    /// never call it.
    fn server() -> Server {
        Server::new(
            Broker::new(
                "http://127.0.0.1:1",
                RoleId::new("backend"),
                RoleId::new("commander"),
            ),
            vec!["api/".to_owned()],
            crew_core::LaneEnforcement::Warn,
        )
    }

    fn handle(server: &mut Server, message: &Value) -> Option<Value> {
        server.handle_line(&message.to_string())
    }

    #[test]
    fn render_inbox_flags_a_general_directive_to_honor_at_once() {
        use crew_client::InboxItem;

        use super::render_inbox;

        let item = |kind: &str, directive: bool| InboxItem {
            id: "11111111-1111-1111-1111-111111111111".to_owned(),
            from: "general".to_owned(),
            channel: "@backend".to_owned(),
            kind: kind.to_owned(),
            detail: String::new(),
            body: "switch to the login bug".to_owned(),
            directive,
        };

        let rendered = render_inbox(&[item("redirect", true)]);
        assert!(
            rendered.contains("[honor now]"),
            "a directive is flagged: {rendered}"
        );
        assert!(rendered.contains("(redirect)"), "and names the kind");

        let plain = render_inbox(&[item("note", false)]);
        assert!(
            !plain.contains("[honor now]"),
            "a plain message is not flagged: {plain}"
        );
    }

    #[test]
    fn initialize_echoes_the_protocol_version_and_advertises_tools() {
        let response = handle(
            &mut server(),
            &json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": { "protocolVersion": "2025-06-18" } }),
        )
        .unwrap();
        let result = &response["result"];
        assert_eq!(
            result["protocolVersion"], "2025-06-18",
            "echoes the client's version"
        );
        assert_eq!(result["serverInfo"]["name"], "crew-mcp");
        assert!(
            result["capabilities"]["tools"].is_object(),
            "advertises tools"
        );
    }

    #[test]
    fn tools_list_offers_the_crew_tools() {
        let response = handle(
            &mut server(),
            &json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
        )
        .unwrap();
        let tools = response["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(
            names,
            [
                "crew_send",
                "crew_order",
                "crew_ask",
                "crew_answer",
                "crew_inbox",
                "crew_roster",
                "crew_lane",
                "crew_claim",
                "crew_ledger",
                "crew_submit",
                "crew_verdict",
                "crew_gate",
                "crew_complete",
                "crew_board",
                "crew_record",
                "crew_briefing"
            ]
        );
        // Each tool documents itself and its arguments.
        for tool in tools {
            assert!(
                !tool["description"].as_str().unwrap().is_empty(),
                "tool has a description"
            );
            assert_eq!(tool["inputSchema"]["type"], "object");
        }
        // crew_send requires a body; crew_order requires a role and a title.
        let send = tools.iter().find(|t| t["name"] == "crew_send").unwrap();
        assert_eq!(send["inputSchema"]["required"], json!(["body"]));
        let order = tools.iter().find(|t| t["name"] == "crew_order").unwrap();
        assert_eq!(order["inputSchema"]["required"], json!(["to", "title"]));
    }

    #[test]
    fn a_notification_gets_no_reply() {
        let reply = handle(
            &mut server(),
            &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
        );
        assert!(reply.is_none(), "a notification is not answered");
    }

    #[test]
    fn an_unknown_method_is_a_method_not_found_error() {
        let response = handle(
            &mut server(),
            &json!({ "jsonrpc": "2.0", "id": 3, "method": "resources/list" }),
        )
        .unwrap();
        assert_eq!(response["error"]["code"], -32601);
    }

    #[test]
    fn malformed_json_is_a_parse_error() {
        let response = server().handle_line("{ not json").unwrap();
        assert_eq!(response["error"]["code"], -32700);
    }

    #[test]
    fn crew_send_without_a_body_is_a_tool_error_not_a_broker_call() {
        // Missing `body` fails before the broker is touched, so the bogus base is fine.
        let response = handle(
            &mut server(),
            &json!({ "jsonrpc": "2.0", "id": 4, "method": "tools/call",
                    "params": { "name": "crew_send", "arguments": {} } }),
        )
        .unwrap();
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("body"));
    }

    #[test]
    fn crew_order_without_a_target_is_a_tool_error_not_a_broker_call() {
        // A missing `to` fails before the broker is touched, so the bogus base is fine.
        let response = handle(
            &mut server(),
            &json!({ "jsonrpc": "2.0", "id": 4, "method": "tools/call",
                    "params": { "name": "crew_order", "arguments": { "title": "do it" } } }),
        )
        .unwrap();
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("to"));
    }

    #[test]
    fn crew_verdict_without_pass_is_a_tool_error_not_a_broker_call() {
        // A missing `pass` fails before the broker is touched, so the bogus base is
        // fine.
        let response = handle(
            &mut server(),
            &json!({ "jsonrpc": "2.0", "id": 6, "method": "tools/call",
                    "params": { "name": "crew_verdict", "arguments": { "task": "login" } } }),
        )
        .unwrap();
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("pass"));
    }

    #[test]
    fn crew_answer_without_in_reply_to_is_a_tool_error_not_a_broker_call() {
        // A missing `in_reply_to` fails before the broker is touched, so the bogus base
        // is fine.
        let response = handle(
            &mut server(),
            &json!({ "jsonrpc": "2.0", "id": 9, "method": "tools/call",
                    "params": { "name": "crew_answer", "arguments": { "body": "use JWT" } } }),
        )
        .unwrap();
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("in_reply_to"));
    }

    #[test]
    fn crew_submit_without_a_task_is_a_tool_error_not_a_broker_call() {
        let response = handle(
            &mut server(),
            &json!({ "jsonrpc": "2.0", "id": 7, "method": "tools/call",
                    "params": { "name": "crew_submit", "arguments": {} } }),
        )
        .unwrap();
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("task"));
    }

    #[test]
    fn crew_record_without_a_key_is_a_tool_error_not_a_broker_call() {
        let response = handle(
            &mut server(),
            &json!({ "jsonrpc": "2.0", "id": 8, "method": "tools/call",
                    "params": { "name": "crew_record", "arguments": { "section": "decision", "body": "x" } } }),
        )
        .unwrap();
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("key"));
    }

    #[test]
    fn an_unknown_tool_is_a_tool_error() {
        let response = handle(
            &mut server(),
            &json!({ "jsonrpc": "2.0", "id": 5, "method": "tools/call",
                    "params": { "name": "crew_dance", "arguments": {} } }),
        )
        .unwrap();
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("unknown tool"));
    }
}
