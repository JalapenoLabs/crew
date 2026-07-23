//! The MCP server: a JSON-RPC 2.0 loop over stdio exposing the crew tools.
//!
//! Speaks the Model Context Protocol over newline-delimited stdio so a Claude Code
//! (or Codex) agent can call [`crew_send`], [`crew_inbox`], and [`crew_roster`]. It
//! handles the `initialize` handshake, `tools/list`, and `tools/call`, dispatching a
//! call to the [`Broker`](crate::Broker) client. The tool docs are written for the
//! agent to get the call right the first try.

use std::io::{BufRead, Write};

use serde_json::{json, Value};

use crate::broker::{Broker, InboxItem, RoleEntry};

/// The MCP protocol version this server implements.
const PROTOCOL_VERSION: &str = "2024-11-05";

/// The server name reported in the `initialize` handshake.
const SERVER_NAME: &str = "crew-mcp";

/// The crew MCP server, driving one agent role's tools over stdio.
#[derive(Debug)]
pub struct Server {
    broker: Broker,
}

impl Server {
    /// Builds a server that dispatches tool calls to `broker`.
    #[must_use]
    pub fn new(broker: Broker) -> Self {
        Self { broker }
    }

    /// Runs the stdio JSON-RPC loop, reading requests and writing responses, until
    /// the input ends.
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

    /// Handles one JSON-RPC line, returning a response for a request or `None` for a
    /// notification (which gets no reply).
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

    /// Runs a `tools/call`, mapping a tool error to an `isError` result (which the
    /// agent reads) rather than a protocol error.
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
            "crew_inbox" => Ok(render_inbox(&self.broker.inbox()?)),
            "crew_roster" => Ok(render_roster(&self.broker.roster()?)),
            other => Err(format!("unknown tool `{other}`")),
        }
    }
}

/// The `initialize` result: echo the client's protocol version (or the default) and
/// advertise the tools capability.
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

/// The tool catalog for `tools/list`, with docs written for a first-try correct call.
fn tool_catalog() -> Value {
    json!([
        {
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
        },
        {
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
        },
        {
            "name": "crew_inbox",
            "description": "Read the messages addressed to you since you last called this. \
                Returns messages on your direct `@role` channel, any pair channel you belong \
                to, and `all-units`, and never your own. Call it to catch up on what teammates \
                have sent you.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "crew_roster",
            "description": "List the unit's roles: each teammate, the paths (lanes) it owns, \
                and whether it is working, idle, stopped, or dead. Use it to see who is on the \
                team and what they own before sending or claiming work.",
            "inputSchema": { "type": "object", "properties": {} }
        }
    ])
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
            format!(
                "- {} on {} ({}): {}",
                item.from, item.channel, item.kind, content
            )
        })
        .collect();
    format!("{} new message(s):\n{}", items.len(), lines.join("\n"))
}

/// Renders the roster an agent reads.
fn render_roster(roles: &[RoleEntry]) -> String {
    if roles.is_empty() {
        return "The roster is empty.".to_owned();
    }
    let lines: Vec<String> = roles
        .iter()
        .map(|role| {
            let owns = if role.owned_paths.is_empty() {
                String::new()
            } else {
                format!(" owns {}", role.owned_paths.join(", "))
            };
            format!("- {} [{}]{}", role.role, role.liveness, owns)
        })
        .collect();
    format!("{} role(s):\n{}", roles.len(), lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::Server;
    use crate::broker::Broker;
    use crew_core::RoleId;

    /// A server whose broker points nowhere; the protocol paths under test never call it.
    fn server() -> Server {
        Server::new(Broker::new(
            "http://127.0.0.1:1",
            RoleId::new("backend"),
            RoleId::new("commander"),
        ))
    }

    fn handle(server: &mut Server, message: &Value) -> Option<Value> {
        server.handle_line(&message.to_string())
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
            ["crew_send", "crew_order", "crew_inbox", "crew_roster"]
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
