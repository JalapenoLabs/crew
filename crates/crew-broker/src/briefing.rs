//! The new-role briefing packet: bounded context so a role starts productive
//! fast (issue #50).
//!
//! A freshly spawned role must not read the whole transcript to catch up, the
//! 100k-token problem (see `docs/communication.md`, context management). `GET
//! /briefing?role=<role>` assembles a small, size-capped packet from what the
//! role actually needs: the current decision board (the crew's durable memory)
//! and a rolling summary scoped to the role's own timeline (what it sent and
//! what is addressed to it, so its lane and the work at hand), never the raw
//! log. The packet is rendered to text, measured, and capped to a byte budget,
//! so joining a long mission costs bounded context.
//!
//! The role's static role card (its lane, acceptance bar, and the coordination
//! rules) is delivered separately at boot (`CREW_ROLE_CARD`, issue #18); this
//! packet is the live situation on top of it. Agents reach it through the
//! `crew_briefing` tool.

use std::fmt::Write as _;

use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use crew_core::{RoleId, TaskId};
use serde::{Deserialize, Serialize};

use crate::{
    error::ApiError,
    state::{AppState, BoardEntry},
    store::{EventFilter, EventQuery},
    summary::{summarize, HistorySummary},
};

/// The default briefing budget, in bytes: a few kilobytes, about a thousand
/// tokens, tiny against the whole-log read it replaces. Bytes stand in for
/// tokens (roughly four to one), since the broker has no tokenizer.
const DEFAULT_BUDGET: usize = 4096;

/// The smallest budget honored, so a pathological request still yields a usable
/// header rather than an empty packet.
const MIN_BUDGET: usize = 256;

/// The briefing route: assemble a bounded packet for a role.
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/briefing", get(briefing))
}

/// The `GET /briefing` query: the role to brief, and optional scoping and
/// budget.
#[derive(Debug, Deserialize)]
struct BriefingQuery {
    /// The role the packet is for; its timeline scopes the rolling summary.
    role: String,
    /// Scope the summary to this task, when the caller has its id.
    #[serde(default)]
    task: Option<TaskId>,
    /// The byte budget for the packet; defaults to [`DEFAULT_BUDGET`].
    #[serde(default)]
    budget: Option<usize>,
}

/// `GET /briefing`: the new-role briefing packet, bounded to a byte budget.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the role is empty.
async fn briefing(
    State(state): State<AppState>,
    Query(query): Query<BriefingQuery>,
) -> Result<Json<BriefingPacket>, ApiError> {
    let role = query.role.trim();
    if role.is_empty() {
        return Err(ApiError::bad_request("role must not be empty"));
    }
    let role = RoleId::new(role);
    let budget = query.budget.unwrap_or(DEFAULT_BUDGET).max(MIN_BUDGET);

    // The rolling summary, scoped to the role's own timeline (what it sent and what
    // is addressed to it) and optionally to the task at hand, so it reads its
    // lane, not the whole crew's chatter.
    let filter = EventFilter {
        agent: Some(role.clone()),
        task: query.task,
        ..EventFilter::default()
    };
    let events = state
        .storage
        .query(&EventQuery {
            filter,
            after: None,
            limit: usize::MAX,
        })
        .events;
    let summary = summarize(&events);

    // The whole decision board: the crew's shared, curated memory a new role needs.
    let board: Vec<BoardLine> = board_lines(&state);

    let (text, capped) = render(&role, query.task, &board, &summary, budget);
    let size = text.len();

    Ok(Json(BriefingPacket {
        role,
        task: query.task,
        text,
        size,
        budget,
        capped,
    }))
}

/// One board entry, flattened for rendering and ordering.
struct BoardLine {
    /// The section rank: decisions first, then interfaces, then gotchas.
    rank: u8,
    /// The rendered line for the entry.
    line: String,
}

/// The board entries as rendered lines, ordered decisions then interfaces then
/// gotchas.
fn board_lines(state: &AppState) -> Vec<BoardLine> {
    let mut lines: Vec<BoardLine> = state
        .board_snapshot()
        .into_iter()
        .map(|(key, entry): (String, BoardEntry)| BoardLine {
            rank: section_rank(&entry),
            line: format!(
                "- [{}] {} (by {}): {}",
                entry.section.label(),
                key,
                entry.author,
                entry.body
            ),
        })
        .collect();
    lines.sort_by(|a, b| a.rank.cmp(&b.rank).then_with(|| a.line.cmp(&b.line)));
    lines
}

/// A board entry's section rank: decisions first, then interfaces, then
/// gotchas.
fn section_rank(entry: &BoardEntry) -> u8 {
    use crew_core::BoardSection::{Decision, Gotcha, Interface};
    match entry.section {
        Decision => 0,
        Interface => 1,
        Gotcha => 2,
    }
}

/// Renders the packet into text bounded by `budget`, returning it and whether
/// it was capped.
///
/// Lines are added in priority order (the header, then the board, then the
/// summary) until the next line would exceed the budget; the rest are dropped
/// and `capped` is set. The header is always kept, so even a tiny budget yields
/// a meaningful packet.
fn render(
    role: &RoleId,
    task: Option<TaskId>,
    board: &[BoardLine],
    summary: &HistorySummary,
    budget: usize,
) -> (String, bool) {
    let mut lines: Vec<String> = Vec::new();

    let mut header = format!("Briefing for {role}");
    if let Some(task) = task {
        let _ = write!(header, " on task {task}");
    }
    header.push('.');
    lines.push(header);
    lines.push(String::new());

    if board.is_empty() {
        lines.push("The situation board has no entries yet.".to_owned());
    } else {
        lines.push("On the situation board (decisions, interfaces, gotchas):".to_owned());
        lines.extend(board.iter().map(|entry| entry.line.clone()));
    }
    lines.push(String::new());

    lines.push("Recent activity on your lane (a rolling summary, not the raw log):".to_owned());
    lines.push(summary.headline.clone());
    for order in &summary.recent_orders {
        lines.push(format!(
            "- order: {} on {} (from {})",
            order.title, order.channel, order.from
        ));
    }
    for artifact in &summary.recent_artifacts {
        lines.push(format!("- artifact: {}", artifact.reference));
    }

    pack(&lines, budget)
}

/// Packs `lines` into a newline-joined string within `budget` bytes.
///
/// The first line (the header) is always kept; each further line is added only
/// while it fits. Returns the text and whether any line was dropped.
fn pack(lines: &[String], budget: usize) -> (String, bool) {
    let mut out = String::new();
    let mut capped = false;
    for (index, line) in lines.iter().enumerate() {
        // +1 for the newline that joins this line to the previous one.
        let addition = line.len() + usize::from(index > 0);
        if index == 0 || out.len() + addition <= budget {
            if index > 0 {
                out.push('\n');
            }
            out.push_str(line);
        } else {
            capped = true;
            break;
        }
    }
    (out, capped)
}

/// The `GET /briefing` response: the bounded packet and how it measured against
/// the budget.
#[derive(Debug, Serialize)]
struct BriefingPacket {
    /// The role the packet is for.
    role: RoleId,
    /// The task it was scoped to, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    task: Option<TaskId>,
    /// The rendered packet text a role reads on boot.
    text: String,
    /// The packet's size in bytes.
    size: usize,
    /// The byte budget it was held to.
    budget: usize,
    /// Whether content was dropped to fit the budget.
    capped: bool,
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::{api, config::Config, state::AppState};

    async fn get(state: &AppState, path: &str) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("GET")
            .uri(path)
            .body(Body::empty())
            .unwrap();
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn post(state: &AppState, path: &str, body: Value) {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        api::build(state.clone()).oneshot(request).await.unwrap();
    }

    /// Records a board decision and posts a note, so a briefing has something
    /// to carry.
    async fn seed(state: &AppState) {
        post(
            state,
            "/board",
            json!({ "role": "commander", "key": "auth-strategy", "section": "decision",
                    "body": "JWT with 15m tokens; stateless, matches the gateway" }),
        )
        .await;
        post(
            state,
            "/channels/@backend/messages",
            json!({ "from": { "kind": "role", "id": "commander" }, "kind": "order",
                    "title": "Build the login endpoint", "scope": "api/", "owned_paths": ["api/"],
                    "acceptance": "tokens expire" }),
        )
        .await;
    }

    #[tokio::test]
    async fn a_new_role_gets_the_board_and_a_summary_bounded_to_a_budget() {
        let state = AppState::new(Config::default());
        seed(&state).await;

        let (status, packet) = get(&state, "/briefing?role=backend").await;
        assert_eq!(status, StatusCode::OK);
        let text = packet["text"].as_str().unwrap();

        // The board decision the crew agreed on is in the packet.
        assert!(text.contains("auth-strategy"), "carries the board: {text}");
        // The order addressed to backend (its lane) is in the summary.
        assert!(
            text.contains("Build the login endpoint"),
            "carries the role's directed work: {text}"
        );
        // The packet measures itself and stays within the default budget.
        let size = usize::try_from(packet["size"].as_u64().unwrap()).unwrap();
        let budget = usize::try_from(packet["budget"].as_u64().unwrap()).unwrap();
        assert_eq!(size, text.len(), "the reported size matches the text");
        assert!(size <= budget, "within budget: {size} <= {budget}");
        assert_eq!(packet["capped"], false, "the small packet is not capped");
    }

    #[tokio::test]
    async fn the_briefing_is_scoped_to_the_role_not_the_whole_crew() {
        let state = AppState::new(Config::default());
        // An order to frontend is not on backend's timeline, so backend's briefing
        // omits it.
        post(
            &state,
            "/channels/@frontend/messages",
            json!({ "from": { "kind": "role", "id": "commander" }, "kind": "order",
                    "title": "Style the dashboard", "scope": "web/", "owned_paths": ["web/"],
                    "acceptance": "renders" }),
        )
        .await;

        let (_, backend) = get(&state, "/briefing?role=backend").await;
        assert!(
            !backend["text"]
                .as_str()
                .unwrap()
                .contains("Style the dashboard"),
            "backend does not see frontend's directed work",
        );
        let (_, frontend) = get(&state, "/briefing?role=frontend").await;
        assert!(
            frontend["text"]
                .as_str()
                .unwrap()
                .contains("Style the dashboard"),
            "frontend sees its own",
        );
    }

    #[tokio::test]
    async fn a_tiny_budget_caps_the_packet_but_keeps_it_usable() {
        let state = AppState::new(Config::default());
        seed(&state).await;
        // Record several more decisions so the full packet exceeds a tiny budget.
        for i in 0..20 {
            post(
                &state,
                "/board",
                json!({ "role": "commander", "key": format!("decision-{i}"), "section": "decision",
                        "body": "a lengthy rationale that eats into the byte budget quickly" }),
            )
            .await;
        }

        let (status, packet) = get(&state, "/briefing?role=backend&budget=300").await;
        assert_eq!(status, StatusCode::OK);
        let text = packet["text"].as_str().unwrap();
        assert_eq!(packet["capped"], true, "the oversized packet is capped");
        assert!(
            text.len() <= 300,
            "the cap is enforced: {} bytes",
            text.len()
        );
        assert!(
            text.starts_with("Briefing for backend"),
            "keeps the header: {text}"
        );
    }

    #[tokio::test]
    async fn an_empty_crew_briefs_gracefully() {
        let state = AppState::new(Config::default());
        let (status, packet) = get(&state, "/briefing?role=backend").await;
        assert_eq!(status, StatusCode::OK);
        let text = packet["text"].as_str().unwrap();
        assert!(
            text.contains("no entries yet"),
            "explains the empty board: {text}"
        );
        assert_eq!(packet["capped"], false);
    }

    #[tokio::test]
    async fn a_briefing_without_a_role_is_a_bad_request() {
        let state = AppState::new(Config::default());
        let (status, _) = get(&state, "/briefing?role=%20").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
