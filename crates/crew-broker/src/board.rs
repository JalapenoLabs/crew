//! The shared situation board: the crew's durable memory (issue #49).
//!
//! The board is distinct from the transient message stream: it holds agreed interfaces,
//! decisions and their rationale, and known gotchas, so the crew stops re-deriving and
//! re-litigating what is settled. `POST /board` records an entry (or retracts one) and
//! `GET /board` reads the live board; every change is a first-class `board` event on the
//! stream (to `all-units`), so the board is auditable and, because it is a projection of
//! those durable events, it survives a restart (see `docs/communication.md`, context
//! management, and `docs/observability.md`).
//!
//! The whole crew reads and writes it; the commander curates it. The board state lives in
//! the broker ([`AppState`]).

use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use crew_core::{
    BoardEvent, BoardSection, ChannelId, Event, EventKind, RoleId, Sender, Timestamp, ALL_UNITS,
};
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::events::JsonBody;
use crate::state::{AppState, BoardEntry};

/// The board routes: read the board, and record or retract an entry.
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/board", get(read).post(record))
}

/// The `GET /board` query: an optional section to filter to.
#[derive(Debug, Default, Deserialize)]
struct BoardFilter {
    /// Keep only entries in this section; omit for the whole board.
    #[serde(default)]
    section: Option<BoardSection>,
}

/// `GET /board`: the live board, every entry ordered by section then topic.
async fn read(State(state): State<AppState>, Query(filter): Query<BoardFilter>) -> Json<BoardView> {
    Json(BoardView::from_state(&state, filter.section))
}

/// The `POST /board` body: a role recording or retracting a board entry.
#[derive(Debug, Deserialize)]
struct BoardChange {
    /// The role recording or retracting the entry, which authors the change.
    role: String,
    /// The entry's stable key: the topic it concerns.
    key: String,
    /// The section for a recorded entry; ignored (and unnecessary) on a retraction.
    #[serde(default)]
    section: Option<BoardSection>,
    /// The entry's content on a recording; unused on a retraction.
    #[serde(default)]
    body: String,
    /// Whether this retracts the entry rather than recording one.
    #[serde(default)]
    retract: bool,
}

/// `POST /board`: record or replace an entry, or retract one (with `retract: true`).
///
/// Recording needs a `section` and a `body`; retracting needs only the `key`. The change
/// updates the live board and publishes a `board` event, so it is durable and auditable.
///
/// # Errors
/// Returns a 400 [`ApiError`] if a required field is empty, or a 404 if a retraction names
/// an entry that is not on the board.
async fn record(
    State(state): State<AppState>,
    JsonBody(change): JsonBody<BoardChange>,
) -> Result<Json<BoardView>, ApiError> {
    let author = RoleId::new(non_empty(&change.role, "role")?);
    let key = non_empty(&change.key, "key")?.to_owned();

    if change.retract {
        let removed = state
            .retract_board(&key)
            .ok_or_else(|| ApiError::not_found(format!("no board entry `{key}` to retract")))?;
        state.publish(board_event(
            author,
            key,
            removed.section,
            String::new(),
            true,
        ));
    } else {
        let section = change
            .section
            .ok_or_else(|| ApiError::bad_request("recording an entry needs a `section`"))?;
        let body = non_empty(&change.body, "body")?.to_owned();
        state.record_board(key.clone(), section, author.clone(), body.clone());
        state.publish(board_event(author, key, section, body, false));
    }

    Ok(Json(BoardView::from_state(&state, None)))
}

/// A board change as a first-class stream event, `from` its author, to `all-units`.
fn board_event(
    author: RoleId,
    key: String,
    section: BoardSection,
    body: String,
    retracted: bool,
) -> Event {
    Event {
        ts: Timestamp::now(),
        from: Sender::Role(author.clone()),
        channel: ChannelId::new(ALL_UNITS),
        task: None,
        kind: EventKind::Board(BoardEvent {
            key,
            section,
            author,
            body,
            retracted,
        }),
    }
}

/// Trims a required field, erroring if it is blank.
fn non_empty<'a>(value: &'a str, field: &str) -> Result<&'a str, ApiError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ApiError::bad_request(format!("{field} must not be empty")));
    }
    Ok(trimmed)
}

/// The `GET /board` response: every entry, ordered by section then topic.
#[derive(Debug, Serialize)]
struct BoardView {
    /// The board's entries.
    entries: Vec<EntryView>,
}

/// One board entry in the view.
#[derive(Debug, Serialize)]
struct EntryView {
    /// The entry's stable key (its topic).
    key: String,
    /// Which section it belongs to.
    section: BoardSection,
    /// The role that recorded it.
    author: RoleId,
    /// The entry's content.
    body: String,
}

impl BoardView {
    /// Builds the view from the live board, optionally filtered to one `section`.
    ///
    /// Entries are ordered by section (decisions, then interfaces, then gotchas) and then
    /// by topic within a section, so the board reads as a grouped document.
    fn from_state(state: &AppState, section: Option<BoardSection>) -> Self {
        let mut entries: Vec<EntryView> = state
            .board_snapshot()
            .into_iter()
            .filter(|(_, entry)| section.is_none_or(|only| entry.section == only))
            .map(|(key, entry): (String, BoardEntry)| EntryView {
                key,
                section: entry.section,
                author: entry.author,
                body: entry.body,
            })
            .collect();
        entries.sort_by(|a, b| {
            section_rank(a.section)
                .cmp(&section_rank(b.section))
                .then_with(|| a.key.cmp(&b.key))
        });
        Self { entries }
    }
}

/// A section's display order: decisions first, then interfaces, then gotchas.
fn section_rank(section: BoardSection) -> u8 {
    match section {
        BoardSection::Decision => 0,
        BoardSection::Interface => 1,
        BoardSection::Gotcha => 2,
    }
}

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use crew_core::{Event, EventKind};
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::api;
    use crate::config::Config;
    use crate::state::AppState;
    use crate::store::LogStore;

    async fn send(
        state: &AppState,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json")
            .body(body.map_or_else(Body::empty, |value| Body::from(value.to_string())))
            .unwrap();
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn post(state: &AppState, path: &str, body: Value) -> (StatusCode, Value) {
        send(state, "POST", path, Some(body)).await
    }

    async fn get(state: &AppState, path: &str) -> Value {
        send(state, "GET", path, None).await.1
    }

    /// The entry with `key` in a board view, if present.
    fn entry<'a>(view: &'a Value, key: &str) -> Option<&'a Value> {
        view["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["key"] == key)
    }

    #[tokio::test]
    async fn a_recorded_decision_is_visible_to_every_role() {
        let state = AppState::new(Config::default());

        let (status, view) = post(
            &state,
            "/board",
            json!({ "role": "commander", "key": "auth-strategy", "section": "decision",
                    "body": "JWT, 15m tokens; stateless matches the gateway" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let recorded = entry(&view, "auth-strategy").expect("the entry is on the board");
        assert_eq!(recorded["section"], "decision");
        assert_eq!(recorded["author"], "commander");
        assert!(recorded["body"].as_str().unwrap().contains("JWT"));

        // Any role reads the same board: GET is not role-scoped.
        let read = get(&state, "/board").await;
        assert!(
            entry(&read, "auth-strategy").is_some(),
            "visible to every role"
        );
    }

    #[tokio::test]
    async fn recording_the_same_key_updates_the_entry() {
        let state = AppState::new(Config::default());
        post(
            &state,
            "/board",
            json!({ "role": "backend", "key": "api-errors", "section": "interface", "body": "v1" }),
        )
        .await;
        let (_, view) = post(
            &state,
            "/board",
            json!({ "role": "commander", "key": "api-errors", "section": "interface", "body": "v2" }),
        )
        .await;
        let updated = entry(&view, "api-errors").unwrap();
        assert_eq!(
            updated["body"], "v2",
            "the entry is replaced, not duplicated"
        );
        assert_eq!(updated["author"], "commander");
        assert_eq!(
            view["entries"].as_array().unwrap().len(),
            1,
            "one entry, updated in place"
        );
    }

    #[tokio::test]
    async fn a_change_publishes_an_auditable_board_event() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();
        post(
            &state,
            "/board",
            json!({ "role": "commander", "key": "auth-strategy", "section": "decision", "body": "JWT" }),
        )
        .await;
        let event = stream.try_recv().unwrap().event;
        match event.kind {
            EventKind::Board(change) => {
                assert_eq!(change.key, "auth-strategy");
                assert!(!change.retracted);
                assert_eq!(change.author.as_str(), "commander");
            }
            other => panic!("expected a board event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn retracting_an_entry_removes_it_and_records_the_retraction() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();
        post(
            &state,
            "/board",
            json!({ "role": "commander", "key": "auth-strategy", "section": "decision", "body": "JWT" }),
        )
        .await;
        let _ = stream.try_recv(); // the record event

        let (status, view) = post(
            &state,
            "/board",
            json!({ "role": "commander", "key": "auth-strategy", "retract": true }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(entry(&view, "auth-strategy").is_none(), "the entry is gone");

        // The retraction is on the stream, carrying the retracted entry's section.
        let event = stream.try_recv().unwrap().event;
        match event.kind {
            EventKind::Board(change) => {
                assert!(change.retracted, "the event marks a retraction");
                assert_eq!(change.section, crew_core::BoardSection::Decision);
            }
            other => panic!("expected a board retraction, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn retracting_a_missing_entry_is_not_found() {
        let state = AppState::new(Config::default());
        let (status, _) = post(
            &state,
            "/board",
            json!({ "role": "commander", "key": "ghost", "retract": true }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn recording_without_a_section_or_body_is_rejected() {
        let state = AppState::new(Config::default());
        let (no_section, _) = post(
            &state,
            "/board",
            json!({ "role": "commander", "key": "x", "body": "y" }),
        )
        .await;
        assert_eq!(no_section, StatusCode::BAD_REQUEST);
        let (no_body, _) = post(
            &state,
            "/board",
            json!({ "role": "commander", "key": "x", "section": "decision" }),
        )
        .await;
        assert_eq!(no_body, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn the_section_filter_narrows_the_board() {
        let state = AppState::new(Config::default());
        for (key, section) in [("d", "decision"), ("i", "interface"), ("g", "gotcha")] {
            post(
                &state,
                "/board",
                json!({ "role": "commander", "key": key, "section": section, "body": "x" }),
            )
            .await;
        }
        let decisions = get(&state, "/board?section=decision").await;
        let keys: Vec<&str> = decisions["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["key"].as_str().unwrap())
            .collect();
        assert_eq!(keys, ["d"], "only the decision section");
    }

    /// A unique temp dir for the durability test, removed on drop.
    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!("crew-board-test-{}-{n}", std::process::id())))
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn a_decision_survives_a_restart() {
        let dir = TempDir::new();

        // First run: record a decision against a durable store, then drop the broker.
        let store = Arc::new(LogStore::open(&dir.0).unwrap());
        let state = AppState::with_storage(Config::default(), store);
        post(
            &state,
            "/board",
            json!({ "role": "commander", "key": "auth-strategy", "section": "decision", "body": "JWT" }),
        )
        .await;
        // The board event is durable in the log.
        assert!(state
            .storage
            .events()
            .iter()
            .any(|e: &Event| matches!(e.kind, EventKind::Board(_))));
        drop(state);

        // Second run: a fresh broker over the same dir rebuilds the board from the log.
        let reopened = Arc::new(LogStore::open(&dir.0).unwrap());
        let restarted = AppState::with_storage(Config::default(), reopened);
        let view = get(&restarted, "/board").await;
        let survived = entry(&view, "auth-strategy").expect("the decision survived the restart");
        assert_eq!(survived["body"], "JWT");
    }
}
