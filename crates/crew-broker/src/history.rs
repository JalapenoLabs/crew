//! The history endpoint: read past events, filtered, ordered, and paginated.
//!
//! `GET /history` lets a consumer or a late joiner read the stored log without
//! holding a stream open. It filters by `channel`, `role`, `kind`, `task`, and
//! `since`, orders deterministically by `ts` then log position, and pages with a
//! stable cursor so concurrent writes never shift a page already returned.
//!
//! This module owns the HTTP surface only: it parses and validates the query string
//! into a backend-neutral [`EventQuery`] and formats the [`EventPage`] the store
//! returns. The filter, ordering, and paging live behind the [`Storage`](crate::Storage)
//! trait (see `store.rs`), so a future indexed backend can push them down. The
//! `summary=true` compaction lands in Phase 2 (this endpoint reserves the hook).

use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use crew_core::{ChannelId, Event, RoleId, TaskId, Timestamp};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ApiError;
use crate::state::AppState;
use crate::store::{EventFilter, EventKindTag, EventQuery};

/// The default page size when a request does not set `limit`.
const DEFAULT_LIMIT: usize = 100;

/// The largest page a single request may ask for, bounding response size and work.
const MAX_LIMIT: usize = 1000;

/// The history route: `GET /history` (read past events, filtered and paginated).
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/history", get(history))
}

/// The query of `GET /history`: filters, pagination, and the summary hook.
///
/// Every field is a raw string so a malformed value yields a typed 400 from this
/// handler rather than an untyped rejection from the extractor.
#[derive(Debug, Deserialize)]
struct HistoryQuery {
    /// Keep only events on this channel (pair member order does not matter).
    channel: Option<String>,
    /// Keep only events sent by this role.
    role: Option<String>,
    /// Keep only events of this kind: `message`, `lifecycle`, or `activity`.
    kind: Option<String>,
    /// Keep only events belonging to this task (a UUID).
    task: Option<String>,
    /// Keep only events at or after this RFC 3339 instant.
    since: Option<String>,
    /// Resume after this opaque cursor (from a previous page's `next_cursor`).
    after: Option<String>,
    /// The maximum number of events to return (defaults to [`DEFAULT_LIMIT`]).
    limit: Option<String>,
    /// Reserved: request the Phase 2 rolling-summary compaction.
    summary: Option<String>,
}

/// A page of history: the events, and the cursor to fetch the next page if any.
#[derive(Debug, Serialize)]
struct HistoryPage {
    /// The matching events, oldest first.
    events: Vec<Event>,
    /// The cursor to pass as `after` for the next page, absent on the last page.
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

/// `GET /history`: read past events, filtered, time-ordered, and paginated.
///
/// # Errors
/// Returns a 400 [`ApiError`] if a filter, the cursor, or `limit` is malformed, and
/// a 501 if `summary=true` is requested (that compaction lands in Phase 2).
async fn history(
    State(state): State<AppState>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<HistoryPage>, ApiError> {
    if wants_summary(query.summary.as_deref()) {
        return Err(ApiError::not_implemented(
            "history summary compaction is not implemented yet (Phase 2)",
        ));
    }

    let request = EventQuery {
        filter: parse_filter(&query)?,
        after: parse_cursor(query.after.as_deref())?,
        limit: parse_limit(query.limit.as_deref())?,
    };
    let page = state
        .storage
        .query(&request)
        .map_err(|_error| ApiError::bad_request("invalid pagination cursor"))?;

    Ok(Json(HistoryPage {
        events: page.events,
        next_cursor: page.next.map(|position| position.to_string()),
    }))
}

/// Parses and validates the filter parameters into a backend-neutral [`EventFilter`].
fn parse_filter(query: &HistoryQuery) -> Result<EventFilter, ApiError> {
    let kind = match nonempty(query.kind.as_deref()) {
        Some(kind) => Some(EventKindTag::parse(kind).ok_or_else(|| {
            ApiError::bad_request(format!(
                "unknown kind `{kind}`; expected message, lifecycle, or activity"
            ))
        })?),
        None => None,
    };
    Ok(EventFilter {
        channel: nonempty(query.channel.as_deref()).map(ChannelId::new),
        role: nonempty(query.role.as_deref()).map(RoleId::new),
        kind,
        task: nonempty(query.task.as_deref())
            .map(|task| from_str::<TaskId>(task).map_err(|_error| bad("task", "a UUID")))
            .transpose()?,
        since: nonempty(query.since.as_deref())
            .map(|since| {
                from_str::<Timestamp>(since).map_err(|_error| bad("since", "an RFC 3339 timestamp"))
            })
            .transpose()?,
    })
}

/// Whether the `summary` flag was set to a truthy value.
fn wants_summary(value: Option<&str>) -> bool {
    matches!(value, Some("1" | "true" | "yes"))
}

/// Parses and clamps the `limit`, defaulting when absent and rejecting a bad value.
fn parse_limit(limit: Option<&str>) -> Result<usize, ApiError> {
    match nonempty(limit) {
        None => Ok(DEFAULT_LIMIT),
        Some(raw) => {
            let limit: usize = raw
                .parse()
                .map_err(|_error| ApiError::bad_request("limit must be a non-negative integer"))?;
            if limit == 0 {
                return Err(ApiError::bad_request("limit must be at least 1"));
            }
            Ok(limit.min(MAX_LIMIT))
        }
    }
}

/// Parses the opaque `after` cursor into a log position, rejecting a bad value.
fn parse_cursor(after: Option<&str>) -> Result<Option<u64>, ApiError> {
    nonempty(after)
        .map(|raw| {
            raw.parse::<u64>()
                .map_err(|_error| ApiError::bad_request("invalid pagination cursor"))
        })
        .transpose()
}

/// The trimmed value if present and not blank, so a bare `?role=` reads as absent.
fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// Deserializes a string into a wire type (e.g. [`TaskId`], [`Timestamp`]) via serde.
fn from_str<T: DeserializeOwned>(value: &str) -> Result<T, serde_json::Error> {
    serde_json::from_value(Value::String(value.to_owned()))
}

/// A 400 for a filter that must be a particular shape.
fn bad(field: &str, shape: &str) -> ApiError {
    ApiError::bad_request(format!("{field} must be {shape}"))
}

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use crew_core::{
        ChannelId, Event, EventKind, Message, MessageId, MessageKind, RoleId, Sender, Timestamp,
    };
    use serde_json::Value;
    use tower::ServiceExt;

    use super::from_str;
    use crate::api;
    use crate::config::Config;
    use crate::state::AppState;

    /// An RFC 3339 timestamp for deterministic ordering in tests.
    fn ts(seconds: u32) -> Timestamp {
        from_str(&format!("2020-01-01T00:00:{seconds:02}Z")).unwrap()
    }

    /// A note message event from `role` on `channel`, stamped at `at`.
    fn message(role: &str, channel: &str, at: Timestamp) -> Event {
        Event {
            ts: at,
            from: Sender::Role(RoleId::new(role)),
            channel: ChannelId::new(channel),
            task: None,
            kind: EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Note,
                body: String::new(),
            }),
        }
    }

    fn seed(state: &AppState, events: impl IntoIterator<Item = Event>) {
        for event in events {
            state.storage.append(event);
        }
    }

    async fn get(state: &AppState, query: &str) -> (StatusCode, Value) {
        let request = Request::builder()
            .uri(format!("/history{query}"))
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

    #[tokio::test]
    async fn the_endpoint_pages_through_filtered_time_ordered_events() {
        let state = AppState::new(Config::default());
        seed(
            &state,
            (0..5).map(|i| message("backend", "all-units", ts(i * 2))),
        );
        seed(&state, [message("frontend", "@frontend", ts(1))]);

        // Filter to backend and page two at a time, following the cursor to the end.
        let mut collected = Vec::new();
        let mut query = "?role=backend&limit=2".to_owned();
        loop {
            let (status, body) = get(&state, &query).await;
            assert_eq!(status, StatusCode::OK);
            for event in body["events"].as_array().unwrap() {
                collected.push(event["ts"].as_str().unwrap().to_owned());
            }
            match body.get("next_cursor").and_then(Value::as_str) {
                Some(cursor) => query = format!("?role=backend&limit=2&after={cursor}"),
                None => break,
            }
        }
        assert_eq!(
            collected.len(),
            5,
            "every backend event, once, across pages"
        );
        let mut sorted = collected.clone();
        sorted.sort();
        assert_eq!(collected, sorted, "events arrive time-ordered across pages");
    }

    #[tokio::test]
    async fn malformed_filters_and_cursor_are_typed_400s() {
        let state = AppState::new(Config::default());
        for query in [
            "?kind=nonsense",
            "?task=not-a-uuid",
            "?since=yesterday",
            "?limit=abc",
            "?limit=0",
            "?after=notanumber",
            "?after=999999", // past the end of an empty log
        ] {
            let (status, body) = get(&state, query).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query} should be 400");
            assert!(
                body.get("error").is_some(),
                "typed error for {query}: {body}"
            );
        }
    }

    #[tokio::test]
    async fn summary_is_a_reserved_501_hook() {
        let state = AppState::new(Config::default());
        let (status, body) = get(&state, "?summary=true").await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(body.get("error").is_some(), "typed error body: {body}");
    }

    #[tokio::test]
    async fn an_empty_log_returns_an_empty_page() {
        let state = AppState::new(Config::default());
        let (status, body) = get(&state, "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["events"].as_array().unwrap().len(), 0);
        assert!(
            body.get("next_cursor").is_none(),
            "no cursor on an empty page"
        );
    }
}
