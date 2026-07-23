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
//! trait (see `store.rs`), so a future indexed backend can push them down. With
//! `summary=true` it returns a rolling-summary compaction instead of a raw page (see
//! [`summary`](crate::summary)): the older events folded into bounded aggregates plus
//! the recent tail, so a late joiner reads bounded context, not the full log.

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use crew_core::Event;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::filter::{nonempty, FilterQuery};
use crate::state::AppState;
use crate::store::{EventFilter, EventQuery};
use crate::summary::{summarize, HistorySummary};

/// The default page size when a request does not set `limit`.
const DEFAULT_LIMIT: usize = 100;

/// The largest page a single request may ask for, bounding response size and work.
const MAX_LIMIT: usize = 1000;

/// The history route: `GET /history` (read past events, filtered and paginated).
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/history", get(history))
}

/// The pagination and summary params of `GET /history`, alongside the shared
/// [`FilterQuery`] that says which events to keep.
///
/// Every field is a raw string so a malformed value yields a typed 400 from this
/// handler rather than an untyped rejection from the extractor.
#[derive(Debug, Deserialize)]
struct HistoryOptions {
    /// Resume after this opaque cursor (from a previous page's `next_cursor`).
    after: Option<String>,
    /// The maximum number of events to return; the tail size under `summary`.
    limit: Option<String>,
    /// Request the rolling-summary compaction instead of a raw page.
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

/// The `summary=true` response: a compaction of older events plus the recent tail.
///
/// The `summary` folds every event older than the tail into bounded aggregates; the
/// `tail` keeps the most recent `limit` events raw so recent detail is not lost.
#[derive(Debug, Serialize)]
struct SummaryResponse {
    /// The bounded compaction of the older events.
    summary: HistorySummary,
    /// The most recent events, kept raw (oldest first), at most `limit`.
    tail: Vec<Event>,
}

/// `GET /history`: read past events, filtered, time-ordered, and paginated.
///
/// With `summary=true` it returns the rolling-summary compaction instead: a digest of
/// the older events plus the recent tail (sized by `limit`), so a late joiner reads
/// bounded context rather than the full log.
///
/// # Errors
/// Returns a 400 [`ApiError`] if a filter, the cursor, or `limit` is malformed.
async fn history(
    State(state): State<AppState>,
    Query(filter): Query<FilterQuery>,
    Query(options): Query<HistoryOptions>,
) -> Result<Response, ApiError> {
    let filter = filter.to_filter()?;
    let limit = parse_limit(options.limit.as_deref())?;

    if wants_summary(options.summary.as_deref()) {
        return Ok(Json(summary_response(&state, filter, limit)?).into_response());
    }

    let request = EventQuery {
        filter,
        after: parse_cursor(options.after.as_deref())?,
        limit,
    };
    let page = state
        .storage
        .query(&request)
        .map_err(|_error| ApiError::bad_request("invalid pagination cursor"))?;

    Ok(Json(HistoryPage {
        events: page.events,
        next_cursor: page.next.map(|position| position.to_string()),
    })
    .into_response())
}

/// Builds the rolling-summary response: older events compacted, recent `tail` kept raw.
///
/// Reads the whole filtered, time-ordered history in one query, then splits off the
/// most recent `tail` events and folds the rest into a [`HistorySummary`].
fn summary_response(
    state: &AppState,
    filter: EventFilter,
    tail: usize,
) -> Result<SummaryResponse, ApiError> {
    let request = EventQuery {
        filter,
        after: None,
        limit: usize::MAX,
    };
    // `after` is None, so the query cannot return an invalid-cursor error here.
    let mut events = state
        .storage
        .query(&request)
        .map(|page| page.events)
        .map_err(|_error| ApiError::bad_request("could not read history"))?;
    let split = events.len().saturating_sub(tail);
    let recent = events.split_off(split);
    Ok(SummaryResponse {
        summary: summarize(&events),
        tail: recent,
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

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use crew_core::{
        ChannelId, Event, EventKind, Message, MessageId, MessageKind, RoleId, Sender, Timestamp,
    };
    use serde_json::Value;
    use tower::ServiceExt;

    use crate::api;
    use crate::config::Config;
    use crate::filter::from_str;
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
    async fn summary_compacts_older_events_and_keeps_the_recent_tail() {
        let state = AppState::new(Config::default());
        // 20 events; ask for a tail of 5, so 15 fold into the summary.
        seed(
            &state,
            (0..20).map(|i| message("backend", "all-units", ts(i))),
        );

        let (status, body) = get(&state, "?summary=true&limit=5").await;
        assert_eq!(status, StatusCode::OK);

        assert_eq!(
            body["summary"]["event_count"], 15,
            "the older 15 are summarized"
        );
        assert_eq!(
            body["tail"].as_array().unwrap().len(),
            5,
            "the recent 5 stay raw",
        );
        // The tail is the most recent events, in order.
        let tail_ts = |i: usize| -> Timestamp {
            serde_json::from_value(body["tail"][i]["ts"].clone()).unwrap()
        };
        assert_eq!(
            tail_ts(0),
            ts(15),
            "the tail starts after the summarized events"
        );
        assert_eq!(tail_ts(4), ts(19), "the tail ends at the newest event");
        // The summary carries bounded aggregates, not the raw older events.
        assert!(
            body["summary"].get("events").is_none(),
            "no raw older events"
        );
        assert_eq!(body["summary"]["senders"][0]["name"], "backend");
        assert_eq!(body["summary"]["senders"][0]["count"], 15);
        assert!(body["summary"]["headline"]
            .as_str()
            .unwrap()
            .contains("15 earlier events"));
    }

    #[tokio::test]
    async fn summary_of_a_short_log_summarizes_nothing() {
        let state = AppState::new(Config::default());
        seed(
            &state,
            (0..3).map(|i| message("backend", "all-units", ts(i))),
        );

        // The tail default (100) exceeds the log, so nothing is old enough to summarize.
        let (status, body) = get(&state, "?summary=true").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["summary"]["event_count"], 0);
        assert_eq!(
            body["summary"]["headline"],
            "No earlier events to summarize."
        );
        assert_eq!(
            body["tail"].as_array().unwrap().len(),
            3,
            "all three stay raw"
        );
    }

    #[tokio::test]
    async fn summary_respects_filters() {
        let state = AppState::new(Config::default());
        seed(
            &state,
            (0..10).map(|i| message("backend", "all-units", ts(i))),
        );
        seed(
            &state,
            (0..4).map(|i| message("frontend", "all-units", ts(20 + i))),
        );

        // Summarize only backend's messages, tail of 2, so 8 fold in.
        let (status, body) = get(&state, "?summary=true&role=backend&limit=2").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["summary"]["event_count"], 8,
            "only backend events counted"
        );
        assert_eq!(body["summary"]["senders"].as_array().unwrap().len(), 1);
        assert_eq!(body["summary"]["senders"][0]["name"], "backend");
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
