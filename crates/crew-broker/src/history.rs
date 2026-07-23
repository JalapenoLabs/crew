//! The history endpoint: read past events, filtered, ordered, and paginated.
//!
//! `GET /history` lets a consumer or a late joiner read the stored log without
//! holding a stream open. It filters by `channel`, `role`, `kind`, `task`, and
//! `since`, orders deterministically by `ts` then log position, and pages with a
//! stable cursor so concurrent writes never shift a page already returned.
//!
//! Ordering and paging use each event's position in the append-only log as the
//! `ts` tiebreaker and the cursor. Because the log only grows, a position never
//! moves, so keyset paging on `(ts, position)` is stable where offset paging would
//! duplicate or skip rows as new events arrive. Filtering scans the whole log for
//! now; issue #13 moves the query behind the storage trait with an index, and the
//! `summary=true` compaction lands in Phase 2 (this endpoint reserves the hook).

use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use crew_core::{Channel, ChannelId, Event, EventKind, RoleId, Sender, TaskId, Timestamp};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ApiError;
use crate::state::AppState;

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

/// The parsed, validated filters a history request applies to the log.
#[derive(Debug, Default)]
struct Filters {
    channel: Option<ChannelId>,
    role: Option<RoleId>,
    kind: Option<String>,
    task: Option<TaskId>,
    since: Option<Timestamp>,
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

    let filters = Filters::parse(&query)?;
    let limit = parse_limit(query.limit.as_deref())?;
    let after = parse_cursor(query.after.as_deref())?;

    let log = state.storage.events();
    let (events, next_cursor) = paginate(&log, &filters, after, limit)?;
    Ok(Json(HistoryPage {
        events,
        next_cursor,
    }))
}

impl Filters {
    /// Parses and validates the filter parameters, rejecting a malformed value.
    fn parse(query: &HistoryQuery) -> Result<Self, ApiError> {
        let kind = match nonempty(query.kind.as_deref()) {
            Some(kind) if is_event_kind(kind) => Some(kind.to_owned()),
            Some(other) => {
                return Err(ApiError::bad_request(format!(
                    "unknown kind `{other}`; expected message, lifecycle, or activity"
                )))
            }
            None => None,
        };
        Ok(Self {
            channel: nonempty(query.channel.as_deref()).map(ChannelId::new),
            role: nonempty(query.role.as_deref()).map(RoleId::new),
            kind,
            task: nonempty(query.task.as_deref())
                .map(|task| from_str::<TaskId>(task).map_err(|_error| bad("task", "a UUID")))
                .transpose()?,
            since: nonempty(query.since.as_deref())
                .map(|since| {
                    from_str::<Timestamp>(since)
                        .map_err(|_error| bad("since", "an RFC 3339 timestamp"))
                })
                .transpose()?,
        })
    }

    /// Whether `event` satisfies every set filter.
    fn matches(&self, event: &Event) -> bool {
        if let Some(since) = self.since {
            if event.ts < since {
                return false;
            }
        }
        if let Some(task) = self.task {
            if event.task != Some(task) {
                return false;
            }
        }
        if let Some(role) = &self.role {
            if !matches!(&event.from, Sender::Role(from) if from == role) {
                return false;
            }
        }
        if let Some(channel) = &self.channel {
            if !channel_matches(&event.channel, channel) {
                return false;
            }
        }
        if let Some(kind) = &self.kind {
            if event_kind(&event.kind) != kind {
                return false;
            }
        }
        true
    }
}

/// Selects one page of events, filtered and ordered by `(ts, log position)`.
///
/// The `after` cursor is a log position from a previous page; paging resumes at the
/// first matching event ordered strictly after that position's `(ts, position)`, so
/// events appended concurrently (which take later positions) never shift a page
/// already returned. Returns the page and the cursor for the next page, if any.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the cursor points past the end of the log.
fn paginate(
    log: &[Event],
    filters: &Filters,
    after: Option<u64>,
    limit: usize,
) -> Result<(Vec<Event>, Option<String>), ApiError> {
    // Resolve the cursor to the `(ts, position)` boundary to resume strictly after.
    let boundary = match after {
        Some(position) => {
            let event = log
                .get(usize::try_from(position).unwrap_or(usize::MAX))
                .ok_or_else(|| ApiError::bad_request("invalid pagination cursor"))?;
            Some((event.ts, position))
        }
        None => None,
    };

    // Pair each event with its stable log position, keep the matches, and order by
    // `(ts, position)`: a total, deterministic order the cursor can resume from.
    let mut matched: Vec<(u64, &Event)> = log
        .iter()
        .enumerate()
        .map(|(position, event)| (position as u64, event))
        .filter(|(_, event)| filters.matches(event))
        .collect();
    matched.sort_by(|a, b| a.1.ts.cmp(&b.1.ts).then_with(|| a.0.cmp(&b.0)));

    let start = match boundary {
        Some(key) => matched.partition_point(|(position, event)| (event.ts, *position) <= key),
        None => 0,
    };
    let rest = &matched[start..];
    let take = rest.len().min(limit);
    let events = rest[..take]
        .iter()
        .map(|(_, event)| (*event).clone())
        .collect();
    // A next page exists only if matches remain past this one; its cursor is the
    // position of the last event returned here.
    let next_cursor = (rest.len() > take).then(|| rest[take - 1].0.to_string());

    Ok((events, next_cursor))
}

/// Whether the `summary` flag was set to a truthy value.
fn wants_summary(value: Option<&str>) -> bool {
    matches!(value, Some("1" | "true" | "yes"))
}

/// The kind tag of an event: `message`, `lifecycle`, or `activity`.
fn event_kind(kind: &EventKind) -> &'static str {
    match kind {
        EventKind::Message(_) => "message",
        EventKind::Lifecycle(_) => "lifecycle",
        EventKind::Activity(_) => "activity",
    }
}

/// Whether `kind` names one of the three event kinds.
fn is_event_kind(kind: &str) -> bool {
    matches!(kind, "message" | "lifecycle" | "activity")
}

/// Whether `channel` matches the `filter`, treating a pair channel as order-independent.
fn channel_matches(channel: &ChannelId, filter: &ChannelId) -> bool {
    if channel == filter {
        return true;
    }
    // Fall back to canonical channel identity so `a+b` matches a stored `b+a`.
    match (
        Channel::parse(channel.as_str()),
        Channel::parse(filter.as_str()),
    ) {
        (Some(channel), Some(filter)) => channel == filter,
        _ => false,
    }
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
        Activity, ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind, RoleId,
        Sender, TaskId, Timestamp,
    };
    use serde_json::Value;
    use tower::ServiceExt;

    use super::{from_str, paginate, Filters};
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

    #[test]
    fn orders_by_timestamp_then_log_position() {
        // Appended out of time order; history must return them time-ordered.
        let log = vec![
            message("backend", "all-units", ts(3)),
            message("backend", "all-units", ts(1)),
            message("backend", "all-units", ts(2)),
        ];
        let (events, next) = paginate(&log, &Filters::default(), None, 10).unwrap();
        let times: Vec<_> = events.iter().map(|event| event.ts).collect();
        assert_eq!(
            times,
            vec![ts(1), ts(2), ts(3)],
            "events come back time-ordered"
        );
        assert!(next.is_none(), "one page holds them all");
    }

    #[test]
    fn ties_on_timestamp_break_by_log_position_deterministically() {
        // Three events share a timestamp; log position is the stable tiebreaker.
        let log = vec![
            message("a", "all-units", ts(1)),
            message("b", "all-units", ts(1)),
            message("c", "all-units", ts(1)),
        ];
        let (first, cursor) = paginate(&log, &Filters::default(), None, 2).unwrap();
        let (second, _) =
            paginate(&log, &Filters::default(), cursor_seq(cursor.as_deref()), 2).unwrap();
        let senders: Vec<_> = first.iter().chain(&second).map(sender_id).collect();
        assert_eq!(
            senders,
            vec!["a", "b", "c"],
            "position order is stable across pages"
        );
    }

    #[test]
    fn paging_is_stable_when_new_events_are_appended_between_pages() {
        let mut log: Vec<Event> = (0..20)
            .map(|i| message("backend", "all-units", ts(i)))
            .collect();

        // Page 1.
        let (page1, cursor1) = paginate(&log, &Filters::default(), None, 8).unwrap();
        assert_eq!(page1.len(), 8);

        // A concurrent writer appends newer events after page 1 was read.
        log.push(message("frontend", "all-units", ts(40)));
        log.push(message("frontend", "all-units", ts(41)));

        // Page 2 and 3 resume from the cursor over the grown log.
        let (page2, cursor2) =
            paginate(&log, &Filters::default(), cursor_seq(cursor1.as_deref()), 8).unwrap();
        let (page3, _) =
            paginate(&log, &Filters::default(), cursor_seq(cursor2.as_deref()), 8).unwrap();

        let seen: Vec<Timestamp> = page1
            .iter()
            .chain(&page2)
            .chain(&page3)
            .map(|e| e.ts)
            .collect();
        // Every original event appears exactly once, in order, with no gaps...
        let originals: Vec<Timestamp> = (0..20).map(ts).collect();
        assert_eq!(
            seen[..20],
            originals[..],
            "the 20 originals page through intact"
        );
        // ...and the two concurrently-appended events land at the end, not mid-stream.
        assert_eq!(
            seen[20..],
            vec![ts(40), ts(41)],
            "new writes append after the cursor"
        );
        assert_eq!(seen.len(), 22, "no duplicates or skips");
    }

    #[test]
    fn filters_compose() {
        let mut ordered = message("backend", "@backend", ts(5));
        ordered.kind = EventKind::Message(Message {
            id: MessageId::new(),
            kind: MessageKind::Note,
            body: String::new(),
        });
        let task = TaskId::new();
        let mut tasked = message("backend", "all-units", ts(6));
        tasked.task = Some(task);

        let log = vec![
            message("backend", "all-units", ts(1)),
            message("frontend", "all-units", ts(2)),
            Event {
                kind: EventKind::Lifecycle(Lifecycle::Started),
                ..message("backend", "all-units", ts(3))
            },
            Event {
                kind: EventKind::Activity(Activity::TurnStarted),
                ..message("backend", "all-units", ts(4))
            },
            tasked.clone(),
        ];

        let page = |filters: &Filters| paginate(&log, filters, None, 100).unwrap().0;

        // role keeps only that sender.
        let by_role = page(&Filters {
            role: Some(RoleId::new("frontend")),
            ..Filters::default()
        });
        assert_eq!(by_role.len(), 1);
        assert_eq!(sender_id(&by_role[0]), "frontend");

        // kind keeps only that event kind.
        let by_kind = page(&Filters {
            kind: Some("lifecycle".to_owned()),
            ..Filters::default()
        });
        assert_eq!(by_kind.len(), 1);
        assert!(matches!(by_kind[0].kind, EventKind::Lifecycle(_)));

        // task keeps only events under that task.
        let by_task = page(&Filters {
            task: Some(task),
            ..Filters::default()
        });
        assert_eq!(by_task, vec![tasked]);

        // since keeps only events at or after the instant.
        let by_since = page(&Filters {
            since: Some(ts(3)),
            ..Filters::default()
        });
        assert_eq!(by_since.len(), 3, "ts 3, 4, and 6 remain");
    }

    #[test]
    fn channel_filter_ignores_pair_member_order() {
        let log = vec![message("backend", "frontend+backend", ts(1))];
        let page = paginate(
            &log,
            &Filters {
                channel: Some(ChannelId::new("backend+frontend")),
                ..Filters::default()
            },
            None,
            100,
        )
        .unwrap()
        .0;
        assert_eq!(
            page.len(),
            1,
            "a pair channel matches regardless of member order"
        );
    }

    fn sender_id(event: &Event) -> &str {
        match &event.from {
            Sender::Role(role) => role.as_str(),
            Sender::General => "general",
        }
    }

    /// Extracts the numeric cursor a page returned, for the next `paginate` call.
    fn cursor_seq(cursor: Option<&str>) -> Option<u64> {
        cursor.map(|c| c.parse().unwrap())
    }

    // --- Endpoint wiring ---

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

    #[test]
    fn message_and_general_sender_helpers_compile() {
        // Exercise the General branch so the sender helper is fully covered.
        let event = Event {
            from: Sender::General,
            ..message("x", "all-units", ts(1))
        };
        assert_eq!(sender_id(&event), "general");
    }
}
