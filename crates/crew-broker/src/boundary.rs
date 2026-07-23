//! The boundary endpoint: report a role reaching outside its lane (issue #46).
//!
//! Lane enforcement decides in-lane vs out-of-lane against the role's owned
//! paths (its authoritative role card, checked at the agent's `crew_lane`
//! tool). This endpoint is the surface: `POST /boundary` records a crossing as
//! a `boundary` event on the stream (from the role, to `all-units`), so the
//! operator sees who reached where and whether the crew's policy warned or
//! blocked. A genuine cross-lane need should go through the commander instead
//! of a silent edit.

use axum::{extract::State, routing::post, Json, Router};
use crew_core::{BoundaryEvent, ChannelId, Event, EventKind, RoleId, Sender, Timestamp, ALL_UNITS};
use serde::Deserialize;

use crate::{error::ApiError, events::JsonBody, state::AppState};

/// The boundary route: report a lane crossing.
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/boundary", post(report))
}

/// The `POST /boundary` body: a role reaching a path outside its lane.
#[derive(Debug, Deserialize)]
struct BoundaryReport {
    /// The role that reached outside its lane.
    role: String,
    /// The out-of-lane path it reached for.
    path: String,
    /// Whether the crew's policy blocked the edit; `false` (a warning) by
    /// default.
    #[serde(default)]
    blocked: bool,
}

/// `POST /boundary`: record a lane crossing on the stream.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the role or path is empty.
async fn report(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<BoundaryReport>,
) -> Result<Json<Event>, ApiError> {
    let role = request.role.trim();
    if role.is_empty() {
        return Err(ApiError::bad_request("role must not be empty"));
    }
    let path = request.path.trim();
    if path.is_empty() {
        return Err(ApiError::bad_request("path must not be empty"));
    }
    let role = RoleId::new(role);

    let event = Event {
        ts: Timestamp::now(),
        from: Sender::Role(role.clone()),
        channel: ChannelId::new(ALL_UNITS),
        task: None,
        kind: EventKind::Boundary(BoundaryEvent {
            role,
            path: path.to_owned(),
            blocked: request.blocked,
        }),
    };
    Ok(Json(state.publish(event).event))
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::{BoundaryEvent, EventKind};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::{api, config::Config, state::AppState};

    async fn post(state: &AppState, body: Value) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/boundary")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
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
    async fn a_crossing_is_recorded_on_the_stream() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();

        let (status, event) = post(
            &state,
            json!({ "role": "backend", "path": "frontend/app.tsx", "blocked": true }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["kind"]["kind"], "boundary");
        assert_eq!(event["from"]["id"], "backend");

        // The boundary event reached the live stream for the operator to see.
        let streamed = stream.try_recv().unwrap().event;
        assert_eq!(
            streamed.kind,
            EventKind::Boundary(BoundaryEvent {
                role: crew_core::RoleId::new("backend"),
                path: "frontend/app.tsx".to_owned(),
                blocked: true,
            })
        );

        // And it is in the durable log, filterable by kind.
        let stored = state.storage.events();
        assert!(matches!(
            stored.last().unwrap().kind,
            EventKind::Boundary(_)
        ));
    }

    #[tokio::test]
    async fn an_empty_role_or_path_is_rejected() {
        let state = AppState::new(Config::default());
        let (status, _) = post(&state, json!({ "role": "", "path": "x" })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = post(&state, json!({ "role": "backend", "path": "  " })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
