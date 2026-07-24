//! Typed API errors that render as a JSON 4xx, never a panic.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

/// An error returned to an API client.
///
/// Renders as a JSON body `{ "error": "..." }` with a 4xx status, so a
/// malformed request never panics the broker and the client always gets a typed
/// reason.
#[derive(Debug)]
pub enum ApiError {
    /// The request was malformed or failed validation (HTTP 400).
    BadRequest(String),
    /// The requested resource does not exist (HTTP 404).
    NotFound(String),
    /// The requester is not allowed to perform this action: retracting a board
    /// entry it neither authored nor commands (HTTP 403).
    Forbidden(String),
    /// The request conflicts with the current state: claiming work another role
    /// already holds, or verifying one's own work or a task not awaiting a
    /// verdict (HTTP 409).
    Conflict(String),
    /// The request is valid but the feature is not built yet (HTTP 501).
    NotImplemented(String),
}

impl ApiError {
    /// Builds a [`ApiError::BadRequest`] from anything that renders as a
    /// string.
    #[must_use]
    pub fn bad_request(message: impl std::fmt::Display) -> Self {
        Self::BadRequest(message.to_string())
    }

    /// Builds a [`ApiError::NotFound`] from anything that renders as a string.
    #[must_use]
    pub fn not_found(message: impl std::fmt::Display) -> Self {
        Self::NotFound(message.to_string())
    }

    /// Builds a [`ApiError::Forbidden`] from anything that renders as a string.
    #[must_use]
    pub fn forbidden(message: impl std::fmt::Display) -> Self {
        Self::Forbidden(message.to_string())
    }

    /// Builds a [`ApiError::Conflict`] from anything that renders as a string.
    #[must_use]
    pub fn conflict(message: impl std::fmt::Display) -> Self {
        Self::Conflict(message.to_string())
    }

    /// Builds a [`ApiError::NotImplemented`] from anything that renders as a
    /// string.
    #[must_use]
    pub fn not_implemented(message: impl std::fmt::Display) -> Self {
        Self::NotImplemented(message.to_string())
    }
}

/// The JSON body of an [`ApiError`].
#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Self::Forbidden(message) => (StatusCode::FORBIDDEN, message),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
            Self::NotImplemented(message) => (StatusCode::NOT_IMPLEMENTED, message),
        };
        (status, Json(ErrorBody { error })).into_response()
    }
}
