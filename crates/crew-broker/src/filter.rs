//! The event filter shared by the aggregate views: `GET /history` and `GET /stream`.
//!
//! Both narrow the one event stream the same way, so a filtered live subscription and
//! a filtered history read agree on which events belong to the view (issue #31). This
//! module parses the query params into a backend-neutral [`EventFilter`]; the store
//! applies it to the log for history, and the live `/stream` applies the very same
//! [`EventFilter::matches`](crate::store::EventFilter::matches) to each fanned-out
//! event. Parsing lives here, in the HTTP layer, so the store stays query-agnostic.

use crew_core::{ChannelId, RoleId, TaskId, Timestamp};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;

use crate::error::ApiError;
use crate::store::{EventFilter, EventKindTag};

/// The filter query params common to the aggregate views: which events to keep.
///
/// Every field is a raw string so a malformed value yields a typed 400 from
/// [`to_filter`](FilterQuery::to_filter) rather than an untyped extractor rejection.
/// An absent or blank field imposes no constraint, so a bare `?role=` reads as unset.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct FilterQuery {
    /// Keep only events on this channel (pair member order does not matter).
    pub channel: Option<String>,
    /// Keep only events sent by this role.
    pub role: Option<String>,
    /// Keep only events on this role's activity timeline: its own events (messages it
    /// sent, its lifecycle, its activity) plus the messages addressed to it (issue #30).
    pub agent: Option<String>,
    /// Keep only events of this kind: `message`, `lifecycle`, or `activity`.
    pub kind: Option<String>,
    /// Keep only events belonging to this task (a UUID).
    pub task: Option<String>,
    /// Keep only events at or after this RFC 3339 instant.
    pub since: Option<String>,
}

impl FilterQuery {
    /// Parses and validates the params into a backend-neutral [`EventFilter`].
    ///
    /// # Errors
    /// Returns a 400 [`ApiError`] if `kind` is not a known kind, or `task` / `since`
    /// is malformed.
    pub(crate) fn to_filter(&self) -> Result<EventFilter, ApiError> {
        let kind = match nonempty(self.kind.as_deref()) {
            Some(kind) => Some(EventKindTag::parse(kind).ok_or_else(|| {
                ApiError::bad_request(format!(
                    "unknown kind `{kind}`; expected message, lifecycle, or activity"
                ))
            })?),
            None => None,
        };
        Ok(EventFilter {
            channel: nonempty(self.channel.as_deref()).map(ChannelId::new),
            role: nonempty(self.role.as_deref()).map(RoleId::new),
            agent: nonempty(self.agent.as_deref()).map(RoleId::new),
            kind,
            task: nonempty(self.task.as_deref())
                .map(|task| from_str::<TaskId>(task).map_err(|_error| bad("task", "a UUID")))
                .transpose()?,
            since: nonempty(self.since.as_deref())
                .map(|since| {
                    from_str::<Timestamp>(since)
                        .map_err(|_error| bad("since", "an RFC 3339 timestamp"))
                })
                .transpose()?,
        })
    }
}

/// The trimmed value if present and not blank, so a bare `?role=` reads as absent.
pub(crate) fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// Deserializes a string into a wire type (e.g. [`TaskId`], [`Timestamp`]) via serde.
pub(crate) fn from_str<T: DeserializeOwned>(value: &str) -> Result<T, serde_json::Error> {
    serde_json::from_value(Value::String(value.to_owned()))
}

/// A 400 for a filter that must be a particular shape.
fn bad(field: &str, shape: &str) -> ApiError {
    ApiError::bad_request(format!("{field} must be {shape}"))
}

#[cfg(test)]
mod tests {
    use super::FilterQuery;
    use crate::store::EventKindTag;

    #[test]
    fn parses_the_named_filters() {
        let filter = FilterQuery {
            channel: Some("@backend".to_owned()),
            role: Some("commander".to_owned()),
            kind: Some("lifecycle".to_owned()),
            ..FilterQuery::default()
        }
        .to_filter()
        .expect("a valid filter");
        assert_eq!(filter.channel.unwrap().as_str(), "@backend");
        assert_eq!(filter.role.unwrap().as_str(), "commander");
        assert!(matches!(filter.kind, Some(EventKindTag::Lifecycle)));
        assert!(filter.task.is_none() && filter.since.is_none());
    }

    #[test]
    fn a_blank_field_imposes_no_constraint() {
        // A bare `?role=` or a whitespace value reads as unset, not "the empty role".
        let filter = FilterQuery {
            role: Some("   ".to_owned()),
            channel: Some(String::new()),
            ..FilterQuery::default()
        }
        .to_filter()
        .expect("blank fields are simply absent");
        assert!(filter.role.is_none(), "a blank role is no constraint");
        assert!(filter.channel.is_none(), "a blank channel is no constraint");
    }

    #[test]
    fn rejects_a_malformed_kind_task_or_since() {
        // Each is a typed 400, so a bad filter never silently matches everything.
        FilterQuery {
            kind: Some("bogus".to_owned()),
            ..FilterQuery::default()
        }
        .to_filter()
        .unwrap_err();
        FilterQuery {
            task: Some("not-a-uuid".to_owned()),
            ..FilterQuery::default()
        }
        .to_filter()
        .unwrap_err();
        FilterQuery {
            since: Some("yesterday".to_owned()),
            ..FilterQuery::default()
        }
        .to_filter()
        .unwrap_err();
    }
}
