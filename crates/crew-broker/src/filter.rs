//! The event filter shared by the aggregate views: `GET /history` and `GET
//! /stream`.
//!
//! Both narrow the one event stream the same way, so a filtered live
//! subscription and a filtered history read agree on which events belong to the
//! view (issue #31). This module parses the query params into a backend-neutral
//! [`EventFilter`]; the store applies it to the log for history, and the live
//! `/stream` applies the very same
//! [`EventFilter::matches`](crate::store::EventFilter::matches) to each
//! fanned-out event. Parsing lives here, in the HTTP layer, so the store stays
//! query-agnostic.

use crew_core::{ChannelId, RoleId, TaskId, Timestamp};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;

use crate::{
    error::ApiError,
    store::{EventFilter, EventKindTag},
};

/// The filter query params common to the aggregate views: which events to keep.
///
/// Every field is a raw string so a malformed value yields a typed 400 from
/// [`to_filter`](FilterQuery::to_filter) rather than an untyped extractor
/// rejection. An absent or blank field imposes no constraint, so a bare
/// `?role=` reads as unset.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct FilterQuery {
    /// Keep only events on this channel (pair member order does not matter).
    pub channel: Option<String>,
    /// Keep only events sent by this role.
    pub role: Option<String>,
    /// Keep only events on this role's activity timeline: its own events
    /// (messages it sent, its lifecycle, its activity) plus the messages
    /// addressed to it (issue #30).
    pub agent: Option<String>,
    /// Keep only events of these kinds, comma-separated (e.g.
    /// `message,ledger,verification`); a single value keeps one kind. Absent or
    /// blank imposes no constraint (issue #125).
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
    /// Returns a 400 [`ApiError`] if any `kind` token is not a known kind, or
    /// `task` / `since` is malformed.
    pub(crate) fn to_filter(&self) -> Result<EventFilter, ApiError> {
        // `kind` is a comma-separated set (issue #125): each non-blank token must
        // name a known kind, and an absent or all-blank value is no constraint.
        let kind = self
            .kind
            .as_deref()
            .into_iter()
            .flat_map(|raw| raw.split(','))
            .filter_map(|token| nonempty(Some(token)))
            .map(|token| {
                EventKindTag::parse(token)
                    .ok_or_else(|| ApiError::bad_request(format!("unknown event kind `{token}`")))
            })
            .collect::<Result<Vec<_>, _>>()?;
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

/// The trimmed value if present and not blank, so a bare `?role=` reads as
/// absent.
pub(crate) fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// Deserializes a string into a wire type (e.g. [`TaskId`], [`Timestamp`]) via
/// serde.
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
        assert_eq!(filter.kind, vec![EventKindTag::Lifecycle]);
        assert!(filter.task.is_none() && filter.since.is_none());
    }

    #[test]
    fn parses_a_comma_separated_set_of_kinds() {
        // A consumer narrows to a subset in one query (issue #125): the stall
        // monitor fetches only message/ledger/verification.
        let filter = FilterQuery {
            kind: Some("message, ledger ,verification".to_owned()),
            ..FilterQuery::default()
        }
        .to_filter()
        .expect("a valid multi-kind filter");
        assert_eq!(
            filter.kind,
            vec![
                EventKindTag::Message,
                EventKindTag::Ledger,
                EventKindTag::Verification
            ]
        );

        // An absent or all-blank value is no constraint, not an error.
        let unset = FilterQuery {
            kind: Some(" , ".to_owned()),
            ..FilterQuery::default()
        }
        .to_filter()
        .expect("a blank kind list is no constraint");
        assert!(unset.kind.is_empty());
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
