//! The strongly-typed identifiers the whole crew shares (C-NEWTYPE).
//!
//! Roles, channels, messages, and tasks are never bare strings: each is a
//! newtype, so the compiler stops a `RoleId` from being passed where a
//! `ChannelId` is meant. Role and channel ids wrap human-meaningful names;
//! message and task ids wrap a random UUID.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Defines a string-backed identifier newtype with the common id impls.
///
/// The leading doc comments become the type's docs, so every generated id is
/// documented per M-CANONICAL-DOCS.
macro_rules! string_id {
    ($(#[$doc:meta])+ $name:ident) => {
        $(#[$doc])+
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Creates a [`", stringify!($name), "`] from any string-like value.")]
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the identifier as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the identifier, returning the owned inner string.
            #[must_use]
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

/// Defines a UUID-backed identifier newtype with the common id impls.
macro_rules! uuid_id {
    ($(#[$doc:meta])+ $name:ident) => {
        $(#[$doc])+
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            #[doc = concat!("Mints a fresh, random (v4) [`", stringify!($name), "`].")]
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Returns the wrapped [`Uuid`].
            #[must_use]
            pub fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            /// Mints a fresh, random id (there is no meaningful fixed default).
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl std::str::FromStr for $name {
            type Err = uuid::Error;

            #[doc = concat!("Parses a [`", stringify!($name), "`] from its canonical UUID text.")]
            ///
            /// This is the inverse of [`Display`](std::fmt::Display), so a
            /// front-end or CLI can turn a task-id argument back into a typed
            /// id (issue #183). The accepted forms are exactly those a
            /// [`uuid::Uuid`] parses from a string.
            ///
            /// # Errors
            /// Returns a [`uuid::Error`] when `value` is not a valid UUID.
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                value.parse::<Uuid>().map(Self)
            }
        }
    };
}

string_id! {
    /// A role's stable identifier, such as `commander`, `backend`, or `qa`.
    ///
    /// # Examples
    /// ```
    /// use crew_core::RoleId;
    /// let backend = RoleId::new("backend");
    /// assert_eq!(backend.as_str(), "backend");
    /// ```
    RoleId
}

string_id! {
    /// A channel's identifier, such as `all-units`, `@backend`, or `frontend+backend`.
    ChannelId
}

uuid_id! {
    /// A message's globally unique identifier, referenced when an `answer`
    /// replies to a `question`.
    MessageId
}

uuid_id! {
    /// A task's globally unique identifier, correlating the events worked under it.
    TaskId
}

/// Who emitted an event: a role-scoped agent, or the General (the human).
///
/// Modeling the human as a distinct variant lets a consumer tell an order from
/// the General apart from one relayed by the commander, without string
/// matching.
///
/// # Examples
/// ```
/// use crew_core::{RoleId, Sender};
/// let from_human = Sender::General;
/// let from_agent = Sender::Role(RoleId::new("backend"));
/// assert_ne!(from_human, from_agent);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum Sender {
    /// A role-scoped agent (`commander`, `backend`, ...).
    Role(RoleId),
    /// The General: the human directing the crew.
    General,
}

#[cfg(test)]
mod tests {
    use super::{ChannelId, MessageId, RoleId, Sender, TaskId};

    #[test]
    fn string_ids_serialize_transparently() {
        assert_eq!(
            serde_json::to_string(&RoleId::new("backend")).unwrap(),
            "\"backend\"",
        );
        assert_eq!(
            serde_json::to_string(&ChannelId::new("all-units")).unwrap(),
            "\"all-units\"",
        );
    }

    #[test]
    fn uuid_ids_round_trip_and_are_unique() {
        let id = MessageId::new();
        assert_ne!(id, MessageId::new());
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(serde_json::from_str::<MessageId>(&json).unwrap(), id);
    }

    #[test]
    fn uuid_ids_parse_from_their_display_string() {
        // FromStr is the inverse of Display, so a CLI or MCP argument round-trips
        // through the wire form (issue #183).
        let id = TaskId::new();
        let parsed: TaskId = id.to_string().parse().unwrap();
        assert_eq!(parsed, id, "the id parses back from its own Display text");
        assert!(
            "not-a-uuid".parse::<TaskId>().is_err(),
            "a non-UUID string is a parse error, not a silent default",
        );
    }

    #[test]
    fn sender_tags_the_human_and_a_role_distinctly() {
        assert_eq!(
            serde_json::to_string(&Sender::General).unwrap(),
            "{\"kind\":\"general\"}",
        );
        assert_eq!(
            serde_json::to_string(&Sender::Role(RoleId::new("qa"))).unwrap(),
            "{\"kind\":\"role\",\"id\":\"qa\"}",
        );
    }
}
