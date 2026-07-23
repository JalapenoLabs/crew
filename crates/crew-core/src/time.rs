//! The timestamp wrapper stamped on every event.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A UTC instant on the crew event timeline.
///
/// Wraps a [`chrono::DateTime<Utc>`] so a timestamp is its own type rather than a
/// bare integer or string, and so the wire format is fixed: it serializes as an
/// RFC 3339 string. Ordered, so events sort chronologically.
///
/// # Examples
/// ```
/// use crew_core::Timestamp;
/// let earlier = Timestamp::now();
/// let later = Timestamp::now();
/// assert!(earlier <= later);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    /// Returns the current UTC instant.
    #[must_use]
    pub fn now() -> Self {
        Self(Utc::now())
    }

    /// Returns the wrapped [`chrono::DateTime<Utc>`].
    #[must_use]
    pub fn to_datetime(self) -> DateTime<Utc> {
        self.0
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Self(value)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // RFC 3339, matching the serialized wire format.
        f.write_str(&self.0.to_rfc3339())
    }
}
