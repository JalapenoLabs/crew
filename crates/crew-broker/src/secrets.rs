//! Masking and scrubbing of secret values (mirrors Seraphim's Scrubber
//! posture).
//!
//! A crew agent might echo a token it was given into a message. [`Scrubber`]
//! removes a configured set of secret values from an [`Event`] before the
//! broker persists it or streams it to subscribers, so a leaked secret never
//! reaches the log, the stream, or a front-end. [`mask`] turns a secret into an
//! identifiable preview (used as the replacement text), so an operator can
//! still tell two tokens apart without either being revealed. Both are pure and
//! unit-tested.

use std::collections::HashSet;

use crew_core::Event;
use serde_json::Value;

/// Recognized secret prefixes whose identifying lead-in is kept when masking.
const KNOWN_PREFIXES: &[&str] = &[
    "sk-ant-",
    "github_pat_",
    "ghp_",
    "gho_",
    "ghs_",
    "sk_live_",
    "sk_test_",
];

/// How many trailing characters stay visible when a known prefix is recognized.
const REVEALED_TAIL: usize = 4;

/// Shortest secret we will scrub. Replacing a very short value (say `"1"`)
/// would corrupt unrelated text, and real tokens are long, so we refuse
/// anything shorter.
const MIN_SCRUB_LEN: usize = 8;

/// Masks a secret into an identifiable, length-preserving preview.
///
/// A value beginning with a known prefix keeps that prefix and its last few
/// characters, with the middle replaced by `*`; any other value is fully
/// masked. An empty input returns an empty string.
///
/// # Examples
/// ```
/// use crew_broker::mask;
/// assert_eq!(mask("sk_live_123456789"), "sk_live_*****6789");
/// assert_eq!(mask("12345"), "*****");
/// ```
#[must_use]
pub fn mask(secret: &str) -> String {
    let total = secret.chars().count();
    if total == 0 {
        return String::new();
    }

    let Some(prefix) = KNOWN_PREFIXES
        .iter()
        .copied()
        .find(|prefix| secret.starts_with(prefix))
    else {
        return "*".repeat(total);
    };

    let prefix_len = prefix.chars().count();
    // Only reveal the tail when a meaningful middle stays hidden; otherwise keep
    // just the prefix so we never expose most of a short secret.
    if total > prefix_len + REVEALED_TAIL {
        let masked = total - prefix_len - REVEALED_TAIL;
        let tail: String = secret.chars().skip(total - REVEALED_TAIL).collect();
        format!("{prefix}{}{tail}", "*".repeat(masked))
    } else {
        format!("{prefix}{}", "*".repeat(total - prefix_len))
    }
}

/// Replaces known secret values with their masked form wherever they appear.
///
/// Built once from the configured secrets, then applied to every persisted or
/// streamed event. Replacements run longest-secret-first, so a secret that
/// contains another is masked before its shorter substring.
#[derive(Debug, Clone, Default)]
pub struct Scrubber {
    /// `(secret, mask)` pairs, sorted by descending secret length.
    replacements: Vec<(String, String)>,
}

impl Scrubber {
    /// Builds a scrubber from raw secret values.
    ///
    /// Empty, duplicate, and too-short values (under 8 characters) are
    /// ignored, so an unset or trivial secret never corrupts unrelated
    /// text.
    #[must_use]
    pub fn new<I: IntoIterator<Item = String>>(secrets: I) -> Self {
        let mut seen = HashSet::new();
        let mut replacements: Vec<(String, String)> = secrets
            .into_iter()
            .filter(|secret| secret.chars().count() >= MIN_SCRUB_LEN)
            .filter(|secret| seen.insert(secret.clone()))
            .map(|secret| {
                let masked = mask(&secret);
                (secret, masked)
            })
            .collect();
        replacements.sort_by(|left, right| right.0.len().cmp(&left.0.len()));
        Self { replacements }
    }

    /// Whether this scrubber has any secrets to replace.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.replacements.is_empty()
    }

    /// Returns `text` with every known secret replaced by its masked form.
    #[must_use]
    pub fn scrub_text(&self, text: &str) -> String {
        let mut scrubbed = text.to_owned();
        for (secret, masked) in &self.replacements {
            if scrubbed.contains(secret.as_str()) {
                scrubbed = scrubbed.replace(secret.as_str(), masked);
            }
        }
        scrubbed
    }

    /// Scrubs every string value inside a JSON value, in place.
    ///
    /// Object keys are field names, not secrets, so they are left untouched;
    /// only string values, including those nested in arrays and objects,
    /// are masked.
    pub fn scrub_value(&self, value: &mut Value) {
        match value {
            Value::String(text) => {
                if self
                    .replacements
                    .iter()
                    .any(|(secret, _)| text.contains(secret.as_str()))
                {
                    *text = self.scrub_text(text);
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|item| self.scrub_value(item)),
            Value::Object(map) => map.values_mut().for_each(|item| self.scrub_value(item)),
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    /// Scrubs every string field of an event, in place, before it is persisted
    /// or streamed: the message body and every per-kind text field.
    ///
    /// A no-op when the scrubber holds no secrets.
    ///
    /// # Panics
    /// Panics only if an [`Event`] fails to round-trip through its own JSON,
    /// which signals a bug in the event model: an `Event` always
    /// serializes, and masking only rewrites string contents, so the
    /// scrubbed value always deserializes back into an `Event`. Failing
    /// loud beats silently persisting an unscrubbed secret.
    pub fn scrub_event(&self, event: &mut Event) {
        if self.is_empty() {
            return;
        }
        let mut value = serde_json::to_value(&*event).expect("an Event always serializes");
        self.scrub_value(&mut value);
        *event = serde_json::from_value(value).expect("a scrubbed Event always deserializes");
    }
}

#[cfg(test)]
mod tests {
    use crew_core::{
        ChannelId, Event, EventKind, Message, MessageId, MessageKind, Sender, Timestamp,
    };

    use super::{mask, Scrubber};

    #[test]
    fn mask_keeps_a_known_prefix_and_tail() {
        assert_eq!(mask("sk_live_123456789"), "sk_live_*****6789");
        assert_eq!(mask("ghp_abcdefghijkl"), "ghp_********ijkl");
    }

    #[test]
    fn mask_fully_hides_an_unrecognized_value_but_keeps_length() {
        assert_eq!(mask("hunter2xxx"), "**********");
        assert_eq!(mask(""), "");
    }

    #[test]
    fn scrub_text_masks_a_secret_and_leaves_other_text_intact() {
        let scrubber = Scrubber::new(["sk-ant-supersecretvalue".to_owned()]);
        let masked = scrubber.scrub_text("the token is sk-ant-supersecretvalue, keep it safe");
        assert!(!masked.contains("sk-ant-supersecretvalue"));
        assert!(masked.contains("the token is "));
        assert!(masked.contains(", keep it safe"));
    }

    #[test]
    fn short_and_empty_secrets_are_ignored() {
        // "abc" is below MIN_SCRUB_LEN, so it must not scrub matching text.
        let scrubber = Scrubber::new(["abc".to_owned(), String::new()]);
        assert!(scrubber.is_empty());
        assert_eq!(scrubber.scrub_text("abc appears here"), "abc appears here");
    }

    #[test]
    fn scrub_event_masks_a_secret_in_the_message_body() {
        let secret = "sk-ant-leakedintothebody";
        let scrubber = Scrubber::new([secret.to_owned()]);
        let mut event = Event {
            ts: Timestamp::now(),
            from: Sender::Role(crew_core::RoleId::new("backend")),
            channel: ChannelId::new("all-units"),
            task: None,
            kind: EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Note,
                body: format!("oops the key is {secret} do not share"),
            }),
        };
        scrubber.scrub_event(&mut event);
        let EventKind::Message(message) = &event.kind else {
            panic!("expected a message event");
        };
        assert!(
            !message.body.contains(secret),
            "body still leaks: {}",
            message.body
        );
        assert!(message.body.contains("oops the key is "));
    }
}
