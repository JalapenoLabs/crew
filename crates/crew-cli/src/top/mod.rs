//! `crew top`: the live terminal cockpit, htop for your crew (issue #51).
//!
//! Mission control in a plain terminal, no Seraphim required. It shows every
//! role with its status, current action, tokens, and cost, plus the recent
//! message flow and the crew's live count and spend, and updates live as the
//! crew works. Filter the feed by role or channel, and drill into a role's
//! activity.
//!
//! It is purely a rendering of the event stream and the roster, so it captures
//! nothing new. The [`Cockpit`](cockpit::Cockpit) state model is seeded once
//! from the `/roster` (issue #32) and `/stats` (issue #55) snapshots and then
//! advanced by folding each live `/stream` event (issue #31), so the display
//! updates by push, never by polling. The pure model and the ratatui
//! [`render`](render) are unit-tested; [`run`] is the thin terminal shell that
//! ties the background stream reader and the key handler to the model.

mod cockpit;
mod render;
mod run;

pub use run::run;
