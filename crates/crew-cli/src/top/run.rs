//! The `crew top` terminal shell: seed, stream, draw, and handle keys (issue
//! #51).
//!
//! This is the thin, I/O side of the cockpit. It seeds a [`Cockpit`] from the
//! `/roster` and `/stats` snapshots (a dead broker fails fast here, before any
//! terminal is touched), then runs the ratatui event loop: a background thread
//! tails the broker's `/stream` SSE and pushes each event down a channel, and
//! the main loop drains the channel into the model, redraws, and translates key
//! presses into cockpit calls. The data updates by push, so the loop's tick is
//! only a render cadence, never a poll of the broker.
//!
//! The rendering and the state are unit-tested elsewhere ([`super::cockpit`],
//! [`super::render`]); this shell needs a real terminal, so it is kept minimal.

use std::{
    sync::mpsc::{self, Receiver, TryRecvError},
    thread,
    time::Duration,
};

use crew_substrate::core::Event;
use eyre::{Result, WrapErr};
use ratatui::crossterm::event::{
    self, Event as TermEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
};

use super::{
    cockpit::{Cockpit, RosterSeed, StatsSeed},
    render,
};
use crate::broker;

/// How often the render loop wakes to drain new stream events and redraw.
///
/// Not a poll of the broker: events arrive by SSE push into the channel, and
/// this only bounds how promptly the loop notices them and how snappy a key
/// press feels.
const TICK: Duration = Duration::from_millis(200);

/// Runs the live cockpit until the user quits (issue #51).
///
/// # Errors
/// Returns an error if the broker configuration is invalid, the broker cannot
/// be reached for the initial roster, or the terminal cannot be driven.
pub fn run(broker: Option<&str>) -> Result<()> {
    let base = broker::resolve_base(broker)?;

    // Seed from the snapshots first, so a wrong address or a broker that is not
    // running fails here with a clear message, not inside the alternate screen.
    let mut cockpit = Cockpit::default();
    cockpit.seed_roster(fetch_roster(&base)?);
    cockpit.seed_stats(fetch_stats(&base));

    // Tail the stream on a background thread, pushing each event to the loop.
    let events = spawn_stream(&base);

    let mut terminal = ratatui::init();
    let outcome = event_loop(&mut terminal, &mut cockpit, &events);
    ratatui::restore();
    outcome
}

/// The draw / drain / key loop, over an already-initialized terminal.
fn event_loop(
    terminal: &mut ratatui::DefaultTerminal,
    cockpit: &mut Cockpit,
    events: &Receiver<Event>,
) -> Result<()> {
    loop {
        // Fold every event that has arrived since the last frame.
        drain(events, cockpit);
        terminal
            .draw(|frame| render::render(frame, cockpit))
            .wrap_err("could not draw the cockpit")?;

        // Wait for a key up to the tick, so the loop still wakes to pick up
        // pushed events even when the operator is not typing.
        if event::poll(TICK).wrap_err("could not read terminal input")? {
            if let TermEvent::Key(key) = event::read().wrap_err("could not read a key")? {
                if key.kind == KeyEventKind::Press && handle_key(cockpit, key) == Flow::Quit {
                    return Ok(());
                }
            }
        }
    }
}

/// Folds every event currently buffered from the stream into the cockpit.
fn drain(events: &Receiver<Event>, cockpit: &mut Cockpit) {
    loop {
        match events.try_recv() {
            Ok(event) => cockpit.apply(&event),
            // Nothing new right now, or the stream thread ended: either way,
            // keep rendering the state we have.
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
        }
    }
}

/// Whether the loop should keep running or quit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flow {
    /// Keep the cockpit running.
    Continue,
    /// Leave `crew top`.
    Quit,
}

/// Applies one key press to the cockpit, returning whether to keep running.
fn handle_key(cockpit: &mut Cockpit, key: KeyEvent) -> Flow {
    // Ctrl-C always quits.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return Flow::Quit;
    }
    match key.code {
        KeyCode::Char('q') => Flow::Quit,
        // Esc backs out of a drill-in first, and quits from the overview.
        KeyCode::Esc => {
            if cockpit.leave_detail() {
                Flow::Continue
            } else {
                Flow::Quit
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            cockpit.select_next();
            Flow::Continue
        }
        KeyCode::Up | KeyCode::Char('k') => {
            cockpit.select_prev();
            Flow::Continue
        }
        KeyCode::Enter => {
            cockpit.toggle_detail();
            Flow::Continue
        }
        KeyCode::Char('f') => {
            cockpit.toggle_role_filter();
            Flow::Continue
        }
        KeyCode::Char('c') => {
            cockpit.cycle_channel_filter();
            Flow::Continue
        }
        KeyCode::Char('x') => {
            cockpit.clear_filter();
            Flow::Continue
        }
        _ => Flow::Continue,
    }
}

/// Tails the broker's `/stream` on a detached thread, pushing each event down a
/// channel for the render loop to drain.
///
/// The tail reconnects on a dropped connection (see
/// [`broker::tail_events`](crate::broker::tail_events)), so a broker restart
/// mid-session recovers on its own; the thread ends only if the very first
/// connection fails, after the roster fetch already succeeded, which leaves the
/// cockpit showing the seeded snapshot.
fn spawn_stream(base: &str) -> Receiver<Event> {
    let (sender, receiver) = mpsc::channel();
    let base = base.to_owned();
    thread::spawn(move || {
        let _ = broker::tail_events(&base, "/stream", |event| {
            // A send failure means the cockpit has quit and dropped the receiver;
            // nothing more to do.
            let _ = sender.send(event.clone());
        });
    });
    receiver
}

/// Fetches and parses the `/roster` snapshot the cockpit seeds from.
fn fetch_roster(base: &str) -> Result<RosterSeed> {
    let text = ureq::get(&format!("{base}/roster"))
        .call()
        .map_err(|err| {
            eyre::eyre!("could not reach the broker at {base}; is `crewd` running? ({err})")
        })?
        .into_string()
        .wrap_err("could not read the roster response")?;
    serde_json::from_str(&text).wrap_err("could not parse the roster response")
}

/// Fetches the `/stats` snapshot, best-effort: an unreachable or malformed
/// stats endpoint leaves the cockpit's tokens and cost at zero until telemetry
/// lands.
fn fetch_stats(base: &str) -> StatsSeed {
    ureq::get(&format!("{base}/stats"))
        .call()
        .ok()
        .and_then(|response| response.into_string().ok())
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use crew_substrate::core::RoleId;
    use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::{handle_key, Flow};
    use crate::top::cockpit::Cockpit;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    fn seeded() -> Cockpit {
        let mut cockpit = Cockpit::default();
        cockpit.seed_roster(
            serde_json::from_value(serde_json::json!({
                "standing": "running",
                "roles": [
                    { "role": "commander", "liveness": "working", "owned_paths": [] },
                    { "role": "backend", "liveness": "idle", "owned_paths": ["api/"] }
                ]
            }))
            .unwrap(),
        );
        cockpit
    }

    #[test]
    fn q_and_ctrl_c_quit() {
        let mut cockpit = seeded();
        assert_eq!(
            handle_key(&mut cockpit, press(KeyCode::Char('q'))),
            Flow::Quit
        );
        assert_eq!(
            handle_key(
                &mut cockpit,
                KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
            ),
            Flow::Quit,
        );
    }

    #[test]
    fn navigation_and_drill_in_keys_drive_the_cockpit() {
        let mut cockpit = seeded();
        // Down moves the selection off the first role and back around.
        assert_eq!(
            cockpit.selected_role().unwrap().role,
            RoleId::new("backend")
        );
        assert_eq!(
            handle_key(&mut cockpit, press(KeyCode::Down)),
            Flow::Continue
        );
        assert_eq!(
            cockpit.selected_role().unwrap().role,
            RoleId::new("commander")
        );
        assert_eq!(handle_key(&mut cockpit, press(KeyCode::Up)), Flow::Continue);
        assert_eq!(
            cockpit.selected_role().unwrap().role,
            RoleId::new("backend")
        );

        // Enter drills in; Esc backs out (not quit); Esc again from the overview quits.
        handle_key(&mut cockpit, press(KeyCode::Enter));
        assert!(cockpit.in_detail(), "Enter drills in");
        assert_eq!(
            handle_key(&mut cockpit, press(KeyCode::Esc)),
            Flow::Continue
        );
        assert!(!cockpit.in_detail(), "Esc backs out of the detail");
        assert_eq!(
            handle_key(&mut cockpit, press(KeyCode::Esc)),
            Flow::Quit,
            "Esc from the overview quits",
        );
    }

    #[test]
    fn filter_keys_toggle_and_clear() {
        let mut cockpit = seeded();
        handle_key(&mut cockpit, press(KeyCode::Char('f')));
        assert_eq!(
            cockpit.filter_label(),
            "role:backend",
            "f filters to the selected role"
        );
        handle_key(&mut cockpit, press(KeyCode::Char('x')));
        assert_eq!(cockpit.filter_label(), "all", "x clears the filter");
    }
}
