//! Rendering the cockpit with ratatui (issue #51).
//!
//! A thin projection of the [`Cockpit`] state: [`render`] draws the whole
//! frame, either the main view (a header, the roles table, and the message
//! feed) or, when drilled in, one role's activity detail, plus a footer of key
//! hints. It reads the model and never mutates it, so the layout is exercised
//! against an in-memory [`TestBackend`](ratatui::backend::TestBackend) with no
//! terminal.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table},
    Frame,
};

use super::cockpit::{Cockpit, RoleRow, Status};

/// Draws the whole cockpit frame from the current state.
pub(crate) fn render(frame: &mut Frame, cockpit: &Cockpit) {
    let area = frame.area();
    // A header line, the body, and a footer of key hints.
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(area);

    frame.render_widget(header(cockpit), rows[0]);
    if cockpit.in_detail() {
        render_detail(frame, rows[1], cockpit);
        frame.render_widget(footer("Esc/Enter back  q quit"), rows[2]);
    } else {
        render_overview(frame, rows[1], cockpit);
        frame.render_widget(
            footer(
                "up/down select  Enter drill-in  f filter role  c cycle channel  x clear  q quit",
            ),
            rows[2],
        );
    }
}

/// The header line: the crew standing, the live count, and the aggregate spend.
fn header(cockpit: &Cockpit) -> Paragraph<'static> {
    let (tokens, cost_micro_usd) = cockpit.aggregate();
    let line = Line::from(vec![
        Span::styled(
            "crew top ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            "{} live / {} roles  {}  {} tokens  {}  filter: {}",
            cockpit.live_count(),
            cockpit.role_count(),
            cockpit.standing(),
            tokens,
            dollars(cost_micro_usd),
            cockpit.filter_label(),
        )),
    ]);
    Paragraph::new(line)
}

/// The overview body: the roles table over the message feed.
fn render_overview(frame: &mut Frame, area: Rect, cockpit: &Cockpit) {
    let panes = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);
    frame.render_widget(roles_table(cockpit), panes[0]);
    frame.render_widget(feed_list(cockpit, panes[1].height), panes[1]);
}

/// The roles table: each role's status, current action, tokens, and cost.
fn roles_table(cockpit: &Cockpit) -> Table<'static> {
    let selected = cockpit.selected();
    let rows = cockpit.roles().into_iter().enumerate().map(|(index, row)| {
        let cells = vec![
            Cell::from(role_label(row)),
            Cell::from(Span::styled(row.status.label(), status_style(row.status))),
            Cell::from(elide_cell(&row.action, 48)),
            Cell::from(row.tokens.to_string()),
            Cell::from(dollars(row.cost_micro_usd)),
        ];
        let base = Row::new(cells);
        if index == selected {
            // Highlight the selected row without a stateful widget, so a static
            // render (and the tests) show the selection.
            base.style(Style::default().add_modifier(Modifier::REVERSED))
        } else {
            base
        }
    });

    Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(9),
            Constraint::Min(20),
            Constraint::Length(10),
            Constraint::Length(9),
        ],
    )
    .header(
        Row::new(["role", "status", "action", "tokens", "cost"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(Block::default().borders(Borders::ALL).title("roles"))
}

/// The message feed: the most recent lines that fit, newest at the bottom.
fn feed_list(cockpit: &Cockpit, height: u16) -> List<'static> {
    let feed = cockpit.feed();
    // The pane's inner height, minus the block's top and bottom borders.
    let capacity = usize::from(height.saturating_sub(2)).max(1);
    let start = feed.len().saturating_sub(capacity);
    let items = feed[start..].iter().map(|line| {
        ListItem::new(Line::from(vec![
            Span::styled(
                format!("{} ", line.channel),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(line.from.clone(), Style::default().fg(Color::Green)),
            Span::raw(format!(" ({}): {}", line.kind, line.summary)),
        ]))
    });
    List::new(items.collect::<Vec<_>>())
        .block(Block::default().borders(Borders::ALL).title("message flow"))
}

/// The drill-in detail: the selected role's header and its recent activity.
fn render_detail(frame: &mut Frame, area: Rect, cockpit: &Cockpit) {
    let Some(row) = cockpit.selected_role() else {
        return;
    };
    let panes = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(0)])
        .split(area);

    let lane = if row.owned_paths.is_empty() {
        "no lane".to_owned()
    } else {
        row.owned_paths.join(", ")
    };
    let summary = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(
                format!("{} ", row.role),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(row.status.label(), status_style(row.status)),
            Span::raw(format!(
                "  {} tokens  {}  lane: {lane}",
                row.tokens,
                dollars(row.cost_micro_usd),
            )),
        ]),
        Line::from(Span::raw(format!("now: {}", short(&row.action, "idle")))),
    ]);
    frame.render_widget(summary, panes[0]);

    let activity = cockpit.detail_activity();
    let capacity = usize::from(panes[1].height.saturating_sub(2)).max(1);
    let start = activity.len().saturating_sub(capacity);
    let items = activity[start..]
        .iter()
        .map(|line| ListItem::new(Span::raw((*line).to_owned())));
    frame.render_widget(
        List::new(items.collect::<Vec<_>>())
            .block(Block::default().borders(Borders::ALL).title("activity")),
        panes[1],
    );
}

/// The footer line of key hints.
fn footer(hints: &str) -> Paragraph<'static> {
    Paragraph::new(Line::from(Span::styled(
        hints.to_owned(),
        Style::default().fg(Color::DarkGray),
    )))
}

/// A role's label, marking a paused role so its brake is visible.
fn role_label(row: &RoleRow) -> String {
    if row.paused {
        format!("{} (paused)", row.role)
    } else {
        row.role.to_string()
    }
}

/// The color for a status, so working, idle, stopped, and dead read at a
/// glance.
fn status_style(status: Status) -> Style {
    let color = match status {
        Status::Working => Color::Green,
        Status::Idle => Color::Yellow,
        Status::Stopped => Color::Gray,
        Status::Dead => Color::Red,
    };
    Style::default().fg(color)
}

/// Renders micro-USD as whole dollars and cents (`$1.23`).
fn dollars(cost_micro_usd: u64) -> String {
    let cents = cost_micro_usd / 10_000;
    format!("${}.{:02}", cents / 100, cents % 100)
}

/// `text` if non-empty, else `fallback`.
fn short(text: &str, fallback: &str) -> String {
    if text.is_empty() {
        fallback.to_owned()
    } else {
        text.to_owned()
    }
}

/// Truncates a cell to `max` characters with an ellipsis, so a long action does
/// not blow out the column.
fn elide_cell(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    let mut out: String = text.chars().take(max.saturating_sub(1)).collect();
    out.push('\u{2026}');
    out
}

#[cfg(test)]
mod tests {
    use ratatui::{backend::TestBackend, buffer::Buffer, Terminal};

    use super::render;
    use crate::top::cockpit::{Cockpit, StatsSeed};

    /// The whole terminal buffer flattened to a searchable string.
    fn buffer_text(buffer: &Buffer) -> String {
        let mut out = String::new();
        for y in 0..buffer.area.height {
            for x in 0..buffer.area.width {
                out.push_str(buffer[(x, y)].symbol());
            }
            out.push('\n');
        }
        out
    }

    /// Renders `cockpit` to an 120x30 test terminal and returns the screen
    /// text.
    fn screen(cockpit: &Cockpit) -> String {
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
        terminal.draw(|frame| render(frame, cockpit)).unwrap();
        buffer_text(terminal.backend().buffer())
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
        cockpit.seed_stats(
            serde_json::from_value::<StatsSeed>(serde_json::json!({
                "roles": [ { "role": "backend", "tokens": 1500, "cost_micro_usd": 45000 } ]
            }))
            .unwrap(),
        );
        cockpit
    }

    #[test]
    fn the_overview_shows_the_header_roles_and_feed() {
        use crew_substrate::core::{
            ChannelId, Event, EventKind, Message, MessageId, MessageKind, RoleId, Sender, Timestamp,
        };

        let mut cockpit = seeded();
        cockpit.apply(&Event {
            ts: Timestamp::now(),
            from: Sender::Role(RoleId::new("commander")),
            channel: ChannelId::new("@backend"),
            task: None,
            kind: EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Note,
                body: "start on the login endpoint".to_owned(),
            }),
        });

        let text = screen(&cockpit);
        // The header shows the live count (working + idle) and the aggregate spend.
        assert!(
            text.contains("2 live / 2 roles"),
            "header shows live/total: {text}"
        );
        assert!(text.contains("running"), "header shows the standing");
        assert!(text.contains("$0.04"), "header shows the aggregate cost");
        // The roles table lists each role with its status.
        assert!(text.contains("commander"), "the commander is listed");
        assert!(text.contains("backend"), "backend is listed");
        assert!(
            text.contains("working") && text.contains("idle"),
            "statuses render"
        );
        assert!(text.contains("1500"), "backend's tokens render");
        // The feed shows the message.
        assert!(
            text.contains("start on the login endpoint"),
            "the message flow renders: {text}",
        );
        // The footer shows the key hints.
        assert!(
            text.contains("drill-in") && text.contains("quit"),
            "the key hints render"
        );
    }

    #[test]
    fn drilling_in_shows_a_role_activity_detail() {
        use crew_substrate::core::{
            Activity, ChannelId, Event, EventKind, RoleId, Sender, Timestamp,
        };

        let mut cockpit = seeded();
        let activity = |a| Event {
            ts: Timestamp::now(),
            from: Sender::Role(RoleId::new("backend")),
            channel: ChannelId::new("@backend"),
            task: None,
            kind: EventKind::Activity(a),
        };
        cockpit.apply(&activity(Activity::ToolCall {
            tool: "Read".to_owned(),
        }));
        cockpit.apply(&activity(Activity::Output {
            text: "editing the router".to_owned(),
        }));

        // Select backend and drill in.
        while cockpit.selected_role().map(|r| r.role.clone()) != Some(RoleId::new("backend")) {
            cockpit.select_next();
        }
        cockpit.toggle_detail();

        let text = screen(&cockpit);
        assert!(
            text.contains("activity"),
            "the detail pane is titled activity"
        );
        assert!(text.contains("tool: Read"), "the role's tool call is shown");
        assert!(
            text.contains("editing the router"),
            "the role's output is shown"
        );
        assert!(
            text.contains("lane: api/"),
            "the detail shows the role's lane"
        );
        assert!(
            text.contains("back"),
            "the detail footer shows how to go back"
        );
    }

    #[test]
    fn an_empty_crew_renders_without_panicking() {
        // A cockpit with no roles or feed still draws its chrome.
        let text = screen(&Cockpit::default());
        assert!(
            text.contains("crew top"),
            "the header renders for an empty crew"
        );
        assert!(text.contains("0 live / 0 roles"));
    }
}
