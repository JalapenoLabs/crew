# crew

Talk to a team, not a terminal.

crew turns separate Claude Code (or Codex) instances into a coordinated
engineering team. You brief a lead; the lead fans work out to role-scoped
teammates. Each teammate keeps its own long-lived context and owns its lane of
the codebase (backend, frontend, tests, CI). You direct; the team builds.

## Why

Running parallel agents by hand is clunky: three terminals to open, a shared file
to pass notes through, and context that balloons until a fresh agent reads the
whole transcript and burns 100k tokens on hello. crew fixes the substrate:

- **Real routing.** A localhost broker delivers messages to the right agent.
  Direct messages go point-to-point; broadcasts are deliberate and rare.
- **No self-echo.** Your own messages never come back to you. The old
  "ignore your own writes" hack is gone.
- **Bounded context.** A late joiner gets a compact summary, not the full log.
- **Auto-spawned team.** One command brings up a whole crew with roles assigned,
  instead of wiring terminals together by hand.

## How it works

crew is a supervisor around N Claude Code processes, not a new agent runtime. A
small Rust program spawns one agent per role, wires each to a message broker, and
exposes an MCP surface (`crew_send`, `crew_inbox`, `crew_roster`) so agents
coordinate with real tools. A human drives it from a CLI now, and from Seraphim
later.

See [`docs/`](docs/) for the design: [architecture](docs/architecture.md),
[communication](docs/communication.md), [roles](docs/roles.md), and the
[roadmap](docs/roadmap.md).

## Layout

A Cargo workspace split into small crates (`M-SMALLER-CRATES`); the dependency
direction flows toward `crew-core`, and nothing depends on `crew-cli`.

```
crates/
  crew-core        shared types + the event model (the dependency-graph root)
  crew-broker      the localhost message broker service
  crew-supervisor  process management: spawn, wire, and lifecycle of role agents
  crew-mcp         the agent-facing MCP surface (crew_send, crew_inbox, ...)
  crew-cli         the human front-end binary (crew)
  crew-telemetry   shared structured-logging (tracing) init + secret redaction
```

Build and test the whole workspace from the root:

```sh
cargo build
cargo test
```

## Status

Design of record plus the workspace scaffold: the five crates above are in place
and build green, as empty homes for the phased build in
[`docs/roadmap.md`](docs/roadmap.md).
