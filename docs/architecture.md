# Architecture

The design of record for crew's substrate. Update this file when a decision here
changes.

## The one idea: substrate plus front-ends

Two concerns separate cleanly:

- **Substrate:** a message **broker** and a process **supervisor**. It spawns
  role-scoped agents, routes typed messages between them, and exposes an
  agent-facing tool surface. It has no opinion about who drives it.
- **Front-end:** how a human drives the substrate. A terminal CLI first, a
  Seraphim panel later.

Build the substrate once as a Rust library. Give it two front-ends. Neither
front-end forks the coordination logic.

crew supervises Claude Code / Codex. It is **not** a new agent runtime. The
runtime already exists; crew is the conductor around N of them.

## Components

### Broker (`crewd`)

A localhost HTTP + SSE service. It owns the message log, the roster, and delivery.

Planned surface (illustrative, not final):

- `POST /channels/:channel/messages` posts a typed message (see
  `communication.md` for the schema).
- `GET /inbox?role=backend` streams messages for a role over SSE, **filtered so
  a role never receives its own messages** (self-echo is impossible by
  construction, not by convention).
- `GET /history?channel=all-hands&summary=true` returns a compact rolling
  summary rather than the full transcript, so a late joiner spends little
  context catching up.
- `GET /roster` lists roles, their liveness, and their owned paths.

Why a broker beats the old shared file:

- **Routing.** The file broadcast to everyone; the broker delivers to the
  addressee. Direct messages stay point-to-point.
- **No self-echo.** The file echoed your own writes back through the tail; the
  broker filters them at the source.
- **Bounded context.** The file grew without limit; the broker summarizes.
- **Typed messages.** The file was freeform prose; the broker carries structured
  intents (handoff, question, status, artifact-ref) that a front-end can render.

### Supervisor

Spawns one agent process per role, each with its role card, wired to the broker,
and manages its lifecycle: lazy start on first work, idle-stop to save context
and money, restart on death. The supervisor is what makes `crew up` bring an
entire team online at once instead of the human opening terminals by hand.

The supervisor targets Claude Code by default and Codex through the same
interface via a CLI shim.

### MCP server (agent-facing surface)

Agents coordinate through MCP tools, not by shelling out to append to a file:

- `crew_send` sends a message to a channel or a role.
- `crew_inbox` reads new messages (or the agent subscribes via a broker stream
  and gets native notifications).
- `crew_roster` lists teammates and their lanes.

MCP is the clean path. A thin CLI shim (`crew send`, `crew inbox`) is the
fallback for a runtime without MCP.

### CLI (`crew`, human front-end)

- `crew up` brings a crew online from a config (roles, owned paths, model).
- `crew send` posts a message as the human, to the coxswain by default.
- `crew watch` tails the conversation with routing visible.
- `crew down` stops the crew.

## Stack and conventions

- **Language:** Rust. Follows `~/.claude/docs/rust.md`.
- **Runtime/web:** tokio + axum, SSE for delivery.
- **Errors:** eyre at the application level (`M-APP-ERROR`).
- **Allocator:** mimalloc as the global allocator (`M-MIMALLOC-APPS`).
- **Toolchain:** pinned via `rust-toolchain.toml`; always `cargo +<pinned>`.
- **Lints:** the clippy set from `~/.claude/docs/rust.md`; structured logging
  with `tracing` and named events.
- **Crates:** err on the side of more, smaller crates (`M-SMALLER-CRATES`). Split
  the broker, supervisor, CLI, and MCP surface so each builds and tests alone.
- **Strong types:** newtype the identifiers (role, channel, message id) rather
  than passing bare strings.

## Persistence

Open question. The standalone broker can run in memory with an on-disk log, or
back onto SQLite for durable history and summaries. Seraphim brings Postgres, so
the Seraphim front-end persists there. Keep the broker storage behind a trait so
the backend is swappable.

## What this deliberately is not

- Not a new agent runtime.
- Not a rewrite of the board-driven autonomous flow (that is Seraphim's job).
- Not a free-for-all broadcast bus (see `communication.md` for the topology).
