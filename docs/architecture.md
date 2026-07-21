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
runtime already exists; crew is the command layer around N of them.

## Components

### Broker (`crewd`)

A localhost HTTP + SSE service. It owns the message log, the roster, and delivery.

Planned surface (illustrative, not final):

- `POST /channels/:channel/messages` posts a typed message (see
  `communication.md` for the schema).
- `GET /inbox?role=backend` streams messages for a role over SSE, **filtered so
  a role never receives its own messages** (self-echo is impossible by
  construction, not by convention).
- `GET /history?channel=all-units&summary=true` returns a compact rolling
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
  intents (order, question, status, artifact) that a front-end can render.

### Supervisor

Spawns one agent process per role, each with its role card, wired to the broker,
and manages its lifecycle: lazy start on first work, idle-stop to save context
and money, restart on death. The supervisor is what makes `crew up` bring an
entire unit online at once instead of the General opening terminals by hand.

It spawns each agent as a `claude -p --output-format stream-json` process and
parses that stream into per-agent activity events, the same way Seraphim parses
the agent stream today (see `observability.md`).

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
- `crew send` posts a message as the General, to the commander by default.
- `crew watch` tails the conversation with routing visible.
- `crew down` stands the crew down.

### Observability

Every message, lifecycle transition, and per-agent activity item is one typed,
timestamped, addressed event. The observability views (task history, per-agent
and aggregate activity logs, the live agent count, and a Runewood visualization)
are projections of that single stream, not separate capture pipelines. The broker
owns communication and roster/liveness; the supervisor owns per-agent activity
parsed from stream-json. See `observability.md`.

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

## Distribution

The substrate is the reusable part, so it ships as a Rust crate (the broker plus
supervisor library) that both front-ends depend on, published under the
**JalapenoLabs** org.

**Constraint:** GitHub Packages does not host cargo registries (it serves npm,
Maven, NuGet, RubyGems, and Docker, not Rust). So "publish under JalapenoLabs"
has three realistic shapes:

1. **Private Git dependency** (current lean). Consumers depend on the crate via
   `git = "ssh://git@github.com/JalapenoLabs/crew"` pinned to a tag. No registry
   to run, private by default, works today. The cost is no semantic-version
   resolution and no `cargo publish` ergonomics.
2. **crates.io.** A real `cargo publish` with proper versioning and discovery.
   The cost is the crate is world-visible; only viable once the API is stable and
   we are content to open it.
3. **Private cargo registry** (Kellnr, Cloudsmith, Shipyard, or self-hosted via
   the sparse registry protocol). True org-private packages with `cargo publish`
   and version resolution. The cost is a service to run or pay for.

The crate split (`M-SMALLER-CRATES`) keeps this clean: publish the substrate
crate, keep the CLI and any Seraphim glue as consumers. Which registry to use is
tracked in `roadmap.md`.

## What this deliberately is not

- Not a new agent runtime.
- Not a rewrite of the board-driven autonomous flow (that is Seraphim's job).
- Not a free-for-all broadcast bus (see `communication.md` for the topology).
