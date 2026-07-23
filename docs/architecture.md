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

Each agent boots from a **role card** (issue #18): a thin TOML document naming the
role, its owned lane, its acceptance bar, and the broker address (see
`docs/roles.md`). `crew_supervisor::provision` writes a card per role and returns
the `CREW_ROLE_CARD` environment plus the briefing the agent starts from. The
standalone flow reads the very same card, so one loader (`crew_core::RoleCard`)
serves both.

The supervisor targets Claude Code by default and Codex through the same
interface via a CLI shim.

### MCP server (agent-facing surface)

Agents coordinate through MCP tools, not by shelling out to append to a file.
The `crew-mcp` crate ships the `crew-mcp` binary: a JSON-RPC 2.0 server over
newline-delimited stdio (protocol `2024-11-05`) that the supervisor spawns one of
per agent. It boots from a role card (`CREW_ROLE_CARD`, issue #18) that names the
role, its lane, and the broker, or falls back to `CREW_ROLE` plus the broker
config. It registers the role on the roster at boot and is a thin client over the
broker's HTTP API; it never touches the store. It exposes three tools (issue #17):

- `crew_send` sends a message as the role to a channel or a teammate. With
  neither `to` nor `channel` it reaches the commander; `to: "backend"` direct
  messages one role, `channel: "all-units"` reaches the unit, and a pair like
  `frontend+backend` reaches just those two.
- `crew_inbox` reads the messages addressed to the role since the last call (its
  direct `@role` channel, any pair it belongs to, and `all-units`), with its own
  messages filtered out. It tracks a per-session cursor over the broker's history.
- `crew_roster` lists every registered teammate, the paths it owns, and its
  liveness (working / idle / stopped / dead).

Each tool documents itself and its arguments in `tools/list` so an agent calls it
right the first time. A tool failure comes back as an `isError` result the agent
reads, not a protocol error.

MCP is the clean path. A thin CLI shim (`crew send`, `crew inbox`) is the
fallback for a runtime without MCP.

The roadmap step is `crew_inbox` push: subscribing to the broker's per-role SSE
stream for native notifications instead of the current history read on each call.

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

The broker keeps its state behind a `Storage` trait, so the backend is swappable
and no handler names a concrete store (issue #13). The trait covers the whole
persisted surface: append an event, query the log with filters and a stable page
cursor, read every event, and read or write the roster. `query` has a default that
scans the in-memory index, so a backend with a real index (a database) overrides it
to push the filter down; the query types stay backend-neutral so no backend type
leaks.

Two backends ship. `MemoryStore` holds everything in memory, for tests and
ephemeral runs. `LogStore` is the durable default the `crewd` daemon uses: an
on-disk append-only log (one JSON-encoded event per line) plus an in-memory index,
rooted at the state directory (`events.jsonl` and `roster.json`). On start it
replays the log into memory, so a restart restores the full history; a torn or
unreadable line (a crash mid-append) is skipped so one bad line never loses the
rest. `SQLite` and Postgres remain future backends behind the same trait; Seraphim
persists to its own Postgres when it is the front-end.

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
