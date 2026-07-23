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

An agent only has the crew tools if its Claude Code process loads the `crew-mcp`
server, and it must load with no per-task approval, the way Seraphim registers the
Playwright MCP. `crew_supervisor::register_server` does this once at startup (issue
#20): it locates the `crew-mcp` binary (a build/boot check that fails loudly if it
is missing) and registers it at **user** scope in the agent config
(`claude mcp add -s user crew -- <path>`, idempotent via remove-then-add). A
`claude -p --permission-mode bypassPermissions` turn then loads it silently; a
project-scoped `.mcp.json` would sit unapproved. Registration records only the
command, so it is a one-time, unit-wide step, not per-agent: each agent's broker
address, role, and lane ride the `CREW_ROLE_CARD` environment its `claude` process
is launched with, which the `crew-mcp` child inherits.

`crew_supervisor::Supervisor::up` ties this together into the auto-spawn flow (issue
#21): register the MCP server, then for each resolved role card provision the card,
register the role on the broker roster, and spawn a `claude -p` process wired to the
broker. The supervisor owns lifecycle, so it is the authority on liveness: it
registers a role on start and deregisters it on exit, so `GET /roster` always
reflects the live unit. Each process's stdout and stderr are captured and streamed as
`Captured` lines for the activity parser (issue #24). The set of roles comes from the
crew config (issue #25); the supervisor consumes resolved role cards, so the two
compose without either owning the other's job.

`crew_supervisor::Fleet` manages each agent's lifecycle so idle roles cost nothing
and crashes recover (issue #22). Each agent runs a small state machine on its own
driver thread:

- **Lazy start.** A fleet launches with every agent stopped and no process; the
  first work (`Fleet::start`) spawns the process and registers the role.
- **Idle-stop.** After a configurable quiet period (no output) the driver stops the
  process but keeps the roster entry, marked idle, so a restart is fast and keeps
  context. An idle role costs nothing.
- **Restart on demand.** A `Fleet::start` on a stopped or idle agent restarts it.

The **defibrillator** recovers an agent whose turn died mid-flight, mirroring
Seraphim's (issue #23). Detection is layered:

- **In-turn heartbeat.** Each driver polls its agent. It reaps a turn that **crashed**
  (its process exited) or **hung** (its process is alive but silent past the
  `heartbeat_timeout`), records an incident with the diagnostic detail, marks the role
  dead, and revives it while it has recovery budget; once the budget is spent it stays
  dead and is handed to the operator.
- **Background watchdog.** A single fleet-wide thread catches a working agent silent
  past the longer `watchdog_timeout`, which the in-turn heartbeat should have caught
  first; only a driver that has itself wedged lets it through, so the watchdog reaps
  the orphan and hands the role to the operator rather than trusting the driver to
  revive it.

Every transition marks the broker roster, so the roster and the stream carry the
matching `lifecycle` event (started / idle / stopped / restarted / died / recovered):
a death emits `died`, a revival `recovered` (the broker derives it from a `dead`
role coming back to `working`), and the live count and every activity view stay a
projection of that one stream. Recorded incidents (read with `Fleet::incidents`) give
the operator the diagnostic behind each death. The policy is configurable, defaulting
to a five-minute idle-stop, a twenty-minute heartbeat under a twenty-five-minute
watchdog, and three recoveries. Precise hang-versus-idle discrimination awaits the
activity parser's turn boundaries (issue #24); until then the heartbeat is a coarse
output-silence signal, so by default a quiet agent parks (idle-stop) rather than being
force-recovered.

The supervisor targets Claude Code by default and Codex through the same
interface via a CLI shim.

### MCP server (agent-facing surface)

Agents coordinate through MCP tools, not by shelling out to append to a file.
The `crew-mcp` crate ships the `crew-mcp` binary: a JSON-RPC 2.0 server over
newline-delimited stdio (protocol `2024-11-05`) that the supervisor spawns one of
per agent. It boots from a role card (`CREW_ROLE_CARD`, issue #18) that names the
role, its lane, and the broker, or falls back to `CREW_ROLE` plus the broker
config. It registers the role on the roster at boot and is a thin client over the
broker's HTTP API; it never touches the store. It exposes four tools (issues #17,
#27):

- `crew_send` sends a message as the role to a channel or a teammate. With
  neither `to` nor `channel` it reaches the commander; `to: "backend"` direct
  messages one role, `channel: "all-units"` reaches the unit, and a pair like
  `frontend+backend` reaches just those two. The target follows one shared rule,
  `crew_core::Channel::resolve` (issue #27).
- `crew_order` issues an order as the role to one specialist: a scoped task with a
  title, scope, owned paths, and acceptance bar (a `MessageKind::Order`). It is the
  commander's fan-out handle for turning the General's brief into work (issue #27).
- `crew_inbox` reads the messages addressed to the role since the last call (its
  direct `@role` channel, any pair it belongs to, and `all-units`), with its own
  messages filtered out. It tracks a per-session cursor over the broker's history,
  and surfaces an order's structured fields so a specialist reads the task.
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

The `crew` binary is the terminal front-end; its argument surface is a `clap`
subcommand tree.

- `crew up` is the headline: one command brings the whole unit online (issue #26). It
  reads the crew config (`crew_core::CrewConfig`, issue #25; `--config <path>`, else
  `./crew.toml`, else the default crew), resolves the broker address, and **starts the
  broker if one is not already listening** (in-process, so an operator can instead run
  a long-lived `crewd` and bring crews up against it). It then launches a
  lifecycle-managed `Fleet` from the config (`Supervisor::launch`) and starts every
  role, so the unit is live and connected: each role registers on the roster and runs
  the model the config assigns it. It surfaces the live roster and the commander entry
  point, then holds the unit online in the foreground until a shutdown signal. Idle
  roles cost nothing: the fleet idle-stops them on the config's timeout, keeping their
  roster entry, so the unit stays visible while quiet roles park.
- `crew down` stands the running crew down gracefully (issue #26): it signals the
  `crew up` process (`SIGTERM` via the pidfile the two rendezvous on under the broker
  state dir), which stops every agent, deregisters it, and drains the broker it
  started. The graceful shutdown itself lives in `crew up` (a single source of truth
  for how a unit stands down), so `crew down` and Ctrl-C take the same path and neither
  leaves an orphaned process. `crew up` owns the crew: it holds the fleet's driver
  threads and, when it started one, the in-process broker, so standing down tears both
  down together.
- `crew send` posts a message as the General, to the commander by default.
- `crew watch` tails the conversation with routing visible.

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
