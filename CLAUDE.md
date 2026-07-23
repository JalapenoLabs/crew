> **Source-of-truth policy.** The USER is the primary source of truth. This
> `CLAUDE.md` is the SECOND source of truth. The codebase is **not** a source of
> truth (it can drift). Whenever a design decision is made or changed, update
> this file in the same change. Keep it routinely up to date.
>
> **Style:** No em dashes anywhere in user-facing text (docs, UI, commits, PRs).
>
> **History policy:** document the current design and the roadmap only. Do not
> record what we "used to do" or "migrated from."

# crew

> Project memory for agents working **on** crew itself. Read this first.

## What it is

crew turns a set of separate Claude Code (or Codex) instances into a **team you
command like a team**. You supply the strategic intent, a commander turns it into
orders for role-scoped specialists, and each specialist keeps its own long-lived
context and owns its lane of the codebase. The point is to feel like a general
directing a unit, not a person hand-feeding prompts to three terminals.

It grows out of the `coworker` skill (a shared markdown file plus a `tail -F`
monitor). crew keeps the idea, working agents that talk to each other, and
replaces the crude parts: the file transport, the manual three-terminal setup,
and the unbounded context growth.

## The core insight

Two concerns separate cleanly, and separating them is the whole design:

1. **The substrate** (a message broker plus a process supervisor). One small
   Rust program that spawns role-scoped agents, routes typed messages between
   them, and lets a human address any one or all of them.
2. **The front-end** (how a human drives it). A terminal CLI for ad-hoc work;
   later, a Seraphim panel for a visual, persistent experience.

Build the substrate once as a library. Give it two front-ends. That is how crew
serves both the Claude Code CLI workflow and Seraphim without forking logic.

**crew is not a new agent runtime.** Claude Code already is the runtime. crew is
the command layer around N Claude Code processes: a supervisor and a message bus,
not a reimplementation of the agent.

## Direction (agreed)

- **Name:** `crew`. **You are the General:** you supply the intent. The lead
  role is the **commander**, the rank below you that issues the orders and
  reports back. The commander does not take the field (does not write code).
- **Substrate:** one Rust crate = a localhost message **broker** + a process
  **supervisor**. Two front-ends consume it (terminal CLI now, Seraphim later).
- **Distribution:** the substrate ships as the `crew-substrate` umbrella crate,
  consumed under the **JalapenoLabs** org as a private Git dependency pinned to a
  release tag (issue #35, decided): no registry to run, private by the repo's own
  access control, and reversible. crates.io is deferred to 1.0. Publishing on a
  `v<version>` tag (issue #36): `release.yml` checks the tag matches the crate
  version, runs the full gate on the pinned toolchain, and cuts a GitHub Release; the
  public API is `crew-substrate`'s re-exports, versioned per semver (pre-1.0, so a
  minor bump may break), and each tag's release notes are its changelog (no
  `CHANGELOG.md`). See `docs/distribution.md` and `docs/architecture.md`.
- **Transport:** a localhost broker (axum + SSE), not a shared file. The broker
  routes messages, filters your own messages out of your stream (no self-echo),
  and serves a compact rolling summary so a late joiner does not read the full
  log. See `docs/architecture.md`, `docs/communication.md`.
- **Agent interface:** an **MCP server** exposing `crew_send`, `crew_inbox`,
  `crew_roster` and friends, so agents get real tools instead of appending to a
  file. A thin CLI shim is the fallback.
- **Topology:** hub-and-spoke by default (the General briefs the commander, who
  fans out orders), with peer direct messages allowed for tight loops and a
  deliberate, rare `all-units` broadcast. Not a free-for-all. See
  `docs/communication.md`.
- **Roles trace the tree.** A role owns a directory boundary that already exists
  (`api/`, `frontend/`, `.github/`). Start with a small crew, expand on demand.
  See `docs/roles.md`.
- **Observability is a first-class output.** The broker + supervisor emit one
  typed, timestamped, addressed event stream. Task history, per-agent and
  aggregate activity logs, and the live agent count are all projections of it,
  never separate capture pipelines. Seraphim renders crew communication into task
  history the way it renders `ci` / `lifecycle` / `screenshot` events today, and
  the live count can drive a Runewood visualization. See `docs/observability.md`.
- **Command and control (the general's console).** The General can interject and
  redirect a role mid-task (`crew redirect` / `crew belay`), gate risky actions
  (push, merge, delete, spend, external post) behind rules-of-engagement
  approval, pause and resume per role and crew-wide plus an emergency stand-down
  (`crew pause` / `crew resume` / `crew standdown`, issue #41), and override the
  commander to command a specialist directly.
- **Coordination robustness.** Parallel roles work in isolated git worktrees and
  integrate through a deliberate step; a work ledger with claims prevents collisions
  (`crew_claim` / `crew_ledger`, issue #45); lane ownership is enforced; nothing is done
  until an adversarial gate fails to break it; the defibrillator also catches coordination
  stalls, not just dead agents.
- **Team memory.** A shared decision board (agreed interfaces, decisions,
  gotchas) the crew reads and writes, distinct from the transient message stream;
  a new role boots from a briefing packet (role card + board + rolling summary),
  never the raw log.
- **Economy.** Model per role (strong for the commander and architect, cheap for
  mechanical roles), a shared token budget with per-role caps, auto-idle with cost
  telemetry, and subscription usage awareness.
- **Stack:** Rust (axum + tokio + eyre + mimalloc), toolchain pinned. Follows the
  global Rust conventions in `~/.claude/docs/rust.md`.
- **Not a new runtime:** crew supervises Claude Code / Codex, it does not replace
  them.

## Open questions

- Commander identity: does the General always talk through the commander, or can
  the General drop into any role's channel directly? (Current lean: both, the
  commander is default.)
- Codex parity: same MCP surface via a CLI shim, or a Codex-native path?
- Persistence: SQLite for the standalone broker vs in-memory with a log file.
  (Seraphim brings Postgres; the standalone need not.)

## Architecture (summary)

The full design is in `docs/architecture.md`. In short:

- **Broker** (`crewd`): a localhost HTTP + SSE service. Agents `POST` a message
  and subscribe to a self-filtered stream; a `history?summary=true` endpoint
  returns a compaction, not the raw transcript.
- **Supervisor:** spawns one agent process per role with its role card, wires
  each to the broker, and manages lifecycle (start, idle-stop, restart).
- **MCP server:** the agent-facing surface (`crew_send`, `crew_inbox`, ...).
- **CLI (`crew`):** the operator front-end (`crew up` / `crew down`, and the
  `crew pause` / `crew resume` / `crew standdown` brake and kill switch), the General's
  command-and-control directives (`crew redirect` / `crew belay` to steer a role
  mid-task), the agent CLI shim (`crew register` / `crew send` / `crew inbox` /
  `crew roster` / `crew claim` / `crew ledger`) for a runtime without MCP, and
  `crew watch` to tail a role's self-filtered inbox stream live.
- **Coworker skill (`skills/coworker/`):** the upgraded `coworker` skill (issue #37),
  a role-card bootstrap that sends with `crew send` and watches with `crew watch`, so
  existing users get the broker's routing, no self-echo, and bounded catch-up. This is
  where the transport upgrade lands: a standalone drop-in the skill points at, using
  the `crew` CLI, not a code dependency. See `docs/communication.md`.

## Roles (summary)

Full roster in `docs/roles.md`. Default crew: **commander** (lead/router),
**backend**, **frontend**, **qa**. On demand: **ci/release**, split
**sdet-unit** / **sdet-e2e**, **security**, **docs**. Prefer few busy agents over
many idle ones; every idle agent still wakes on traffic and spends context.

## Relationship to Seraphim

Seraphim already plans **Railways**: parallel agent lanes partitioned by repo,
running autonomously off a board. crew is the other axis: roles collaborating on
shared work with a human in the loop. Same primitive underneath (parallel lanes
plus a message bus). The broker crew builds is the coordination backbone
Railways can adopt so lanes can talk. Seraphim becomes crew's second front-end,
adding a UI and Postgres-backed persistence.

## Repo layout

```
Cargo.toml         workspace root: members, shared deps, the lint set
rust-toolchain.toml pinned toolchain (1.88, rustfmt + clippy)
CLAUDE.md          README.md          .gitignore
docs/
  architecture.md    the substrate: broker, supervisor, MCP surface, distribution
  communication.md   topology, channels, message schema, context management
  observability.md   one event stream: task history, activity logs, live count
  stream-contract.md the public stream contract an external viz (Runewood) consumes
  distribution.md    how the substrate is distributed (private Git dep) and consumed
  roles.md           the roster and the ownership model
  config.md          the declarative crew config `crew up` reads
  codex.md           the Codex adapter: the agent CLI shim and its MCP parity
  roadmap.md         phased plan
crates/
  crew-core          shared types + the event model (the dependency-graph root)
  crew-broker        the localhost broker service + the `crewd` binary
  crew-supervisor    process management: spawn/wire/lifecycle of role agents
  crew-mcp           the agent-facing MCP surface (crew_send, crew_inbox, ...)
  crew-substrate     the umbrella crate: re-exports the four above as one dependency
  crew-cli           the human front-end binary (`crew`)
  crew-telemetry     shared structured-logging (tracing) init + secret redaction
skills/
  coworker/          the upgraded `coworker` skill: a role-card bootstrap over the broker
```

Crate split follows `M-SMALLER-CRATES`: every crate builds and tests on its own,
the dependency direction flows toward `crew-core`, and nothing depends on
`crew-cli` (the CLI is a consumer only). `crew-telemetry` is a standalone
infrastructure crate the binaries call to initialize logging, so the library
crates never pull a subscriber. `crew-core` holds the domain types and event
model (issue #6). `crew-substrate` is the umbrella (issue #34): it re-exports the
four substrate crates (`crew-core`, `crew-broker`, `crew-supervisor`, `crew-mcp`) as
the modules `core` / `broker` / `supervisor` / `mcp`, so a front-end takes one
dependency and depends only on the substrate's public API. External consumers (the
CLI, later Seraphim) use `crew-substrate`; a substrate crate's own binary (`crewd`,
`crew-mcp`) uses its siblings directly, since routing through the umbrella would be a
dependency cycle. The umbrella carries the canonical crate docs
(`M-CANONICAL-DOCS`, `M-MODULE-DOCS`) that define the public API and note the
third-party types it deliberately leaks for interop (`M-DONT-LEAK-TYPES`, footnote 2:
an umbrella may leak siblings' types), so the substrate builds and documents as a
standalone, publishable crate. See `docs/architecture.md` (Distribution).

## Status

Design of record plus the workspace scaffold. The crates build/test green.
`crew-telemetry` carries the shared logging init and the `crew` binary boots with
structured logging (issue #4); `crew-core` carries the shared, strongly-typed
vocabulary (issue #6): the identifier newtypes (`RoleId`, `ChannelId`,
`MessageId`, `TaskId`), the `Timestamp` wrapper, the `Sender`, and the `Event` /
`EventKind` (`Message` with a `MessageKind`, `Lifecycle`, `Activity`) stream
model, all serde round-tripping, plus the `Channel` model (issue #11) that parses
the three channel names (`all-units`, direct `@role`, `a+b` pair), canonicalizes a
pair regardless of member order, and resolves which roles a channel reaches; and
`crewd` (the broker, issues #7, #8, #9, #10, #11, #12, #13 and #14)
starts on loopback, serves `GET /health`, and shuts down gracefully. It keeps state
behind a swappable `Storage` trait (append, query, roster read/write; issue #13): the
daemon uses the durable `LogStore` (an on-disk append-only JSONL log plus an in-memory
index, rooted at the state dir), so a restart replays the full log; tests use the
in-memory `MemoryStore`. It stores the
event model with typed per-kind message fields and typed 4xx on malformed input,
reads the log over `GET /events`, and accepts messages over `POST
/channels/{channel}/messages`: the channel comes from the path, the broker stamps
`ts` and `id` server-side (rejecting any client-supplied `ts`, `id`, or `channel`),
masks configured secret values out of the event, persists it, and fans it to every
subscriber. Subscribers read either `GET /stream`, the whole live feed, or
`GET /inbox?role=<role>`, a role's live events filtered to its direct, pair, and
`all-units` channels with its own messages dropped at the source and resumable from
a `Last-Event-ID` cursor without loss (issue #10). `GET /history` reads past events
filtered by `channel`, `role` (sent by), `agent` (a role's activity timeline), `kind`,
`task`, and `since`, ordered by `ts` then log
position, and paged with an opaque cursor (`after`/`next_cursor`) that stays stable
under concurrent writes (issue #12); `summary=true` returns the rolling-summary
compaction instead (issue #19): the older events folded into bounded aggregates
(counts by sender, message kind, and lifecycle state, plus a capped digest of recent
orders and artifacts and a one-line headline) plus the recent raw `tail` sized by
`limit`, so joining a long conversation costs bounded context, not the full log. The
**per-agent activity log** serves one role's full timeline (issue #30): its own events
(messages it sent, its lifecycle, its activity) plus the messages it received, defined
by `crew_core::Event::in_timeline_of`. `GET /history?agent=<role>` reads it as history
and `GET /activity?agent=<role>` streams it live over SSE (the inbox's replay-and-live
engine, shared via one per-event predicate); unlike the inbox the timeline is not
self-filtered, and unlike the sender-only `role` filter it also carries what the role
received. The
`/roster` endpoints expose who is in the unit (issue
#14): `GET /roster` lists roles with their owned paths and liveness (working / idle
/ stopped / dead), a role registers on join with `POST /roster` and leaves with
`DELETE /roster/{role}`, and every change publishes a `lifecycle` event to
`all-units`, so it rides history, `/stream`, and each inbox. `GET /roster` also
reports the **live agent count** (issue #32): a `count` with the headline `live`
number (roles `working` or `idle`, present and up or resumable) and the per-liveness
breakdown, so a UI reads it once and keeps it current from the lifecycle events on
the stream, showing the count update as agents start, idle, stop, and die with no
polling. Its `ChannelRouter`
resolves a channel to the roles it reaches (issue #11), filtering a supplied roster
through the `Channel` membership test. The live roster that feeds routing is still a
scaffold waiting for the phased build in `docs/roadmap.md`. Verify with `cargo build`
and `cargo test` at the root.

This whole surface is a stable public contract an external visualization (Runewood)
consumes with no crew-specific capture (issue #33): `docs/stream-contract.md` documents
the event envelope and every payload, `/stream` (live SSE, `id` = the log sequence that
bridges to the `/history` cursor), catch-up via `/history` and its `summary` compaction,
and the `/roster` snapshot with the live count, plus a minimal subscribe example and the
additive-only stability promise. An integration test
(`a_consumer_renders_the_unit_from_the_stream_alone`) proves a consumer renders agents,
messages, and the live count from the stream alone.

The event-stamping guarantee is enforced at the source, so observability is never a
retrofit (issue #29). `ts` / `from` / `channel` / `kind` are mandatory in
`crew_core::Event`, and every event, whatever its kind, enters the store and stream
through the one `AppState::publish` choke point, which asserts `Event::is_well_formed`
(a present, non-blank channel and role sender); the HTTP handlers validate untrusted
input first, so no event reaches the store or stream missing a required field. Task
correlation rides the same envelope: a message carries the `task` its sender threads
(the broker preserves it), and a lifecycle event carries the task the supervisor is
working via `RosterClient::with_task` (a role fully leaving the unit is not
task-scoped). Activity events thread the task the same way once the stream-json parser
lands. See `docs/observability.md`.

`crew-mcp` carries the agent-facing MCP surface (issue #17): the `crew-mcp` binary
speaks JSON-RPC 2.0 over newline-delimited stdio (protocol `2024-11-05`,
`initialize` / `tools/list` / `tools/call`), which the supervisor spawns one of per
agent. It boots from a role card (`CREW_ROLE_CARD`, issue #18), registers the role on
the roster at boot, and is a thin synchronous client (`ureq`) over the broker's HTTP
API; it never touches the store. It exposes four
tools with self-documenting schemas: `crew_send` (post as the role to a channel or a
teammate, defaulting to the commander), `crew_order` (issue an order, a scoped task
with a title, scope, owned paths, and acceptance, to one specialist; the commander's
fan-out handle, issue #27), `crew_inbox` (read the messages addressed to the role
since the last call, self-filtered, over a per-session history cursor, surfacing an
order's structured fields), and `crew_roster` (list registered teammates, their owned
paths, and liveness). A tool failure returns as an `isError` result, not a protocol
error. The roadmap step is `crew_inbox` push over the per-role SSE stream instead of
the current history read.

`crew-core` also carries the role card (issue #18): `RoleCard` is the thin bootstrap
an agent boots from, a TOML document naming the role, its owned lane, its acceptance
bar, the crew's `commander`, and the broker address (`BrokerEndpoint`), with
`from_toml` / `to_toml` the loader and `briefing()` rendering the boot prompt (the
shape the `coworker` skill shrinks to). One loader serves both paths:
`crew_supervisor::provision` writes a role's card and returns the `CREW_ROLE_CARD`
environment plus its briefing, and the `crew-mcp` binary reads the same card to boot
and register. See `docs/roles.md`.

The card names the crew's commander so the hub-and-spoke topology is live (issue #27).
`RoleCard::is_commander` tells a role whether it leads, and `briefing()` renders
accordingly: the commander's card states its duties (decompose the brief, issue orders
with `crew_order`, arbitrate, report to the General), and a specialist's names its
commander and the default addressing. The one addressing rule, `Channel::resolve`
(a named role wins, else a named channel, else the commander), is shared by the
General's front-end and an agent's `crew_send`, so an unaddressed brief reaches the
commander everywhere. `to_cards` stamps every card with the config's commander. See
`docs/communication.md`.

`crew-core` also carries the crew config (issue #25): `CrewConfig` is the declarative
description `crew up` reads, a TOML document naming the roles and the lane each owns,
the model (a crew default with per-role overrides), the repos in scope, the idle-stop
timeout, and the commander. `from_toml` resolves the defaults (an omitted `roles`
yields the default crew: commander, backend, frontend, qa) and validates the whole,
so a documented config produces a valid crew and an invalid one fails with a precise
`ConfigError` (an unknown commander, an overlapping ownership boundary, a duplicate or
empty role, or a typo'd field). `to_cards(&broker)` produces the per-role `RoleCard`s
the supervisor spawns, and `model_for(role)` resolves the spawn model. See
`docs/config.md`.

`crew-supervisor` also auto-registers the crew MCP server so a spawned agent gets the
crew tools with no per-task approval (issue #20), the way Seraphim registers the
Playwright MCP. `locate_server` finds the `crew-mcp` binary (a build/boot check that
fails loudly if it is missing) and `register_server` records it at user scope
(`claude mcp add -s user crew -- <path>`, idempotent via remove-then-add), so a
`claude -p --permission-mode bypassPermissions` turn (`agent_turn_argv`) loads it
silently. Registration is one-time and unit-wide: per-agent role and broker ride the
`CREW_ROLE_CARD` environment the `crew-mcp` child inherits.

`crew_supervisor::Supervisor::up` ties these together into the auto-spawn flow (issue
#21): register the MCP server, then per resolved role card provision the card,
register the role on the broker roster, and spawn one `claude -p` process wired to the
broker. The supervisor owns lifecycle, so it registers a role on start and
deregisters it on exit (via `RosterClient`), keeping `GET /roster` a true picture of
the live unit; each process's stdout and stderr are captured and streamed as
`Captured` lines for the activity parser (issue #24). `Crew::spawn` holds the
lifecycle mechanics and takes fully-resolved `AgentCommand`s, so it is exercised in
tests with a stub process instead of a real `claude`. Shutting the crew down kills the
processes and deregisters every role; a dropped crew still kills its processes.
The roster of roles comes from the crew config (issue #25), which `up` consumes as
role cards.

`crew_supervisor::Fleet` manages each agent's lifecycle so idle roles cost nothing and
crashes recover (issue #22). Each agent runs a state machine on its own driver thread:
**lazy start** (the fleet launches with every agent stopped and no process; the first
work via `Fleet::start` spawns and registers it), **idle-stop** (after a configurable
quiet period the driver stops the process but keeps the roster entry, marked idle, so
a restart is fast and keeps context), and **restart on demand** (`Fleet::start` on a
stopped or idle agent restarts it).

The **defibrillator** (issue #23) recovers an agent whose turn died mid-flight,
mirroring Seraphim's, with layered detection: each driver's in-turn **heartbeat**
reaps a turn that crashed (its process exited) or hung (alive but silent past
`heartbeat_timeout`), and a fleet-wide **watchdog** backs the drivers up for a working
agent silent past the longer `watchdog_timeout`, which only a wedged driver lets
through. On a death it records an `Incident` with the diagnostic detail (readable via
`Fleet::incidents`), marks the role dead, and revives it within a `max_recoveries`
budget, handing it to the operator once spent. Every transition marks the broker
roster (via `RosterClient::mark` / `register` / `deregister`), so the roster and the
stream carry the matching `lifecycle` event (started / idle / stopped / restarted /
died / recovered): the broker derives `recovered` from a `dead` role coming back to
`working` (issue #23 adds the `Recovered` variant to `crew_core::Lifecycle`). The
`LifecyclePolicy` defaults to a five-minute idle-stop, a twenty-minute heartbeat under
a twenty-five-minute watchdog, and three recoveries. Precise hang-versus-idle
discrimination awaits the activity parser's turn boundaries (issue #24), so by default
a quiet agent parks rather than being force-recovered. (Unifying the eager `Crew` from
#21 into the lifecycle-managed `Fleet` is a later cleanup.)

`crew-cli` carries the headline `crew up` / `crew down` orchestration (issue #26). The
`crew` binary is a `clap` subcommand tree. `crew up` reads the crew config
(`--config`, else `./crew.toml`, else the default crew), resolves the broker address,
and starts the broker in-process only if none is already listening (via a `GET /health`
probe), so an operator can instead run a long-lived `crewd` and bring crews up against
it. It then launches a lifecycle-managed `Fleet` from the config
(`Supervisor::launch`, which registers the MCP server, provisions a card per role, and
runs each role's configured model by appending `--model`) and starts every role
(`Fleet::start_all`), so the unit is live and connected: each role registers on the
roster, and idle roles idle-stop on the config's timeout while keeping their roster
entry. It surfaces the live roster and the commander entry point, then holds the unit
online in the foreground until Ctrl-C or `SIGTERM`, when it stands the crew down
gracefully (`Fleet::shutdown` stops and deregisters every agent, then the in-process
broker drains) and removes its pidfile. `crew down` signals that process (`SIGTERM` via
the pidfile the two share under the broker state dir), so `crew down` and Ctrl-C take
the one graceful-shutdown path and neither leaves an orphaned process. The broker
exposes `run_until(config, shutdown)` (the setup behind `run`) so `crew up` drives the
in-process broker's shutdown itself.

`crew-cli` also carries the agent CLI shim (issue #28): `crew register`, `crew send`,
`crew inbox`, `crew roster`, `crew claim`, and `crew ledger` let an agent on a runtime
without MCP, such as Codex, coordinate through subcommands instead of tools. Each boots from the same role context
the `crew-mcp` binary reads (`CREW_ROLE_CARD`, else `CREW_ROLE` plus the `CREW_BROKER_*`
config) and reuses the same `crew_mcp::Broker` client, so a shim agent's I/O maps onto
the broker identically to the MCP path: it registers on boot (appearing on the roster
and stream) and sends and reads the same way. The shim is stateless (a short-lived
process per call), so `crew inbox` reports every message currently addressed to the
role, not only those since a previous call; that and the other parity gaps (no push,
operator-launched rather than supervisor-spawned) are in `docs/codex.md`. The General's
own `crew send` / `crew watch` front-end follows.

**Running `crewd`:** `cargo run --bin crewd`. It binds `127.0.0.1:2739` by
default. Configure via env: `CREW_BROKER_HOST`, `CREW_BROKER_PORT`,
`CREW_BROKER_STATE_DIR` (default `.crew`, where the durable log `events.jsonl` and
`roster.json` live), `CREW_BROKER_ALLOW_NON_LOCAL` (`1`/`true`/`yes`), and
`CREW_BROKER_SECRETS` (a whitespace-separated list of secret values the broker masks
out of every message before storing or streaming it).
Binding a non-loopback address is refused unless the non-local flag is set, so the
broker never exposes itself to the network by accident.

**Running the crew:** `cargo run --bin crew -- up` brings the unit online (add
`--config <path>` to point at a crew config; it defaults to `./crew.toml`, then the
default crew). It runs in the foreground, holding the unit online until Ctrl-C. From
another terminal, `cargo run --bin crew -- down` stands it down. The broker address and
state dir come from the same `CREW_BROKER_*` env as `crewd`, so `crew up` reuses an
already-running `crewd` or starts its own.

## Local conventions

- **No em dashes** in any user-facing text.
- **Rust toolchain pinned** to `1.88` in `rust-toolchain.toml` (with `rustfmt` +
  `clippy`), so a fresh clone, CI, and any container all build with the same
  compiler. rustup selects it automatically inside the repo; pass `cargo
  +<pinned>` (e.g. `cargo +1.88 build`) when you need it explicitly from outside.
- **Formatting** is pinned in `rustfmt.toml` (stable options only). Run
  `cargo fmt` to apply it and `cargo fmt --check` to verify.
- **Application conventions** (issue #4): **eyre** is the single application error
  type (M-APP-ERROR); **mimalloc** is the global allocator in every binary
  (M-MIMALLOC-APPS); logging is **tracing** with structured, named events
  `<component>.<operation>.<state>` and `{{property}}` message templates
  (M-LOG-STRUCTURED). Binaries call `crew_telemetry::init()` once at startup, and
  any secret in a field goes through `crew_telemetry::redact::secret` first.
- **Lints** (compiler + clippy, with selected `restriction` lints) live in
  `[workspace.lints]` and every crate inherits them. Override a lint locally with
  `#[expect(..., reason = "...")]`, never `#[allow]` (M-LINT-OVERRIDE-EXPECT).
- **CI gate** (`.github/workflows/ci.yml`): every pull request and every push to
  `main` and `develop` runs three independent jobs on the pinned toolchain, so
  each reports its own status and one failure never blocks the rest: `cargo fmt
  --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test`. Keep the
  tree clippy-clean at that level.
- Read the applicable `~/.claude/docs/*.md` before editing code in that language.
- **Git:** commit and push only when asked. Never add a co-author trailer. Never
  self-assign PR credit.
