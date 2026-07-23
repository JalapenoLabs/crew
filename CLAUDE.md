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
- **Distribution:** the substrate ships as a Rust crate consumed by both
  front-ends, published under the **JalapenoLabs** org. GitHub Packages has no
  cargo registry, so the path is a private Git dependency (current lean),
  crates.io (public), or a private cargo registry. See `docs/roadmap.md`.
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
  approval, pause or stand down per role and globally, and override the commander
  to command a specialist directly.
- **Coordination robustness.** Parallel roles work in isolated git worktrees and
  integrate through a deliberate step; a commander-maintained work ledger with
  claims prevents collisions; lane ownership is enforced; nothing is done until an
  adversarial gate fails to break it; the defibrillator also catches coordination
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
- Distribution registry: private Git dependency vs a private cargo registry (see
  `docs/roadmap.md` for the tradeoff).
- Where the coworker-skill transport upgrade lands: a `crew` dependency, or a
  standalone drop-in the skill points at. Tracked as a separate effort.

## Architecture (summary)

The full design is in `docs/architecture.md`. In short:

- **Broker** (`crewd`): a localhost HTTP + SSE service. Agents `POST` a message
  and subscribe to a self-filtered stream; a `history?summary=true` endpoint
  returns a compaction, not the raw transcript.
- **Supervisor:** spawns one agent process per role with its role card, wires
  each to the broker, and manages lifecycle (start, idle-stop, restart).
- **MCP server:** the agent-facing surface (`crew_send`, `crew_inbox`, ...).
- **CLI (`crew`):** the human front-end (`crew up`, `crew send`, `crew watch`).

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
  roles.md           the roster and the ownership model
  roadmap.md         phased plan
crates/
  crew-core          shared types + the event model (the dependency-graph root)
  crew-broker        the localhost broker service + the `crewd` binary
  crew-supervisor    process management: spawn/wire/lifecycle of role agents
  crew-mcp           the agent-facing MCP surface (crew_send, crew_inbox, ...)
  crew-cli           the human front-end binary (`crew`)
  crew-telemetry     shared structured-logging (tracing) init + secret redaction
```

Crate split follows `M-SMALLER-CRATES`: every crate builds and tests on its own,
the dependency direction flows toward `crew-core`, and nothing depends on
`crew-cli` (the CLI is a consumer only). `crew-telemetry` is a standalone
infrastructure crate the binaries call to initialize logging, so the library
crates never pull a subscriber. `crew-core` holds the domain types and event
model (issue #6); the other substrate crates are scaffolds, each filled in by its
phase in `docs/roadmap.md`.

## Status

Design of record plus the workspace scaffold. The crates build/test green.
`crew-telemetry` carries the shared logging init and the `crew` binary boots with
structured logging (issue #4); `crew-core` carries the shared, strongly-typed
vocabulary (issue #6): the identifier newtypes (`RoleId`, `ChannelId`,
`MessageId`, `TaskId`), the `Timestamp` wrapper, the `Sender`, and the `Event` /
`EventKind` (`Message` with a `MessageKind`, `Lifecycle`, `Activity`) stream
model, all serde round-tripping; and `crewd` (the broker, issues #7, #8, #9 and #10)
starts on loopback, serves `GET /health`, and shuts down gracefully. It stores the
event model with typed per-kind message fields and typed 4xx on malformed input,
reads the log over `GET /events`, and accepts messages over `POST
/channels/{channel}/messages`: the channel comes from the path, the broker stamps
`ts` and `id` server-side (rejecting any client-supplied `ts`, `id`, or `channel`),
masks configured secret values out of the event, persists it, and fans it to every
subscriber. Subscribers read either `GET /stream`, the whole live feed, or
`GET /inbox?role=<role>`, a role's live events filtered to its direct, pair, and
`all-units` channels with its own messages dropped at the source and resumable from
a `Last-Event-ID` cursor without loss. The canonical channel model and membership
(issue #11), the roster, and the rolling-summary history are still scaffolds waiting
for the phased build in `docs/roadmap.md`. Verify with `cargo build` and `cargo
test` at the root.

**Running `crewd`:** `cargo run --bin crewd`. It binds `127.0.0.1:2739` by
default. Configure via env: `CREW_BROKER_HOST`, `CREW_BROKER_PORT`,
`CREW_BROKER_STATE_DIR` (default `.crew`), `CREW_BROKER_ALLOW_NON_LOCAL`
(`1`/`true`/`yes`), and `CREW_BROKER_SECRETS` (a whitespace-separated list of secret
values the broker masks out of every message before storing or streaming it).
Binding a non-loopback address is refused unless the non-local flag is set, so the
broker never exposes itself to the network by accident.

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
