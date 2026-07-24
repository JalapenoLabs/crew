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
  commander to command a specialist directly (`crew command <role>`, issue #42: the
  General orders a role itself, bypassing the commander, and the commander is informed
  rather than bypassed silently), and reassign an in-flight task from one role to another
  against the work ledger, informing both roles and the commander (`crew reassign`, issue
  #42's second half, done).
- **Coordination robustness.** Parallel roles work in isolated git worktrees
  (`worktrees` in the crew config, issue #43) and integrate through a deliberate step
  (`crew integrate`, issue #44: merge each role's `crew/<role>` branch into one branch,
  surface conflicts rather than drop them, and run the acceptance checks on the merged
  result, so the integrated whole is green before it ships);
  a commander-maintained work ledger with claims prevents collisions (`crew_claim` /
  `crew_ledger`, issue #45); lane ownership is enforced (`lane_enforcement` policy plus
  the `crew_lane` tool, issue #46: a role checks a path against its owned lane before an
  out-of-lane edit, which is reported to the unit as a `boundary` event and, under a
  blocking policy, refused, so a cross-lane change routes through the commander instead of
  a silent edit); nothing is done until an adversarial done-gate fails to break it
  (`crew_submit` / `crew_verdict` / `crew_gate`, issue #47: a role submits finished work
  for verification instead of asserting it done, an independent role tries to break it
  against the acceptance and passes or hands it back, and the broker refuses a self-verdict
  so "done" means an independent role could not break it); the defibrillator also catches
  coordination stalls, not just dead agents (issue #48: a fleet-wide stall monitor reads
  the event stream for a deadlock, an unanswered question, or a ledger with no forward
  motion, and escalates the specific cause to the General, telling a true deadlock from a
  legitimate wait for input).
- **Team memory.** A shared situation board (`crew_board` / `crew_record`, issue #49):
  agreed interfaces, decisions and their rationale, and known gotchas the crew reads and
  writes, distinct from the transient message stream, curated by the commander. It is a
  projection of `board` events, so it is auditable and rebuilt from the durable log across
  an idle-stop or a restart. A new role boots from a bounded briefing packet
  (`crew_briefing`, issue #50: role card plus the board plus a lane-scoped rolling summary,
  size-capped), never the raw log, so it joins mid-mission and acts in its lane in seconds.
- **Economy.** Model per role (issue #53, done: a `strong` / `standard` / `cheap` tier
  per role over a per-crew tier map, with a sensible default mapping by name, strong for
  the commander and architect, cheap for mechanical roles, standard for the builders, so
  changing the mapping changes spend with no code change); a shared token budget with
  per-role caps (issue #54, done: a crew-wide `token_budget` and per-role `token_cap`,
  enforced by idle-stopping a role or the crew at a cap and surfaced as a `budget` event,
  never a silent overrun); auto-idle on quiet with cost and token telemetry (issue #55,
  done: the lifecycle machine idle-stops a quiet role, and per-turn `telemetry` events plus
  the roles' `lifecycle` events feed a `GET /stats` rollup of tokens, cost, and working time
  per role and in aggregate); and subscription usage awareness (issue #56, done: one shared
  usage gauge across the crew auto-pauses new work when a reading crosses
  `CREW_BROKER_USAGE_THRESHOLD`, lifts lazily at the window reset, and lets the operator
  resume early with `crew resume`, distinct from the manual pause).
- **Stack:** Rust (axum + tokio + eyre + mimalloc), toolchain pinned. Follows the
  global Rust conventions in `~/.claude/docs/rust.md`.
- **Not a new runtime:** crew supervises Claude Code / Codex, it does not replace
  them.

## Open questions

- Commander identity: does the General always talk through the commander, or can
  the General drop into any role's channel directly? (Current lean: both, the
  commander is default.)
- Codex parity: the CLI shim is built and `crew up` supervisor-spawns a Codex role from
  the config's per-role `runtime` (issue #128); a Codex-native MCP path is still open.
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
  plain send (`crew brief` to post a free-form note as the General to the commander by
  default, a role, or a channel, issue #118), the General's
  command-and-control directives (`crew redirect` / `crew belay` to steer a role
  mid-task, `crew command` to order a role directly, bypassing the commander while
  keeping it informed, and `crew reassign` to move an in-flight task to a new owner in the
  work ledger while informing both roles and the commander, issue #42), `crew integrate`
  to merge the roles' branches into one
  coherent, green branch (issue #44), the agent CLI shim (`crew register` / `crew send` /
  `crew order` / `crew ask` / `crew answer` / `crew inbox` /
  `crew roster` / `crew lane` / `crew claim` / `crew ledger` / `crew submit` /
  `crew verdict` / `crew gate` / `crew board` / `crew record`) for a
  runtime without MCP, `crew watch` to tail a role's self-filtered inbox stream live
  (auto-reconnecting like `tail -F`, resuming from `Last-Event-ID` without loss, issue #117),
  `crew top` for the live terminal cockpit (issue #51: htop for the crew, every role's status,
  action, and spend plus the message flow, updating live off the stream), `crew notify` to
  push a native notification on each actionable moment (a question, a
  death, a stand-down, a coordination stall, a mission completion) over that same stream, and
  `crew usage` to read the shared-subscription usage gauge (issue #56).
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
  crew-client        the shared broker client (send/inbox/roster), used by MCP + shim
  crew-supervisor    process management: spawn/wire/lifecycle of role agents
  crew-mcp           the agent-facing MCP surface (crew_send, crew_inbox, ...)
  crew-substrate     the umbrella crate: re-exports the five above as one dependency
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
model (issue #6). `crew-client` holds the thin synchronous broker client
(`Broker`, `InboxItem`, `RoleEntry`, and the view types) both agent-facing
front-ends send and read their inbox through, so neither the MCP server nor the
CLI shim owns it and the shim needs no dependency on `crew-mcp` (issue #129).
`crew-substrate` is the umbrella (issue #34): it re-exports the
five substrate crates (`crew-core`, `crew-broker`, `crew-client`, `crew-supervisor`, `crew-mcp`) as
the modules `core` / `broker` / `client` / `supervisor` / `mcp`, so a front-end takes one
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
`EventKind` (`Message` with a `MessageKind`, `Lifecycle`, `Activity`, `Ledger`, `Boundary`,
`Verification`, `Board`, `Budget`, `Telemetry`, `Usage`) stream
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
subscriber. Subscribers read either `GET /stream`, the live aggregate feed, or
`GET /inbox?role=<role>`, a role's live events filtered to its direct, pair, and
`all-units` channels with its own messages dropped at the source and resumable from
a `Last-Event-ID` cursor without loss (issue #10). A subscriber that lags off the
broadcast (its buffer overruns the capacity under load) logs a `broker.inbox.lagged`
event with the role and skipped count, so the gap is visible to the operator rather
than silent; the client still recovers it from the cursor on reconnect (issue #116).
`GET /history` reads past events
filtered by `channel`, `role` (sent by), `agent` (a role's activity timeline), `kind`
(a comma-separated set keeps several kinds in one query, e.g.
`kind=message,ledger,verification`, issue #125),
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
received. The **aggregate activity log** takes the same filter both ways (issue #31):
`GET /stream` accepts the same `channel` / `role` / `agent` / `kind` / `task` / `since`
params as `/history` (the shared `FilterQuery`, applied live with the very same
`EventFilter::matches`), so a filtered live subscription and a filtered history read
agree; with no filter `/stream` is the firehose. Like `/inbox`, `/stream` resumes from a
`Last-Event-ID` cursor: a dropped or lagged consumer reconnects and the stream replays
the matching events it missed before the live tail, so it needs no separate `/history`
call (issue #134); `/inbox`, `/activity`, and `/stream` share one replay-then-live SSE
engine (`sse::resume_stream`), and a `/stream` lag logs `broker.stream.lagged`. The
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
task-scoped). Activity events thread the task the same way through `Event.task` (issue
#24). The task is minted where work is assigned (issue #132): `crew_order` mints a
`TaskId` and stamps it on the order, and the assigned role adopts it from its inbox
(`Broker::inbox`) so its own `crew_send` / `crew_order` stamp the same id, correlation
on the envelope rather than a role-to-task table. The shim persists the adopted task
per role beside the inbox cursor. Threading the task onto the assignee's supervisor
`RosterClient` at runtime (the fleet watching the order stream, not a broker query) is
the remaining step. See `docs/observability.md`.

`crew-mcp` carries the agent-facing MCP surface (issue #17): the `crew-mcp` binary
speaks JSON-RPC 2.0 over newline-delimited stdio (protocol `2024-11-05`,
`initialize` / `tools/list` / `tools/call`), which the supervisor spawns one of per
agent. It boots from a role card (`CREW_ROLE_CARD`, issue #18), registers the role on
the roster at boot, and dispatches each tool to a `crew_client::Broker`, the shared
thin synchronous client (`ureq`) over the broker's HTTP API (issue #129); it never
touches the store. It exposes eighteen
tools with self-documenting schemas: `crew_send` (post a note as the role to a channel or a
teammate, defaulting to the commander), `crew_order` (issue an order, a scoped task
with a title, scope, owned paths, and acceptance, to one specialist; the commander's
fan-out handle, issue #27), the typed-message pair `crew_ask` / `crew_answer` (post a
`question` or an `answer` rather than a plain note, so an unanswered question surfaces a
coordination stall instead of stalling silently; `crew_answer` names the question by the
`in_reply_to` id the inbox shows, issue #123), the typed-message pair `crew_status` /
`crew_artifact` (post a progress `status` or a reference to a produced `artifact` (a
`branch` / `pull_request` / `file` / `route`) rather than a plain note, so the typed
rendering and any projection that keys on the kind is not lost; issue #167), `crew_inbox`
(read the messages addressed to
the role since the last call, self-filtered, over a per-session history cursor, surfacing an
order's structured fields and each message's id so a reply can name it), `crew_roster`
(list registered teammates, their owned paths, and liveness), `crew_lane` (check a path against the role's owned lane
before an out-of-lane edit; in-lane it says proceed, out-of-lane it reports a `boundary`
event and, under a blocking policy, refuses, routing the change through the commander;
issue #46), the work-ledger pair `crew_claim` / `crew_ledger` (claim a task before
touching shared work, moving the claim through `in_progress` / `blocked` / `done`, and
read the ledger; the broker refuses a claim another role holds, issue #45), the
adversarial done-gate trio `crew_submit` / `crew_verdict` / `crew_gate` (submit finished
work for verification instead of asserting it done, judge a teammate's work as an
independent skeptic, and read the gate; issue #47), `crew_complete` (report the mission
gracefully finished, typically as the commander, with a short `summary` of what shipped
that the completion push renders, so `crew notify` fires on a true completion rather than a
stand-down; it announces, it never gates the crew: completion is a report the crew makes,
not a control the General issues, so gating on it would let a role halt everyone by
declaring victory and would cut in-flight work short. A finished mission has no work to
pull, so its roles idle-stop on their own (issue #55); to stop the crew the General uses
`crew standdown` or `crew down`. This keeps the crew `Standing` a three-level brake,
`Running` / `Paused` / `StoodDown`, all cleared by `crew resume`, with no terminal
`Complete` level; issues #121, #154, #155), the situation-board pair `crew_board`
/ `crew_record` (read the crew's durable memory, and record or retract a decision,
interface, or gotcha; issue #49), and `crew_briefing` (the bounded new-role briefing
packet: the board plus a lane-scoped rolling summary, size-capped; issue #50). A tool
failure returns as an `isError` result, not a protocol error.

`crew_inbox` has a push path (issue #76): `Broker::subscribe` opens the broker's per-role
SSE inbox (`GET /inbox?role=<role>`) at boot, seeds the backlog from history once (a fresh
stream starts at the live tail), and a background thread buffers events as they arrive,
resuming from a `Last-Event-ID` cursor across reconnects and deduplicating the seed overlap
by message id. A read drains the buffered batch (`InboxStream`) instead of refetching the
whole message history, so it is O(new) not O(total) per call. The pull-based history read
stays the fallback when the stream cannot be opened (a runtime without streaming); surfacing
native MCP notifications as events arrive is the remaining refinement.

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
the model tier each runs (issue #53), the runtime each runs on (`claude` or `codex`,
issue #128), the repos in scope, the idle-stop
timeout, and the commander. `from_toml` resolves the defaults (an omitted `roles`
yields the default crew: commander, backend, frontend, qa) and validates the whole,
so a documented config produces a valid crew and an invalid one fails with a precise
`ConfigError` (an unknown commander, an overlapping ownership boundary, a duplicate or
empty role, or a typo'd field). `to_cards(&broker)` produces the per-role `RoleCard`s
the supervisor spawns, and `model_for(role)` resolves the spawn model. Each `repos` entry
is a path or a bare name; `repo_paths(config_dir)` resolves it for worktree isolation
(issue #126): an absolute path as-is, else joined under the `workspace` root, which
defaults to the crew config file's own directory (overridable with the `workspace` field).
Anchoring to the config, not the shell's cwd, means a name points at the same clone
wherever `crew up` runs. `repo_paths` is a pure path join (crew-core is sans-io), so before
bring-up the supervisor's `prepare` pre-flight validates that every resolved path is an
existing git repository (`worktree::validate_repos`, issue #164): a typo'd `repos` name or a
misdirected `workspace` fails fast with one message naming every missing or non-git repo,
rather than one late per-role failure in `Worktree::create` after some worktrees were
already made and rolled back. Each role also carries a `runtime` (issue #128): `to_cards` stamps
it onto the role card, and the supervisor's `agent_command` spawns `claude -p` (MCP tools)
for a Claude role or `codex exec --dangerously-bypass-approvals-and-sandbox
--skip-git-repo-check` (wired to the CLI shim) for a Codex role, adapting the card briefing
to name the `crew <cmd>` shim invocations. The codex invocation is verified against
codex-cli 0.145.0 (issue #162): `exec` is the non-interactive mode, the bypass flag runs
the agent unattended (no approval gate or sandbox), and `--skip-git-repo-check` lets a
scratch-dir role boot outside a Git repo. The MCP server is registered only when the crew
has a Claude role, so a Codex-only crew needs no `claude`, and `crew up` brings up a
mixed-runtime unit in one command. See `docs/config.md` and `docs/codex.md`.

Model per role spends strong-model budget where it matters and a cheap model everywhere
else (issue #53, `crew_core::model`). A role runs a **model tier** (`strong` / `standard`
/ `cheap`, `ModelTier`), an intent rather than an exact build; `ModelTiers` maps each tier
to a concrete alias, defaulting to `opus` / `sonnet` / `haiku` and overridable per crew in
the config `[models]` table, so retuning spend is a config change, not a code change.
`default_tier_for(role)` gives every role a sensible default tier by name (the lead and
architect strong, docs / ci / lint / test cheap, the builders standard), so the default
crew already spends well with no model config. `model_for(role)` resolves most-specific-
first: a role's exact `model`, else its explicit `tier`, else a crew-wide `model`
override, else the default tier for its name, always through the crew's tier map. The
supervisor spawns each role with the resolved alias via `--model` (unchanged), so changing
the mapping changes spend with no code change. See `docs/config.md` (model per role).

Lane-ownership enforcement makes those owned paths a checked boundary (issue #46). The
config carries a crew-wide `lane_enforcement` policy (`warn` the default, `block`, or
`off`) that `to_cards` stamps onto every `RoleCard`. `crew_core::path_in_lane` decides
in-lane against a role's owned paths, matched on whole path segments so `api/` never
matches `apiv2/`, and a role with no owned paths is unrestricted. Before an out-of-lane
edit a role checks the path with the `crew_lane` MCP tool (or `crew lane` on the shim):
in-lane it proceeds, out-of-lane the broker records a `boundary` event (`POST /boundary`,
a new `EventKind::Boundary` carrying the role, path, and whether it was blocked) on the
stream to `all-units`, and under `block` the edit is refused so a cross-lane change routes
through the commander instead of a silent edit. The event is filterable with
`GET /history?kind=boundary`. See `docs/roles.md` (lane enforcement) and
`docs/observability.md` (the `boundary` event).

The adversarial done-gate makes "done" mean verified, not asserted (issue #47), so
confident-but-wrong work never ships. A role does not report its own task done: it submits
the finished work with `crew_submit` (or `crew submit`), an independent role tries to break
it against the acceptance criteria and records a pass or a failure with `crew_verdict`, and
`crew_gate` reads the live gate. The gate lives in `AppState` behind one lock (mirroring the
#41 pause `control`): `POST /gate/submit` records the task awaiting verification and, when a
reviewer is named, notifies it; `POST /gate/verdict` holds the lock across the check and the
update, refusing a verdict from the task's own owner (409) or on a task not awaiting one, so
a task reaches `Passed` only when a role other than the owner could not break it. A `Failed`
verdict posts an actionable handback to the owner's inbox with the specific failure. Each
step is a first-class `verification` event (a new `EventKind::Verification` carrying the
task, owner, verifier, verdict, and detail) published to `all-units` and filterable with
`GET /history?kind=verification`; `GET /gate` reads live ownership. Like the situation board
(issue #49), the gate is a **projection of those `verification` events**: `AppState::with_storage`
rebuilds it (`Gate::rebuild`) by folding the log the store just replayed, the latest event
per task winning, so a task mid-verification survives a broker restart rather than being lost
(issue #181). The broker stays the live authority that enforces the gate. Added
`ApiError::Conflict` (409) and a `Verification` history-kind tag. Each role card's briefing now
instructs a role to verify before done and to be the skeptic on a teammate's work. See
`docs/roles.md` (the done-gate) and `docs/observability.md`.

The shared situation board is the crew's durable memory (issue #49), distinct from the
transient message stream: agreed interfaces, decisions and their rationale, and known
gotchas, so the crew stops re-deriving and re-litigating what is settled. `POST /board`
records or (with `retract`) removes an entry keyed by a stable topic, and `GET /board`
reads it (filterable by section: decision / interface / gotcha); `crew_board` and
`crew_record` are the tools, `crew board` / `crew record` the shim commands. Every change
is a first-class `board` event (a new `EventKind::Board` carrying the key, section, author,
body, and a retracted flag) published to `all-units` and filterable with
`GET /history?kind=board`. Crucially, the board is a **projection of those durable events**:
the broker rebuilds it in `AppState::with_storage` by folding the log the store just
replayed, so a decision recorded before an idle-stop or a broker restart is still on the
board after it (this is what satisfies the acceptance, and why the board survives a restart,
as the done-gate now does too (issue #181), where the in-memory pause control does not). The whole crew reads and writes
it; the commander curates it, and each role card's briefing points a role at it. Added a
`Board` history-kind tag. See `docs/communication.md` (context management), `docs/roles.md`
(the situation board), and `docs/observability.md`.

The new-role briefing packet solves the 100k-token join problem (issue #50): a freshly
spawned role catches up from a bounded packet, not the raw transcript. `GET
/briefing?role=<role>` assembles the current decision board plus a rolling summary scoped
to the role's own timeline (via the `agent` filter, so its lane and the work addressed to
it), renders it to text, and caps it to a byte budget (`DEFAULT_BUDGET`, ~4KB, overridable
with `?budget=`), reporting the measured `size` and a `capped` flag so the packet stays
small no matter how long the mission ran. `crew_briefing` is the tool (and `crew briefing`
the shim); each role card's briefing now tells a role to call it first thing on boot as the
deliberate catch-up path, so it joins mid-mission with bounded context and acts in its lane
in seconds. The endpoint reuses the existing rolling summary (issue #19) and board (issue
#49) rather than adding an event kind or projection.

So the packet lands in context even if the agent skips the tool, the supervisor also pushes
it into the agent's opening `claude -p` turn at spawn (issue #122). `RosterClient::briefing`
fetches the packet and `spawn::boot_command` folds it into the boot prompt after the card
briefing at the spawn moment (the `Fleet`'s `AgentLifecycle::spawn`), so it is fetched at
spawn rather than provision and is current for a lazily started role. It is best-effort: an unreachable broker leaves the agent booting on
its card briefing alone (a debug `supervisor.briefing.skipped` log), and a stub command with
no `-p` prompt is untouched, so `crew_briefing` stays the re-read path. See `docs/roles.md`
(the briefing packet) and `docs/communication.md` (context management).

`crew-supervisor` also auto-registers the crew MCP server so a spawned agent gets the
crew tools with no per-task approval (issue #20), the way Seraphim registers the
Playwright MCP. `locate_server` finds the `crew-mcp` binary (a build/boot check that
fails loudly if it is missing) and `register_server` records it at user scope
(`claude mcp add -s user crew -- <path>`, idempotent via remove-then-add), so a
`claude -p --permission-mode bypassPermissions` turn (`agent_turn_argv`) loads it
silently. Registration is one-time and unit-wide: per-agent role and broker ride the
`CREW_ROLE_CARD` environment the `crew-mcp` child inherits.

`crew_supervisor::Supervisor::launch` ties these together into the auto-spawn flow
(issues #21, #26): register the MCP server (for Claude roles), then per resolved role
card provision the card and build its spawn command, and hand the fully-resolved agents
to the lifecycle-managed `Fleet`, the single spawn engine. The `Fleet` owns lifecycle,
so it registers a role on start and deregisters it on exit (via `RosterClient`), keeping
`GET /roster` a true picture of the live unit; each process's stdout and stderr are
captured and streamed as `Captured` lines the activity parser reads (issue #24, below).
`spawn::prepare` builds fully-resolved `AgentCommand`s, so the process management is
exercised in tests with stub processes instead of a real `claude`. The roster of roles
comes from the crew config (issue #25), which `launch` consumes as role cards. The
`Fleet` owns the per-role worktrees and cleans them up on stand-down
(`Fleet::with_worktrees`, issue #127): when the config opts into worktree isolation,
`prepare` creates each role's worktree and hands them to the fleet, so an unchanged
worktree is removed on stand-down and a failed bring-up removes any already created.

`crew_supervisor::Fleet` manages each agent's lifecycle so idle roles cost nothing and
crashes recover (issue #22). Each agent runs a state machine on its own driver thread:
**lazy start** (the fleet launches with every agent stopped and no process; the first
work via `Fleet::start` spawns and registers it), **idle-stop** (after a configurable
quiet period the driver stops the process but keeps the roster entry, marked idle, so
a restart is fast and keeps context), and **restart on demand** (`Fleet::start` on a
stopped or idle agent restarts it).

The supervisor captures **what each agent does inside its own process**, the per-agent
activity log the broker cannot see (issue #24, `crew_supervisor::activity`). Each agent
runs with `--output-format stream-json --verbose` (`agent_turn_argv`), so it emits one
JSON object per stdout line. `activity::parse` distills each line into `crew_core::Activity`
items, modeled on Seraphim's parser: a session `init` is a `TurnStarted`, the `result`
line a `TurnEnded`, an assistant `tool_use` a `ToolCall`, and assistant `text` an `Output`;
the partial-message usage firehose, tool results, and rate-limit notices are dropped (the
per-turn token feed comes from the `result` line, issue #177; the subscription-usage feed is
issue #113). A line the parser does not recognize
becomes `Activity::Other` rather than an error, so a schema drift across Claude Code
versions is visible on the stream, never a crash. `activity::forward_activity` is the
runtime half, started by `Fleet::launch`: a detached thread drains the fleet's captured
output, parses each stdout line, records every activity through
`RosterClient::emit_activity` (`POST /activity`), and charges each turn's parsed usage
against the crew budget through the shared `UsageRecorder` (issue #177). The broker keys the
`activity` event to
the role on its own `@role` channel (via `Channel::Direct`), so it rides the aggregate
stream and the role's per-agent timeline (`GET /activity?agent=<role>`) but never floods
another role's inbox, and threads the supervisor's task (issue #29) for correlation.
Recording is best-effort: a broker hiccup is logged, never fatal.

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
a twenty-five-minute watchdog, and three recoveries. The activity parser now puts turn
boundaries on the stream (issue #24), but the watchdog does not yet key on them for precise
hang-versus-idle discrimination, so by default a quiet agent still parks rather than being
force-recovered. (Wiring the watchdog to those turn boundaries is a later cleanup.)

The defibrillator also catches a crew stuck waiting on itself, not just a dead agent
(issue #48, `crew_supervisor::stall`). A fleet-wide **stall monitor** thread reads a
recent window of the event stream (`RosterClient::history_since`, over the stable stream
contract rather than `crew_core::EventKind`, so a kind it does not model passes through;
it fetches only the `message` / `ledger` / `verification` kinds it inspects, so a busy
crew's high-volume `activity` events never ride the wire each scan, issue #125). The scan
is incremental (issue #165): a stateful rolling `ScanWindow` keeps the lookback window's
events, and each tick fetches only the events since the previous scan's newest one
(`since` = the buffered cursor), splices them onto the buffer (the inclusive `since` re-reads
the boundary-timestamp run, which `ScanWindow::refresh` drops before re-appending, so there
are no duplicates), and trims the aged-out front, keeping each scan O(new) rather than
re-reading the whole window; a cold start or a wholly aged-out buffer resets to a full
window read. It runs `detect_stalls` over the buffer on the `stall_scan_interval`, a pure
function that finds three
shapes past the `stall_timeout` (ten minutes by default, below the process heartbeat): a
**deadlock** (a cycle of unanswered questions), an **unanswered question** (a one-sided
wait with no cycle), and a **stalled ledger** (a task with no forward motion, a `ledger`
claim not yet `done` from issue #45 or a `verification` submission with no verdict from
issue #47). A question broadcast to `all-units`, or to anyone who is not a live agent, is
a legitimate wait for the General, not a deadlock, so it is never escalated. A new stall
is escalated as a specific `supervisor.stall.detected` warning (who is waiting on what)
and recorded (read with `Fleet::stalls`), once per stall until it resolves. The monitor
also surfaces each stall on the stream as a first-class `stall` event (`POST /stall`,
`EventKind::Stall`, issue #120): a `detected` event when a stall crosses the threshold and
a `resolved` event once it clears, from the General to `all-units`, carrying the stall's
kind (`deadlock` / `unanswered_question` / `ledger_stall`), the roles caught in it, and
the specific detail. That is what lets `crew notify` fire the "a role is stalled" moment
(issue #52) and the `crew top` cockpit (issue #51) render live stalls; the stream write is
best-effort, so a broker hiccup never takes the monitor down. The event is filterable with
`GET /history?kind=stall`.

The **integration step** brings the roles' isolated work together (issue #44,
`crew_supervisor::integrate`). Parallel roles work on `crew/<role>` branches in their own
worktrees (issue #43); their work is done only when it merges into one coherent whole.
`Integrator::integrate` resets the integration branch (`crew/integration`) to a base in a
dedicated worktree (so it never disturbs the main checkout or the role worktrees), merges
each role branch in order, and runs the crew's acceptance checks (a `sh -c` command, build
or tests) on the merged result. It returns an `IntegrationReport` with a `Standing`
(`Green` / `Merged` / `Conflicts` / `ChecksFailed`). Conflicts are resolved, not dropped: a
conflicting merge is aborted and reported with the branch and the files it collides on, for a
human or the commander to resolve, never a force-merge that discards a role's work. Migrations
and other ordering concerns stay linear because the acceptance checks run on the integrated
branch, so a collision that breaks the build or the tests fails the integration rather than
shipping. `crew integrate --repo . --base HEAD --check "cargo test"` runs it from the CLI:
it discovers the `crew/<role>` branches, integrates them, and prints the standing and what to
do next (resolve conflicts, or push `crew/integration` and open a PR). The stacked-PR strategy
for roles that build on each other is to order the branches so a dependency merges before its
dependents. Depends on the done-gate (issue #47): the gate judges a part done, the integration
judges the whole green. See `docs/roles.md` (the integration step).

The fleet also keeps a crew from quietly burning a fortune (issue #54, `crew_core::budget`).
A crew sets a crew-wide `token_budget` and optional per-role `token_cap` in its config;
`CrewConfig::budget()` builds the pure `Budget` accountant (a crew ceiling, per-role caps,
running totals, modeled on the Workflow budget pattern), which the `Fleet` holds.
`Fleet::record_spend(role, tokens)` charges the spend, publishes a `budget` event (spend
against budget, to `all-units`, so a cap hit is never silent), and on a breach idle-stops
the role (its own cap) or the whole crew (the crew budget) rather than overrun; a ceiling
fires once, and an unbounded crew records nothing. The token feed is live (issue #177):
`record_spend` (wrapped by `record_usage`) is driven by each turn's usage the stream-json
activity parser (issue #24) distills from captured stdout. The activity forwarder parses the
`result` line's `usage` (its token fields summed) and `total_cost_usd`, then calls
`record_usage(role, tokens, cost_micro_usd)` per turn, so budget enforcement charges against
real spend; `UsageRecorder` is the shared handle the `Fleet` and the forwarder both hold
(the fleet delegates its `record_usage` / `record_spend` to it). The broker accepts a report
at `POST /budget` and streams it as `EventKind::Budget`, filterable with
`GET /history?kind=budget`. It also folds those `budget` events into a snapshot served at
`GET /budget` (issue #176): current spend against budget per role (cumulative spend and cap)
and crew-wide (the crew total and budget). Like the situation board (issue #49) and the
`GET /stats` rollup, it is a projection of the durable log, latest-wins per role and
crew-wide, rebuilt by folding the `budget` events on a restart, so the `crew top` cockpit
(issue #51) reads a snapshot rather than replaying the stream. See `docs/observability.md`
(token budget) and `docs/config.md`.

Auto-idle on quiet with cost and token telemetry makes spend legible per role and overall
(issue #55). Idle-stop is the lifecycle machine from issue #22: a role quiet past its
`idle_stop` timeout is stopped, keeping its roster entry, so an idle role costs nothing.
Telemetry is always-on: `Fleet::record_usage(role, tokens, cost_micro_usd)` reports each
turn's usage as a `telemetry` event (`POST /telemetry`), whether or not a budget is set, and
also charges the tokens against the budget (it wraps `record_spend`). The broker folds a
`Stats` projection from the `telemetry` events (tokens, cost) and the roles' `lifecycle`
events (working time, entering vs leaving the working state), rebuilt from the durable log on
a restart like the board, and serves it at `GET /stats`: per role and in aggregate, the
cumulative tokens, cost (micro-USD), and working seconds, with a live role's open working
interval counted through the read instant. Cost and tokens ride the same stream-json feed as
the budget (issue #177): the activity parser reads the stream-json `result` line's `usage`
tokens (its fields summed) and `total_cost_usd`, and the forwarder calls `record_usage` per
turn; working time needs no feed. This is the data the
`crew top` cockpit (issue #51) and the Seraphim per-role stats render. See
`docs/observability.md` (cost, tokens, and time telemetry).

Subscription usage awareness keeps a crew from exhausting the shared window (issue #56). The
crew shares one subscription, so the broker keeps one `Usage` gauge across the crew (in
`AppState`, distinct from the manual pause `Control`). `POST /usage` records a reading (the
window percent plus its reset); at or above `CREW_BROKER_USAGE_THRESHOLD` (default 90) it
auto-pauses new work, publishing a `usage` event with the reset time so the pause is never
silent. `is_role_paused` folds in the usage auto-pause, so every role is gated (one shared
subscription). The gate lifts lazily at the reset instant, so work auto-resumes; `crew
resume` (which now also clears a usage pause) is the escape hatch to resume early. A
background sweep tied to the server (`serve.rs`, a tokio task aborted on shutdown) polls for a
pause whose window has reset and, when one has, clears it and publishes a `usage` lift event,
so the lazy auto-resume is observable on the stream, not only reflected in the gate (issue
#112). The sweep is idempotent (`AppState::expire_usage_pause` clears the armed reset), so the
lift is announced once. `GET /usage` and `crew usage` read the gauge. The usage signal is the
supervisor's to detect from the agents' rate-limit output (the stream-json parser, issue #24)
and report via `RosterClient::report_usage` (that wiring is issue #113, paused until #24
supplies the detection); the auto-pause mechanism is exercised through `POST /usage` directly
until then. See `docs/observability.md` (subscription usage auto-pause).

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
`crew order` (issue a scoped order to a specialist, issue #27),
`crew ask` / `crew answer` (post a typed question or answer, issue #123),
`crew status` / `crew artifact` (post a typed progress status or a reference to a produced
branch, PR, file, or route, issue #167),
`crew inbox`, `crew roster`, `crew lane`, `crew claim`, `crew ledger` (issue #45), the
done-gate trio `crew submit` / `crew verdict` / `crew gate` (issue #47), `crew complete
[summary]` (report the mission gracefully finished, with an optional summary of what
shipped, issues #121, #155), the
situation-board pair `crew board` / `crew record` (issue #49), and `crew briefing`
(issue #50) let an agent on a runtime without MCP, such
as Codex, coordinate through subcommands instead of tools (`crew lane <path>` is the
shim's `crew_lane`, issue #46). Each boots from the same role context
the `crew-mcp` binary reads (`CREW_ROLE_CARD`, else `CREW_ROLE` plus the `CREW_BROKER_*`
config) and reuses the same `crew_client::Broker` the MCP server dispatches to (issue
#129), so a shim agent's I/O maps onto
the broker identically to the MCP path: it registers on boot (appearing on the roster
and stream) and sends and reads the same way. The shim is a short-lived process per
call, so where the MCP server holds its inbox cursor in memory, the shim persists a
per-role cursor on disk (`<state_dir>/shim-cursors/<role>.cursor`, the id of the last
inbox message read), so `crew inbox` shows only messages since the last call (issue
#130). Keying the cursor on the message id, not a count, lets it survive a broker log
reset (issue #160): a stale id absent from a fresh, shorter log replays the log rather
than silently skipping new messages until it grows past the old count. Both the shim
cursor and the MCP server's in-memory one share this pull path in `crew-client`. The
remaining parity gap (no push) is in `docs/codex.md`. A Codex role no longer needs an operator to launch it: with
`runtime = "codex"` in the config, `crew up` supervisor-spawns it alongside the Claude
roles (issue #128).

`crew brief` is the General's plain send (issue #118), the operator-facing counterpart to the
agent shim's `crew send`. It posts a free-form `note` as the General, so unlike the shim it
needs no role card, only the broker address. The target follows the crew's one addressing rule
(`Channel::resolve`): `--to` a role wins, else `--channel` a name (`all-units` or a pair), else
the commander (`--commander`, default `commander`). So `crew brief "..."` is the default brief
that sets the unit to work, and `crew brief --channel all-units "..."` is the General's
broadcast. It shares the General front-end path with `crew redirect` / `crew belay` / `crew
command` in `src/control.rs` (posting as the General, no role card), so it needs no broker
change. See `docs/communication.md`.

`crew command` is the General's direct override (issue #42): it commands a specialist itself,
bypassing the commander's fan-out, without breaking the chain of command. It posts an `order`
from the General to the role's `@role` channel (with `--scope` / `--acceptance` filling the
order's fields), then a note to the commander's feed announcing the direct order, so the
commander is informed rather than bypassed silently and adjusts its plan around it; ordering
the commander itself carries no notice. Briefing the commander (`Channel::resolve`) stays the
default and the override is explicit, so the chain of command is intact, not broken. Built on
the same General front-end path as `crew redirect` / `crew belay` (posting as the General, no
role card), it needs no broker change: the direct order and the commander notice are two posts
to the ordinary message endpoint.

`crew reassign` is the override's other half (issue #42): the General moves an in-flight task
from one role to another in the work ledger (issue #45). It POSTs to the broker's
`POST /ledger/reassign`, which overrides the ledger's one-owner invariant to take a held task
from its current holder, preserves the task's state and title so the work moves in place, and
publishes a `ledger` event with the new owner so the change rides the stream like any claim
(from the owner, to `all-units`). The broker refuses a move on a task that is not held (absent
or done), held by a role other than an optional `--from` guard, or already owned by the target
(each a 409). `crew reassign <task> --to <role> [--from <role>]` then posts a General `note` to
each party so no one is surprised: the old owner is told to hand off, the new owner to pick the
work up where it stands, and the commander that the General moved it, unless the commander is
one of the two roles (already notified as a party). See `docs/communication.md` (direct
override) and `docs/roles.md`.

`crew notify` lets the General walk away and be pulled back only when it matters (issue
#52). It tails the firehose (`GET /stream`, the same event stream `crew watch` reads,
sharing the `broker::tail_events` read half, so there is no separate signal path, and it
auto-reconnects on a dropped connection along with `crew watch`, issue #117) and
pushes a native notification on each **actionable moment**: a General-facing question asked
(`message`/`question`), a role dead (`lifecycle`/`died`), the crew stood down
(`lifecycle`/`stood_down`), the crew stalled (a `stall`/`detected` event, issue #120; a
`resolved` stall stays quiet), the mission complete (a `mission` event, issues #121,
#155: the crew's graceful finish, reported through `crew_complete`, distinct from the
stand-down that used to approximate it; its push renders the reporter's summary of what
shipped), or the budget exhausted (a `budget` event with a `breach`, issues #54, #175: a
role hit its cap or the crew hit its budget, idle-stopping the role or the whole crew; a
within-budget spend report stays quiet). A question is General-facing only when it
is broadcast to `all-units` or addressed to a role that is not a live agent (issue #119); a
directed question to a live teammate is peer coordination the crew resolves itself and stays
quiet, mirroring the stall monitor's rule (issue #48). To scope it, the notifier tracks
roster liveness: it seeds the roster once from `GET /roster` on connect (issue #32, #170), so
a quiet, already-registered role is known live on attaching to a running crew, then keeps it
current by folding the `lifecycle` events on the same stream (a role is live while working or
idle); an addressee still not known to be live (absent from the seed and unseen on the
stream) is treated as General-facing, and a real question is never dropped. Other routine chatter
(status, notes, orders, answers, artifacts, ordinary lifecycle, activity, board, boundary,
verification) stays quiet by default. The classifier, `notification_for` over the
liveness-tracking `Roster`, decides per event, so the policy is fully unit-tested; `--mute
<moments>` (`question,died,stood-down,stalled,complete,budget`) narrows the set and `--no-sound`
drops the terminal bell. Each push prints a log line (the durable record), sounds the bell
(mirroring Seraphim's notification sound), and calls the platform desktop notifier
(`notify-send` on Linux, `osascript` on macOS), degrading quietly when no notifier is
present. An approval pending (issue #40) plugs into the same classifier when its event
lands, exactly as the stall moment did once the monitor began surfacing stalls. See
`docs/observability.md` (push notifications).

`crew top` is the live terminal cockpit, htop for the crew (issue #51, `src/top/`). It shows
every role with its status (working / idle / stopped / dead, plus a paused flag), current
action, tokens, and cost, over a live message feed, with a header carrying the live count and
the aggregate spend. The split is the repo's usual pure-core-plus-thin-shell: `top::cockpit`
is a pure state model that seeds from the `/roster` (issue #32) and `/stats` (issue #55)
snapshots and then folds each live `/stream` event (a `lifecycle` moves a role's status, an
`activity` sets its current action, a `telemetry` adds to its tokens and cost, a `message`
lands on the feed), so it captures nothing new and is fully unit-tested; `top::render` draws
it with `ratatui` (exercised headlessly against a `TestBackend`), and `top::run` is the thin
terminal shell, a background thread tailing `/stream` through `broker::tail_events` (the same
reader `crew watch` and `crew notify` share) and feeding events down a channel while the main
loop drains them, redraws, and handles keys. The display updates by push, never by polling;
the loop's tick is only a render cadence. Keys: up/down select a role, Enter drills into its
activity, `f` filters the feed to the selected role, `c` cycles the channel filter, `x`
clears it, and `q` / Esc / Ctrl-C quits. A dead broker fails fast on the initial roster fetch,
before any terminal is touched. See `docs/observability.md` (the cockpit).

**Running `crewd`:** `cargo run --bin crewd`. It binds `127.0.0.1:2739` by
default. Configure via env: `CREW_BROKER_HOST`, `CREW_BROKER_PORT`,
`CREW_BROKER_STATE_DIR` (default `.crew`, where the durable log `events.jsonl` and
`roster.json` live), `CREW_BROKER_ALLOW_NON_LOCAL` (`1`/`true`/`yes`),
`CREW_BROKER_SECRETS` (a whitespace-separated list of secret values the broker masks
out of every message before storing or streaming it), and `CREW_BROKER_USAGE_THRESHOLD`
(the shared-subscription usage percent at which new work auto-pauses, default 90; issue #56).
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
- **Formatting** is pinned in `rustfmt.toml`. The stable options apply on the pinned
  build toolchain; run `cargo fmt` to apply them and `cargo fmt --check` to verify.
  The richer import-grouping and comment options (`group_imports`,
  `imports_granularity`, `wrap_comments`, `format_code_in_doc_comments`) are
  nightly-only (issue #60): run `cargo +<nightly> fmt` to apply them, where
  `<nightly>` is the date pinned as `NIGHTLY` in `.github/workflows/ci.yml`. The
  stable `cargo fmt` ignores those options (it warns and passes), so both toolchains
  agree on the committed formatting. `wrap_comments` needs a second `fmt` pass to
  settle; run it until `--check` is clean.
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
  `main` and `develop` runs five independent jobs, so each reports its own status
  and one failure never blocks the rest. Four run on the pinned build toolchain:
  `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`,
  and `Doc` (`cargo doc --no-deps --workspace` under `RUSTDOCFLAGS="-D warnings"`,
  so a broken intra-doc link fails the gate rather than shipping broken rustdoc,
  issue #161). The fifth, `Format (nightly)`, runs `cargo +<nightly> fmt --all
  --check` on the date-pinned nightly (the `NIGHTLY` env) to enforce the
  nightly-only formatting options (issue #60). Keep the tree clippy-clean,
  formatted at that level, and free of broken doc links.
- Read the applicable `~/.claude/docs/*.md` before editing code in that language.
- **Git:** commit and push only when asked. Never add a co-author trailer. Never
  self-assign PR credit.
