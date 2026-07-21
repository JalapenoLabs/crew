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

## Repo layout (planned)

```
CLAUDE.md          README.md          .gitignore
docs/
  architecture.md    the substrate: broker, supervisor, MCP surface, distribution
  communication.md   topology, channels, message schema, context management
  roles.md           the roster and the ownership model
  roadmap.md         phased plan
crates/            (planned) broker + supervisor + cli + mcp
```

## Status

Design stage. No code yet. This repo currently holds the design of record. See
`docs/roadmap.md` for the build order.

## Local conventions

- **No em dashes** in any user-facing text.
- **Rust toolchain pinned** (`rust-toolchain.toml`); use `cargo +<pinned>`.
- **eyre** for application errors, **mimalloc** as the global allocator, the
  clippy lint set from `~/.claude/docs/rust.md`.
- Read the applicable `~/.claude/docs/*.md` before editing code in that language.
- **Git:** commit and push only when asked. Never add a co-author trailer. Never
  self-assign PR credit.
