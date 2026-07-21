# Roadmap

The build order. Each phase is shippable and de-risks the next. Update as phases
complete or the plan changes.

## Phase 0: design of record (done)

This repo: `CLAUDE.md` plus the `docs/`. The design is written down before the
code, so the substrate is agreed before the heavier integration work starts.

## Phase 1: broker plus CLI MVP (standalone)

The smallest thing that beats the file transport, driven from a terminal.

- `crewd` broker: post a message, subscribe to a self-filtered inbox stream,
  read history. In-memory store with an on-disk log to start.
- `crew` CLI: `send`, `watch`, and a manual two-agent flow (the General wires two
  Claude Code sessions to the broker).
- Proves the transport end to end and lets us dogfood in a terminal immediately.

## Phase 2: MCP surface plus role bootstrap

Agents coordinate with real tools instead of a CLI shim.

- MCP server: `crew_send`, `crew_inbox`, `crew_roster`.
- Role cards: a role boots knowing its lane and how to reach the unit.
- Typed messages (order, question, status, artifact) with a rolling summary.

## Phase 3: supervisor and auto-spawned unit

The "one command brings up a team" experience.

- Supervisor spawns one agent per role from a crew config, with lifecycle
  (lazy start, idle-stop, restart).
- `crew up` brings the whole unit online with roles assigned.
- Commander topology live: the General briefs the commander, the commander fans
  orders out.

## Phase 4: publish the substrate crate

Make the substrate a consumable package under the **JalapenoLabs** org, so the
CLI and Seraphim depend on the same versioned crate.

- Split the substrate (broker + supervisor) into its own crate, CLI and glue as
  consumers.
- Wire the chosen distribution path (see the open decision below).
- CI publishes on tag.

## Phase 5: Seraphim integration

crew's second front-end.

- Seraphim depends on the published substrate crate, adds a UI panel plus
  Postgres persistence.
- Railways adopt the broker so lanes can talk to each other.

## Parallel track: coworker skill transport upgrade

Independent of the phases above. Upgrade the `coworker` skill to use a broker
instead of the shared file plus `tail -F` monitor, so existing users get routing,
no self-echo, and bounded context without waiting on all of crew. Can land as
soon as Phase 1's broker is usable.

## Open decisions that gate later phases

- **Distribution registry.** GitHub Packages has no cargo registry, so the choice
  is: a private Git dependency (simplest, private, no infra; the current lean), a
  crates.io publish (public, real versioning), or a private cargo registry
  (org-private with `cargo publish`, but a service to run or pay for). See
  `docs/architecture.md` for the full tradeoff.
- **Persistence backend** for the standalone broker (in-memory plus log vs
  SQLite).
- **Codex parity** path (CLI shim vs native).
- Whether the General always speaks through the commander or can address any role.
