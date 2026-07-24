# Roadmap

The build order. Each phase is shippable and de-risks the next. Update as phases
complete or the plan changes.

## Phase 0: design of record (done)

This repo: `CLAUDE.md` plus the `docs/`. The design is written down before the
code, so the substrate is agreed before the heavier integration work starts.

## Phase 1: broker plus CLI MVP (standalone)

The smallest thing that beats the file transport, driven from a terminal.

- `crewd` broker: post a message, subscribe to a self-filtered inbox stream,
  read history. In-memory store with an on-disk log to start. Stamp every event
  with `ts` / `role` / `channel` / `kind` from day one, so observability stays a
  projection of the stream rather than a later retrofit (see
  `docs/observability.md`).
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

- Split the substrate into one consumable crate, CLI and glue as consumers (issue
  #34, done): the `crew-substrate` umbrella re-exports the four substrate crates and
  defines the public API; the CLI depends only on it.
- Distribute it as a private Git dependency pinned to a release tag (issue #35,
  decided): a consumer takes `crew-substrate` via `git` + `tag`, no registry to run,
  private by the repository's access control. crates.io is deferred to the 1.0 line;
  see `docs/distribution.md` and `docs/architecture.md`. `publish = false` stays,
  since a Git dependency does not publish to a registry.
- Publish on tag with a semver policy (issue #36, done): a release is a `v<version>`
  tag on `main`; `release.yml` checks the tag matches the crate version, runs the full
  gate on the pinned toolchain, and cuts a GitHub Release so a consumer can pin it. The
  public API is `crew-substrate`'s re-exports; the per-tag release notes are the
  changelog. See `docs/distribution.md` (Semantic versioning, Changelog).

## Phase 5: Seraphim integration

crew's second front-end.

- Seraphim depends on the published substrate crate, adds a UI panel plus
  Postgres persistence.
- Observability views (see `docs/observability.md`): crew communication rendered
  into task history (reusing the `ci` / `lifecycle` / `screenshot` event
  pattern), a per-agent activity log, an aggregate activity log, and a live agent
  count on screen.
- Runewood consumes the same event stream to visualize the unit live.
- Railways adopt the broker so lanes can talk to each other.

## Capability tracks (parallel to the phases)

Cross-cutting tracks that harden crew into a team you command, watch, and trust.
Each is a milestone of its own; they layer onto the phase work rather than block
it.

- **Command & Control.** Brief the crew with the General's plain send (`crew brief`,
  issue #118, done: a free-form note to the commander by default, a role, or a channel,
  the operator-facing counterpart to the agent shim's `crew send`); interject to redirect
  and belay a role mid-task (`crew
  redirect` / `crew belay`, issue #38, done); rules-of-engagement approval gates for
  risky actions; pause, resume, and emergency stand-down per role and crew-wide
  (`crew pause` / `crew resume` / `crew standdown`, issue #41, done); direct override of
  the commander (`crew command <role>`, issue #42: the General orders a specialist itself,
  bypassing the commander while keeping it informed; and `crew reassign` moves an in-flight
  task from one role to another against the work ledger, informing both roles and the
  commander, done).
- **Coordination Robustness.** Worktree-per-role isolation (`worktrees` in the crew
  config, issue #43, done) with an integration step to follow (`crew integrate`, issue #44,
  done: merge each role's `crew/<role>` branch into one branch, surface conflicts rather than
  drop them, and run the acceptance checks on the merged result), a work ledger with claims
  (`crew_claim` / `crew_ledger`, issue #45, done), lane-ownership enforcement
  (`lane_enforcement` and the `crew_lane` tool, issue #46, done), an adversarial done-gate
  (`crew_submit` / `crew_verdict` / `crew_gate`, issue #47, done), and coordination-stall
  detection (the defibrillator's fleet-wide stall monitor, issue #48, done).
- **Team Memory + Cockpit.** A shared situation board (`crew_board` / `crew_record`,
  issue #49, done: the crew's durable memory of decisions, interfaces, and gotchas,
  rebuilt from the log across a restart), a new-role briefing packet (`crew_briefing`,
  issue #50, done: the board plus a lane-scoped rolling summary, size-capped, so a fresh
  role catches up in seconds without the whole log), the `crew top` terminal cockpit, and
  push notifications on the actionable moments (`crew notify`, issue #52, done: a native
  notification when a question is asked, a role dies, or the crew stands down, configurable
  per moment and quiet on routine chatter, over the same event stream).
- **Economy.** Model per role (`ModelTier` plus a per-crew tier map and a sensible
  default mapping by role, issue #53, done: strong for the lead and architect, cheap for
  docs / ci / lint / test, standard for the builders, retunable in config with no code
  change); a shared token budget with per-role caps (issue #54, done: a crew-wide
  `token_budget` and per-role `token_cap`, enforced by idle-stopping a role or the crew at a
  cap and surfaced as a `budget` event over the stream, with the live token feed wired per
  turn from the stream-json activity parser (issue #24) through `Fleet::record_usage`, issue
  #177 done); auto-idle on quiet with cost and
  token telemetry (issue #55, done: the lifecycle machine idle-stops a quiet role, and a
  `GET /stats` rollup folds per-turn `telemetry` events and the roles' `lifecycle` events
  into tokens, cost, and working time per role and in aggregate, feeding the cockpit and the
  Seraphim stats); and subscription usage awareness (issue #56, done: one shared usage gauge
  auto-pauses new work when a reading crosses `CREW_BROKER_USAGE_THRESHOLD`, lifts at the
  window reset, and lets the operator resume early with `crew resume`, with the live
  rate-limit detection that feeds the gauge awaiting the stream-json activity parser, issue
  #24, wired into `RosterClient::report_usage` under issue #113).

## Parallel track: coworker skill transport upgrade (done)

Independent of the phases above. The `coworker` skill uses the broker instead of the
shared file plus `tail -F` monitor, so existing users get routing, no self-echo, and
bounded context without waiting on all of crew (issue #37). crew ships the upgraded
skill at `skills/coworker/`: it sends with `crew send`, watches its self-filtered inbox
with `crew watch --role <role>`, and shrinks to a role-card bootstrap, with a graceful
message when no broker is reachable. See `docs/communication.md`.

## Open decisions that gate later phases

- **Persistence backend** for the standalone broker (in-memory plus log vs
  SQLite).
- **Codex parity.** A Codex agent joins a crew through the CLI shim (issue #28):
  `crew register` / `crew send` / `crew inbox` / `crew roster` reach the broker through
  the same client the MCP tools use (see `docs/codex.md`). Open: auto-spawning a Codex
  agent per role from the crew config (a per-role runtime choice), and whether a
  Codex-native MCP path is worth adding beyond the shim.
- Whether the General always speaks through the commander or can address any role.
