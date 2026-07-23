# Roles

The roster and the ownership model. Update this file when the default crew or the
ownership rules change.

## The chain of command

**You are the General.** You supply the strategic intent. crew gives you a
**commander**, the rank below you, who turns intent into orders and reports back.
The commander directs; it does not take the field. Below the commander are the
specialists, each owning a lane of the codebase.

You brief the commander by default, but command stays yours. You can steer any role
mid-task without standing the crew down, with `crew redirect <role>` (adjust course,
keep the task) and `crew belay <role>` (halt and re-task); the role honors the
directive at once (see `docs/communication.md`, command and control). A brake and a
kill switch gate the crew's work: `crew pause [role]` and `crew resume [role]` gate one
role or the whole crew, and `crew standdown` halts every role at once and preserves the
state, so the crew is recoverable. A gated role pulls no new work until you lift it; the
state is visible on the roster and the stream (see `docs/observability.md`).

## The ownership model

A role owns a boundary that **already exists in the tree**. Most repos hand you
clean ones: `api/`, `frontend/`, `workspace/`, `.github/`. An agent that owns a
directory has a crisp lane and rarely collides with a teammate. Ownership is the
organizing principle, not ranks for their own sake.

Two rules keep a unit healthy:

1. **Roles trace directory boundaries.** If two roles keep editing the same
   files, the split is wrong; redraw it.
2. **Prefer few busy agents over many idle ones.** Every idle specialist still
   wakes on channel traffic and spends context deciding a message is not theirs.
   An idle role is a running tax. Spin roles up on demand and stand them down when
   the push is over.

Ownership by directory sets the lanes; the **work ledger** enforces them on live work
(issue #45). A role claims a task before it starts (`crew_claim`, or the shim `crew
claim`) and moves the claim to `in_progress`, `blocked`, and `done` as it goes, so two
roles never grab the same work or edit the same file blind. The broker keeps one owner
per task and refuses a conflicting claim, naming the holder, so a clash surfaces rather
than races. `crew_ledger` shows who holds what. See `docs/observability.md`.

A crew is described by a declarative config (`crew_core::CrewConfig`, issue #25): the
roles and the lane each owns, the model they run, the repos in scope, the idle-stop
timeout, and the commander. `crew up` reads it, and it validates itself (an unknown
commander or an overlapping ownership boundary fails with a precise message). Omit it
and the default crew below applies. See `docs/config.md`.

## Default crew (start with 3 to 4)

- **commander** is the lead and router. It owns decomposition, interface
  decisions, arbitration, and the interface to the General. It issues orders and
  reports back; it does not write feature code. This is the agent the General
  briefs.
- **backend** owns server code, the database, and migrations (for example
  `api/`).
- **frontend** owns the UI (for example `frontend/`).
- **qa** owns verification and tests. It is the "did it actually work" gate. Keep
  unit and e2e in one role at first.

## On-demand roster

Spin these up for a specific push, then stand them down:

- **ci / release** owns the pipeline (`.github/workflows`, Earthfile, Docker
  builds). Bring it up when the work touches CI.
- **sdet-unit** and **sdet-e2e** are the split of `qa` for a test-heavy effort.
- **security** reviews a change for an authorization or secrets gap.
- **docs** keeps the design docs and user-facing text current.

## Role cards

Each role boots with a **role card**: its name, its owned paths, its acceptance
bar, and how to reach the unit (the broker plus the MCP tools). The card is the
thin bootstrap; the coordination rules live in crew itself, not restated per
agent. This is the shape the `coworker` skill should shrink to once crew exists:
"you are the backend role, here is your lane, here is the broker."

### Format

A card is a TOML document, chosen so a human can read and author one at a glance
(issue #18). The type is `crew_core::RoleCard`:

```toml
role = "backend"
owned_paths = ["api/", "db/"]
acceptance = "Tests green, migrations reversible, no clippy warnings."
commander = "commander"

[broker]
host = "127.0.0.1"
port = 2739
```

`role` and `[broker]` are required; `owned_paths` and `acceptance` default to
empty, and `commander` defaults to `commander`. Only these per-agent facts live
here. The channels, the message schema, and the chain of command are common to the
crew and stay in crew.

The `commander` names the unit's hub (issue #27), so every card carries it. From
it a role knows two things: where an unaddressed message goes (to the commander)
and whether it is itself the commander. The briefing reflects that: the commander's
card states its duties (decompose the brief, issue orders with `crew_order`,
arbitrate at lane boundaries, report to the General), and a specialist's card names
its commander and says an unaddressed `crew_send` reaches it.

### Loader

One loader serves both boot paths, so a card means the same thing everywhere:

- **Supervised.** `crew_supervisor::provision` writes a role's card into the
  agent's directory and returns the `CREW_ROLE_CARD` path plus the briefing. The
  supervisor spawns the agent with that environment set.
- **Standalone.** The `crew-mcp` server reads `CREW_ROLE_CARD`, parses it with
  `RoleCard::from_toml`, and registers the role on the roster so the unit sees it.
  Without a card it falls back to `CREW_ROLE` plus the broker's own config, so a
  bare manual boot still works.

`RoleCard::briefing()` renders the thin bootstrap prompt: it names the role,
states its lane and acceptance bar, and points at the broker and the MCP tools,
and stops there. That prompt is the shape the `coworker` skill shrinks to.

## Sizing guidance

Match the crew to the mission:

- A focused change: commander plus one or two specialists.
- A feature across the stack: commander, backend, frontend, qa.
- A test or pipeline hardening push: add ci and the sdet split for its duration.

When in doubt, start smaller. Adding a role mid-mission is cheap; a barracks full
of idle specialists is not.
