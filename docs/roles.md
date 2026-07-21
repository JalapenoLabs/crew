# Roles

The roster and the ownership model. Update this file when the default crew or the
ownership rules change.

## The chain of command

**You are the General.** You supply the strategic intent. crew gives you a
**commander**, the rank below you, who turns intent into orders and reports back.
The commander directs; it does not take the field. Below the commander are the
specialists, each owning a lane of the codebase.

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

## Sizing guidance

Match the crew to the mission:

- A focused change: commander plus one or two specialists.
- A feature across the stack: commander, backend, frontend, qa.
- A test or pipeline hardening push: add ci and the sdet split for its duration.

When in doubt, start smaller. Adding a role mid-mission is cheap; a barracks full
of idle specialists is not.
