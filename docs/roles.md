# Roles

The roster and the ownership model. Update this file when the default crew or the
ownership rules change.

## The ownership model

A role owns a boundary that **already exists in the tree**. Most repos hand you
clean ones: `api/`, `frontend/`, `workspace/`, `.github/`. An agent that owns a
directory has a crisp lane and rarely collides with a teammate. Ownership is the
organizing principle, not job titles for their own sake.

Two rules keep a crew healthy:

1. **Roles trace directory boundaries.** If two roles keep editing the same
   files, the split is wrong; redraw it.
2. **Prefer few busy agents over many idle ones.** Every idle specialist still
   wakes on channel traffic and spends context deciding a message is not theirs.
   An idle role is a running tax. Spin roles up on demand and tear them down when
   the push is over.

## Default crew (start with 3 to 4)

- **coxswain** (`cox`) is the lead and router. It owns decomposition, interface
  decisions, arbitration, and the human interface. It steers and calls the pace;
  it does not pull an oar (it does not write feature code). This is the agent the
  human briefs.
- **backend** owns server code, the database, and migrations (for example
  `api/`).
- **frontend** owns the UI (for example `frontend/`).
- **qa** owns verification and tests. It is the "did it actually work" gate. Keep
  unit and e2e in one role at first.

## On-demand roster

Spin these up for a specific push, then retire them:

- **ci / release** owns the pipeline (`.github/workflows`, Earthfile, Docker
  builds). Bring it up when the work touches CI.
- **sdet-unit** and **sdet-e2e** are the split of `qa` for a test-heavy effort.
- **security** reviews a change for an authorization or secrets gap.
- **docs** keeps the design docs and user-facing text current.

## Role cards

Each role boots with a **role card**: its name, its owned paths, its acceptance
bar, and how to reach the crew (the broker plus the MCP tools). The card is the
thin bootstrap; the coordination rules live in crew itself, not restated per
agent. This is the shape the `coworker` skill should shrink to once crew exists:
"you are the backend role, here is your lane, here is the broker."

## Sizing guidance

Match the crew to the work:

- A focused change: cox plus one or two builders.
- A feature across the stack: cox, backend, frontend, qa.
- A test or pipeline hardening push: add ci and the sdet split for its duration.

When in doubt, start smaller. Adding a role mid-flight is cheap; a room full of
idle specialists is not.
