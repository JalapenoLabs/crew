# Crew config

The declarative config that describes a crew, so `crew up` has something to read
(issue #25). Update this file when the format or the defaults change.

The config is a TOML document. It names the roles and the lane each owns, the model
they run, the repos in scope, the idle-stop timeout, and which role is the commander.
The type is `crew_core::CrewConfig`; it is broker-agnostic and produces the per-role
`RoleCard`s with `to_cards(&broker)`, taking the broker address at that point (the
`RoleCard` format itself is in `docs/roles.md`).

## Format

```toml
commander = "commander"   # the lead and router the General briefs
idle_stop = "10m"         # how long a role may be quiet before it is stopped
repos = ["api", "web"]    # the repos in scope
worktrees = true          # give each role its own git worktree (issue #43)
lane_enforcement = "warn" # what happens on an out-of-lane edit (issue #46)

[models]                  # what each model tier resolves to (issue #53)
strong = "opus"           # the lead and architect
standard = "sonnet"       # the builders
cheap = "haiku"           # docs, ci, lint, test

[[roles]]
role = "commander"
owned_paths = []          # the commander routes; it owns no lane
                          # (defaults to the strong tier)

[[roles]]
role = "backend"
owned_paths = ["api/", "db/"]
acceptance = "Tests green, migrations reversible."
model = "haiku"           # an exact model, the escape hatch that wins over any tier

[[roles]]
role = "frontend"
owned_paths = ["web/"]    # defaults to the standard tier

[[roles]]
role = "qa"
owned_paths = ["tests/"]
tier = "cheap"            # an explicit tier for this role
```

Every field is optional and takes a default:

- `roles` defaults to the **default crew**: `commander`, `backend`, `frontend`, `qa`
  (see `docs/roles.md`). Each role's `owned_paths` and `acceptance` default to empty, and
  its model resolves through the tier system below.
- `commander` defaults to `commander`.
- `idle_stop` defaults to `5m`. It accepts a number of seconds or a number with an
  `s` / `m` / `h` suffix (`30s`, `5m`, `2h`).
- `repos` defaults to empty.
- `worktrees` defaults to `false`. With it on and `repos` set, the supervisor gives
  each role its own git worktree of those repos (on a `crew/<role>` branch), so
  parallel roles never clobber each other's edits (issue #43). An unchanged worktree is
  cleaned up on stand-down; one with uncommitted changes is kept for integration (#48).
- `lane_enforcement` defaults to `warn`. It sets what happens when a role reaches
  outside its owned paths (issue #46): `warn` reports the crossing to the unit and lets
  the role proceed, `block` reports and refuses the out-of-lane edit, and `off` disables
  the check. The policy is crew-wide and rides each role card; a role checks a path with
  the `crew_lane` tool before editing outside its lane. See `docs/roles.md` (lane
  enforcement) and `docs/observability.md` (the `boundary` event).

An empty config document (`""`) therefore resolves to the default crew, so `crew up`
with no config still brings a full unit online.

## Model per role (issue #53)

The crew spends its strong-model budget where reasoning matters and runs a cheap model
everywhere else. Each role runs a **model tier**, an intent rather than an exact build:

- `strong`: the lead and the architect, where reasoning pays off.
- `standard`: the builders (backend, frontend, qa) doing the real work.
- `cheap`: the mechanical roles (docs, ci, release, lint, test, the `sdet` split).

The `[models]` table maps each tier to a concrete model alias, defaulting to the Claude
Code aliases `opus` / `sonnet` / `haiku` (which resolve to the current build of each).
Remapping a tier retunes spend for every role on it at once: set `cheap = "sonnet"` and
every cheap role moves up, with no per-role edit and no code change.

Every role gets a **sensible default tier by name**: the lead roles default to `strong`,
the mechanical roles to `cheap`, and every other role (including a custom one) to
`standard`. So the default crew already spends well with no model config at all.

A role's model resolves most-specific-first:

1. the role's exact `model`, if set (the escape hatch that pins a build precisely);
2. else the role's explicit `tier`, resolved through `[models]`;
3. else a crew-wide `model`, if set (runs every un-tiered role on one build);
4. else the default tier for the role's name, resolved through `[models]`.

So a quick whole-crew override is `model = "sonnet"` at the top level, a per-role bump is
`tier = "cheap"` (or `tier = "strong"`) on that role, and a precise pin is `model =
"..."` on that role.

## Validation

The config validates itself; an invalid one fails with a precise message that names
the offending value:

- **Unknown commander.** The `commander` must be one of the declared roles, or the
  error lists the roles it could be.
- **Overlapping ownership.** No two roles may own overlapping lanes. Lanes are
  directory boundaries, so `api/` and `api/routes/` overlap (one nested under the
  other) and so do `api` and `api/` (the same lane written two ways), but `api/` and
  `apiv2/` do not (distinct directories that only share a string prefix).
- **Duplicate or empty role.** Every role's name must be present and declared once.
- **Malformed input.** A TOML syntax error or an unknown field (a typo like `modle`)
  is rejected at parse time, naming the field.

Inspect a `ConfigError` with `is_parse()` and `is_invalid()`; its `Display` carries
the reason.
