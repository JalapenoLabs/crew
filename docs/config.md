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
model = "sonnet"          # the default model every role runs
commander = "commander"   # the lead and router the General briefs
idle_stop = "10m"         # how long a role may be quiet before it is stopped
repos = ["api", "web"]    # the repos in scope

[[roles]]
role = "commander"
owned_paths = []          # the commander routes; it owns no lane

[[roles]]
role = "backend"
owned_paths = ["api/", "db/"]
acceptance = "Tests green, migrations reversible."
model = "haiku"           # a per-role override of the crew default

[[roles]]
role = "frontend"
owned_paths = ["web/"]

[[roles]]
role = "qa"
owned_paths = ["tests/"]
```

Every field is optional and takes a default:

- `roles` defaults to the **default crew**: `commander`, `backend`, `frontend`, `qa`
  (see `docs/roles.md`). Each role's `owned_paths` and `acceptance` default to empty,
  and its `model` falls back to the crew default.
- `commander` defaults to `commander`.
- `model` defaults to `opus` (a Claude Code alias that resolves to the current build).
- `idle_stop` defaults to `5m`. It accepts a number of seconds or a number with an
  `s` / `m` / `h` suffix (`30s`, `5m`, `2h`).
- `repos` defaults to empty.

An empty config document (`""`) therefore resolves to the default crew, so `crew up`
with no config still brings a full unit online.

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
