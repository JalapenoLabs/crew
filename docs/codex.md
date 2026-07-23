# Codex adapter (CLI shim)

How an agent on a runtime without MCP joins a crew, so a unit can mix runtimes
(issue #28). Update this file when the shim or its parity with the MCP path changes.

## Why a shim

A Claude Code agent gets the crew tools over MCP: the supervisor registers the
`crew-mcp` server once and the agent calls `crew_send`, `crew_inbox`, and
`crew_roster` as tools (see `docs/architecture.md`). A runtime without that MCP
surface, such as Codex, cannot load those tools. The CLI shim is the fallback: the
same operations as `crew` subcommands the agent shells out to.

The shim is thin on purpose. Each command uses the very same `Broker` client the MCP
server dispatches to (`crew_client::Broker`, the shared client crate, issue #129), so a
shim agent's I/O lands on the broker identically to the MCP path: same registration,
same message shape, same self-filtered inbox. Parity comes from sharing the client, not
from re-implementing it. The client lives in its own crate, so the shim no longer
depends on `crew-mcp` for it, and MCP-specific churn leaves the shim untouched.

## The commands

Every command boots from the same role context the `crew-mcp` binary reads:

- `CREW_ROLE_CARD` points at a role card (`docs/roles.md`) naming the role, its lane,
  and the broker address. This is what the supervisor writes.
- Failing that, `CREW_ROLE` names the role and the broker's own `CREW_BROKER_HOST` /
  `CREW_BROKER_PORT` give the address, for a bare manual boot with an empty lane.

So the supervisor hands a Codex agent the same environment it hands a Claude agent;
the agent just shells out to `crew` instead of calling a tool.

| Command | Acts as | Mirrors |
| --- | --- | --- |
| `crew register` | registers the role and its lane on the roster | the MCP server's boot registration |
| `crew send [--to ROLE] [--channel CHAN] BODY` | posts a note as the role | `crew_send` |
| `crew ask [--to ROLE] [--channel CHAN] [--option TEXT]... BODY` | asks a typed question (the kind stall detection keys on) | `crew_ask` |
| `crew answer [--to ROLE] [--channel CHAN] --in-reply-to ID BODY` | answers a question, naming its id | `crew_answer` |
| `crew inbox` | prints the messages addressed to the role, each with its id | `crew_inbox` |
| `crew roster` | lists the unit's roles, lanes, and liveness | `crew_roster` |

A Codex agent participates like this:

1. On boot, run `crew register`, so the unit sees it on the roster and the stream.
2. During work, `crew send` to message a teammate, a channel, or the commander, and
   `crew inbox` to read what is addressed to it.

## Parity and gaps

The shim reaches the same broker with the same client, so a shim agent appears on the
roster and the stream exactly as an MCP agent does, and its messages route and
self-filter the same way. These gaps remain, by the nature of a stateless CLI:

- **No per-session inbox cursor.** The MCP server is one long-lived process, so
  `crew_inbox` returns only what arrived since the last call. Each `crew inbox` is its
  own short-lived process with no memory, so it reports every message currently
  addressed to the role. An agent that wants "new since last look" tracks that itself
  for now; a persisted per-role cursor is a future enhancement.
- **No push.** The MCP roadmap subscribes to the broker's per-role SSE stream for
  native notifications. The shim polls with `crew inbox`; it does not hold a stream
  open.
- **No `crew_order` yet.** The MCP surface added `crew_order` for the commander to
  issue a structured order (issue #27); the shim does not expose it, so a shim agent
  sends notes, questions, and answers (`crew send` / `crew ask` / `crew answer`) but
  cannot yet issue a structured order. A shim agent
  still reads orders addressed to it: `crew inbox` renders an order's structured detail
  the same way the MCP path does. Adding a `crew order` subcommand is a small follow-up.
- **Auto-spawned from the config (issue #128).** `crew up` spawns each role on its
  configured runtime: a per-role `runtime` in the crew config (`claude`, the default, or
  `codex`) tells the supervisor which CLI to launch. A `codex` role is spawned as a
  headless `codex exec` wired to the shim, handed the same `CREW_ROLE_CARD` environment as
  a Claude role, and its briefing is adapted to name the `crew <cmd>` shim invocations
  instead of the MCP tools; the MCP server is registered only when the crew has a Claude
  role, so a Codex-only unit never needs `claude` on `PATH`. The broker and roster do not
  care which runtime produced a role, so `crew up` brings up a mixed unit in one command.
  A Codex agent can still join a running crew through the shim, launched by the operator or
  a wrapper script, the same way. (The exact `codex exec` autonomy flags may need tuning
  against the installed `codex` version.)

## References

- `docs/architecture.md` for the MCP surface the shim mirrors and the supervisor that
  spawns agents.
- `docs/roadmap.md` for the Codex parity decision (CLI shim now, a Codex-native path
  open).
