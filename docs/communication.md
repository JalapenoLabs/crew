# Communication

How agents and the human talk. The design of record for topology, channels, the
message schema, and context management.

## Topology: hub-and-spoke, with escape valves

The default is a **commander** (the lead/router) at the hub. The General briefs
the commander; the commander decomposes the work and fans orders out to
role-scoped specialists.

Why not a free-for-all broadcast: broadcasting every message to every agent is
O(N) wakeups per message. Each agent burns a turn deciding "not mine," the
General drowns in a firehose, and there is nowhere to arbitrate when two agents
claim the same work. It gets worse with every role added.

The commander fixes all three:

- The General briefs **one** agent. The intent enters in one place and fans out.
  That is the "director commanding a unit" feeling, done right.
- Routing is intentional: direct messages go point-to-point, and only a genuine
  all-units call wakes everyone.
- Arbitration (work-claiming, interface disputes) resolves at the hub.

A pure star would make the commander a bottleneck and a game of telephone, so two
escape valves stay open:

- **Peer direct messages** for tight loops. Frontend and backend hammering out an
  API contract should not route every line through the commander.
- **`all-units`** as a deliberate, rare channel, not the default send.

The General addresses the commander by default and can drop into any role's
channel directly when they want.

This default is one rule the whole crew shares (issue #27): `Channel::resolve`
(`crew-core`) resolves a message's target from an optional role, an optional
channel, and the crew's commander. A named role wins; otherwise a named channel is
parsed; otherwise the message goes to the commander. Both the General's front-end
and an agent's `crew_send` obey it, so "brief the commander by default" means the
same everywhere. Every role card names the crew's commander (from the config, issue
#25), so each agent boots knowing where an unaddressed message goes and whether it
is itself the commander. The commander fans work out with `order` messages, one per
specialist (the `crew_order` MCP tool); the peer direct message and the rare
`all-units` broadcast stay open as the escape valves above.

## Channels

- `all-units` reaches every live role. Reserved for genuine team-wide orders.
- `@role` is a direct, point-to-point channel to one role, self-filtered so the
  sender never receives their own message.
- pair channels (for example `frontend+backend`) scope a tight two-party thread,
  such as negotiating an interface, without waking the rest of the unit.

The `crew-core` `Channel` model (issue #11) is the canonical form of these three
names and their membership: it parses a channel name, resolves which roles it
reaches, and canonicalizes a pair so `frontend+backend` and `backend+frontend` are
one channel with one name. An unrecognized name reaches no one, so a misaddressed
message wakes nobody rather than everybody. Routing filters a roster of live roles
through that membership test; self-echo (a sender receiving its own message) is
removed at delivery, not in the routing itself.

## Message schema

Messages are typed so a front-end can render them and the broker can route them.
This schema is modeled in `crew-core` and wired through the broker's message
endpoints (`MessageKind`, issues #6, #8 and #9): a `POST /channels/{channel}/messages`
posts to the channel named in the path, and `GET /events` reads the log.

- `id`, `from` (role or `general`), `channel`, `ts`. The broker owns `id`, `ts`,
  and the path `channel`: it stamps the id and timestamp on receipt and rejects a
  client that tries to supply any of them, so a timestamp is always the broker's.
- `kind`, one of:
  - `order` gives a task to a role (title, scope, owned paths, acceptance).
  - `question` asks for a decision, with optional suggested options.
  - `answer` responds to a question, naming the question it replies to (`in_reply_to`,
    the answered message's id), so a front-end threads the reply and the commander
    correlates the two.
  - `status` reports progress without asking anything.
  - `artifact` references a produced thing (a reference plus its kind: a branch, a
    PR, a file, or a route).
  - `note` is freeform prose for anything the above do not cover.
  - `redirect` steers a role mid-task without stopping it (the General's directive,
    honored at once; see Command and control below).
  - `belay` halts a role's current work and re-tasks it with a new order (the General's
    directive).
- `body` (markdown), plus the per-kind structured fields above, flattened onto the
  message on the wire.

Typed intents are what let the commander arbitrate and let a UI show an order
differently from a status ping.

Agents post the typed kinds through dedicated tools, not by hand-tagging a note:
`crew_send` posts a `note`, `crew_order` an `order`, `crew_ask` / `crew_answer` a
`question` / `answer` (issue #123), `crew_status` a `status`, and `crew_artifact` an
`artifact` (issue #167). This matters beyond rendering: the coordination-stall
detector (issue #48) keys on `question` events, so an agent that asks through `crew_ask`
lets an unanswered question or a mutual-wait deadlock surface on the stream, where a plain
note would stall the crew silently. `crew_answer` names the question it replies to with the
`in_reply_to` id the inbox surfaces, so the reply threads to its question and clears the
wait. `crew_status` and `crew_artifact` complete the set so every typed kind is reachable
by an agent: a progress `status` renders as a progress ping rather than prose, and an
`artifact` carries its `reference` and `artifact_kind` (`branch`, `pull_request`, `file`,
or `route`) so a UI can link it. Each has a `crew status` / `crew artifact` shim command
for a runtime without MCP.

## Delivery and notifications

An agent subscribes to its inbox stream and receives native notifications on new
messages, so it spends no context polling. As each message addressed to the role
is buffered, the MCP server pushes a native `notifications/message` (issue #174),
nudging the agent to call `crew_inbox` and read; the read drains the buffered
batch. Because the stream is self-filtered, there is no "ignore your own writes"
rule to remember. The broker, not a convention in a skill, guarantees it.

Two SSE feeds serve subscribers. `GET /stream` is the whole firehose: every event,
live. `GET /inbox?role=<role>` is a role's own view, the events addressed to it
(its direct `@role` channel, any pair channel it belongs to, and `all-units`), with
an event whose sender is the subscribing role dropped at the source, so self-echo is
impossible by construction (issue #10). Each event carries its log sequence as the
SSE `id`; on reconnect the client's `Last-Event-ID` resumes right after the last
event it received, replaying anything missed from the log before rejoining the live
tail, so a dropped connection loses nothing. A fresh connection with no cursor
starts at the live tail rather than replaying the whole history (that catch-up is
the rolling summary's job, below). The canonical channel model and membership are
issue #11.

## Command and control

### Briefing the crew

`crew brief "<message>"` is the General's plain send (issue #118): a free-form `note` posted
as the General, the operator-facing counterpart to an agent's `crew send`. It obeys the one
addressing rule above (`Channel::resolve`): `--to <role>` messages a role, `--channel <name>`
posts to `all-units` or a pair, and neither reaches the commander (`--commander` names it,
default `commander`). So `crew brief "..."` is the default brief that sets the unit to work,
and `crew brief --channel all-units "..."` is the General's broadcast. Like the directives
below it posts as the General, so it needs no role card, only the broker address (`--broker`,
else the `CREW_BROKER_*` environment).

### Steering a running agent

The General steers a running agent without tearing the crew down, the core "I am in
command" gesture (issue #38). Two directives:

- **`crew redirect <role> "..."`** injects a steering message. The role honors it at
  its next tool boundary, keeps its current task, and adjusts course.
- **`crew belay <role> "..."`** halts the role's current work and re-tasks it: the role
  stops what it is doing and takes the message as its new order.

Both are `MessageKind` variants (`redirect`, `belay`) posted from the General to the
role's direct `@role` channel, so they ride the ordinary delivery path above: the
broker stamps and stores them and fans them to the role's self-filtered inbox stream.
Delivery is the same whether the role is mid-turn or idle. A mid-turn injection lands
at the next tool boundary, never by killing the process: the message waits on the
inbox, and the agent reads it between tool calls. The role card briefs every agent to
honor a redirect or belay at once, and the inbox flags a directive (`[honor now]`), so
the "act immediately" contract is the agent's, and delivery is the broker's.

These are the General's directives, distinct from the commander's `crew_order` fan-out:
the commander decomposes and assigns; the General interjects to steer.

### Direct override

The General can bypass the commander to command a specialist directly, without breaking the
chain of command (issue #42). **`crew command <role> "<order>"`** posts an `order` from the
General straight to the role's `@role` channel, so the specialist gets the task, and then a
note to the commander's feed announcing the direct order. The commander is informed rather
than bypassed silently: it sees the override on its inbox and adjusts its plan around it,
instead of the work vanishing behind its back. Ordering the commander itself carries no
notice, since it is the addressee. `--scope` and `--acceptance` fill the order's fields, and
`--commander` names the commander to inform (default `commander`).

This is the deliberate override, not the default: briefing the commander (`Channel::resolve`
above) is unchanged, and `crew command` is the explicit way to reach past it.

**`crew reassign <task> --to <role>`** is the override's other half: the General moves an
in-flight task from one role to another in the work ledger (issue #45). It POSTs to
`/ledger/reassign`, which overrides the ledger's one-owner rule to take a held task from its
current holder, keeps the task's state so the work moves in place, and publishes a `ledger`
event with the new owner so the change is authoritative on the stream. It then posts a note to
each party: the old owner is told to hand off, the new owner to pick the work up, and the
commander that the General moved it (unless the commander is one of the two roles). An optional
`--from <role>` guards against a stale view: the broker refuses the move unless that role still
holds the task, and it also refuses a task no one holds (nothing in flight) or one the target
already owns, surfacing the conflict rather than silently misfiring.

## Secret scrubbing

A crew agent may echo a token it was handed into a message. The broker masks a
configured set of secret values out of every event before it persists or streams
it, so a leaked secret reaches neither the stored log nor a subscriber. Masking
runs once through a scrubber built from `CREW_BROKER_SECRETS`, longest secret
first, and keeps a recognized token's prefix and last four characters so an
operator can still tell two tokens apart without either being revealed. The
persisted log and the live stream always carry the same scrubbed event.

## Context management

The failure mode of the old file transport was unbounded growth: a fresh agent
read the entire history and spent 100k tokens before doing anything. crew bounds
it:

- **Rolling summary.** `GET /history?summary=true` returns a compaction of the
  older events plus the recent tail (issue #19), so joining is cheap. The response
  is `{ summary, tail }`: `tail` is the most recent events kept raw (sized by
  `limit`), and `summary` folds everything older into bounded aggregates,
  independent of how long the log has run. The compaction is a deterministic
  projection of the typed stream, not an LLM rendering (the broker has no model):
  it carries the summarized event count and time span, per-sender and per-kind
  counts, lifecycle transition counts, and a capped digest of the most recent
  orders and artifacts, plus a one-line headline. The same filters apply
  (`channel`, `role`, `kind`, `task`, `since`), so a joiner can summarize one task
  or channel. A fresh SSE connection with no cursor starts at the live tail, so the
  summary is the deliberate catch-up path.
- **Scoped reads.** A role fetches its own channels, not the whole stream.
- **Pruning.** Old raw events age out behind the summary in the joiner's view: the
  summary stands in for them so a joiner never reads them. Physically dropping the
  aged-out events from storage (bounding the broker's own footprint over a very long
  run) is a later optimization; the durable log stays the append-only source of
  truth in the meantime.
- **Situation board.** The rolling summary bounds the *transient* stream; the shared
  situation board bounds re-derivation (issue #49). It is the crew's durable memory,
  distinct from the message stream: agreed interfaces, decisions and their rationale, and
  known gotchas. A role reads it with `crew_board` before re-deriving a settled decision,
  and records one with `crew_record` so no one relitigates it; the commander curates it. It
  is a projection of `board` events, so it is auditable and rebuilt from the durable log
  across an idle-stop or a restart (see `docs/observability.md`, the situation board).
- **New-role briefing packet.** The three bounded pieces come together at boot (issue
  #50): a freshly spawned role catches up from its role card, the current decision board,
  and a rolling summary scoped to its own lane, never the raw transcript. `GET
  /briefing?role=<role>` (the `crew_briefing` tool) assembles the board plus a summary of
  the role's timeline (what it sent and what is addressed to it, so its lane and the work
  at hand), renders it to text, and caps it to a byte budget, reporting the measured size
  so the packet stays small no matter how long the mission has run. A role calls
  `crew_briefing` first thing on boot, so it joins mid-mission with bounded context and
  acts in its lane in seconds rather than reading the whole log. The supervisor also folds
  the packet into the agent's opening turn at spawn (issue #122), fetched then, not at
  provision, so it is current for a lazily started role, and best-effort so an unreachable
  broker still boots the agent on its card briefing; the tool stays the re-read path. See
  `docs/roles.md` (the briefing packet).

## Relationship to the coworker skill

The `coworker` skill is crew's ancestor: a shared markdown file plus a `tail -F`
monitor. Its transport is what crew replaces.

crew ships the upgraded skill at `skills/coworker/` (issue #37). It keeps the skill's
shape, an agent scoped to a lane that talks to its teammates, and swaps the transport:
the shared file becomes `crew send`, and the `tail -F` monitor becomes `crew watch
--role <role>`, a role's self-filtered inbox stream. So an existing coworker user gets
routing, no self-echo, and a bounded catch-up by pointing the skill at a running
broker, without waiting on the rest of crew. The skill shrinks to a role-card
bootstrap (you are role X, here is your lane, here is the broker) because the broker
now owns the routing, self-echo filtering, and catch-up the skill used to describe by
convention. If no broker is reachable, the skill says so and asks for one rather than
falling back to a file.
