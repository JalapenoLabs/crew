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
  - `answer` responds to a question.
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

## Delivery and notifications

An agent subscribes to its inbox stream and receives native notifications on new
messages, so it spends no context polling. Because the stream is self-filtered,
there is no "ignore your own writes" rule to remember. The broker, not a
convention in a skill, guarantees it.

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
- **Scoped reads.** A role fetches its own channels, not the whole board.
- **Pruning.** Old raw events age out behind the summary in the joiner's view: the
  summary stands in for them so a joiner never reads them. Physically dropping the
  aged-out events from storage (bounding the broker's own footprint over a very long
  run) is a later optimization; the durable log stays the append-only source of
  truth in the meantime.

## Relationship to the coworker skill

The `coworker` skill is crew's ancestor: a shared markdown file plus a `tail -F`
monitor. Its transport is what crew replaces. Upgrading the skill to use a broker
instead of a file is a **separate, tracked effort** (see `roadmap.md`), so the
skill improves for existing users without waiting on all of crew.
