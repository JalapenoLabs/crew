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

## Channels

- `all-units` reaches every live role. Reserved for genuine team-wide orders.
- `@role` is a direct, point-to-point channel to one role, self-filtered so the
  sender never receives their own message.
- pair channels (for example `frontend+backend`) scope a tight two-party thread,
  such as negotiating an interface, without waking the rest of the unit.

## Message schema

Messages are typed so a front-end can render them and the broker can route them.
This schema is modeled in `crew-core` and wired through the broker's `/events`
endpoints (`MessageKind`, issues #6 and #8):

- `id`, `from` (role or `general`), `channel`, `ts`.
- `kind`, one of:
  - `order` gives a task to a role (title, scope, owned paths, acceptance).
  - `question` asks for a decision, with optional suggested options.
  - `answer` responds to a question.
  - `status` reports progress without asking anything.
  - `artifact` references a produced thing (a reference plus its kind: a branch, a
    PR, a file, or a route).
  - `note` is freeform prose for anything the above do not cover.
- `body` (markdown), plus the per-kind structured fields above, flattened onto the
  message on the wire.

Typed intents are what let the commander arbitrate and let a UI show an order
differently from a status ping.

## Delivery and notifications

An agent subscribes to its inbox stream and receives native notifications on new
messages, so it spends no context polling. Because the stream is self-filtered,
there is no "ignore your own writes" rule to remember. The broker, not a
convention in a skill, guarantees it.

A role subscribes over `GET /inbox?role=<role>`, a Server-Sent-Events stream of
the events addressed to it: its direct `@role` channel, any pair channel it
belongs to, and `all-units` (issue #10). The broker drops an event whose sender is
the subscribing role at the source, so self-echo is impossible by construction.
Each event carries its log sequence as the SSE `id`; on reconnect the client's
`Last-Event-ID` resumes the stream right after the last event it received,
replaying anything missed from the log before rejoining the live tail, so a
dropped connection loses nothing. A fresh connection with no cursor starts at the
live tail rather than replaying the whole history (that catch-up is the rolling
summary's job, below). The canonical channel model and membership are issue #11.

## Context management

The failure mode of the old file transport was unbounded growth: a fresh agent
read the entire history and spent 100k tokens before doing anything. crew bounds
it:

- **Rolling summary.** `history?summary=true` returns a compaction of older
  messages plus the recent tail, so joining is cheap.
- **Scoped reads.** A role fetches its own channels, not the whole board.
- **Pruning.** Old raw messages age out behind the summary once compacted.

## Relationship to the coworker skill

The `coworker` skill is crew's ancestor: a shared markdown file plus a `tail -F`
monitor. Its transport is what crew replaces. Upgrading the skill to use a broker
instead of a file is a **separate, tracked effort** (see `roadmap.md`), so the
skill improves for existing users without waiting on all of crew.
