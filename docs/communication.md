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
- `body` (markdown), plus the per-kind structured fields above, flattened onto the
  message on the wire.

Typed intents are what let the commander arbitrate and let a UI show an order
differently from a status ping.

## Delivery and notifications

An agent subscribes to its inbox stream and receives native notifications on new
messages, so it spends no context polling. Because the stream is self-filtered,
there is no "ignore your own writes" rule to remember. The broker, not a
convention in a skill, guarantees it. Today a subscriber connects to `GET /stream`,
an SSE feed of every event; the per-role self-filtering is a later refinement.

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

- **Rolling summary.** `history?summary=true` returns a compaction of older
  messages plus the recent tail, so joining is cheap.
- **Scoped reads.** A role fetches its own channels, not the whole board.
- **Pruning.** Old raw messages age out behind the summary once compacted.

## Relationship to the coworker skill

The `coworker` skill is crew's ancestor: a shared markdown file plus a `tail -F`
monitor. Its transport is what crew replaces. Upgrading the skill to use a broker
instead of a file is a **separate, tracked effort** (see `roadmap.md`), so the
skill improves for existing users without waiting on all of crew.
