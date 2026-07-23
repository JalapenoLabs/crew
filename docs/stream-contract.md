# Event stream consumer contract

The public contract an external consumer builds against to render a crew live, with
no crew-specific capture (issue #33). Runewood is the motivating consumer: agents are
entities, messages are particles between them, lifecycle transitions are spawns and
fades, and the live count sits on screen. This document is the stable surface; update
it in the same change whenever the wire shape or an endpoint changes.

The broker (`crewd`) serves this over plain HTTP + Server-Sent Events on loopback
(default `127.0.0.1:2739`, set by `CREW_BROKER_HOST` / `CREW_BROKER_PORT`). There is
no auth: the broker is loopback-only, so a consumer runs on the same host or behind an
operator's proxy. Nothing about the consumer is special to the broker; it subscribes
and reads, exactly like any other reader of the one event stream (see
`docs/observability.md`).

## The event envelope

Every item on the stream is one `crew_core::Event`, a single JSON object. The
envelope carries what every projection needs; the `kind` carries the payload:

```json
{
  "ts": "2026-07-23T05:49:00.123Z",
  "from": { "kind": "role", "id": "commander" },
  "channel": "@backend",
  "task": "550e8400-e29b-41d4-a716-446655440000",
  "kind": { "kind": "message", "data": { "id": "…", "kind": "order", "…": "…" } }
}
```

- **`ts`** is when it happened, an RFC 3339 / ISO 8601 UTC timestamp. The broker stamps
  it at the source; it is always present.
- **`from`** is who emitted it: `{ "kind": "role", "id": "<role>" }` for an agent, or
  `{ "kind": "general" }` for the human directing the crew. Always present.
- **`channel`** is who it is addressed to, a string: `all-units` (everyone), `@<role>`
  (one role), or `<a>+<b>` (a two-role pair, members in canonical sorted order).
  Always present.
- **`task`** is the task the event belongs to, a UUID, when a task context applies. It
  is **omitted** when there is none, so a consumer treats a missing `task` as "no task".
- **`kind`** is the typed payload, tagged: `{ "kind": "<message|lifecycle|activity>",
  "data": <payload> }`.

These four envelope fields (`ts`, `from`, `channel`, `kind`) are guaranteed present and
non-blank on every event, enforced at the broker's single publish choke point (issue
#29). A consumer can rely on them without null checks.

## The three kinds and their payloads

### `message` (inter-agent communication)

`kind.data` is a message: an `id`, a typed intent (the inner `kind` discriminant) with
its per-kind fields flattened alongside, and a markdown `body`.

```json
{ "kind": "message", "data": {
  "id": "9f1c…uuid",
  "kind": "order",
  "title": "Build the login endpoint",
  "scope": "POST /login only",
  "owned_paths": ["api/"],
  "acceptance": "tests green, no clippy warnings",
  "body": "coordinate the token shape with frontend"
} }
```

The intent (`data.kind`) is one of:

| `data.kind` | Extra fields | Meaning |
| --- | --- | --- |
| `order` | `title`, `scope`, `owned_paths`, `acceptance` | Give a role a scoped task. |
| `question` | `options` (array, may be empty) | Ask for a decision. |
| `answer` | none | Respond to a question. |
| `status` | none | Report progress. |
| `artifact` | `reference`, `artifact_kind` (`branch` / `pull_request` / `file` / `route`) | Point at a produced thing. |
| `note` | none | Freeform prose. |

Every message has `id` and `body`; only the fields in the table are added per intent.

### `lifecycle` (a supervised state change)

`kind.data` is a single string: the transition. The envelope's `from` names the role
it is about, and `channel` is `all-units`.

```json
{ "ts": "…", "from": { "kind": "role", "id": "backend" },
  "channel": "all-units", "kind": { "kind": "lifecycle", "data": "started" } }
```

The transition is one of `started`, `idle`, `stopped`, `restarted`, `died`,
`recovered`, `paused`, `resumed`, `stood_down`, or `mission_complete`. `started` and
`recovered` bring a role up; `idle` parks it (still present); `stopped` and `died` take it
down; `paused` / `resumed` gate and ungate a role (issue #41); `stood_down` is the crew's
emergency halt (issue #41); and `mission_complete` is the crew's graceful finish (issue
#121, the true completion the General is notified on, distinct from a stand-down).

### `activity` (an agent's own work)

`kind.data` is a typed activity item, parsed from the agent's `claude -p` stream-json.
The envelope's `from` is the role.

```json
{ "kind": "activity", "data": { "kind": "tool_call", "tool": "cargo" } }
```

Items: `{ "kind": "turn_started" }`, `{ "kind": "turn_ended" }`,
`{ "kind": "tool_call", "tool": "<name>" }`, `{ "kind": "output", "text": "<text>" }`.
The activity vocabulary is stable; the supervisor begins producing these once the
stream-json parser lands, so a consumer that handles them today needs no change then.

### `stall` (a coordination stall)

`kind.data` is a coordination stall the fleet-wide monitor detected or resolved (issue
#48, #120): the crew stuck waiting on itself rather than one dead agent. The envelope's
`from` is `general` and `channel` is `all-units`, since a stall is a crew-level finding,
not one role's action.

```json
{ "kind": "stall", "data": {
  "kind": "deadlock",
  "status": "detected",
  "roles": ["backend", "frontend"],
  "detail": "deadlock: backend waits on frontend, and frontend waits on backend"
} }
```

- `kind` is `deadlock` (a cycle of unanswered questions), `unanswered_question` (a
  one-sided wait), or `ledger_stall` (a held task with no forward motion).
- `status` is `detected` when the stall crosses the threshold and `resolved` once it
  clears, so a consumer lights a stall up and later takes it down off the same stream.
- `roles` are the roles caught in the stall, sorted; `detail` names who is waiting on
  what.

Other supervisor and broker kinds ride the same envelope and are filterable by `kind`:
`ledger` (issue #45), `boundary` (issue #46), `verification` (issue #47), `board` (issue
#49), `budget` (issue #54), `telemetry` (issue #55), and `usage` (issue #56).

## Endpoints

### `GET /stream` (live, Server-Sent Events)

The whole unit's live feed. Each event arrives as one SSE record:

```
id: 42
data: {"ts":"…","from":{"kind":"role","id":"backend"},"channel":"all-units","kind":{"kind":"lifecycle","data":"started"}}
```

- The `data` line is the event JSON above.
- The `id` line is the event's **log sequence**, a monotonic integer and its position in
  the durable log. It is the cursor that bridges the live stream to history (below).
- A fresh connection starts at the **live tail**: it delivers events from the moment it
  connects, not the backlog. The stream is live-only; a consumer backfills the past
  through `/history`.
- Periodic keep-alive comment lines (`:`) hold the connection open through idle spells.

### Catch-up: `GET /history`

Read the past without holding a stream open. Returns a JSON page, oldest first, with a
stable cursor:

```json
{ "events": [ …Event… ], "next_cursor": "58" }
```

- Filters (all optional, combine with AND): `channel`, `role` (sent by that role),
  `agent` (a role's full activity timeline: what it sent and received), `kind`
  (`message` / `lifecycle` / `activity` / `ledger` / `boundary` / `verification` /
  `board` / `budget` / `telemetry` / `usage` / `stall`), `task`, `since` (an RFC 3339
  instant).
- Ordering is deterministic: by `ts`, then log position.
- Pagination: pass `limit` (default 100, max 1000) and `after=<cursor>`. `next_cursor`
  is the position to resume from; it is **omitted** on the last page.
- The cursor space is the **same** as the stream's `id`: a consumer that last saw
  `id: N` on `/stream` calls `GET /history?after=N` to fetch everything since, then
  resumes the live stream. That is the reconnect and initial-backfill path.

`GET /events` returns the entire stored log as `{ "events": [ … ] }` in one response,
for a simple full read of a short-lived unit.

### Bounded catch-up: `GET /history?summary=true`

For a long-running unit, a full backfill is unbounded. `summary=true` returns a
compaction plus the recent raw tail, so joining costs bounded context:

```json
{
  "summary": {
    "event_count": 412,
    "since": "…", "through": "…",
    "headline": "412 earlier events across 4 roles",
    "senders": [ { "name": "backend", "count": 190 }, … ],
    "message_kinds": [ { "name": "status", "count": 120 }, … ],
    "lifecycle": [ { "name": "started", "count": 4 }, … ],
    "recent_orders": [ { "title": "…", "channel": "@backend", "from": "commander" }, … ],
    "recent_artifacts": [ { "reference": "…", "artifact_kind": "pull_request" }, … ]
  },
  "tail": [ …the most recent Events, raw… ]
}
```

The same filters apply, so a consumer can summarize one role, channel, or task. `limit`
sizes the raw tail.

### Roster snapshot: `GET /roster`

The current membership and the live agent count, a point-in-time projection a consumer
reads once and keeps current from the `lifecycle` events on the stream (issues #14, #32):

```json
{
  "count": { "live": 3, "working": 2, "idle": 1, "stopped": 0, "dead": 1 },
  "roles": [
    { "role": "backend", "owned_paths": ["api/"], "liveness": "working" },
    { "role": "commander", "owned_paths": [], "liveness": "working" }
  ]
}
```

`count.live` is the headline number: roles `working` or `idle` (present and up or
resumable). A `stopped` role has left the field and a `dead` one gave up, so neither is
counted, though both stay listed with their `liveness`.

## What a consumer renders from the stream alone

Everything a viz needs is on the one stream; there is no side channel to poll:

- **Agents (entities).** `lifecycle` events name the role in `from` and the transition in
  `data`: `started` / `recovered` spawn or revive an entity, `idle` parks it, `stopped` /
  `died` fade it. `GET /roster` gives the current set with their owned paths for an
  initial layout.
- **Messages (particles).** `message` events carry the source in `from`, the destination
  in `channel` (a role, a pair, or `all-units`), and the intent in `data.kind`, so an
  `order` can render differently from a `status`.
- **The live count.** Keep a running tally from the `lifecycle` transitions, or read the
  snapshot in `GET /roster` `count.live`. The two agree.
- **An agent's own work.** `activity` events (`from` the role) carry turn boundaries,
  tool calls, and output.

## Minimal subscribe example

Explore the live stream with `curl` (`-N` disables buffering so records arrive as they
happen):

```sh
curl -N http://127.0.0.1:2739/stream
```

A browser or Node consumer subscribes with `EventSource` and renders each event:

```js
const es = new EventSource("http://127.0.0.1:2739/stream");

es.onmessage = (e) => {
  const event = JSON.parse(e.data);           // the Event envelope
  const seq = Number(e.lastEventId);           // the log sequence (the `id` line)
  switch (event.kind.kind) {
    case "lifecycle": renderTransition(event.from.id, event.kind.data); break;
    case "message":   renderMessage(event.from, event.channel, event.kind.data); break;
    case "activity":  renderActivity(event.from.id, event.kind.data); break;
    default:          /* ignore unknown kinds; see Stability */ break;
  }
};

// On reconnect (or to backfill on first load), fetch what was missed, then resubscribe:
//   GET /history?after=<last seq>       // everything since a known point
//   GET /history?summary=true           // bounded catch-up on a long-running unit
```

The in-repo worked example is the `a_consumer_renders_the_unit_from_the_stream_alone`
integration test in `crates/crew-broker/tests/integration.rs`: it subscribes and derives
agents, a message, and the live count from the stream alone.

## Stability

The contract is **additive only**. A consumer must ignore what it does not recognize so
it keeps rendering across upgrades:

- **Unknown `kind` discriminants.** New event kinds, message intents, lifecycle
  transitions, activity items, or artifact kinds may be added. A consumer that meets one
  it does not handle skips it rather than failing.
- **Unknown fields.** New envelope or payload fields may be added; a consumer ignores
  fields it does not read.

Existing field names, shapes, and enum values do not change or disappear without a
version bump. The stream is typed, timestamped, and addressed by construction (issue
#29), so these guarantees hold for every event, forever.
