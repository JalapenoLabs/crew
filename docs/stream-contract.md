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
- **`kind`** is the typed payload, tagged: `{ "kind": "<discriminant>", "data": <payload>
  }`. The discriminant is one of `message`, `lifecycle`, `activity`, `ledger`,
  `boundary`, `verification`, `board`, `budget`, `telemetry`, `usage`, `stall`, or
  `mission`; each payload is documented below.

These four envelope fields (`ts`, `from`, `channel`, `kind`) are guaranteed present and
non-blank on every event, enforced at the broker's single publish choke point (issue
#29). A consumer can rely on them without null checks.

## The event kinds and their payloads

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
| `answer` | `in_reply_to` (the answered question's `id`) | Respond to a question. |
| `status` | none | Report progress. |
| `artifact` | `reference`, `artifact_kind` (`branch` / `pull_request` / `file` / `route`) | Point at a produced thing. |
| `note` | none | Freeform prose. |

Every message has `id` and `body`; only the fields in the table are added per intent.
An `answer`'s `in_reply_to` must name an existing `question` message: the broker rejects
one that does not with `400`, so a reply always threads to a real question and never
dangles (issue #211).

### `lifecycle` (a supervised state change)

`kind.data` is a single string: the transition. The envelope's `from` names the role
it is about, and `channel` is `all-units`.

```json
{ "ts": "…", "from": { "kind": "role", "id": "backend" },
  "channel": "all-units", "kind": { "kind": "lifecycle", "data": "started" } }
```

The transition is one of `started`, `idle`, `stopped`, `restarted`, `died`,
`recovered`, `paused`, `resumed`, or `stood_down`. `started` and `recovered` bring a role
up; `idle` parks it (still present); `stopped` and `died` take it down; `paused` /
`resumed` gate and ungate a role (issue #41); and `stood_down` is the crew's emergency
halt (issue #41). The crew's graceful finish is its own `mission` kind, below, not a
lifecycle transition.

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

### `ledger` (a work-ledger change)

`kind.data` is a change to the shared work ledger: a role claiming a work item or moving
its claim forward (issue #45). The envelope's `from` is the owner role and `channel` is
`all-units`.

```json
{ "kind": "ledger", "data": {
  "task": "login-endpoint",
  "owner": "backend",
  "state": "in_progress",
  "title": "Build the login endpoint"
} }
```

- `task` is the work item's stable key (a path, a feature name, or an order's title); two
  roles must not hold the same key at once.
- `owner` is the role that holds the claim (it also names the envelope's `from`).
- `state` is `claimed`, `in_progress`, `blocked`, or `done`; `done` releases the claim.
- `title` is a short human title for the ledger view; may be empty.

### `boundary` (a lane crossing)

`kind.data` is a role reaching outside its owned lane (issue #46). The envelope's `from`
is the role and `channel` is `all-units`.

```json
{ "kind": "boundary", "data": {
  "role": "backend",
  "path": "frontend/app.tsx",
  "blocked": true
} }
```

- `role` is the role that reached out of lane (it also names the envelope's `from`).
- `path` is the out-of-lane path it reached for.
- `blocked` is `true` when the crew's policy refused the edit, `false` when it only
  warned and let the role proceed.

### `verification` (a done-gate step)

`kind.data` is a step through the adversarial done-gate: a submission or a verdict (issue
#47). The envelope's `from` is the acting role (the owner on a submission, the verifier on
a verdict) and `channel` is `all-units`.

```json
{ "kind": "verification", "data": {
  "task": "Build the login endpoint",
  "owner": "backend",
  "verifier": "qa",
  "verdict": "passed",
  "detail": ""
} }
```

- `task` is the task under the gate, named by its (order) title.
- `owner` is the role whose work is under verification: the one that submitted it.
- `verifier` is the independent role that returned the verdict; **omitted** on the
  submission itself.
- `verdict` is `submitted` (awaiting a verifier), `passed` (an independent role could not
  break it, so it is done), or `failed` (a verifier broke it; the work returns to the
  owner).
- `detail` is the acceptance criteria claimed on a submission, or the specific failure on
  a failed verdict; empty when there is none.

### `board` (a situation-board change)

`kind.data` is a change to the shared situation board: an entry recorded or retracted
(issue #49). The envelope's `from` is the author role and `channel` is `all-units`.

```json
{ "kind": "board", "data": {
  "key": "auth-strategy",
  "section": "decision",
  "author": "commander",
  "body": "JWT with 15-minute access tokens; refresh via httpOnly cookie",
  "retracted": false
} }
```

- `key` is the entry's stable topic; recording the same key again updates the entry.
- `section` is `decision`, `interface`, or `gotcha`.
- `author` is the role that recorded or retracted it (it also names the envelope's
  `from`).
- `body` is the entry's content; empty on a retraction.
- `retracted` is `true` when this change removes the entry from the board.

### `budget` (a token-spend report)

`kind.data` is a token-spend report against the crew budget, and any cap it hit (issue
#54). The envelope's `from` is the role the spend is about and `channel` is `all-units`.

```json
{ "kind": "budget", "data": {
  "role": "backend",
  "role_spent": 82000,
  "role_cap": 100000,
  "crew_spent": 240000,
  "crew_budget": 500000
} }
```

- `role` is the role whose spend this reports.
- `role_spent` and `crew_spent` are cumulative token totals for the role and the whole
  crew.
- `role_cap` and `crew_budget` are the role's and the crew's caps; each is **omitted**
  when there is no cap (unbounded).
- `breach` is added only when the spend hits a ceiling: `role` when the role hit its cap
  (it idle-stops) or `crew` when the crew hit its budget (the whole crew idle-stops). It
  is **omitted** when the spend is still within budget.

### `telemetry` (a per-turn usage report)

`kind.data` is a per-turn token-and-cost report (issue #55): the tokens and cost a role
spent on one turn, emitted every turn whether or not a budget is set. The envelope's
`from` is the role and `channel` is `all-units`.

```json
{ "kind": "telemetry", "data": {
  "role": "backend",
  "tokens": 3200,
  "cost_micro_usd": 48000
} }
```

- `role` is the role whose turn this usage belongs to.
- `tokens` is the tokens the turn spent.
- `cost_micro_usd` is the turn's cost in micro-USD (millionths of a US dollar).

### `usage` (a shared-subscription reading)

`kind.data` is a shared-subscription usage reading and its auto-pause (issue #56): how
full the shared window is and whether the crew is paused on it. The envelope's `from` is
`general` and `channel` is `all-units`, since the gauge is crew-wide, not one role's
action.

```json
{ "kind": "usage", "data": {
  "percent": 92,
  "window_reset": "2026-07-23T09:00:00Z",
  "paused": true
} }
```

- `percent` is the window's fill against the shared subscription limit, a percent in
  `0..=100`.
- `paused` is `true` when this reading engaged the auto-pause (new work halts), `false`
  when it lifted (the window reset, or the operator resumed early).
- `window_reset` is when the window resets and the pause lifts, an RFC 3339 instant;
  present on a pause and **omitted** when the pause lifts.

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

### `mission` (the crew's graceful finish)

`kind.data` carries a `summary` of what shipped. The crew, typically through the
commander, reports the mission gracefully complete (issues #121, #155); the envelope's
`from` is the reporting role and `channel` is `all-units`. This is the true completion the
General is notified on, distinct from a `stood_down` emergency halt. It announces the
finish, it does not halt the crew.

```json
{ "kind": "mission", "data": { "summary": "shipped the auth gateway; all tasks verified" } }
```

- `summary` is a short account of what the mission shipped, rendered in the completion
  notification. It is an empty string when the reporter gave none.

Every kind above rides the same envelope and, except `mission`, is filterable by `kind`
on `GET /history` and `GET /stream` (see the filter list under Endpoints).

## Endpoints

### `GET /stream` (live, Server-Sent Events)

The whole unit's live feed. Each event arrives as one SSE record:

```
id: 42
data: {"ts":"…","from":{"kind":"role","id":"backend"},"channel":"all-units","kind":{"kind":"lifecycle","data":"started"}}
```

- The `data` line is the event JSON above.
- The `id` line is the event's **log sequence**, a monotonic integer assigned on append,
  never reused, and never renumbered, so it stays stable even after aged events are pruned
  from the log (issue #201). It is the cursor that bridges the live stream to history
  (below).
- A fresh connection starts at the **live tail**: it delivers events from the moment it
  connects, not the backlog. A consumer backfills the past through `/history`.
- **Reconnect resumes losslessly** (issue #134): send the last `id` you saw as a
  `Last-Event-ID` request header (an `EventSource` does this for you), and the stream
  replays the matching events after that cursor before returning to the live tail, so a
  dropped or lagged connection loses nothing without a separate `/history` call. The
  replay honors the same filter params as the live tail. With no `Last-Event-ID` the
  connection starts at the live tail, as above.
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
  instant). `kind` accepts a comma-separated set to keep several kinds in one query
  (e.g. `kind=message,ledger,verification`), so a consumer narrows to a subset
  server-side rather than fetching everything and filtering client-side. The same
  filters narrow `GET /stream`.
- Ordering is deterministic: by `ts`, then the event's stable sequence.
- Pagination: pass `limit` (default 100, max 1000) and `after=<cursor>`. `next_cursor`
  is the position to resume from; it is **omitted** on the last page.
- The cursor space is the **same** as the stream's `id`: a consumer that last saw
  `id: N` reconnects to `/stream` with `Last-Event-ID: N` to replay everything since on
  the stream itself (issue #134), or calls `GET /history?after=N` to page the gap
  without holding a stream open. Use `/history` for the initial backfill (a first load
  with no prior cursor) and for a large gap you would rather page; use the stream's own
  `Last-Event-ID` resume for an ordinary reconnect.

`GET /history` is the one read path for the stored log: it filters, orders, and
paginates with a ceiling (`limit` default 100, max 1000), so a read is always bounded
even as the log grows. An unpaginated full-log dump is intentionally not offered (issue
#209): page with `after=<cursor>`, or use `summary=true` (below) for bounded catch-up.

The broker prunes aged-out events so its memory and log stay bounded on a long-running
unit (issue #201). Only ephemeral kinds age out (`message`, `activity`, `boundary`,
`usage`, `stall`); the state-bearing kinds a projection rebuilds (`lifecycle`,
`verification`, `board`, `ledger`, `telemetry`, `budget`, `mission`) are kept regardless.
Because sequences are never reused, a reconnect whose `Last-Event-ID` names a pruned event
still resumes without gap or duplicate among the surviving events; the pruned events
themselves are simply older than the retention window and no longer replayable.

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

// On a dropped connection EventSource reconnects on its own, sending the last `id` as
// `Last-Event-ID`, and the stream replays what was missed (issue #134), so no manual
// catch-up is needed. For the initial backfill on first load, or a large gap, page it:
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
