# Observability

Design of record for capturing and surfacing "what is happening" across a crew:
inter-agent communication, per-agent and aggregate activity, and the live agent
count. Written with the Seraphim front-end in mind, but the substrate owns the
data.

## Principle: one event stream, many projections

The broker and supervisor together emit a single, typed, timestamped, addressed
**event stream**. Every observability view is a *projection* of that one stream,
never a separate capture pipeline:

- **task history** = communication events filtered to a task
- **per-agent activity log** = every event for one role
- **aggregate activity log** = the whole stream, ordered and filterable
- **live agent count** = the current liveness projection of the roster
- **Runewood viz** = the same stream rendered as motion

Build the stream once; every surface reads from it.

## Two event sources

- **Broker** owns **communication** and **roster/liveness**. Every message an
  agent sends (order, question, answer, status, artifact, note) and every roster
  change (a role joins, goes idle, stops, restarts, dies) is a broker event.
- **Supervisor** owns **per-agent activity**. It spawns each agent as a
  `claude -p --output-format stream-json` process, so it parses that stream into
  activity events (turn start and end, tool calls, output) exactly as Seraphim
  parses the agent stream today. This is the agent's own timeline, which the
  broker cannot see because it happens inside the agent's process.

Both funnel into the one event log, keyed by `role`, correlated to a `task` when
one applies, and timestamped, so a consumer gets a unified ordered stream.

## Event kinds

- `message` inter-agent communication, carrying the `communication.md` kind
  (order / question / answer / status / artifact / note).
- `lifecycle` an agent's supervised state change (started, idle, stopped,
  restarted, died, recovered), including the defibrillator's death and recovery.
- `activity` an agent's own work, parsed from its stream-json (turn boundaries,
  tool calls, text output).

## The views

### Task history (communication in context)

When a crew works a task, the communication between agents belongs in that task's
history. In Seraphim this reuses the existing pattern: crew events become rows in
the `events` table and ride the task SSE stream, rendered inline the way the
synthetic `ci`, `lifecycle`, and `screenshot` events are today. No new transport.
An `order` renders as a handoff, a `question` as an escalation, an `artifact` as
a link, so the task view reads as the team's conversation about that task.

### Per-agent activity log

One role's full timeline: its lifecycle transitions, the messages it sent and
received, and its own stream-json activity (tools, output, turns). This is the
"open the backend engineer and watch what it is doing" view. It is the event
stream filtered to one `role`.

### Aggregate activity log

The whole unit's combined stream, ordered by time and filterable by role,
channel, or kind. This is the "what is happening across the team right now" view,
structured rather than a firehose. The broker serves it as `GET /history` (issue
#12): filters (`channel`, `role`, `kind`, `task`, `since`), deterministic ordering
by `ts` then log position, and cursor pagination that stays stable under concurrent
writes, so a consumer or a late joiner reads the past without holding the stream
open. `summary=true` returns the rolling compaction instead (issue #19): the older
events folded into bounded aggregates plus the recent raw tail, so joining a
long-running conversation costs bounded context rather than the full log.

## Live agent count and roster

The supervisor knows the roster and each agent's liveness, so the broker exposes
it (`GET /roster`) plus a roster-change event on every transition. A UI shows the
live agent count and per-role status (working, idle, stopped, dead) cheaply, with
no polling. The count is simply the current liveness projection.

The broker implements the substrate (issue #14): `GET /roster` lists roles with
their owned paths and liveness; a role or the supervisor registers on join with
`POST /roster` (body `{role, owned_paths?, liveness?}`, defaulting to `working`)
and leaves with `DELETE /roster/{role}`. Every change is a first-class event on the
stream, published as a `lifecycle` event (`started` / `restarted` / `idle` /
`stopped` / `died` / `recovered`) to `all-units`, so it rides `/history`, `/stream`,
and each role's inbox with no separate capture path. The broker derives the
transition from the liveness change and the prior state: a role reaching `working`
for the first time `started`, coming back from `dead` `recovered`, and otherwise
`restarted`. The roster lives behind the storage trait, so a durable backend keeps it
across a restart. The live count is the projection of these lifecycle events, computed
by any consumer from the stream.

The supervisor's lifecycle state machine drives these transitions (issue #22): it
marks a role working on start, idle after a quiet period, and stopped on stand-down,
each time re-registering with the right liveness. Its defibrillator (issue #23) marks
a role dead when its turn crashes or hangs (a `died` event) and, within the recovery
budget, revives it (a `recovered` event); it also records the diagnostic incident
behind each death. So the stream and the live count reflect lazy start, idle-stop,
restart, and death-and-recovery with no separate signal.

## Runewood

Runewood (the Gource-style WebGL visualization spun off from Seraphim's watch
page) is a natural consumer of this stream: agents are entities, messages are
particles between them, lifecycle transitions are spawns and fades, and the live
count sits on screen. Because the stream is already typed, timestamped, and
addressed, no crew-specific capture is needed; Runewood subscribes and renders.
It is a consumer, not a dependency; the stream stands on its own.

## What the substrate must guarantee

For all of the above to be projections rather than rebuilds, the broker and
supervisor must, from day one:

- stamp every event with `ts`, `role` (`from`), `channel`, and `kind`
- correlate events to a `task` when a task context exists
- expose the stream over SSE for live consumers and a paginated history endpoint
  for catch-up (with the `summary=true` compaction from `communication.md`)
- treat the roster and liveness as part of the same event stream, not a side API
