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
- **Supervisor** owns **per-agent activity** (issue #24). It spawns each agent as a
  `claude -p --output-format stream-json --verbose` process, parses that stream into
  activity events (turn start and end, tool calls, text output) modeled on Seraphim's
  parser, and posts each to the broker (`POST /activity`, keyed by role). This is the
  agent's own timeline, which the broker cannot see because it happens inside the agent's
  process. An unrecognized stream shape is kept as `other` rather than dropped, so the log
  survives a schema drift across Claude Code versions.

Both funnel into the one event log, keyed by `role`, correlated to a `task` when
one applies, and timestamped, so a consumer gets a unified ordered stream.

## Event kinds

- `message` inter-agent communication, carrying the `communication.md` kind
  (order / question / answer / status / artifact / note).
- `lifecycle` an agent's supervised state change (started, idle, stopped,
  restarted, died, recovered), including the defibrillator's death and recovery,
  and the General's control gestures (paused, resumed, stood_down; issue #41).
- `activity` an agent's own work, parsed from its stream-json (issue #24): turn
  boundaries, tool calls, text output, or `other` for a shape the parser does not model.
  Keyed to the role on its own `@role` channel, so it rides the aggregate stream and the
  role's timeline without reaching other roles' inboxes.
- `boundary` a role reaching outside its owned lane (issue #46): the role, the
  out-of-lane path, and whether the crew's policy blocked the edit or only warned.
- `ledger` a change to the shared work ledger: a role claiming a task or moving it
  to `in_progress`, `blocked`, or `done` (issue #45).
- `verification` a step through the adversarial done-gate (issue #47): the task, its
  owner, the independent verifier (once one judges it), and the verdict (submitted,
  passed, or failed) with the acceptance claimed or the specific failure.
- `board` a change to the shared situation board (issue #49): an entry recorded or
  retracted, with its key, section (decision / interface / gotcha), author, and content.
- `budget` a token-spend report against the crew budget (issue #54): the role, its
  cumulative spend and cap, the crew's cumulative spend and budget, and any cap the spend
  hit (role or crew).
- `telemetry` a per-turn token-and-cost usage report (issue #55): the role, the tokens the
  turn spent, and its cost in micro-USD. Always emitted, whether or not a budget is set.
- `usage` a shared-subscription usage reading and its auto-pause (issue #56): the window
  percent, whether new work is paused, and (while paused) when the window resets.

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
stream filtered to one role's timeline.

The broker serves it both ways (issue #30): `GET /history?agent=<role>` reads the
timeline as history (time-ordered, cursor-paginated like any `/history` query, and
`summary=true` compacts it), and `GET /activity?agent=<role>` streams it live over
SSE, resuming from `Last-Event-ID` like the inbox. The timeline is defined by
`crew_core::Event::in_timeline_of`: the role's own events (its sent messages, its
lifecycle, its activity, all stamped `from` the role) plus the messages addressed to
it (its `@role` channel, a pair it belongs to, or `all-units`). Unlike the inbox it
is not self-filtered, because a timeline is what the role does as well as what
reaches it; unlike the aggregate log's sender-only `role` filter, it also carries
what the role received. Another role's lifecycle or activity is excluded even when
broadcast to `all-units`, since only messages count as "received". The live stream
and the history read share one predicate, so they never disagree.

### Aggregate activity log

The whole unit's combined stream, ordered by time and filterable by role,
channel, or kind. This is the "what is happening across the team right now" view,
structured rather than a firehose.

The broker serves it both ways under one filter, so the live and historical views
always agree (issues #12, #31). Historically, `GET /history` reads the past: filters
(`channel`, `role` sent-by, `agent` a role's timeline, `kind` (a comma-separated set
keeps several kinds in one query, issue #125), `task`, `since`),
deterministic ordering by `ts` then log position, and cursor pagination that stays
stable under concurrent writes, so a consumer or a late joiner reads the past without
holding the stream open. `summary=true` returns the rolling compaction instead (issue
#19): the older events folded into bounded aggregates plus the recent raw tail, so
joining a long-running conversation costs bounded context rather than the full log.
Live, `GET /stream` delivers the same view over SSE, narrowed by the same filter
params applied with the very same `EventFilter::matches`: with no filter it is the
firehose, and with one it delivers only matching events. It resumes losslessly like
`GET /inbox` (issue #134): a consumer subscribes to `/stream` for what follows, and on
a lagged or dropped connection it reconnects with its `Last-Event-ID` and the stream
replays the matching events it missed before returning to the live tail, so it needs no
separate `/history` catch-up. A fresh connection with no cursor starts at the live tail.
The replay reuses the same `EventFilter::matches` as the live tail and `/history`, so
the three agree; `/inbox`, `/activity`, and `/stream` share one replay-then-live engine.

### Lane boundary crossings

When a role reaches for a file outside its owned paths, the crossing is surfaced on the
stream rather than passing silently (issue #46). The agent checks a path with the
`crew_lane` tool before an out-of-lane edit; the broker records the result as a
`boundary` event (from the role, to `all-units`) over `POST /boundary`, so the operator
sees who reached where and whether the crew's policy warned or blocked. It rides
`/history`, `/stream`, and each inbox like any other event, and `GET /history?kind=boundary`
filters to just the crossings. The policy itself (`warn` / `block` / `off`) lives in the
crew config and rides each role card; see `docs/config.md` and `docs/roles.md` (lane
enforcement).

### The done-gate

The adversarial done-gate is a projection of the same stream (issue #47). "Done" is not a
role's own assertion: it submits finished work with `crew_submit`, an independent role
tries to break it against the acceptance criteria and records a verdict with
`crew_verdict`, and only a pass from a role other than the owner marks the task done. Each
step is a `verification` event (to `all-units`), so the operator watches the gate in
action on `/stream` and in `crew watch`, and `GET /history?kind=verification` reads just
the gate's history.

The broker holds the live gate in memory, the authority on ownership like the pause
control, and `GET /gate` reads it: every task under verification, its owner, its verifier,
and whether it is submitted, passed, or failed. A failed verdict also posts an actionable
handback to the owner's inbox, so the rework routes back through the normal message path.
The gate is enforced at the broker: `POST /gate/verdict` refuses a verdict from the task's
own owner (409) and one on a task that is not awaiting a verdict, so confident-but-wrong
work cannot slip through. See `docs/roles.md` (the done-gate).

### The situation board

The shared situation board is the crew's durable memory (issue #49): agreed interfaces,
decisions and their rationale, and known gotchas, distinct from the transient message
stream so the crew stops re-deriving and re-litigating what is settled. A role records or
retracts an entry with `crew_record` (`POST /board`) and reads the board with `crew_board`
(`GET /board`, filterable by section); the whole crew reads and writes it, and the
commander curates it. Recording is open to every role, but retraction is enforced curation
(issue #180): `POST /board` with `retract: true` succeeds only for the entry's author or the
crew's commander, and any other role is refused a 403, so a stray role cannot erase a
curated decision. The broker resolves the commander from its config (`crew up` sets it from
the crew config; `CREW_BROKER_COMMANDER` sets it for a bare `crewd`).

Every change is a `board` event (to `all-units`), so the board is auditable on `/stream`,
in `crew watch`, and via `GET /history?kind=board`. The board itself is a **projection of
those events**: the broker rebuilds it from the durable log on startup, so a decision
recorded before an idle-stop or a broker restart is still on the board after it. The
done-gate rebuilds the same way, so a task mid-verification survives a restart too (issue
#181). Unlike the pause control, which is in-memory and resets on a broker restart, both
read back from the log (see `docs/communication.md`, context management).

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
across a restart.

The live count is the current liveness projection, and `GET /roster` exposes it
directly (issue #32) so a UI needs no polling: alongside the roles it returns a
`count` with the headline `live` number (agents `working` or `idle`, present and up
or resumable; a `stopped` role has left the field and a `dead` one gave up) and the
per-liveness breakdown. A UI reads that snapshot once and keeps it current from the
`lifecycle` events every roster change publishes, so it shows the live count updating
as agents start, idle, stop, and die.

**Pause and stand-down** ride the same roster and stream (issue #41). The General's
brake and kill switch are broker control endpoints: `POST /pause` and `POST /resume`
gate work per role (a `role` in the body) or crew-wide (none), and `POST /standdown`
halts the whole crew at once. Each records a `lifecycle` event (`paused` / `resumed` /
`stood_down`, a per-role change `from` the role and a crew-wide one `from` the General)
to `all-units`, so the change rides `/history`, `/stream`, and each inbox like any
other. `GET /roster` carries the state too: a crew-wide `standing` (`running` /
`paused` / `stood_down`) and a `paused` flag per role. A role is gated from new work
whenever it is paused on its own or the crew is not `running`. An agent honors this by
pulling no work while gated (its role card says so), and the supervisor Fleet enforces it
at the process level (issue #187): a fleet-wide pause monitor reads the gate from the
roster and actually stops a gated role's process and refuses to restart it until it is
resumed, so a non-compliant or wedged agent is stopped rather than trusted to idle. The
control state lives in the
broker, the live recoverable authority, and a stand-down preserves the durable log and
roster, so the crew resumes with `crew resume` or a fresh `crew up`. Persisting the
control state across a broker restart is a later refinement; the stream already records
every transition.

The supervisor's lifecycle state machine drives these transitions (issue #22): it
marks a role working on start, idle after a quiet period, and stopped on stand-down,
each time re-registering with the right liveness. Its defibrillator (issue #23) marks
a role dead when its turn crashes or hangs (a `died` event) and, within the recovery
budget, revives it (a `recovered` event); it also records the diagnostic incident
behind each death. So the stream and the live count reflect lazy start, idle-stop,
restart, and death-and-recovery with no separate signal.

## The terminal cockpit

`crew top` is the live terminal cockpit, htop for the crew, mission control in a plain
terminal with no Seraphim required (issue #51). It is purely a rendering of the stream and
the roster, so it captures nothing new: it seeds a state model once from the `/roster` (the
roles, their liveness, and the live count) and `/stats` (per-role tokens and cost) snapshots,
then folds each live `/stream` event, a `lifecycle` moving a role's status, an `activity`
setting its current action, a `telemetry` adding to its tokens and cost, and a `message`
landing on the flow. It shows every role's status, action, and spend over the recent message
feed, with the live count and the aggregate cost in the header, and lets the operator filter
the feed by role or channel and drill into one role's activity. The state model and the
render are pure and unit-tested; the terminal shell reuses the same `/stream` reader `crew
watch` and `crew notify` do, so the display updates by push, not by polling.

## Work ledger

The **work ledger** keeps two roles from grabbing the same work or editing the same
file blind (issue #45). It is another projection of the stream, like the roster: a
role claims a task before it starts, and the broker records who holds what.

`POST /ledger` (body `{task, owner, state?, title?}`) claims a task or moves a claim
forward. The `task` is a stable key the crew agrees on (a path, a feature, an order's
title). The broker is the authority: it serializes claims under one lock and refuses a
claim on a task another role already holds (`409`, naming the holder), so a conflict is
surfaced rather than raced. A task is **held** while `claimed`, `in_progress`, or
`blocked`; `done` frees it, so a finished task may be claimed again. Only the owner may
move its own task. `GET /ledger` reads the live ownership, sorted by key.

An order seeds a claim for its recipient automatically (issue #184): when an `order`
message is posted to a role's direct channel, the broker claims the work for that role
(owner = the ordered role, state = `claimed`, keyed and titled by the order title), so
assigned-but-not-started work shows on the ledger without a manual claim. The seed is
non-destructive: it claims only when no one currently holds the task, so a re-order never
regresses an in-flight claim and never steals a task another role holds (the General uses
`crew reassign` for that). It rides the stream like any claim.

Every change also rides the stream as a `ledger` event (from the owner, to `all-units`,
filterable with `history?kind=ledger`), so the ledger is reconstructable from the log
and the cockpit can render it. Agents use the ledger through `crew_claim` and
`crew_ledger` (or the CLI shim `crew claim` / `crew ledger`); every role card tells a
role to claim before it touches shared work. The live ledger lives in the broker; like
the pause state, rebuilding it from the durable log on a restart is a later refinement.

## Coordination-stall detection

A dead agent is not the only way a crew stops making progress. A crew can be fully alive
yet stuck waiting on itself: two roles each holding for the other to answer, or a task
that no one moves. Silence then reads as progress when it is really a deadlock. The
supervisor's defibrillator extends to this (issue #48): a fleet-wide **stall monitor**
reads a recent window of the event stream (`GET /history?since=`) on a timer and finds
the three shapes of a coordination stall. The read is incremental (issue #165): the
monitor keeps a rolling buffer of the lookback window and each tick fetches only the
events since its last scan (`since` = the newest buffered event), splicing them on and
dropping the aged-out front, so a chatty crew's scan costs O(new) rather than re-reading
the whole window every tick.

- **A deadlock**: a cycle of unanswered questions on the stream (`backend` asked
  `frontend` and is waiting, `frontend` asked `backend` and is waiting), so neither can
  proceed.
- **An unanswered question**: one agent has waited past the threshold for another to
  answer, with no cycle; the blocker is simply not responding.
- **A stalled ledger**: a task with no forward motion past the threshold, read from the
  stream as a held work-ledger claim (a `ledger` event not yet `done`, issue #45) or a
  done-gate submission with no verdict (a `verification` event still `submitted`, issue
  #47).

The monitor distinguishes a true deadlock from a legitimate wait for input: a question
broadcast to `all-units`, or addressed to anyone who is not a live agent, is the crew
waiting on the General, not on itself, so it is never escalated. When it does find a
stall, it escalates the specific cause to the operator, who is waiting on what, as a
warning in the `crew up` foreground (a `supervisor.stall.detected` event) rather than a
generic timeout, and records it (read with `Fleet::stalls`) so a persistent stall is
escalated once and a resolved-then-recurring one afresh.

The monitor also surfaces each stall on the stream as a first-class `stall` event
(`POST /stall`, issue #120), so a stall is no longer only a supervisor log. It publishes
a `detected` event when a stall first crosses the threshold and a `resolved` event once
it clears, from the General to `all-units`, carrying the stall's `kind`
(`deadlock` / `unanswered_question` / `ledger_stall`), the `roles` caught in it, and the
specific `detail`. This is what lets `crew notify` fire the "a role is stalled" moment
(issue #52) and the `crew top` cockpit (issue #51) render live stalls. The event carries
the detection the monitor built; the stream write is best-effort, so a broker hiccup
never takes the monitor down (the stall is still logged and in `Fleet::stalls`). It is
filterable with `GET /history?kind=stall`.

## Push notifications

The General should be able to walk away and be pulled back only when it matters (issue
#52). `crew notify` subscribes to the firehose (`GET /stream`, the same event stream every
reader consumes, so there is no separate signal path) and pushes a native notification on
each **actionable moment**, a moment that changes what the General would do next. Routine
chatter passes silently, so the General is not drowned in status pings.

The actionable moments are the ones the stream carries today:

- **A General-facing question is asked** (a `message` of kind `question`): a role wants a
  decision the General would field, rather than peer coordination.
- **A role dies** (a `lifecycle` `died`): a role crashed or hung past recovery.
- **The crew stands down** (a `lifecycle` `stood_down`): every role halts and the mission
  is on hold.
- **The crew is stalled** (a `stall` event with `status` `detected`, issue #120): the crew
  is stuck waiting on itself and needs the General. A `resolved` stall is good news and
  stays quiet.
- **The mission completes** (a `mission` event, issues #121, #154, #155): the crew gracefully
  finished its work. This is the true completion, reported by the crew (typically the
  commander through `crew_complete`), distinct from the `stood_down` emergency halt that
  used to stand in for it. The event carries a `summary` of what shipped, which the
  completion push renders so the General reads the outcome at a glance. It announces, it does
  not gate the crew (a deliberate decision, issue #154): completion is a report, not a
  control, so a finished crew idle-stops on its own (issue #55), and the General stops it
  deliberately with `crew standdown` or `crew down`.
- **The budget is exhausted** (a `budget` event with a `breach`, issues #54, #175): a role
  hit its token cap or the crew hit its crew-wide budget, idle-stopping that role or the
  whole crew rather than overrun. The push names the scope, `crew` (every role idle-stops)
  or `role` (just that role), and the spend against the ceiling it crossed. A within-budget
  spend report (no breach) is routine and stays quiet, like a resolved stall.

Not every question needs the General (issue #119). A peer loop (`@backend` asking a live
`@frontend`) is coordination the crew resolves on its own, so pushing it would drown the
General in chatter that is not theirs to answer. A question is **General-facing** only when
it is broadcast to `all-units`, or addressed to a role that is not a live agent (stopped,
dead, or never in the unit); a directed question to a live teammate stays quiet. This
mirrors the stall monitor's rule (issue #48): a directed question to a live teammate is a
wait on the crew, not on the General. The notifier tracks roster liveness: it seeds the
roster once from `GET /roster` on connect (the canonical read-once-then-fold pattern, issue
#32, #170), so an already-registered but quiet role is known to be live on attaching to a
running crew, then keeps it current by folding the `lifecycle` events on the same stream (a
role is live while it is working or idle, and drops out when it stops or dies). A role
still not known to be live (absent from the seed and unseen on the stream) is treated as
General-facing, so a real question is never silently dropped.

Everything else, status and notes, orders, answers and artifacts, ordinary lifecycle such
as `started` or `idle`, activity, board, boundary, verification, and a within-budget spend
report, is routine and never notifies. The policy is configurable per moment: `--mute
question,died,stood-down,stalled,complete,budget`
suppresses any subset (for a General who does not want peer questions, say), and
`--no-sound` drops the terminal bell while keeping the desktop notification and the log
line.

Each push does three things, so it lands whatever the environment: it prints a log line
(the durable record, shown even on a headless server), sounds the terminal bell (the
audible pull, mirroring Seraphim's notification sound), and shows a desktop notification
through the platform notifier (`notify-send` on Linux, `osascript` on macOS). A missing or
failing notifier is not an error: the printed line and the bell already carry the alert, so
delivery degrades quietly.

One further moment in the notification scope waits on its event reaching the stream: an
approval pending (the approval gates of issue #40). It lights up here for free when its
event lands, since the classifier is one match over the event kinds, exactly as the stall
moment above plugged in once the monitor began surfacing stalls (issue #120).

## Token budget

A crew should not quietly burn a fortune (issue #54). A crew sets a crew-wide **token
budget** and optional per-role **caps** in its config (see `docs/config.md`), and the
supervisor holds the crew's [`Budget`](../crates/crew-core/src/budget.rs): a spend
accountant modeled on the Workflow budget pattern (a total, the spend so far, and the
remaining headroom, with the cap a hard bound).

As the supervisor records each turn's token usage, it charges the spend to the role and the
crew and does two things:

- **Surfaces it.** Every record publishes a `budget` event (spend against budget) to
  `all-units`, so a UI reads spend off the stream and a cap hit is never silent. It rides
  `/stream`, `/history?kind=budget`, and each inbox like any event.
- **Enforces it.** When a role's spend reaches its cap, the supervisor idle-stops that role;
  when the crew's total reaches the crew budget, it idle-stops every role. The role keeps
  its roster entry (marked stopped) and is restartable once the General raises the cap, so
  the crew is bounded, not overrun. A ceiling fires its stop and its event once, not on
  every later spend.

The token feed is live: the supervisor's `Fleet::record_spend` seam, wrapped by
`record_usage`, is driven by each turn's usage the stream-json activity parser distills from
captured stdout (issues #24, #177). The parser reads the `result` line's `usage` (its token
fields summed) and `total_cost_usd`, and the activity forwarder calls `record_usage(role,
tokens, cost_micro_usd)` per turn, so budget enforcement charges against real spend rather
than a directly poked seam. An unbounded crew (no budget and no caps) records nothing, so a
crew that opts out pays no overhead.

Beyond the stream, the broker folds the `budget` events into a **snapshot** and serves it at
**`GET /budget`** (issue #176): current spend against budget per role (its cumulative spend
and cap) and crew-wide (the crew total and budget). Each `budget` event carries the running
totals the supervisor computed, so the projection is latest-wins, the newest report for a
role carries its current spend and the newest overall the crew total. Like the situation
board (issue #49) and the `GET /stats` rollup, it is a projection of the durable log, rebuilt
by folding the `budget` events on a restart, so the cockpit (issue #51) reads current spend
from one snapshot rather than replaying the stream.

## Cost, tokens, and time telemetry

Beyond enforcing a budget, a crew should make spend legible: how much each role costs, in
tokens and dollars, and how long it works (issue #55). Two mechanisms carry it, and both
are projections of the one stream.

- **Idle-stop on quiet.** A role that goes quiet past its `idle_stop` timeout is stopped by
  the supervisor's lifecycle machine (issue #22), keeping its roster entry so a restart is
  fast. This is what makes an idle role cost nothing; see `docs/config.md` (`idle_stop`).
- **Usage telemetry.** The supervisor's `Fleet::record_usage` emits a per-turn `telemetry`
  event (from the role, to `all-units`) over `POST /telemetry` whether or not a budget is
  set, and charges the same tokens against the budget. Cost rides the wire as micro-USD
  (millionths of a dollar) so it sums exactly. A turn's tokens and cost come from the agent's
  stream-json: the activity parser reads the `result` line's `usage` tokens (its fields
  summed) and `total_cost_usd` (converted to micro-USD), and the activity forwarder calls
  `record_usage(role, tokens, cost_micro_usd)` per turn (issues #24, #177). Working time
  needs no feed, since the broker reads it from the role's `lifecycle` events already on the
  stream.

The broker folds these into a rollup and serves it at **`GET /stats`**: per role and in
aggregate, the cumulative tokens, cost (micro-USD), and working seconds. Working time is a
fold of the lifecycle transitions (entering `started` / `restarted` / `recovered` /
`resumed` opens a working interval; `idle` / `stopped` / `died` / `paused` / `stood_down`
closes it, as does a `mission` completion), and a role working right now has its open
interval counted through the read instant, so a live role's time keeps climbing. Like the
situation board, the rollup is a
projection of the durable log, so it is rebuilt on a restart rather than kept separately.
This is the data the `crew top` cockpit (issue #51) and the Seraphim per-role stats render,
mirroring Seraphim's per-railway stats.

## Subscription usage auto-pause

A crew shares one subscription, so it must not exhaust the shared window (issue #56). The
broker keeps **one usage gauge** across the crew, mirroring Seraphim's usage auto-pause. The
supervisor carries the reporting seam, `RosterClient::report_usage` (`POST /usage`, the
window fill plus when it resets). Detecting that fill from the agents' rate-limit output is
the stream-json parser's job (issue #24), so the seam stays unwired until that parser lands:
feeding detection into `report_usage` is issue #113, paused on #24. Until then the gauge is
exercised through `POST /usage` directly. When #24 arrives it maps the rate-limit signal from
Claude Code's stream-json onto `report_usage(percent, window_reset)`; the exact percent-used
and reset-time field names are the open question to settle then.

When a reading reaches the threshold (`CREW_BROKER_USAGE_THRESHOLD`, default 90 percent), the
broker auto-pauses new work and publishes a `usage` event carrying the reset time, so the
pause is visible on the stream, never silent. The auto-pause gates every role, since the
subscription is shared; it is distinct from the manual pause control (issue #41), so it never
clobbers a manual pause and a reset never lifts one. The gate lifts lazily at the reset
instant (the pause event advertises it), so work auto-resumes with no further reading. A
lightweight background sweep in the broker (a tokio task tied to the server) announces that
auto-resume: when a pause whose window has reset is found, it clears the pause and publishes a
`usage` lift event, so the resume is observable on the stream, not only reflected in the gate
(issue #112). The lift is announced once, whether by the sweep or by a manual resume. The
operator resumes early with `crew resume`, the one escape hatch, which lifts a manual pause
and the usage auto-pause together and surfaces the lift as a `usage` event. `GET /usage` (and
`crew usage`) reads the gauge: the latest reading, the threshold, and any pause. The usage
percent rides the wire as a whole number (`0..=100`) so the event stays exactly comparable.

## Runewood

Runewood (the Gource-style WebGL visualization spun off from Seraphim's watch
page) is a natural consumer of this stream: agents are entities, messages are
particles between them, lifecycle transitions are spawns and fades, and the live
count sits on screen. Because the stream is already typed, timestamped, and
addressed, no crew-specific capture is needed; Runewood subscribes and renders.
It is a consumer, not a dependency; the stream stands on its own.

The consumer contract is the stable surface a viz builds against (issue #33):
`docs/stream-contract.md` documents the event envelope and every payload, the live
SSE feed (`GET /stream`), catch-up (`GET /history`, with the `summary` compaction),
the roster snapshot and live count (`GET /roster`), and how the stream's `id`
bridges to the history cursor for reconnect. It carries a minimal subscribe example
and the additive-only stability promise, so a consumer renders the unit from the
stream alone.

## What the substrate must guarantee

For all of the above to be projections rather than rebuilds, the broker and
supervisor must, from day one:

- stamp every event with `ts`, `role` (`from`), `channel`, and `kind`
- correlate events to a `task` when a task context exists
- expose the stream over SSE for live consumers and a paginated history endpoint
  for catch-up (with the `summary=true` compaction from `communication.md`)
- treat the roster and liveness as part of the same event stream, not a side API

### How the guarantee is enforced (issue #29)

The stamp is not left to each emitter's discipline. `ts`, `from`, `channel`, and
`kind` are mandatory in the `crew_core::Event` type, so an event cannot be
constructed without them, and the broker stamps them **at the source**: a posted
message takes its `ts` and `id` from the broker and its `channel` from the path
(a client that sends any of those is rejected, not trusted), and a roster change
becomes a lifecycle event stamped the same way. Every event, whatever its kind,
enters the store and the stream through one choke point, `AppState::publish`, which
asserts `Event::is_well_formed` (a present, non-blank channel and role sender). So
no event reaches the store or stream missing a required field, and a future emitter
cannot regress the invariant without tripping the assertion in every test run.

Task correlation rides the same envelope. A message carries the `task` its sender
threads (the broker preserves it). A lifecycle event carries the task the
supervisor is working: `RosterClient::with_task` sets the task context, and every
registration and liveness mark it publishes correlates to that task, so `started`,
`idle`, and `restarted` all carry it (a role fully leaving the unit is not
task-scoped, so its `stopped` carries none). Activity events thread the task the same
way through the `Event.task` field (issue #24): `RosterClient::emit_activity` carries the
supervisor's task context onto each `POST /activity`. An event produced outside any task
carries no id, which serializes to an omitted field.

A task is minted where work is assigned, not centrally (issue #132): the
commander's `crew_order` mints a `TaskId` and stamps it on the order, so the order
event correlates. The assigned role adopts that task the moment it reads the order
from its inbox (`Broker::inbox`), so the messages it sends next (`crew_send`,
`crew_order`, ...) stamp the same id, and its work correlates to the assignment
with no central role-to-task table to keep. Correlation stays on the envelope,
never a new broker query. A specialist already working a task threads it onto a
sub-order, so a chain of orders shares one id. The MCP server holds the adopted
task in its long-lived client; the stateless CLI shim persists it per role beside
the inbox cursor, so its next process stamps the same task. Threading the task
onto the assigned role's supervisor (`RosterClient::with_task`) so its own
lifecycle and activity events correlate is the remaining step: the fleet watches
the order stream the way the stall monitor does, not a broker query (issue #24
turn boundaries give it a clean seam).
