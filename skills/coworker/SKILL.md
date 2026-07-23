---
name: coworker
description: Work alongside and communicate with other Claude agents through the crew broker
argument-hint: "You are the frontend worker"
allowed-tools: [Bash, Read, Edit, Write, Glob, Grep, Agent]
---

During this session other Claude agents run in parallel, so fix only the
lint/typecheck issues in your own lane.

# Coworker

You are one role on a **crew**: an agent scoped to a lane (frontend, backend, qa,
infrastructure, ...), coordinating with the other agents through the **crew broker**,
a localhost message bus. The broker does the hard parts for you:

- **Routing.** A direct message reaches one teammate; a broadcast is deliberate and
  rare. You address a role or a channel, not a file everyone shares.
- **No self-echo.** Your own messages never come back to you. There is no "ignore
  your own writes" rule to remember: the broker drops them at the source.
- **Bounded catch-up.** A joining agent reads a compact summary of what it missed,
  not the whole history, so joining stays cheap however long the crew has run.

This replaces the old transport (a shared markdown file plus a `tail -F` monitor).
You send with `crew send` and watch with `crew watch`, never by appending to a file.

## Bootstrap: your role card

You boot knowing three things. The user (or the supervisor that spawned you) supplies
them, in `$ARGUMENTS` or the conversation:

- **Who you are.** Your role, for example `frontend`. Also set as `CREW_ROLE`.
- **Your lane.** The directory or paths you own. Work there; ask before you touch
  another role's lane.
- **The broker.** Where it listens, from the `CREW_BROKER_HOST` / `CREW_BROKER_PORT`
  environment (or a role card at `CREW_ROLE_CARD`, which carries all three). Override
  ad hoc with `crew send --broker <url>` / `crew watch --broker <url>`.

If any of the three is missing, ask the user before you proceed.

## Send a message

Post as your role with `crew send`:

- To one teammate (its direct `@role` channel): `crew send --to backend "API is ready on /v1/tasks"`
- To the commander (the default when you name no target): `crew send "blocked on the schema decision"`
- To a named channel: `crew send --channel all-units "standing up the new service"`, or a
  pair like `crew send --channel frontend+backend "let's lock the contract"`.

The broker stamps the time, routes the message, and keeps your own copy out of your
inbox. You write the body; it owns everything else.

## Watch your inbox live

Subscribe once so each new message becomes a native notification, with no context
spent polling. Use the `Monitor` tool on your self-filtered inbox stream
(`crew watch --role <you>`):

```
Monitor(
  description: "<your-role> inbox",
  persistent: true,
  timeout_ms: 3600000,
  command: 'crew watch --role <your-role>',
)
```

Every line the monitor delivers is a teammate's message, rendered as
`from -> channel (kind) body`. Because the stream is self-filtered, none of them are
yours: act on what arrives. You do not have to reply to everything; a bottomless
back-and-forth helps no one. Reply when you have something to say, or once your work
is done.

For a one-shot read instead of a live stream (for example right after you boot), run
`crew inbox`: it prints every message currently addressed to you. `crew roster` lists
the unit: each role, its lane, and whether it is live.

## If no broker is reachable (fallback)

`crew send` and `crew watch` fail with a clear message when no broker answers
("could not reach the broker at ...; is `crewd` running?"). If that happens:

1. Tell the user the broker is unreachable and ask them to bring one up (`crew up`),
   or to give you its address (`--broker <url>`, or `CREW_BROKER_HOST` / `PORT`).
2. Do **not** silently fall back to a shared file or guess at what teammates are
   doing. Surface the problem, then keep making progress in your own lane where you
   can without coordination.

## Working as a team

Treat the crew like real coworkers. Use the channel to delegate, clarify scope, and
stay out of each other's lanes: "I'll take the `/tasks` endpoint, you own the table?"
or "what shape do you need the response in?" You can still talk to the user directly
and think things through with them. The goal is not to route around the user; it is
to work as a team.

Every agent on the crew uses this same skill, so you never need to explain the
protocol to a teammate over the channel: they already know it.
