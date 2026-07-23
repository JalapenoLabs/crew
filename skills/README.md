# Skills

Drop-in Claude Code skills that drive a crew through the broker.

## `coworker`

Work alongside other agents, coordinating through the crew broker instead of a shared
file. Each agent boots from a role card (you are role X, here is your lane, here is the
broker), sends with `crew send`, and watches its self-filtered inbox with `crew watch`,
so it gets routing, no self-echo, and a bounded catch-up (issue #37; see
`docs/communication.md`).

This is the upgraded transport for the `coworker` skill that crew grew out of: the
same "agents that talk to each other" idea, with the file-plus-`tail -F` transport
replaced by the broker.

### Adopting it

The skill needs the `crew` binary on `PATH` and a reachable broker:

1. Install `crew` (`cargo install --path crates/crew-cli`, or build the workspace).
2. Bring a broker up (`crew up`), or point the skill at a running one with
   `CREW_BROKER_HOST` / `CREW_BROKER_PORT`.
3. Copy `coworker/SKILL.md` into your Claude Code skills directory
   (`.claude/skills/coworker/SKILL.md`), and invoke it with your role, for example
   "You are the backend worker."

Without a broker the skill fails gracefully: it tells you the broker is unreachable
and asks you to start one, rather than falling back to the old file transport.
