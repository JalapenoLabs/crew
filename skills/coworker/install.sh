#!/usr/bin/env sh
# Install (or update) the crew `coworker` skill into a Claude Code skills
# directory.
#
# The skill's canonical copy lives here in the crew repo; an agent loads it from
# its own Claude Code config (`~/.claude/skills/coworker/SKILL.md`, or a repo's
# `.claude/`). Running this points that installed copy at the current version, so
# an existing coworker user gets the broker-based transport without hand-copying
# the file, and a re-run keeps it in sync rather than letting it drift (issue #191).
#
# Usage:
#   skills/coworker/install.sh                 # into $CLAUDE_CONFIG_DIR, else ~/.claude
#   skills/coworker/install.sh --dest <dir>    # into <dir>/skills/coworker (a repo's .claude)
set -eu

# The skill file sits next to this script, so the installer works from any cwd.
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
source_skill="$script_dir/SKILL.md"

# Default to the personal Claude Code config dir. `CLAUDE_CONFIG_DIR` and the
# `--dest` flag override it, so the same script targets a project's `.claude` too.
dest_root="${CLAUDE_CONFIG_DIR:-$HOME/.claude}"
case "${1:-}" in
    --dest)
        [ "$#" -ge 2 ] || { echo "install.sh: --dest needs a directory" >&2; exit 2; }
        dest_root="$2"
        ;;
    --dest=*)
        dest_root="${1#--dest=}"
        ;;
    "")
        : # No argument: use the default destination.
        ;;
    *)
        echo "usage: install.sh [--dest <claude-config-dir>]" >&2
        exit 2
        ;;
esac

dest_dir="$dest_root/skills/coworker"
mkdir -p "$dest_dir"
cp "$source_skill" "$dest_dir/SKILL.md"
echo "Installed the coworker skill to $dest_dir/SKILL.md"
