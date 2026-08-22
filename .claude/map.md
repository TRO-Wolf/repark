# map — .claude/

## Purpose

Claude Code's discovery directory, and nothing else. It holds **one tracked entry**: `skills`, a
symlink to `../.agents/skills`. Claude Code only loads skills found under `.claude/skills/`, so
without the symlink the runbooks in [.agents/skills/](../.agents/skills/map.md) are readable files
that no session can invoke by name.

The symlink is the whole point: the skills keep their single home under `.agents/`, where the
tool-neutral contract applies to them, and Claude gains native invocation without a copy that
could drift. Any other agent tool that wants the same should add its own symlink here or in its
own discovery directory rather than duplicating a skill.

This directory carries **zero authoritative facts**, the same contract as
[.agents/](../.agents/map.md). The rules live in [../AGENTS.md](../AGENTS.md).

## Contents

- `skills` → `../.agents/skills` — symlink (git mode `120000`). Adding a directory under
  `.agents/skills/` makes it invocable in Claude with no change here.
- `scheduled_tasks.lock` — untracked Claude Code runtime state; ignore it.

## I want to...

| ...do this | go to |
|---|---|
| Read or edit a skill | [../.agents/skills/map.md](../.agents/skills/map.md) — never through this path |
| Add a new skill | a `<verb-noun>/` directory under `.agents/skills/`; the symlink picks it up |
| Read the Claude tool mechanics | [../CLAUDE.md](../CLAUDE.md) |
| Read the authoritative contract | [../AGENTS.md](../AGENTS.md) |

## Pointers

- Up: [../map.md](../map.md)
- Skill sources: [../.agents/skills/map.md](../.agents/skills/map.md)
- Claude adapter: [../CLAUDE.md](../CLAUDE.md)

## Debug

| Symptom | First check |
|---|---|
| A skill does not appear in a Claude session | `ls -l .claude/skills` resolves, and the skill has a `SKILL.md` with `name` + `description` frontmatter |
| The symlink checked out as a text file | The clone has `core.symlinks=false` (Windows default); `git config core.symlinks true` and re-checkout |
| A real file appears in this directory | Bug — it belongs under `.agents/`; this directory holds only the symlink |
