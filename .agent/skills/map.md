# map — .agent/skills/

## Purpose

Agent-facing **runbook skills**: step-by-step procedures for recurring operations, written for
any tool's agent. Each skill is a directory holding a `SKILL.md` with YAML frontmatter (`name` +
a `description` that says when to reach for it **and when not to**), the same shape as
[skills/sepmo/SKILL.md](../../skills/sepmo/SKILL.md), so a skill is discoverable and invocable
rather than a file an agent has to already know to open. A skill records a proven *sequence*; it
defines no policy and carries no authoritative project fact — every rule it leans on is a pointer into the spine
([AGENTS.md](../../AGENTS.md) + [STATUS.md](../../STATUS.md) + the doc each step cites), and on
any conflict the spine wins. This keeps the `.agent/` zero-authoritative-facts contract intact:
deleting a skill loses a convenience, never a project truth.

## Contents

- [publish-pypi/](publish-pypi/map.md) — cut a versioned release: the release-PR shape, squash
  tree verification, the annotated tag, the `release.yml` pipeline with its owner approval gate,
  registry verification. Owner merges and approvals stay owner actions.
- [compact-context-docs/](compact-context-docs/map.md) — the post-landing truth-up ritual:
  reconcile STATUS.md, sweep restatements and stale lifecycle claims, keep `map.md` lockstep,
  archive closed campaigns to `docs/history/`, validate with `make ci`.
- [check-disk-headroom/](check-disk-headroom/map.md) — is there room to do this? Measured
  consumers (`target/debug` dominates), how to budget for the operation rather than the repo at
  rest, and a reclaim order that says what **not** to delete as clearly as what to.

## I want to...

| ...do this | go to |
|---|---|
| Release a new version to PyPI | [publish-pypi/SKILL.md](publish-pypi/SKILL.md) |
| True up the docs after work lands | [compact-context-docs/SKILL.md](compact-context-docs/SKILL.md) |
| Find out whether there is disk room for a big build | [check-disk-headroom/SKILL.md](check-disk-headroom/SKILL.md) |
| Add a new skill | a `<verb-noun>/` directory here with `SKILL.md` (frontmatter + pointers, no policy) and its own `map.md`, plus a Contents row |
| Read the authoritative contract | [../../AGENTS.md](../../AGENTS.md) |

## Pointers

- Up: [../map.md](../map.md)
- Authoritative spine: [../../AGENTS.md](../../AGENTS.md), [../../STATUS.md](../../STATUS.md),
  [../../docs/release.md](../../docs/release.md).

## Debug

| Symptom | First check |
|---|---|
| A skill states a project rule | Bug — move the rule to the spine, leave a pointer (`.agent/` contract) |
| A skill is a bare `.md`, not a directory | Pre-2026-08-21 shape — convert it to `<name>/SKILL.md` with frontmatter + a `map.md` |
| A skill step no longer matches reality | Fix the skill in the same PR as the change that falsified it |
