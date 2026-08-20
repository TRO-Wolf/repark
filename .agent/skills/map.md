# map — .agent/skills/

## Purpose

Agent-facing **runbook skills**: step-by-step procedures for recurring operations, written for
any tool's agent. A skill records a proven *sequence*; it defines no policy and carries no
authoritative project fact — every rule it leans on is a pointer into the spine
([AGENTS.md](../../AGENTS.md) + [STATUS.md](../../STATUS.md) + the doc each step cites), and on
any conflict the spine wins. This keeps the `.agent/` zero-authoritative-facts contract intact:
deleting a skill loses a convenience, never a project truth.

## Contents

- `publish-pypi.md` — cut a versioned release: the release-PR shape, squash tree verification,
  the annotated tag, the `release.yml` pipeline with its owner approval gate, registry
  verification. Owner merges and approvals stay owner actions.
- `compact-context-docs.md` — the post-landing truth-up ritual: reconcile STATUS.md, sweep
  restatements and stale lifecycle claims, keep `map.md` lockstep, archive closed campaigns to
  `docs/history/`, validate with `make ci`.

## I want to...

| ...do this | go to |
|---|---|
| Release a new version to PyPI | [publish-pypi.md](publish-pypi.md) |
| True up the docs after work lands | [compact-context-docs.md](compact-context-docs.md) |
| Add a new skill | one `<verb-noun>.md` here (pointers, no policy) + a Contents row |
| Read the authoritative contract | [../../AGENTS.md](../../AGENTS.md) |

## Pointers

- Up: [../map.md](../map.md)
- Authoritative spine: [../../AGENTS.md](../../AGENTS.md), [../../STATUS.md](../../STATUS.md),
  [../../docs/release.md](../../docs/release.md).

## Debug

| Symptom | First check |
|---|---|
| A skill states a project rule | Bug — move the rule to the spine, leave a pointer (`.agent/` contract) |
| A skill step no longer matches reality | Fix the skill in the same PR as the change that falsified it |
