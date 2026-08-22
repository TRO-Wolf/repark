# map — .agents/skills/publish-pypi/

## Purpose

One skill: cut a versioned release and publish the wheel to PyPI. It records the **proven
sequence** every shipped tag has followed and carries **no authoritative facts** — the release
policy lives in [docs/release.md](../../../docs/release.md), the gate in
[STATUS.md](../../../STATUS.md), the pipeline in
[.github/workflows/release.yml](../../../.github/workflows/release.yml), and the contract in
[AGENTS.md](../../../AGENTS.md). On any conflict the spine wins.

## Contents

- [SKILL.md](SKILL.md) — the runbook: preconditions, the four-file release PR shape, the squash
  tree-equality check (the only guard against unreviewed content reaching a tag), the annotated
  tag and the API-tag hygiene caveat, `release.yml` with its owner-approved deployment gate,
  registry verification, and the gotchas that have each bitten a real release.

## Pointers

- Up: [../map.md](../map.md)
- Runs after: [../compact-context-docs/SKILL.md](../compact-context-docs/SKILL.md) (step 4.2)
- Authoritative: [../../../AGENTS.md](../../../AGENTS.md),
  [../../../docs/release.md](../../../docs/release.md), [../../../STATUS.md](../../../STATUS.md)

## Debug

| Symptom | First check |
|---|---|
| A step states a project rule | Bug — the rule belongs in the spine; the skill gets a pointer |
| A step no longer matches `release.yml` | Fix the skill in the same PR as the workflow change |
| The `release` environment approval 422s | Expected race — confirm the run finished, do not retry |
