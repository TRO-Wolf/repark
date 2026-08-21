# map — .agent/skills/compact-context-docs/

## Purpose

One skill: the post-landing truth-up that keeps the context documents lean, current, and
single-homed. It records a **ritual**, not policy — the single-home rule it enforces is
[AGENTS.md](../../../AGENTS.md)'s, and the state it reconciles to is
[STATUS.md](../../../STATUS.md)'s.

## Contents

- [SKILL.md](SKILL.md) — the runbook: what counts as a context document, the seven-step ritual
  (STATUS.md first, then restatements, stale lifecycle claims, `map.md` lockstep, archiving to
  `docs/history/`, `make ci`, one PR), and the gotchas — chiefly that runbooks and onboarding
  docs rot fastest because nothing mechanical validates their prose.

## Pointers

- Up: [../map.md](../map.md)
- Runs before: nothing — it is the closing ritual. Called by
  [../publish-pypi/SKILL.md](../publish-pypi/SKILL.md) at close-out.
- Authoritative: [../../../AGENTS.md](../../../AGENTS.md), [../../../STATUS.md](../../../STATUS.md)

## Debug

| Symptom | First check |
|---|---|
| A fact appears in two documents | That is the defect this skill exists to remove — pick the home, leave a pointer |
| `make ci` red after a sweep | The manifest checker or map oracle caught structural drift; fix before the PR |
