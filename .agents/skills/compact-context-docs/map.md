# map — .agents/skills/compact-context-docs/

## Purpose

One skill: the truth-up that keeps the context documents lean, current, and single-homed — run
after a unit lands, and again in scoped form as the **pickup ritual** when the next unit starts.
It records a **ritual**, not policy — the document classes and lifecycle rules it executes are
[AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle", and the state it reconciles to is
[STATUS.md](../../../STATUS.md)'s.

## Contents

- [SKILL.md](SKILL.md) — the runbook: what counts as a context document, the ritual (step 0
  `make ledger-archive` — mechanical, zero tokens — then STATUS.md first, restatements, stale
  lifecycle claims, `map.md` lockstep, the unit's ledger `move`d to `completed/` and campaign
  briefs to `docs/history/`, `make ci`, one PR), the **pickup ritual (scoped mode)** — confirm
  the prior PR merged and its departure edit is in the local base, file the finished ledgers,
  run the drift checks, compact against the just-merged delta only, land it as a docs-only first
  commit; delegable to a smaller model under orchestrator review — and the gotchas, chiefly that
  runbooks and onboarding docs rot fastest because nothing mechanical validates their prose.

## Pointers

- Up: [../map.md](../map.md)
- Runs at both ends of a unit: scoped mode opens it, the full ritual closes it. Called by
  [../publish-pypi/SKILL.md](../publish-pypi/SKILL.md) at close-out.
- Authoritative: [../../../AGENTS.md](../../../AGENTS.md), [../../../STATUS.md](../../../STATUS.md)

## Debug

| Symptom | First check |
|---|---|
| A fact appears in two documents | That is the defect this skill exists to remove — pick the home, leave a pointer |
| `make ci` red after a sweep | The manifest checker or map oracle caught structural drift; fix before the PR |
