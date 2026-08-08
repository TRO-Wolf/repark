# map — docs/port/

## Purpose

The V2 port plan: how the private v1 repository's engine is copied and re-homed into this repo,
phase by phase, and what "done" means mechanically.

## Contents

- [PLAN.md](PLAN.md) — copy-then-re-home rules, the four phases, the census-multiset acceptance
  gate, v1-freeze trigger, public ≠ released, the cutover open item.
- [census.md](census.md) — the recorded census procedure (phase-3 PR-4): the pinned environment
  recipe and why the pandas major is load-bearing; the exact argument vectors for all four
  cohorts on **both** sides (the classic cohort runs `--classic` here and an explicit
  `--modules` list on the port source — never `--stretch`); the mandatory stability run and the
  quarantine rule; the full-extras facade cohort with its environment clauses; the comparator's
  usage, exit codes, and attribution rule; the two golden corpora's `basis:` designations.

## I want to...

| ...do this | go to |
|---|---|
| See what phase we're in and what's next | [PLAN.md](PLAN.md) + [../../task/todo.md](../../task/todo.md) |
| Check the port's acceptance gate | [PLAN.md](PLAN.md) "The acceptance gate" |
| Run a census cohort / compare two runs | [census.md](census.md) |
| Find out why a run was refused as incomparable | [census.md](census.md) §1, §5 |
| Check how tests may move | [../testing.md](../testing.md) "Relocation discipline" |

## Pointers

- Up: [../map.md](../map.md)
- The phase-0 execution brief: [../../briefs/phase-0-bootstrap.md](../../briefs/phase-0-bootstrap.md)
- ADR for the port shape: [../adr/0003-copy-then-rehome-port.md](../adr/0003-copy-then-rehome-port.md)

## Debug

- Plan vs reality disagree → the code/census output is truth; update PLAN.md deliberately (it is
  precedence-bearing via PROJECT.md, not a scratchpad).
- Census numbers moved during a re-home → stop; that is an acceptance-gate finding, not drift to
  re-baseline.
