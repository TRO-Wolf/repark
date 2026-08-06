# map — docs/port/

## Purpose

The V2 port plan: how the private v1 repository's engine is copied and re-homed into this repo,
phase by phase, and what "done" means mechanically.

## Contents

- [PLAN.md](PLAN.md) — copy-then-re-home rules, the four phases, the census-multiset acceptance
  gate, v1-freeze trigger, public ≠ released, the cutover open item.

## I want to...

| ...do this | go to |
|---|---|
| See what phase we're in and what's next | [PLAN.md](PLAN.md) + [../../task/todo.md](../../task/todo.md) |
| Check the port's acceptance gate | [PLAN.md](PLAN.md) "The acceptance gate" |
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
