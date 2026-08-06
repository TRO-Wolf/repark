# map — docs/

## Purpose

Engineering contracts, decision records, and operating documentation for this repo.

## Contents

- [testing.md](testing.md) — the mandatory testing contract (tests-with-code hard block,
  test-per-change, divergence-class claims, calibration-per-domain, the entry-point matrix,
  relocation discipline, the forbidden list). Read before any code change.
- [port/](port/map.md) — the V2 port plan ([port/PLAN.md](port/PLAN.md)): copy-then-re-home
  rules, the four phases, the census multiset acceptance gate, the v1-freeze trigger.
- [adr/](adr/map.md) — Architecture Decision Records (dated, append-only "why" docs): the owned
  iceberg-rust fork, the two SQL doors, the copy-then-re-home port, server-prep disciplines.
- [skills/](skills/map.md) — per-model-tier operating manuals (Opus / Sonnet / Haiku).

## I want to...

| ...do this | go to |
|---|---|
| Understand the testing rules | [testing.md](testing.md) |
| See the port phases / acceptance gate | [port/PLAN.md](port/PLAN.md) |
| Understand why a load-bearing decision was made | [adr/map.md](adr/map.md) |
| Read the manual for your tier | [skills/map.md](skills/map.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../AGENTS.md](../AGENTS.md) is the project contract; these docs expand parts of it.

## Debug

First checks: if a rule is unclear, [testing.md](testing.md) + [../AGENTS.md](../AGENTS.md) are
authoritative. Escalate to:
[../map.md#debug](../map.md).
