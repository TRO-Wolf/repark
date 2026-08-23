# map — task/census/baseline-fc3f48102/

## Purpose
The freeze-point census baseline at the port pin `fc3f48102` — reduced on 2026-08-23 (DL-1) to
the one cohort a gate still reads. The four classic/expand cohorts, the stability self-diff, the
quarantine list and the environment manifests are in history at `main` `b13b22c`
(`git show b13b22c:task/census/baseline-fc3f48102/map.md` carries the regeneration record).
Evidence, not source — never hand-edited; a re-run replaces the whole directory in one commit.

## Contents
- [facade/](facade/map.md) — the full-extras facade cohort at the v1 pin: `collected.txt` (the
  2,509 node ids) and `facade.xml` (the JUnit multiset), read by
  `python/repark-parity/tests/test_deferred_ledger.py` — the deferred-test ledger's pins.

## Pointers
- Up: [../map.md](../map.md)
- Procedure: [../../../docs/port/census.md](../../../docs/port/census.md)
