# map — docs/perf/config-profiles-2026-09-12/step3-remeasure

## Purpose

PROFILES-1 step-3 re-measure CSVs (2026-09-12): the three knobs whose step-2
wins rested on cells touched by the bed's desktop noise re-ran through the same
harness (`run_profiles.py`, three repetitions, `@default` swept fresh) on the
same release module and quiet box, so every profile entry carries a second
measurement beside the step-2 row. Same row shape as the step-2 CSVs
(`dataset,query,knob,value,repetition,seconds`); the C-009 pin recomputes each
profile win from these files. The step-3 derivation and the re-measure reading
live in [../../../../task/ledgers/staging/profiles-1-ledger.md](../../../../task/ledgers/staging/profiles-1-ledger.md).

## Contents

- [datafusion.execution.batch_size.csv](datafusion.execution.batch_size.csv) —
  `@default`, `16384`, `262144`.
- [datafusion.execution.target_partitions.csv](datafusion.execution.target_partitions.csv) —
  `@default`, `32`, `128`.
- [datafusion.optimizer.repartition_joins.csv](datafusion.optimizer.repartition_joins.csv) —
  `@default`, `false`, `true`.
- `map.md` — this file.

## Pointers

- Up: [../map.md](../map.md)
- The pins: [../../../../python/repark-parity/tests/test_profiles_bed.py](../../../../python/repark-parity/tests/test_profiles_bed.py)
