# map — task/ledgers/staging/array-null-1-spikes/

## Purpose
Scratch measurement harnesses for ARRAY-NULL-1 step 0 (2026-09-14). Not product code —
the route spike itself was applied, measured, and reverted; these files are the
measurement scripts whose outputs the ledger cites. Branch `fix/array-null-1`.
pins: array-null-1/C-001, C-002, C-003

## Contents
- [oracle_spark.py](oracle_spark.py) — live PySpark 4.1.2 oracle: the nine D-2 cells
  for `F.array_append`/`F.array_prepend` on explicit-schema frames (one `local[1]`
  session, ANSI default, zulu-17); prints `RESULT{json}` answer rows, result type
  and `containsNull` per cell.
- [repark_today.py](repark_today.py) — the same cells through the repark facade AND
  the SQL door (`spark.sql("SELECT array_append(a, e) FROM v")` / prepend spelling).
  Route-selectable via `REPARK_ARRAY_NULL_1_ROUTE` while the spike was applied; on
  the base tree it measures today's `_glue_element` facade and the DF-native door.
- [rss_table.py](rss_table.py) — depth 4/8/12/14/16 RSS table for the base-tree
  chain plus the flat 40-append control; subprocess workers under
  `RLIMIT_AS = VmSize + 3 × 8 GB`, build-only deltas where collect is impractical.
- [route_measure.py](route_measure.py) — route (a) `ScalarUDF` vs route (b) CASE
  depth-12/40 RSS delta and build/collect wall time; run against the release native
  (`maturin develop --release`) while the spike was applied.

## Pointers
- Up: [../map.md](../map.md)
- Ledger: [../array-null-1-ledger.md](../array-null-1-ledger.md)
