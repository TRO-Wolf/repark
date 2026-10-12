# task/wo/ice-ntz-nested-wall-1/evidence/ — step 0 measurements

Outputs of [../probe.py](../probe.py) at main `2c33f289`. Ticks are microseconds
from `toArrow()` cast to int64.

- [repark-main.jsonl](repark-main.jsonl) — RePark, 1188 cells plus one source
  check per zone.
- [spark-4.1.2.jsonl](spark-4.1.2.jsonl) — Spark 4.1.2 with Iceberg 1.11, the
  same 1188 cells.
- [summary.md](summary.md) — door, zone and source against both ticks.
- [repark-head.jsonl](repark-head.jsonl) — RePark at the nested-wall head, the
  same cells.
- [summary-head.md](summary-head.md) — the five counts from `probe.py summary-head`.
- [pin-red-r8-null.txt](pin-red-r8-null.txt) — the null-struct pin before the guard.
  `INSERT … SELECT` stored a struct of null members.
- [pin-red-r8-barrier.txt](pin-red-r8-barrier.txt) — the planner errors after
  the `rand()` barrier was removed.
- [df-before.txt](df-before.txt) — free space on the build disk before the run.

Up: [../map.md](../map.md).
