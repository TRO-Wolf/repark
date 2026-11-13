# task/wo/signed-zero-normalize-1/evidence/part-b/after/delta2/ — the second delta (2026-11-13)

- [delta2.md](delta2.md) — the round's record: the commits and the items each carries, red and green runs, the
  configuration-key inventory, the DataFusion 54.1.0 citations, the chain finding, and what is still open.
- [n2/](n2/n2_phases.py) — the n2 attribution harness: `n2_phases.py` times each of the 310 step-0 Spark-door read
  cells per phase (`spark.sql`, `EXPLAIN`, `spark.sql` plus `collect`, five repeats after a warm-up) and counts the
  optimizer passes from `EXPLAIN VERBOSE`; `n2_summary.py` sums the per-cell head-minus-base medians by statement group.

Up: [../map.md](../map.md).
