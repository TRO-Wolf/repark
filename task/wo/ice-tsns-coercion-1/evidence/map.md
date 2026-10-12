# task/wo/ice-tsns-coercion-1/evidence/ — slice 0 outputs

Generated tables. Do not edit the summary by hand; regenerate it with
`_summarize_ice_tsns_coercion_1.py`.

- [df-before.txt](df-before.txt) — `df -h` of the volume before the measurement.
- [twins-summary.md](twins-summary.md) — per sibling and door, where the twin matches
  the plain column, where the kind differs from Spark, and the range cells.
- [gate.md](gate.md) — why the gate-open rate is uncountable from public `EXPLAIN`,
  the missing hook, and the pandas `datetime64[ns]` source lines.
- [cost-baseline.json](cost-baseline.json) + [cost-baseline.md](cost-baseline.md) —
  the timer output (three sets of fifteen, medians per statement) and its run
  context: the lock was free, the load was decaying, nothing was building.
- [pins.md](pins.md) — the tests that expect microseconds for a card spelling,
  and the inspected tests that would not move.
- [casts.json](casts.json) + [written-casts.md](written-casts.md) — 256
  written-cast positions, resolved or not, with the plan line each.
- [try-repark.json](try-repark.json) + [try-spark.json](try-spark.json) +
  [try-cast.md](try-cast.md) — `TRY_CAST` for microsecond, nanosecond and
  string inputs, RePark and Spark 4.1.2.
- [lambda.json](lambda.json) + [lambda.md](lambda.md) — whether the lambda
  variable is typed before coercion, with the analyzed plan.
- [gate-rate.json](gate-rate.json) + [gate-rate.md](gate-rate.md) — slice 1
  round 1 facade gate-open rate. The JSON is the reduced copy (rows with
  `opened > 0`, plus the full-run totals). The markdown is the check, the
  per-file opens, and which file has a pandas `datetime64[ns]` frame as its
  only nanosecond source.
- [cost-slice1-round1.md](cost-slice1-round1.md) — slice 1 round 1 cost.
  The first run against [cost-baseline.json](cost-baseline.json) put `merge`
  and `merge_mor` above 1.05 with another lane's build between its sets; the
  interleaved baseline-vs-head rerun (0.98 to 1.01 on all three statements)
  does not reproduce the halt, and no product change followed either run.
- [interleaved-sets.json](interleaved-sets.json) +
  [interleaved-cost.json](interleaved-cost.json) — the interleaved rerun: ten
  raw sets (five baseline, five head, per-repeat times and counter snapshots)
  and the per-statement medians, ratios and pair ranges.
- [round2-pin6-halt.md](round2-pin6-halt.md) — slice 1 round 2 pin-6 halt
  analysis: the full-sweep mismatch classes (DML store-cast fallback,
  error-quote, head-exceeds-twin, stale fixture text, read field-id metadata)
  with counts, root causes and specimens.

Up: [../map.md](../map.md).
