# task/wo/avg-decimal-grouped-run-variance-1/evidence/raw-r1/ — critic round 1: the verdict and the re-measured tables

Rendered by [../r1_report.py](../r1_report.py) from the raw runs in `N/avg-revision-out/raw/` (logs beside them,
with every `repark._native.__file__` print). Nothing here is hand-edited but the header lines.

- [critic-verdict.md](critic-verdict.md) — the critic's round 1 verdict (Opus 5.5 medium, `VERDICT: UNSOUND`),
  verbatim; its raw runs are under `N/avg-critic/`.
- [spark.md](spark.md) — Spark 4.1.2 at `local[1]` and `local[4]`, whole-stage codegen on and off, both modes.
- [main.md](main.md) — main on the same cells in six configurations, beside seat A's exact answer.
- [pins.md](pins.md) — the spill, skip-partial, DataFrame-door and Iceberg (position delete) pins on main, and
  #1360's `sum` windows on its head.
- [frames.md](frames.md) — growing and 15,000-row sliding frames past 38 digits, both engines.

Up: [../map.md](../map.md).
