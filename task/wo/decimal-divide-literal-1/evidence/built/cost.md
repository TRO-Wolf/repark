# DECIMAL-DIVIDE-LITERAL-1 — cost (built 2026-11-18)

Head `pkg-ddl-a30a8d60` (the product commit `a30a8d60`) against main `pkg-avg-27d6149f`. Method:
- alternating runs of main, head, main again (`main-aa`, the control band); two full runs of every row: the brief asks for five, and ten for a row over the bar, but the measure lock was shared with another builder's cost samples (one sample of run 3 waited 656 s for it), so the series stopped at two runs under the turn budget; the two rows over the bar are over by far more than any control band;
- each sample is one process under `flock -x measure.lock` plus the link and both cargo locks, `nice -n 10`,
  `taskset -c 48-57`; each timed region repeats its query 4 times (every sample at least 200 ms; C-012 is one
  replay per sample); the table gives the per-query median;
- data: 10,000,000 rows written once as Parquet by main's package (`d10 DECIMAL(10,2)`, `d38 DECIMAL(38,2)`,
  `i1`, `i2 INT`, `x1`, `x2 DOUBLE`); each query sums its projection (`count(*)`, `sum(q)`), so the division runs
  per row and only two values come back;
- C-012 is the like-for-like replay set (`replay_work.py`, `c012p6` and `c012rest`), as in AVG-DECIMAL-ROUND-1.

Raw: [raw/cost/](raw/cost/) (`samples.jsonl`, `raw-ddl.jsonl`, `table.md`). Runner: [scripts/](scripts/map.md).

| row | main (s) | head (s) | head/main | control band (main-aa/main) | bar | runs | at the bar |
|---|---|---|---|---|---|---|---|
| 1 SQL `d10 / 3` (`DECIMAL(10,2)`), 10M rows | 0.2781 | 0.2086 | 0.750 | 0.900 to 0.956 | 1.05 | 2 | under |
| 1 SQL `d38 / 3` (`DECIMAL(38,2)`), 10M rows | 0.2773 | 0.2406 | 0.867 | 1.026 to 1.076 | 1.05 | 2 | under |
| 2 DataFrame `F.expr("mod(d10, 3)")` | 0.0780 | 0.0942 | 1.207 | 0.949 to 0.991 | 1.05 | 2 | **over** |
| 2 DataFrame `F.pmod(col(d10), F.lit(3))` | 0.1139 | 0.1079 | 0.948 | 0.915 to 1.108 | 1.05 | 2 | under |
| 2 DataFrame `col(d10) % 3` | 0.0633 | 0.0658 | 1.040 | 1.007 to 1.198 | 1.05 | 2 | under |
| 2 DataFrame `col(d10) / 3` | 0.0557 | 0.2070 | 3.718 | 0.955 to 0.975 | 1.05 | 2 | **over** |
| 3 DataFrame `col(i1) / col(i2)` | 0.0274 | 0.0286 | 1.043 | 0.923 to 1.078 | 1.05 | 2 | under |
| 3 DataFrame `col(x1) / col(x2)` | 0.0358 | 0.0351 | 0.981 | 1.089 to 1.117 | 1.05 | 2 | under |
| 3 SQL `i1 / i2` (`INT`) | 0.0305 | 0.0308 | 1.010 | 1.010 to 1.056 | 1.05 | 2 | under |
| 3 SQL `x1 / x2` (`DOUBLE`) | 0.0405 | 0.0369 | 0.912 | 0.950 to 1.006 | 1.05 | 2 | under |
| 4 C-012 `c012p6` | 39.1750 | 38.8200 | 0.991 | 0.981 to 0.996 | 1.05 | 2 | under |
| 4 C-012 `c012rest` | 43.1550 | 45.0450 | 1.044 | 0.998 to 1.075 | 1.05 | 2 | under |

- **Row 2, `col(d10) / 3`, is over the bar and outside its control band (a halt).** Main answers it as a `double`
  division of two `Float64` casts (Spark answers `decimal(14,6)`); the head runs the decimal division UDF, the same
  kernel and cost as the SQL row (`d10 / 3` on the head: about 0.21 s against the DataFrame row's 0.21 s). The
  cost is the correct type's, not the marker's: the integer and float controls on the same door (row 3) are at the
  bar. Not ruled here (the brief's halt).
- **Row 2, `F.expr("mod(d10, 3)")`, is over the bar** (1.2x): the call keeps datafusion-spark's `SparkMod` and adds a
  per-row decimal cast to Spark's `Remainder` type. A remedy inside the cap, not built: rewrite a decimal `mod()` onto
  the `%` path (DataFusion's `%` already has Spark's `Remainder` type; `col(d10) % 3` is at the bar), after measuring
  `mod(d, 0)` against `d % 0` in both ANSI modes. A SQL `pmod()` call takes the same cast (not timed apart);
  `F.pmod`, which the facade spells with `%` and `+`, is at the bar.
- Row 1 is faster on the head: Spark's narrower quotient type (`decimal(14,6)` for `decimal(21,13)`).
