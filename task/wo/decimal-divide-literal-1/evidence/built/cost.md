# DECIMAL-DIVIDE-LITERAL-1 — cost (delta 1, 2026-11-18)

> **Delta 1 replaced the build's two-run table** (kept in [raw/cost/build-2run/](raw/cost/build-2run/), head
> `pkg-ddl-a30a8d60`). This series runs on the delta's head, `pkg-ddl-8efe8e69` (`BUILT_FROM` `8efe8e69`, a decimal
> `mod()` on the `%` path), against main `pkg-avg-27d6149f`. It carries Fable's guidance under the owner's overnight
> delegation (not an owner ruling): the series in full, the SQL `mod()`/`pmod()` rows, and three bases for the
> DataFrame `col(d10) / 3` row.

Method:
- alternating runs of main, head, main again (`main-aa`, the control band); **five runs of every row, and runs six
  to ten for every row over the bar after five** (SQL `pmod(d10, 3)`, `F.pmod`, the DataFrame `col(d10) / 3`);
- each sample is one process under `flock -x measure.lock` plus the link and both cargo locks, `nice -n 10`,
  `taskset -c 48-57`; each timed region repeats its query 4 times after one warm run (every sample at least
  200 ms; C-012 is one replay per sample); the table gives the per-query median; the first sample waited 983 s for
  the shared measure lock (another builder's `make py-test-facade`), the rest ran without a wait;
- data: 10,000,000 rows written once as Parquet by main's package (`d10 DECIMAL(10,2)`, `d38 DECIMAL(38,2)`,
  `i1`, `i2 INT`, `x1`, `x2 DOUBLE`); each query sums its projection (`count(*)`, `sum(q)`), so the operator runs per
  row and only two values come back; "answers equal" is false where the head answers Spark's decimal type (rows 1
  and 2 `/`) or a `double` sum differs in its last digit from run to run (row 3, main alike);
- C-012 is the like-for-like replay set (`replay_work.py`, `c012p6` and `c012rest`), as in AVG-DECIMAL-ROUND-1.

Raw: [raw/cost/](raw/cost/) (`samples.jsonl`, `raw-ddl.jsonl`, `table.md`). Runner and table:
[scripts/](scripts/map.md) (`run.sh`, `ddl_cost.py`, `analyze.py`).

| row | main (s) | head (s) | head/main | control band (main-aa/main) | bar | runs | answers equal | at the bar |
|---|---|---|---|---|---|---|---|---|
| 1 SQL `d10 / 3` (`DECIMAL(10,2)`) | 0.2695 | 0.2074 | 0.769 | 0.949 to 1.018 | 1.05 | 5 | False | under |
| 1 SQL `d38 / 3` (`DECIMAL(38,2)`) | 0.2633 | 0.2542 | 0.966 | 0.907 to 1.185 | 1.05 | 5 | True | under |
| 1 SQL `mod(d10, 3)` | 0.0771 | 0.0670 | 0.869 | 1.012 to 1.095 | 1.05 | 5 | True | under |
| 1 SQL `pmod(d10, 3)` | 0.1365 | 0.1558 | 1.142 | 0.950 to 1.064 | 1.05 | 10 | True | **over, outside its band** |
| 2 DataFrame `F.expr("d10 / 3")` (a base for row 2 `/`) | 0.2742 | 0.2111 | 0.770 | 0.950 to 1.160 | 1.05 | 5 | False | under |
| 2 DataFrame `F.expr("mod(d10, 3)")` | 0.0817 | 0.0648 | 0.794 | 0.881 to 1.110 | 1.05 | 5 | True | under |
| 2 DataFrame `F.pmod(col(d10), F.lit(3))` | 0.1087 | 0.1076 | 0.990 | 0.927 to 1.113 | 1.05 | 10 | True | under |
| 2 DataFrame `col(d10) % 3` | 0.0665 | 0.0622 | 0.936 | 0.933 to 1.038 | 1.05 | 5 | True | under |
| 2 DataFrame `col(d10) / 3` | 0.0525 | 0.2067 | 3.935 | 0.891 to 1.139 | 1.05 | 10 | False | **over: the owner's row** |
| 3 DataFrame `col(i1) / col(i2)` | 0.0262 | 0.0270 | 1.032 | 0.939 to 1.062 | 1.05 | 5 | True | under |
| 3 DataFrame `col(x1) / col(x2)` | 0.0375 | 0.0379 | 1.011 | 0.879 to 1.008 | 1.05 | 5 | False | under |
| 3 SQL `i1 / i2` (`INT`) | 0.0289 | 0.0300 | 1.040 | 0.899 to 1.062 | 1.05 | 5 | False | under |
| 3 SQL `x1 / x2` (`DOUBLE`) | 0.0374 | 0.0388 | 1.038 | 1.040 to 1.164 | 1.05 | 5 | True | under |
| 4 C-012 `c012p6` | 38.9300 | 39.1300 | 1.005 | 0.952 to 1.004 | 1.05 | 5 | True | under |
| 4 C-012 `c012rest` | 42.3800 | 42.8800 | 1.012 | 0.953 to 1.007 | 1.05 | 5 | True | under |

The owner's row, `col(d10) / 3` on the head, against three bases on main:

| base on main | main (s) | head `col(d10) / 3` (s) | ratio |
|---|---|---|---|
| main's DataFrame `col(d10) / 3` (a `double` division) | 0.0525 | 0.2067 | 3.935 |
| main's SQL `d10 / 3` | 0.2695 | 0.2067 | 0.767 |
| main's `F.expr("d10 / 3")` on the DataFrame door | 0.2742 | 0.2067 | 0.754 |

- **SQL `pmod(d10, 3)` is over the bar after ten runs and outside its control band: 1.142x (band 0.950 to 1.064) —
  a halt, not ruled here.** The head keeps datafusion-spark's `SparkPmod` and casts each row to Spark's `Pmod` type
  (`decimal(3,2)`; main answers `decimal(12,2)`). The only rewrite step 3 allows, onto the spelling `F.pmod` uses
  (`((a % n) + n) % n`), was measured against Spark first and answers a negative divisor unlike Spark
  (`F.pmod(7.25, -3)` is `-1.75`, Spark `1.25`; [bar.md](bar.md) §0.2), where the SQL `pmod(d, -3)` answers as
  Spark: it would trade the cost for wrong values, so it is not built.
- **The DataFrame `col(d10) / 3` is the owner's row** (Fable's guidance on question 1): measurements only, no
  verdict here. Against main's DataFrame `col(d10) / 3` (a `double` division of two `Float64` casts, the wrong type)
  it is 3.935x; against main's SQL `d10 / 3` 0.767x; against main's `F.expr("d10 / 3")` on the same DataFrame door
  (the closest same-door base that answers a decimal) 0.754x. The head runs the decimal division UDF, the same cost
  as its own SQL and `F.expr` rows (0.207 s against 0.207 s and 0.211 s).
- **`F.expr("mod(d10, 3)")` is under the bar: 0.794x** (the build's head: 1.207x). The `%` path drops the build's
  per-row cast; the SQL `mod(d10, 3)` is 0.869x.
- `F.pmod` was over after five runs (1.081x, band to 1.079) and is under after ten (0.990x, band 0.927 to 1.113); its
  plan is the same on the build's head and the delta's.
- The build's near rows are settled under the bar: `col(d10) % 3` 0.936x, DataFrame `i1 / i2` 1.032x, C-012
  `c012rest` 1.012x. Row 1 is faster on the head (Spark's narrower quotient type).
