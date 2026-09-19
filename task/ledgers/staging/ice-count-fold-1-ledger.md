# Unit ledger — ICE-COUNT-FOLD-1 · count(*) folds from an exact Iceberg row count on both doors

## Round 1 (2026-09-19)

**Date:** 2026-09-19 · **Branch:** `perf/ice-count-fold-1` · **Base:** `0e3a899f` (origin/main)
**Model:** Claude Opus 5 (claude-opus-5), high effort · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** — two analyzer rules share one count-star walker;
no fork change, no scan-statistics change, no `Precision::Exact` condition touched, no other
aggregate, no new dependency, no Python decision, no `STATUS.md`, no `Cargo.toml` / `Cargo.lock`.

**Why now.** The orchestrator measured (2026-09-19, a 200-file 10,000,000-row Iceberg table, no
deletes) `EXPLAIN SELECT count(*) FROM t` with `datafusion.explain.show_statistics=true`: the
`IcebergTableScan` advertised `Rows=Exact(10000000)`, yet the plan kept
`AggregateExec(Partial/Final)` over the scan and execution read every data file's footer
(200 × 512 KiB). DataFusion's `AggregateStatistics` rule asks `Count::value_from_stats`, which
folds only a column with an exact null count or the literal `COUNT_STAR_EXPANSION` =
`Int64(1)`. RePark's integer-literal typing narrowed that literal to `Int32(1)`
(`count(Int32(1)) AS count(Int64(1))`), so the existing fold never fired.

**Not in this unit:** the fork, the scan's statistics, the Exact conditions, `COUNT(column)`
(wave 2 ICE-SCAN-STATS-1 in the
[read-perf slate](../../roadmap/mid-term/ice-read-perf-slate-2026-09-18.md)), the output-name
gaps against Spark recorded below, equality deletes (RePark writes none, so no test idiom
exists for them).

## Fix

Two rules narrowed the literal, not one. The brief named `SparkIntegerLiteral` in
`crates/repark-functions/src/spark_result_types.rs`; the Spark SQL and Python doors uninstall
it (`spark_door_post_coercion_rules`) and narrow with the early `SparkIntegralLiteral` in
`crates/repark-spark/src/spark_literal_typing.rs`, which runs before `type_coercion`. Both now
walk expressions through one public helper, `transform_keeping_count_star` (top-down, leaf
rewrite as a parameter). A non-distinct `count` whose only argument is the literal `1`
(`Int32` or `Int64`) keeps, or gets, `Int64(1)`, and its argument is skipped; its `FILTER`
and `ORDER BY` still narrow. `SparkIntegralLiteral`'s plan pre-check also fires on
`needs_count_star_expansion`, because the DataFrame door's `groupBy().count()` and
`F.count('*')` build `count(Int32(1))` and carry no `Int64` literal at all. `NamePreserver`
keeps every output name; the result type is `Int64` either way.

**Design choice — `count(1)` and `count(5)`.** SQL `count(1)` plans as `count(Int64(1))`,
the same expression as `count(*)`. DataFusion cannot tell them apart, and Spark's `count(1)`
is count-star, so it keeps `Int64(1)` and folds. Its name stays main's `count(Int64(1))`.
`count(5)` still narrows to `Int32(5)` and does not fold, because `value_from_stats` compares
against `Int64(1)` exactly. Rewriting any non-null constant to the expansion would be
semantically sound, but it changes a user-written argument for a rare shape and widens the
unit past the defect. `count(DISTINCT 1)` narrows as before; the distinct fold needs a column.

## Names, types and values main answers (probe on main's release native, unchanged after)

| Door / expression | Name (main = after) | Type | Spark 4.1.2 name |
|---|---|---|---|
| `spark.sql("SELECT count(*) …")` | `count(*)` | LongType | `count(1)` |
| `spark.sql("SELECT count(1) …")` | `count(Int64(1))` | LongType | `count(1)` |
| `spark.sql("SELECT count(5) …")` | `count(Int64(5))` | LongType | `count(5)` |
| `df.groupBy().count()` | `count` | LongType | `count` |
| `df.agg(F.count('*'))` / `F.count(lit(1))` | `count(1)` | LongType | `count(1)` |
| `df.count()` | Python `int` | — | Python `int` |

The three SQL-door names differ from Spark on main already; recorded, not changed here.

## PROPOSITION LEDGER — ICE-COUNT-FOLD-1 — 2026-09-19

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | The defect: integer-literal typing narrows `COUNT_STAR_EXPANSION` to `Int32(1)`, so `Count::value_from_stats` declines and `count(*)` scans every file although the Iceberg scan reports an exact row count. | Pre-fix EXPLAIN below; red runs in §Red evidence. | PROVEN | Both `SparkIntegerLiteral` and `SparkIntegralLiteral` narrowed it. |
| C-002 | A non-distinct `count` of the literal `1` analyzes to `Int64(1)` under both rules, with `FILTER` / `ORDER BY` literals still narrowed, `count(5)` / `count(DISTINCT 1)` unchanged, and output names preserved. | `spark_result_types/tests.rs` (5 new), `spark_literal_typing.rs` tests (2 new), the three `map.md` rows. | PROVEN | The `FILTER` pin is mutation-checked. |
| C-003 | `count(*)` folds (no `IcebergTableScan`) and answers correctly where the fork's Exact conditions hold: plain table, `count(1)`, `LIMIT` above, DataFrame `count_all()` and `count(lit(1_i32))`, after a copy-on-write DELETE, `VERSION AS OF` an older snapshot. It scans and answers correctly where they do not: `WHERE` residual, `LIMIT` subquery, v2 position delete, v3 deletion vector. An empty table answers 0. | `crates/repark-spark/src/tests/count_fold.rs` (9 tests, production `ReparkSession`), `test_ice_count_fold_1.py` fold pins, the six F-27 pins in `test_perf_ice_scan_1.py` that now run instead of skipping. | PROVEN | No equality-delete idiom exists. |
| C-004 | No Spark-visible change: names, LongType and values on both doors equal main's. | `test_ice_count_fold_1.py` name/type/value pins (green on main's native before the fix and after it); probe table above. | PROVEN | The SQL-door name gaps predate this unit. |
| C-005 | The bench before/after (the brief's C-9) on unit 0's bed: `SELECT count(*)` over the 200-file 10,000,000-row table — wall time and data-file footer reads. | Before: `TBD-orchestrator`. After: `TBD-orchestrator`. | OPEN | Measured by the orchestrator at the merged head. |
| C-006 | Every touched directory map is current in the same commits; the comment ban, the targeted Rust and Python runs, clippy and fmt are green. | §Gate evidence; `map.md` rows with `pins:` citations. | PROVEN | |

## Red evidence

- `cargo test -p repark-functions --lib spark_result_types` before the fix: 13 passed, 4
  failed — `count_star_keeps_the_int64_expansion`, `count_one_keeps_the_int64_expansion`,
  `count_of_an_int32_one_widens_to_the_expansion`, `count_star_filter_literal_still_narrows`,
  each `left: [Literal(Int32(1), None)]`, `right: [Literal(Int64(1), None)]`.
- `cargo test -p repark-spark --lib tests::count_fold` on the production session with only the
  repark-functions half applied: 5 passed, 4 failed (plain, copy-on-write, `VERSION AS OF`,
  DataFrame). With the `SparkIntegralLiteral` change reverted, the two new
  `spark_literal_typing` pins fail as well (23 passed, 6 failed).
- `test_ice_count_fold_1.py` on main's release native: 5 passed, 3 failed (the SQL,
  `groupBy().count()` and `F.count('*')` fold pins; every name/type/value pin passed).
- Mutation: leaving the `FILTER` expression un-narrowed turns
  `count_star_filter_literal_still_narrows` red.

Pre-fix physical plan (`count_fold.rs`, local Iceberg table):

```text
ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
  AggregateExec: mode=Single, gby=[], aggr=[count(1) as count(Int64(1))]
    CooperativeExec
      IcebergTableScan projection:[] predicate:[] snapshot_id=… N=1
```

Post-fix (`EXPLAIN SELECT count(*)` on the Python door, and `groupBy().count().explain()`):

```text
ProjectionExec: expr=[12 as count(*)]
  PlaceholderRowExec
ProjectionExec: expr=[12 as count]
  PlaceholderRowExec
```

## Gate evidence

Recorded in the hand-back, `/tmp/oc-worker/run24/pa/handback-cf-1.md`.
