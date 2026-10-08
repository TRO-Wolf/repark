# Unit ledger — OFFSET-NESTED-SORT-1 · an `OFFSET` under a nested `ORDER BY` answers no rows at one input partition

**Date:** 2026-10-08 · **Branch:** `fix/offset-nested-sort-1` · **Base:** `535d51f5` (`main`)
**Model:** Claude Opus 5.5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's product PR merges.

**Card:** [../../roadmap/mid-term/offset-nested-sort-1-card-2026-10-08.md](../../roadmap/mid-term/offset-nested-sort-1-card-2026-10-08.md).
**Registry:** row `OFFSET-NESTED-SORT-1` at the end of §7 of
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

**Why now.** The C-3 lane reported that
`SELECT v FROM (SELECT v FROM t ORDER BY v LIMIT 5 OFFSET 4990) ORDER BY v` answers no rows at
one input partition and 5 rows at 16. The report is confirmed, and it is wider than reported:
the defect is in the pinned DataFusion 54.1.0, it has a second form that is wrong at every
partition count, and through the Python facade the reported statement was wrong at 1, 2 and 16.

**Not in this unit:** `Cargo.toml` (the DataFusion pin is unchanged), `.github/`, `STATUS.md`.

## 1. Where the defect is

**DataFusion 54.1.0, the `EnforceSorting` physical rule**, in its sort pushdown
(`datafusion-physical-optimizer-54.1.0/src/enforce_sorting/sort_pushdown.rs`). The rule ignores
the `skip` of a `GlobalLimitExec`. No RePark rule or extension is involved: a bare
`datafusion::prelude::SessionContext` reproduces every wrong cell RePark showed over the same
sources.

Two code paths, two wrong plan forms:

1. **A row cap pushed through a skip** (`assign_initial_requirements`, line 74, and the
   `satisfy_parent` branch of `pushdown_sorts_helper`, line 190). The rule gives each child the
   `fetch()` of its parent as a cap. For `GlobalLimitExec: skip=4990, fetch=5` it gives the sort
   below a cap of 5, where the sort must keep `skip + fetch` = 4995 rows. `SortExec:
   TopK(fetch=4995)` becomes `TopK(fetch=5)`; the limit then skips 4990 of 5 rows.
2. **A sort pushed through a skip** (`pushdown_requirement_to_children`, the branch for a node
   with a `fetch` at line 297, and `handle_custom_pushdown` for a limit with no `fetch`). A
   limit maintains its input order, so the rule moves an outer `SortExec` below it. Below a
   limit with `skip > 0` that changes which rows are skipped.

**Rule bisect.** `EXPLAIN VERBOSE` prints the plan after every rule. For the reported statement
at one partition the plan is right after every logical rule and after every physical rule up to
`CombinePartialFinalAggregate`; `EnforceSorting` is the first rule whose output is wrong
(`TopK(fetch=5)` under `skip=4990`). `LimitPushdown` then folds that plan, consistently, to the
`fetch=0` form the report quoted. Replacing only `EnforceSorting` in RePark's rule list with
DataFusion's own (the `stock_context` helper of the pin module) brings every wrong cell back;
replacing it with the guarded rule removes every one.

**DataFusion 55.0.0** (`~/.cargo/registry/src/…/datafusion-physical-optimizer-55.0.0/src/ensure_requirements/enforce_sorting/sort_pushdown.rs`):
the same `fetch: child.plan.fetch()` assignment and the same branches, with a distribution
field added. No `skip` appears in the pushdown. Read, not run: 55.0.0 is not the pin and this
unit does not change it. `limit_pushdown.rs` differs between the two only in a statistics call.

## 2. The reported statement, on a memory table (`CREATE TABLE m AS …`, 5000 rows)

Logical plan (the same at 1, 2 and 16 partitions):

```text
initial                                      optimized
Sort: m.v ASC NULLS LAST                     Sort: m.v ASC NULLS LAST
  Projection: m.v                              Limit: skip=4990, fetch=5
    Limit: skip=4990, fetch=5                    Sort: m.v ASC NULLS LAST, fetch=4995
      Sort: m.v ASC NULLS LAST                     TableScan: m projection=[v]
        Projection: m.v
          TableScan: m
```

Physical plan, stock DataFusion 54.1.0, **1 partition** — answers **0 rows**:

```text
initial_physical_plan
SortExec: expr=[v@0 ASC NULLS LAST], preserve_partitioning=[false]
  GlobalLimitExec: skip=4990, fetch=5
    SortExec: TopK(fetch=4995), expr=[v@0 ASC NULLS LAST], preserve_partitioning=[false]
      DataSourceExec: partitions=1, partition_sizes=[1]
after EnforceSorting
OutputRequirementExec: order_by=[(v@0, asc)], dist_by=SinglePartition
  GlobalLimitExec: skip=4990, fetch=5
    SortExec: TopK(fetch=5), expr=[v@0 ASC NULLS LAST], preserve_partitioning=[false]
      DataSourceExec: partitions=1, partition_sizes=[1]
after LimitPushdown (the final plan)
GlobalLimitExec: skip=4990, fetch=0
  SortExec: TopK(fetch=4990), expr=[v@0 ASC NULLS LAST], preserve_partitioning=[false]
    DataSourceExec: partitions=1, partition_sizes=[1]
```

Stock, **2 partitions** — answers 5 rows (16 partitions: the same plan with `partitions=16`):

```text
after EnforceDistribution
OutputRequirementExec: order_by=[(v@0, asc)], dist_by=SinglePartition
  SortExec: expr=[v@0 ASC NULLS LAST], preserve_partitioning=[false]
    GlobalLimitExec: skip=4990, fetch=5
      SortExec: TopK(fetch=4995), expr=[v@0 ASC NULLS LAST], preserve_partitioning=[false]
        CoalescePartitionsExec
          DataSourceExec: partitions=2, partition_sizes=[1, 0]
final
GlobalLimitExec: skip=4990, fetch=5
  SortPreservingMergeExec: [v@0 ASC NULLS LAST], fetch=4995
    SortExec: TopK(fetch=4995), expr=[v@0 ASC NULLS LAST], preserve_partitioning=[true]
      DataSourceExec: partitions=2, partition_sizes=[1, 0]
```

So the partition count that matters is the scan's, not the session's: a memory table built by
`CREATE TABLE AS` has `target_partitions` partitions, which is why the report saw 1 against 16.
A scan that has one partition at every setting is wrong at every setting (§3).

With the guard, 1 partition — answers `4991..4995`:

```text
GlobalLimitExec: skip=4990, fetch=5
  SortExec: TopK(fetch=4995), expr=[v@0 ASC NULLS LAST], preserve_partitioning=[false]
    DataSourceExec: partitions=1, partition_sizes=[1]
```

The second form, `SELECT v FROM (SELECT v FROM m ORDER BY v OFFSET 4990) ORDER BY v DESC`, at
16 partitions. Stock answers `10..1`; Spark and the guarded plan answer `5000..4991`:

```text
stock                                               guarded
GlobalLimitExec: skip=4990, fetch=None              SortExec: expr=[v@0 DESC]
  SortExec: expr=[v@0 DESC]                           GlobalLimitExec: skip=4990, fetch=None
    SortPreservingMergeExec: [v@0 ASC NULLS LAST]       SortPreservingMergeExec: [v@0 ASC NULLS LAST]
      SortExec: expr=[v@0 ASC NULLS LAST]                 SortExec: expr=[v@0 ASC NULLS LAST]
        DataSourceExec: partitions=16                       DataSourceExec: partitions=16
```

## 3. The grid against Spark

70 SQL statements over a 5000-row relation `(v = 1..5000, k = v % 7)`, each recorded on live
Spark 4.1.2 at 1 and at 16 input partitions (the two recordings agree for every statement).
The statements, their Spark rows and their family are
`crates/repark-core/src/session/tests/skipping_limit_grid.tsv`. Each statement ran at
`target_partitions` 1, 2 and 16 over four sources: `m` (a `CREATE TABLE AS` memory table), `t`
(a view over `generate_series`, which declares its order), `p` (a one-file parquet scan written
in shuffled order) and `ice.s.i` (an Iceberg table in a memory catalog, RePark only).

| Engine | Cells | Wrong |
|---|---|---|
| bare DataFusion 54.1.0 `SessionContext` (`m`, `t`, `p`) | 630 | 121 |
| RePark with DataFusion's own `EnforceSorting` (main's behaviour) | 840 | 175 |
| RePark with the guard | 840 | 0 |

Wrong cells per source and partition count, RePark with the stock rule (bare DataFusion has
the same counts for `m`, `t`, `p`):

| Source | 1 | 2 | 16 |
|---|---|---|---|
| `m` memory table | 20 | 9 | 9 |
| `p` parquet, one file | 20 | 17 | 17 |
| `ice.s.i` Iceberg | 20 | 17 | 17 |
| `t` sorted series view | 11 | 9 | 9 |

The 20 wrong statements. "Scan one" means: wrong wherever the scan has one partition (`m` at 1;
`p` and Iceberg at 1, 2 and 16).

| Statement (fixture name) | Shape | Wrong where | Stock answers | Spark answers |
|---|---|---|---|---|
| `reported` | `LIMIT 5 OFFSET 4990` under the same-key sort | scan one | 0 rows | 5 rows |
| `cte` | the same through a CTE | scan one | 0 | 5 |
| `triple` | two nested outer sorts | scan one | 0 | 5 |
| `offset_small` | `LIMIT 5 OFFSET 2` | scan one | 3 rows `3,4,5` | 5 rows `3..7` |
| `limit_past_end` | `LIMIT 50 OFFSET 4990` | scan one | 0 | 10 |
| `join_inner` | the inner sort is over a join | scan one | 0 | 5 |
| `window_between` | a window between the limit and the outer sort | scan one | 0 | 5 |
| `distinct_inner` | `SELECT DISTINCT` below | `m`, `p`, Iceberg at 1 | 0 | 5 |
| `filter_below` | a filter below the inner sort | `m`, `p`, Iceberg at 1 | 0 | 5 |
| `group_inner` | `GROUP BY` below, `LIMIT 2 OFFSET 4` | every source at 1 | 0 | 2 |
| `outer_looser_key` | inner `ORDER BY v, k`, outer `ORDER BY v` | scan one, and `t` at 1 | 0 | 5 |
| `outer_stricter_key` | inner `ORDER BY v`, outer `ORDER BY v, k` | every cell | 0 | 5 |
| `window_below` | a window below the inner sort | every cell | 0 | 5 |
| `offset_only_desc` | `OFFSET 4990`, outer `ORDER BY v DESC` | every cell | `10..1` | `5000..4991` |
| `offset_only_other_key` | `OFFSET 4990`, outer `ORDER BY k, v` | every cell | the last 10 rows of the `k, v` order (`4934..4997`, all `k = 6`) | rows `4991..5000`, ordered by `k, v` |
| `offset_only_union` | two offset-only arms, outer `DESC` | every cell | `10..1` twice | `5000..4991`, `10..1` |
| `offset_only_window` | offset-only, a window above | every cell | `10..1` | `5000..4991` |
| `window_below_offset_only` | a window below, offset-only | every cell | `10..1` | `5000..4991` |
| `group_inner_offset_only` | `GROUP BY`, `OFFSET 4`, outer `DESC` | every cell | `2,1,0` | `6,5,4` |
| `triple_offsets` | two nested offsets, opposite sorts | every cell | `991..1000` | `4001..4010` |

The 50 statements that were already right are the controls: `LIMIT` without `OFFSET`, `OFFSET`
past the row count, `OFFSET 0`, `LIMIT 0`, an offset-only limit under a same-key sort, the
limit under a sort on a different key or direction (`outer_other_key`, `outer_desc`), under a
projection, under a filter, under and over a `UNION ALL`, over a join, an outer `LIMIT` and
`OFFSET`, an aggregate above, a scalar subquery, an `IN` subquery, a top-level
`ORDER BY … LIMIT … OFFSET`, and no sort at all on one side. Fetch pushed into a sort (TopK)
and not: the offset-only statements have no TopK; the `LIMIT` ones do.

**The facade on main** (a module built from main's rule list, then one built with the guard;
`createDataFrame` rows in shuffled order, registered as a view):

| Door | Cells | Wrong on main | Wrong with the guard |
|---|---|---|---|
| Spark-dialect SQL (`spark.sql`), 65 statements × 3 partition counts | 195 | 47 | 0 |
| native ANSI (`repark.sql`), 65 statements × 3 | 195 | 23 | 0 |
| DataFrame, 15 shapes × 3 | 45 | 18 | 0 |

A `createDataFrame` view scans one partition at every `spark.sql.shuffle.partitions`, so on the
Spark door the reported statement answered no rows at 1, 2 and 16. The DataFrame shapes wrong
on main: `orderBy("v").offset(4990).limit(5).orderBy("v")`,
`sort("v").limit(4995).offset(4990).orderBy("v")`, the same under `orderBy("v", "k")`,
`orderBy("v").offset(4990).orderBy(desc("v"))`, and `orderBy("v").offset(4990).orderBy("k",
"v")`, each at all three counts. (The facade run used the 65 statements of the grid before the
last five were added; the pinned facade set is 31 statements and 15 shapes, §5.)

## 4. The guard

`crates/repark-core/src/session/df_guards/skipping_limit.rs`; the design, its cost and the
routes not taken are in that directory's
[map.md](../../../crates/repark-core/src/session/df_guards/map.md). In short:
`SkipSafeEnforceSorting` wraps DataFusion's `EnforceSorting` under the same name and at the
same position. When the plan has a `GlobalLimitExec` with `skip > 0` it optimizes each such
limit's input as a plan of its own, replaces the limit with a leaf (`SealedSkippingLimitExec`)
that reports the limit's properties, runs the rule on what is above, and puts the limits back.
The rule can then push neither a cap nor a sort through a skip. When the plan has no skipping
limit the wrapper calls the inner rule on the plan untouched.

**Why it is correct.** A `GlobalLimitExec` asks no ordering of its input, and its
one-partition requirement is met before this rule runs. So the limit's input is a complete
plan, and optimizing it alone loses no requirement. The leaf reports the truth about the
limit's output (its ordering, one partition), so the rule's decisions above it stay valid; the
only thing the rule can no longer do is move work across the skip, and every such move is one
of the two defects or the harmless merge below.

**What it gives up.** When an outer sort refines the inner one (`ORDER BY v, k` over
`ORDER BY v`), DataFusion merged the two sorts below the limit. RePark now sorts the rows left
after the `OFFSET` a second time. Only a statement with an `OFFSET` under a sort pays this.

## PROPOSITION LEDGER — OFFSET-NESTED-SORT-1 — 2026-10-08

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The reported statement answers no rows at one partition and 5 rows at 16 on main, in a `repark-core` session and on a bare DataFusion 54.1.0 `SessionContext`; the plans at 1, 2 and 16 are those of §2. | A Rust reproduction that runs DataFusion's own rule on a RePark session state, and the plan text. | PROVEN | `stock_enforce_sorting_still_loses_the_rows_at_one_partition_only` (stock: `0 []` at 1, `5 [4991..4995]` at 16; guarded: 5 at 1) and `reported_plan_keeps_the_inner_top_k_as_wide_as_skip_plus_fetch` (stock text `skip=4990, fetch=0` over `TopK(fetch=4990)`). The bare-context run is the 630-cell row of §3 (scratch probe, not kept in the tree: the stock-rule pins hold the same answers on RePark's state). |
| C-002 | Through the facade the defect reproduces on main on the Spark-dialect SQL door, the native ANSI door and the DataFrame door, and every measured cell answers Spark's rows with the guard. | A module built from main's rule list and one built with the guard, the same probe on each; then pins on all three doors. | PROVEN | §3 facade table (47 / 23 / 18 wrong on main, 0 / 0 / 0 with the guard). `python/repark/tests/test_offset_nested_sort_1.py`: `test_spark_sql_door_answers_spark_rows` and `test_dataframe_door_answers_spark_rows` at 1, 2, 16 with value and Arrow type; `test_native_sql_door_answers_spark_rows` at 1, 2, 16 by value (the native door keeps DataFusion's own types, for example `uint64` for `row_number`). |
| C-003 | The shape family is measured against live Spark 4.1.2: 70 statements, 20 of them wrong somewhere, 50 controls; every wrong cell is recorded. | The grid of §3 and its fixture. | PROVEN | `skipping_limit_grid.tsv` (70 lines: 11 `one-partition`, 9 `every-partition`, 50 `control`, the family being the memory-table behaviour); `grid_fixture_carries_every_recorded_cell` holds the counts and the reported statement's text. The brief's shapes are all present: OFFSET without LIMIT, LIMIT without OFFSET, OFFSET past the row count, a nested sort on a different key, under a projection, a filter, a UNION, over a join, over an Iceberg scan and a parquet scan, with and without TopK. |
| C-004 | The defect is in DataFusion 54.1.0's `EnforceSorting` sort pushdown, which ignores `GlobalLimitExec::skip`; no RePark rule takes part; DataFusion 55.0.0 carries the same code. | The rule bisect, the bare-context reproduction, the source reading of §1. | PROVEN | §1. `stock_enforce_sorting_still_loses_the_rows_at_one_partition_only` and `stock_enforce_sorting_still_sorts_below_an_offset` swap only that one rule and get the wrong answers back at the recorded partition counts. The 55.0.0 claim is a source reading, not a run. |
| C-005 | RePark's session installs DataFusion's recommended physical rule list with `EnforceSorting` wrapped by the guard, in the same position and under the same name, followed by the three RePark physical rules. | A pin on the installed rule names. | PROVEN | `guarded_rule_list_is_stock_datafusion_in_order_then_the_repark_rules`. |
| C-006 | With the guard every one of the 70 statements answers Spark's rows at 1, 2 and 16 partitions over a memory table, a sorted series view, a one-file parquet scan and an Iceberg scan (840 cells). | Rust pins per family and per source. | PROVEN | `one_partition_family_answers_spark_rows_at_one_partition` / `_at_two_partitions` / `_at_sixteen_partitions`, `every_partition_family_answers_spark_rows_at_each_partition_count`, `control_cells_keep_spark_rows_at_each_partition_count`, `grid_answers_spark_rows_over_a_sorted_series_view`, `…_over_a_parquet_scan`, `…_over_an_iceberg_scan`. |
| C-007 | A live Spark cell replays every facade cell and follows the live-cell rules. | The cell, run with `REPARK_PARITY_LIVE=1`, collected together with another live module. | PROVEN | `test_live_spark_answers_every_recorded_cell` uses the shared `spark_engine` fixture (the conftest guard fails any test that stops the context), registers one temp view of its own name (`offset_nested_sort_1_oracle`; no catalog is created, so none can collide), and reads or pops no environment variable. Run together with `test_win_slide_1.py`'s live cells: `15 passed, 152 deselected`. |
| C-008 | The pins cover the partition-dependent path and the guard's own branches. | Mutations, each run against the pin module. | PROVEN | §5: five mutations, each red; M1 is red at one partition and green at two and sixteen for the one-partition family. |
| C-009 | The guard adds no work to a statement without an `OFFSET` beyond one walk of the physical plan, and such a statement plans exactly as stock DataFusion plans it. | A plan-identity pin, the rule-list pin, a measurement. | PROVEN | `statements_without_offset_plan_exactly_as_stock_datafusion` (12 statements at 1, 2, 16: the same plan text as the stock rule gives, including `SELECT … ORDER BY … LIMIT n`, a nested `LIMIT` under a refining sort, a `LIMIT` over a `UNION ALL`). §6 has the measurement. |
| C-010 | The card row is closed with its date and the registry row names the upstream defect and the retirement event. | The card, the registry row, the retirement note. | PROVEN | Card status line and its `map.md` row; registry row `OFFSET-NESTED-SORT-1` (§7 of the parity registry); retirement note in `crates/repark-core/src/session/df_guards/map.md`: the two `stock_enforce_sorting_still_*` pins go red when DataFusion fixes the rule. |
| C-011 | The unit's gates pass on the final tree. | The gate runs. | PROVEN | §7. |

## 5. Mutations

Each mutation was applied to the working tree, the pin module (`cargo test -p repark-core --lib
-- session::tests::skipping_limit`, 16 tests) was run, and the tree was restored.

| # | Mutation | Result |
|---|---|---|
| M1 | Remove the guard: delete the `with_physical_optimizer_rules(…)` line in `df_guards.rs`. | **8 red, 8 green.** `one_partition_family_answers_spark_rows_at_one_partition` is **red**; `…_at_two_partitions` and `…_at_sixteen_partitions` stay **green**, which shows the pin covers the partition-dependent path. Also red: the every-partition family, the three source grids (series view, parquet, Iceberg), the plan-text pin, and the guarded half of both stock-rule pins. Green: the controls, the plan identity, the rule list (it compares names), the seal pins. |
| M2 | Seal only a limit that has a `fetch` (`skip() > 0 && fetch().is_some()`). | **5 red.** The every-partition family, the three source grids and `stock_enforce_sorting_still_sorts_below_an_offset`; the reported statement and the one-partition family stay green. The offset-only form is pinned on its own. |
| M3 | Leave the limit's input unoptimized (seal the limit over its original children). | **5 red.** Planning fails where the rule had to add a sort or a merge below the limit: the every-partition family, the three source grids, `no_seal_survives_into_a_final_plan`. |
| M4 | Leave the seals in the final plan (skip the unseal step). | **Red.** `no_seal_survives_into_a_final_plan`, the plan-text pin and every grid that runs a sealed plan. (First run, before the seal refused to execute: 7 red, among them a DataFusion panic in `OutputRequirementExec::execute`, the marker node the later rule could not see inside a seal. That finding is why `execute` on a seal is now an internal error.) |
| M5 | Seal every limit, with or without a skip (`is_some()`). | First run: **0 red** — a gap in the pins. The plan-identity pin gained four statements with a `GlobalLimitExec` that has no skip; rerun result below. |

Mutation reruns on the final pins:

| # | Rerun | Result |
|---|---|---|
| M4 | with `execute` on a seal an internal error | **12 red, 4 green** — every test that plans or runs a statement with an `OFFSET` fails loudly, `no_seal_survives_into_a_final_plan` among them. Green: the fixture count, the rule list, the seal's own pin, the plan identity (no `OFFSET`). |
| M5 | with the four added statements | **1 red, 15 green** — `statements_without_offset_plan_exactly_as_stock_datafusion` is red: sealing a limit that has no skip changes the plan of a statement without an `OFFSET`. |

## 6. Perf guard

**What a plain `SELECT … ORDER BY … LIMIT n` executes that it did not before:** one
`TreeNode::exists` walk of the physical plan inside the `EnforceSorting` pass, which visits
each node once and allocates nothing. No rule pass is added (the guard replaces a rule in
place), and the plan that comes out is the same text as before
(`statements_without_offset_plan_exactly_as_stock_datafusion`), so execution is unchanged.

Measured anyway, in one process, debug build, the guarded context against a context that
differs only in carrying DataFusion's own `EnforceSorting`, and against a second such context
as the noise floor; 41 rounds, the three contexts in rotating order, the median of the
per-round ratios:

| Statement | Guarded | Stock rule | Stock rule again | Guarded / stock | Stock again / stock |
|---|---|---|---|---|---|
| `SELECT 1`, plan and run, 200 per sample | 1396.1 µs | 1392.9 µs | 1400.9 µs | +0.16 % | +0.17 % |
| `SELECT 1`, plan only, 200 per sample | 1307.8 µs | 1306.5 µs | 1311.9 µs | +0.08 % | +0.03 % |
| TopK over 1M rows (`SELECT s FROM big ORDER BY s LIMIT 10`), plan and run, 4 per sample | 27139.3 µs | 27212.1 µs | 27060.2 µs | −0.25 % | −0.27 % |
| the same, plan only, 100 per sample | 3905.1 µs | 3908.0 µs | 3904.0 µs | +0.16 % | +0.19 % |

The guarded column differs from the stock column by no more than two identical stock contexts
differ from each other (at most 0.27 %). Nothing is slower by 2 %. The times are debug-build
times on a shared box, taken while holding the cargo lock; they compare the two rule lists and
say nothing about release speed. An earlier run of this probe with 3 runs per sample and no
noise floor showed +5.95 % for the TopK statement end to end while its plan-only figure was
+0.89 % and its plan text was identical; that run is not evidence either way and is why the
noise-floor column exists. The probe was a scratch test and is not in the tree.

## 7. Gates

Every cargo command ran under the build lock, on the final tree (the scratch probe removed).

| Gate | Result |
|---|---|
| `cargo fmt --check` | exit 0 |
| `cargo test -p repark-core -p repark-spark -p repark-sql --lib` | exit 0 — `repark-core` 1308 passed, 1 ignored; `repark-spark` 2630 passed, 5 ignored; `repark-sql` 392 passed |
| `cargo test -p repark-core --lib -- session::tests::skipping_limit` | 16 passed |
| `make rust-clippy` | exit 0 |
| `make rust-panic-ban` | exit 0 |
| `make develop` | exit 0 |
| `pytest python/repark/tests -k "limit or offset or sort or order" -n 8` | 677 passed, 43 skipped, 3 xfailed |
| `pytest python/repark/tests -n 8` (the whole facade suite, as a regression check) | 15692 passed, 485 skipped, 153 xfailed, 0 failed |
| `pytest python/repark/tests/test_offset_nested_sort_1.py` | 10 passed, 1 skipped (the live cell, without the live flag) |
| the live cell, `REPARK_PARITY_LIVE=1`, with `test_win_slide_1.py`'s live cells | 15 passed |
| `ruff check .`, `ruff format --check .` (0.15.22) | clean |
| `python3 scripts/check_rust_file_size.py` | 1124 files clean |
| `./scripts/check_lib_rs.sh`, `./scripts/check_lib_py.sh`, `./scripts/check_crate_dag.sh` | clean |
| `python3 scripts/sync_map_md.py --check`, `bash scripts/check_map_md.sh --base origin/main` | clean |
| `python3 scripts/check_docs_links.py` | clean |
| `python3 scripts/check_ledger_grammar.py` | clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xsec1 origin/main HEAD` | 0 hits |

`pytest-xdist` is not in the clone's `.venv`; the `-n 8` runs used
`uv run --no-sync --with pytest-xdist`, which layers it without changing the environment. The
live cell needs PySpark, which the `.venv` lacks: it ran on the `.venv` interpreter with the
Spark environment's site-packages appended to `sys.path`.

## 8. Notes for the orchestrator

- **Upstream.** No upstream issue was filed (this lane does not use the network). The two
  locations in §1 and the two statements of §2 are a complete report for
  `apache/datafusion`.
- **The card's second gate** ("an Opus verifier on the product PR") is the orchestrator's.
- **Wider than the card.** The card says "at one input partition". On the Spark door, over a
  `createDataFrame` view, a one-file parquet scan and an Iceberg scan, the reported statement
  was wrong at every partition count, and the offset-only form was wrong everywhere.
- **A parquet file written from a sorted plan declares its order**, so a scan of it never
  showed the defect. The parquet source of the pins is written in shuffled order for that
  reason.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: offset-nested-sort-1
  complete: true
  reattested: []
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-011 walked against behavior — the reproduction on stock DataFusion and on RePark's state, the three facade doors on a module built from main and one built with the guard, the 70-statement Spark grid over four sources at three partition counts, the rule list, the live cell and the gates.
      artifacts: [task/ledgers/staging/offset-nested-sort-1-ledger.md, crates/repark-core/src/session/tests/skipping_limit.rs, python/repark/tests/test_offset_nested_sort_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary inputs run as grid cells — OFFSET 0, OFFSET past the row count with and without LIMIT, LIMIT 0, LIMIT past the end, a small offset, nested offsets, a skip with no fetch and a fetch with no skip, sorted and unsorted sources, one and many scan partitions.
      artifacts: [crates/repark-core/src/session/tests/skipping_limit_grid.tsv]
    - id: AT-3
      status: ATTACKED
      evidence: The seal's two failure paths are pinned — execute refuses with an internal error and with_new_children refuses a child; mutation M3 shows a limit input left unoptimized fails planning loudly instead of answering wrong rows.
      artifacts: [crates/repark-core/src/session/tests/skipping_limit.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The rule holds no state — the wrapper owns only the inner rule, and a seal owns only its limit; nested skipping limits are sealed bottom-up and restored top-down (triple_limits, triple_offsets, join_two_windows cells); no seal survives planning for any grid statement at 1 and 16 partitions.
      artifacts: [crates/repark-core/src/session/df_guards/skipping_limit.rs, crates/repark-core/src/session/tests/skipping_limit.rs]
    - id: AT-5
      status: N/A
      justification: A physical-plan rewrite with no privileged action, no secret, no parsed input and no path handling.
    - id: AT-6
      status: ATTACKED
      evidence: The defect is a wrong row count and wrong rows; every cell compares the full ordered row list with Spark's, and the facade cells compare the Arrow types as well; statements without an OFFSET keep the stock plan text.
      artifacts: [crates/repark-core/src/session/tests/skipping_limit.rs, python/repark/tests/offset_nested_sort_1_spark_oracle.json]
    - id: AT-7
      status: ATTACKED
      evidence: No rule pass is added; a statement without an OFFSET gains one walk of the physical plan and keeps its plan; the measurement in section 6 puts the difference inside the noise floor; the one cost given up (a second small sort above an OFFSET under a refining sort) is recorded.
      artifacts: [task/ledgers/staging/offset-nested-sort-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: The DataFusion 54.1.0 behaviour is read at its source lines and pinned by two stock-rule tests that go red when upstream changes; the 55.0.0 claim is marked as a reading, not a run; the rule-list pin holds DataFusion's recommended order.
      artifacts: [crates/repark-core/src/session/tests/skipping_limit.rs, crates/repark-core/src/session/df_guards/map.md]
    - id: AT-9
      status: N/A
      justification: No log, metric or error text reaches a user from this change; the only new error texts are two internal errors that planning never raises on a correct tree, and both are pinned.
    - id: AT-10
      status: ATTACKED
      evidence: Five mutations run against the pin module; M5 found a gap (sealing every limit stayed green) and the plan-identity pin was widened until it went red; M1 is red at one partition and green at two and sixteen for the one-partition family.
      artifacts: [task/ledgers/staging/offset-nested-sort-1-ledger.md, crates/repark-core/src/session/tests/skipping_limit.rs]
```
