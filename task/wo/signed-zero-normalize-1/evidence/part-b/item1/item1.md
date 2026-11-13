# SIGNED-ZERO-NORMALIZE-1 part B part 1, build item 1: cites, re-measure, added cells

Model `claude-opus-5-5`, 2026-11-12. Records and measurement only; no product code. The branch
`fix/signed-zero-normalize-1-part-b` is on main `276dc503` (#1307 on `ae247767`; #1307 changed two test files only). The
release module is the same build as step 0's: `_native.abi3.so` sha256
`1d3ca28a79ef05fd85f5b3a98ec60d42ba2f5761eb8abe4c38a942d1e48cdf49`, rebuilt on `276dc503` with no recompile. Every leg
ran inside the build wrapper, the Spark legs also under the JVM lock, one JVM at a time. The RePark leg headers name
`32559bbe` (item 0's records commit on `276dc503`), and the spill and explain legs say `clone_dirty: true` because item
1's own files were not yet committed; the product tree was `276dc503` throughout. The order is
[../../../order-part-b.md](../../../order-part-b.md) §2.

**Result: no premise is broken, so there is no HALT.** Three cites moved by a line or two. The RePark legs reproduce
step 0 label for label, and 18 of the 19 added cells are WRONG-READ on the base, as the clause expects.

## 1. Cites (order §2.1)

Every `file:line` in [../inventory.md](../inventory.md), sketch §3 and §4 and the order's §4 design bindings was
opened on `276dc503`. DataFusion, arrow and the fork are pinned, and every DataFusion 54.1.0 cite holds unchanged:
- `hash_utils.rs:187-192`;
- `optimizer.rs:281-310`, `:282`, `:288-289`, `:291`, `:294`, `:306`;
- `execution/context/mod.rs:734-760`;
- `config.rs:1077`;
- `expr_rewriter/mod.rs:319-332`, `:323-330`, `:361-373`;
- `plan.rs:3667-3671`, `:3520`, `:3539`, `:3998-4014`, `:2715`, `:2999`, `:4235-4243`, `:3987`, `:2266-2276`;
- `expr.rs:1100-1109`, `:1232-1247`, `:1245-1246`, `:1311-1370`, `:901`, `:845`, `:1341`;
- `builder.rs:1414-1435`;
- `replace_distinct_aggregate.rs:88-123`, `:124-192`;
- `tree_node.rs:615-630`;
- `expr_simplifier.rs:614-620`;
- `udf.rs:735-741`, `:938+`.

RePark cites:

| Cite (where) | On `276dc503` |
|---|---|
| `df_guards.rs:27`, "RePark's sort-name guards" (sketch §3, `Sort` row) | **moved to `:29`** (`pub(crate) mod sort_names;`). `:27` is now `mod signed_zero;`, which part A added |
| `df_guards.rs:89`, `ParallelWindowRule` (sketch §3, item 8) | **moved to `:94`** |
| `spark_typed.rs:18`, `__repark_spark_as__` (sketch §4) | **moved to `:19`** |
| `expr_build.rs:276-305`, `build_expr_context` (inventory §1 row 3) | holds; the function spans `:275-308` |
| `expr_build.rs:24-34`, `reciprocal_trig_or_inf` (sketch §3, found-beyond item 5) | holds in place, but **its content changed in part A** (`04d84ab9`): the THEN arm is now `cot(divisor)`, which carries the divisor's sign (`cot(±0) = ±Inf`). The item's "`csc(-0.0)` returns `+Inf`" is settled by part A. It was part A's seat, not a part B premise |
| `df_guards.rs:100` (`register_repark_isnan`), `:117-142` (`unnest_safe_optimizer_rules`, function `:116-142`), `:122-130` (the three RePark subquery rules) | hold. Seat 1b still sits right after `simplify_expressions` in the loop, so "append seat 2 after the loop" still holds |
| `repark-python` `dataframe/mod.rs:571, 629, 658, 698, 714`; `dataframe_names.rs:144`; `df_guards/join_exact.rs:23` | hold |
| `core.py:2151, 2348, 2628, 2680`; `core.py:2632-2660` (Q2: the DataFrame `ALL` forms refuse); `joins_columns.py:506` | hold (`:2628` is `intersect`, whose body is the SQL text set operation) |
| `temp_views.rs:166`; `parallel_window/rule.rs:56-58` | hold |
| `merge_sql.rs:132-145`; `merge/mod.rs:516`; `merge/copy_on_write.rs:230-235` | hold |
| `write/distribution/router.rs:33-36`; `write/distribution.rs:275`; `distribution/canonical_float.rs:17-64` | hold |
| `merge/target_scan.rs:31`; `write/scan_prune.rs:248, 342` | hold |
| `repark-distributed` `codec.rs:64, 97-98, 157`; `cluster.rs:479` | hold |
| `transpose.rs:269, 292`; `microbatch/run.rs:407` | hold |
| `is_duplicated.rs:138-152` (`:146`); `freq_items.rs:153-160` | hold |
| `spark_literal_typing.rs:140, 142, 152`; `df_guards/window_rescan.rs:51` | hold |
| `repark-connect/src/pushdown.rs:149-160`; `repark-spark/src/view_ddl/temp_view.rs:82-87` | hold |
| `function_dispatch.rs:208, 212`; `dynamic_flatten.rs:356`; `df_guards/subquery.rs:387`; `generator.rs:760` | hold (the last three are integer comparisons) |
| `repark-iceberg/src/catalog/scan_batches.rs:99-109` (no float arm) | holds |
| `bool_decimal.rs:22` | holds |
| `collection/key_by_position.rs:243` (order §9) | holds |

## 2. Re-measure (order §2.2)

All three step-0 parts ran again on `276dc503`'s RePark legs ([raw/](raw/map.md)) and were labelled against step 0's
Spark legs and the Q3 re-run's Spark floatkeys leg ([remeasure/](remeasure/map.md)). Spark did not change, so its legs
were not re-run. `compare_legs.py` compared every RePark cell with step 0's: outcome, error text, columns, rows and
label. Step 0's floatkeys variant `matrix` was compared with today's `bare`, which is the same seed under its new name.

| Difference | Cells | Allowed? |
|---|---|---|
| a label | 0 | — |
| rows, same multiset, other order | 231, all unordered cells (group-by 71, set-op 48, write 43, distinct 35, prepared 8, floatkeys 8, partition 6, compare 5, nan-payload 4, temp-view 3) | yes: row order only |
| new cells: floatkeys variant `matrix` with #1307's seeds | 28, all non-EQUAL (22 WRONG-READ, 2 SIGN-ONLY+WRONG-READ, 4 WRONG-WRITE) | yes: the item 0.2 move |
| **pivot column order** | **2: `GB-11\|double` typed, `GB-11\|float` typed** | **no: listed here.** The same rows and the same label (WRONG-READ), but the pivot's value columns came out in another order. Finding F2 |

Each difference is in [remeasure/differences.jsonl](remeasure/differences.jsonl) with its cell id and both answers.

## 3. Added cells (order §2.3)

All the cells are on step 0's seeds and doors. The raw legs are in [raw/](raw/map.md), labelled in
[new-cells/](new-cells/map.md).

**D16, spill** ([spill.md](spill.md)).
- **Setup:** 2,000,000 rows. `d` is `-0.0` when `id % 4 = 0`, `0.0` when `id % 4 = 1`, and `id` otherwise; `f` is
  `CAST(d AS FLOAT)`. A 96M `FairSpillPool` (`datafusion.runtime.memory_limit`, set at build) and 4 target
  partitions, on both RePark doors.
- **Every cell spills.** `spill_count` is between 2 and 8, from `AggregateExec`, `SortExec` and `RepartitionExec`, and
  `SortMergeJoinExec` is the join of the sort-merge cell.
- **All 16 cells are WRONG-READ on the base.** Each has the same wrong answer as the non-spill cells: the zeros split.

| Cell | Truth (Spark) | RePark, both doors, both types |
|---|---|---|
| SP-group (a million groups) | 1000001 groups, largest 1000000 | 1000002, 500000 |
| SP-window-order (`row_number` over `ORDER BY k, id`, first four ids) | 0, 1, 4, 5 | 0, 4, 8, 12 |
| SP-window-partition (`rank` over `PARTITION BY k`, count of rank 1) | 1000001 | 1000002 |
| SP-sortmerge (self join on `k`, `a.id % 1000 = 0`) | 2000000000 | 1000000000 |

A smaller pool (32M) refused on every cell with "Resources exhausted", and the default 16 partitions at 256M never
spilled the aggregate. The legs that settled these settings are not kept.

**D14, plan shape** ([plans.md](plans.md), verbatim).
- **Coverage:** one `EXPLAIN` per sketch §3 node kind, on the RePark Spark door and on native: 26 statements
  (X-distinct-on on native only), plus the typed `repartition` on the facade.
- **Spark:** Spark's `EXPLAIN EXTENDED` of each, where Spark has the form.
- **Every RePark plan answered.** Spark's X-recursive printed its analysis error (`UNION` in a recursive CTE).
- **Spark's `normalizenanandzero` positions** (its `NormalizeFloatingNumbers`):
  - present on group keys (physical plan), join keys, window partition keys and the set operations' keys;
  - absent from sort keys, join filters, non-literal comparisons, the scalar-subquery comparison and **the null-aware
    `NOT IN` anti join** (0 / 0). The last agrees with Q1's measurement.

**`Distinct::On`** (native, plan shape). At `EXPLAIN` it is already an `Aggregate` on `d` with
`first_value(… ORDER BY d, id)` and a `Sort`, as sketch §3 says. Its answer is recorded as ORACLE-MISSING:
`-0.0` and `0.0` come out as two rows.

**FLOAT key joined to a DOUBLE key** (order §5, type-variant inventory). The cell is X-float-double,
`t.f = u.d` with `u = un`. It is WRONG-READ on both doors: RePark matches the zero rows by sign only (ids 2, 4), while
Spark matches ids 1–4. Spark's plan wraps both sides, outside the cast:
`normalizenanandzero(cast(f as double)) = normalizenanandzero(d)`.

## 4. Findings (outside part 1; for the orchestrator, NOT RULED)

- **F1. No RePark cell reaches a `Repartition` node.**
  - Step 0's native GB-10 text has no `DISTRIBUTE BY`: it is a plain `GROUP BY`.
  - `DISTRIBUTE BY` refuses on native too, with "Physical plan does not support DistributeBy partitioning" (X-repartition).
    So `SPARK-DISTRIBUTE-BY-1` covers both doors.
  - The typed `repartition(4, 'd')` leaves no `Repartition` in the optimized plan (X-repartition-typed).
  - So sketch §3's `Repartition` row has no measurable cell. Its pin has to be a hand-built plan, and the GB-10 typed and
    native rows are group-key cells. The step-0 table §7 now says so.
- **F2. The pivot's discovered value columns come out in a run-dependent order on float pivots** (GB-11 typed).
  - **Cause:** `_pivot_sort_discovered_values` (`grouped_pivot.py:109-117`) sorts with Python's `list.sort`. Because
    `NaN` compares false both ways, and `-0.0 == 0.0`, those values stay in hash-aggregate discovery order.
  - **Spark 4.1.2** orders the columns `null, 0.0, 1.5, NaN`: NULL first, NaN last, and the name `NaN`.
  - **RePark** puts `null` last and names the column `nan`. The decimal twin's column order also differs from
    Spark's, but only by NULL's place (`… 9.99, null`).
  - A source comment at `joins_columns.py:454` says NULL last matches Spark; the measurement says otherwise.
  - **Proposed card:** `PIVOT-COLUMN-ORDER-1` (S3: positional access to a pivot's columns reads the wrong column). It
    sits beside `PIVOT-EMPTY-CELL-COUNT-1`, and part 1's NaN and zero normalisation does not remove the NaN sort
    problem.
- **F3. `WITH RECURSIVE r(x) AS (SELECT d FROM t UNION …) SELECT x FROM r` refuses on both RePark doors with "No
  field named x".** RePark does not apply the CTE's column list when the anchor names its column; `SELECT d AS x` is
  accepted. Not investigated further, and there is no oracle, because Spark refuses `UNION` in a recursive CTE.
