# Card FILTER-PROJECTION-SWAP-RAISE-1: under ANSI, a Parquet read raises an error from a row its `WHERE` removes

**Date:** 2026-11-25. **Filed by:** the orchestrator (`claude-opus-5-5`), from the "still unlike Spark" list of
MAKE-DT-INTERVAL-OVERFLOW-1, re-measured here on main. **Severity:** S2 proposed; the owner rates it. It is a false
refusal, not a wrong read: a job that Spark runs fails with an error raised by a row the filter drops. **Status:** filed,
not dispatched.

## Measured

Measured on live Spark 4.1.2 and on a build of main `14d2404a6` (#1429's tested head; main `8c07d6197` adds only
Markdown), in UTC, with `spark.sql.ansi.enabled=true`. The Parquet file and the Iceberg table hold
`(id, a, b, s) = (1, 10, 5, '1'), (2, 20, 0, 'x'), (3, 30000000000, 1, '3')`.

| Statement | Spark 4.1.2 | Main, Parquet | Main, Iceberg |
|---|---|---|---|
| `SELECT id, CAST(s AS INT) + a AS r FROM t WHERE id <> 2` | `(1, 11), (3, 30000000003)` | **`CAST_INVALID_INPUT`** (`'x'`) | as Spark |
| the same through `where(F.col("id") != 2).select("id", (F.col("s").cast("int") + F.col("a")).alias("r"))` | as above | **`CAST_INVALID_INPUT`** | as Spark |
| `SELECT id, CAST(a + b AS INT) AS r FROM t WHERE id = 1` | `(1, 15)` | **`CAST_OVERFLOW`** | as Spark |
| `SELECT id, a * b * 1000000000 AS r FROM t WHERE id = 1` | `(1, 50000000000)` | **`ARITHMETIC_OVERFLOW`** | as Spark |
| `SELECT id, a / b AS r FROM t WHERE b <> 0` | `(1, 2.0), (3, 30000000000.0)` | as Spark | as Spark |
| `SELECT id, CAST(x AS INT) AS r FROM t WHERE id = 1` (one input column besides `id`) | answers | as Spark | as Spark |

- **ANSI off is not affected.** The same statements answer as Spark, because nothing raises.
- **MAKE-DT-INTERVAL-OVERFLOW-1** adds 20 cells of the same shape: `make_dt_interval(d, h, m, s) … WHERE id = k` over a
  Parquet file that holds an overflowing row raises `INTERVAL_ARITHMETIC_OVERFLOW` on that branch.

## Seat

DataFusion 54.1.0 `FilterExec::try_swapping_with_projection` (`datafusion-physical-plan-54.1.0/src/filter.rs:590`) moves
a `ProjectionExec` below the `FilterExec` when the projection narrows the schema and every predicate column survives
it. The projection then runs on every scanned row, and a raising expression raises for a row the filter would have
dropped. The swap does not happen when the predicate reads a column the projection drops (`WHERE b <> 0` over
`a / b`), which is why the guarded division answers. Spark evaluates the projection only on rows that pass the filter.

## First step

Measure the domain first: every raising expression family (ANSI casts from string, numeric narrowing, arithmetic
overflow, `element_at` / array index, `make_*` constructors, `assert_true`, `raise_error`), predicates on kept and
dropped columns, nested projections, `selectExpr`, temp views, joins with a filtered side, and Parquet with
`pushdown_filters = true` if the Spark door accepts it. Then decide between a RePark physical rule that keeps a
projection holding a raising expression above its filter, and a guard that wraps the raising call in the filter
condition (the CAST-SHIELD-INT-DECIMAL-OVERFLOW-1 design). Patching DataFusion is out of scope. No cell equal to Spark on main may
move, and pruning must stay as on main.

Up: [map.md](map.md).
