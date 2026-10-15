# Card UPDATE-SUBQUERY-STORE-1: store Spark's value for `UPDATE … SET col = (SELECT …)` where the identity path does not run

**Date:** 2026-10-15. **Filed by:** Claude (claude-opus-5-5), ICE-STRUCT-PAIRING-1 exception round, under owner ruling 90 (a).

**Status:** open, not scheduled.

**Retires:** when every `UPDATE … SET col = (SELECT …)` shape that now refuses stores Spark's value on both doors, the ruling 90 refusal is deleted, and its registry rows ICE-STRUCT-PAIRING-1-UPDATE-SUBQUERY and ICE-STRUCT-PAIRING-1-UPDATE-SUBQUERY-WHERE-FALSE (the cells that match no row, owner ruling 95) close with the pins turned to stores, the no-match cells succeeding with nothing written.

## Why

Before 2026-10-15, `UPDATE t SET col = (SELECT … FROM src)` with `WHERE true`, an `IN` list,
no `WHERE`, or any other predicate the identity path declines, reported success and wrote
nothing, for every column type, on main and on the unit head (verifier 2's V2-01; step 0 of
the exception round showed the same for a flat INT column). Spark 4.1.2 stores the subquery's
value in every matched row. Owner ruling 74 (a) calls a success that writes nothing a silent
failure; ruling 90 (a) made the shape refuse loudly and left storing the value to this card.

## The refusal it replaces

`PreExecute::guard` (`crates/repark-core/src/pre_execute.rs`, `refuse_update_from_subquery`)
refuses an `UPDATE` plan that reaches it with a `SET` value that is a scalar subquery, before
anything is written, on every door that plans through the belt (the Spark SQL door's
passthrough, the ANSI door's `delegate`, and `repark.sql()`'s `DataFusionDialect`). The text:

> UPDATE with SET column = (SELECT ...) is not supported on this statement: RePark cannot
> write the subquery value here, so it refuses before any write.

The identity path runs before the belt and still stores Spark's value for a scalar-comparison
`WHERE` (`WHERE id = 0`, `id >= 0`, `1 = 1`, `id <> 99`, `OR` and `AND` of those) and for
`WHERE col IN (SELECT …)` on the Spark SQL door.

## Known facts (step 0 of the exception round, 2026-10-15)

- Admission: `crates/repark-iceberg/src/write/predicate_dml/plain.rs:200-226`
  (`allowed_update_with`) admits only scalar-comparison predicates to the identity path. No
  `WHERE` is declined at `:221`; `WHERE true` and `IN (…)` fail `is_scalar_comparison` at
  `:127-135`. The `SET` gate is `scalar_set_assignments`
  (`crates/repark-iceberg/src/write/predicate_dml.rs:665`) through `subquery_outside` at `:717`,
  which admits only a single-column, uncorrelated, single-relation subquery.
- Fall-through: the declined statement leaves `try_execute_identity_dml`
  (`crates/repark-spark/src/spark_ast.rs:371-421`) and reaches `execute_passthrough`
  (`crates/repark-spark/src/router/delete_update.rs:90`), then the fork's
  `IcebergTableProvider::update`.
- Why nothing is written: DataFusion 54.1's optimizer turns the scalar subquery into a join, so
  the projection's assignment becomes a column of the joined subquery. `extract_update_assignments`
  (`datafusion-54.1.0/src/physical_planner.rs:2347`) strips the qualifier, and when the
  subquery's column has the target column's name the assignment reads as `col = col` and is
  dropped as an identity assignment. A subquery column with another target column's name
  writes that column's own value; any other name fails `No field named …`.
- Round 3's fold (`crates/repark-spark/src/update_cast.rs:47`) pairs a reordered struct by
  name, so the reordered shapes type-check on the head where main refused them with Arrow's
  `arguments need to have the same data type`.
- `repark.sql()` (`DataFusionDialect`) has no identity path, so before the refusal even
  `WHERE id = 0` wrote nothing there.

## The ask

- A store path for the declined shapes: either widen the identity path's admission
  (`allowed_update_with`) to `WHERE true`, `IN` lists, no `WHERE` and the other declined
  predicates, with `scalar_set_assignments` admitting the subquery forms Spark stores, or
  evaluate the subquery before the provider's `update` sees the plan.
- `repark.sql()` needs the same store path, or an identity path of its own.
- Each stored shape replaces its refusal pin with a store pin on both doors, held to the live
  Spark recording (`x_update` in `python/repark/tests/ice_struct_pairing_1_spark_oracle.json`).
