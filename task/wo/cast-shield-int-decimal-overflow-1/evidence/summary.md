# CAST-SHIELD-INT-DECIMAL-OVERFLOW-1: summary

The card is [cast-shield-int-decimal-overflow-1](../../../roadmap/mid-term/cards-2026-11-23/cast-shield-int-decimal-overflow-1-card-2026-11-24.md)
(S2). The full table is [domain-table.md](domain-table.md). Everything below was measured on 2026-11-25 with Spark
4.1.2, main (`57c92b41`) and the head (`075ce6a6`).

## What moved

| ANSI | cells | equal to Spark on main | equal to Spark on head | moved off Spark |
|---|---|---|---|---|
| on | 509 | 376 | 484 | 0 |
| off | 510 | 399 | 493 | 0 |
| on, conjunction and `OR` shapes | 114 | 82 | 114 | 0 |

- **No cell that equals Spark on main moves,** in either mode, across the step-0 table, the 114 consult shapes, the delta-1 verifier's 177 shapes and 18 writes, and the 15 guard shapes (delta 1, 2026-11-25).
- **Refusals with ANSI on keep refusing.** 66 of them now carry Spark's `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`
  text in place of Arrow's.
- **The native door is byte-identical to main:** its 25 step-0 cells, their EXPLAIN, and the consult's 7 native cells.
- **Cells still unlike Spark,** all present on main with the same answer, are listed on the card.

## Ruling 1: the design

The first build closed every silent cell, but 21 ANSI-on cells that equal Spark on main started to over-raise. All had
the shape `A AND <cast> = k`, where `A` removes the overflowing rows. DataFusion 54.1 evaluates the right side of an
`AND` on the whole batch unless the left keeps 20% of rows or fewer. The orchestrator sent that hand-back to a Fable
5.2 consult (2026-11-25), measured on 26 shapes. The orchestrator adopted the consult, and the build follows it:

- **Door gate.** `SessionExtension::shields_integer_decimal_casts` defaults to false and is true for the Spark door. It
  feeds the three `CastShieldRule` installs. The native door keeps main's behaviour.
- **Run-once guard.** `stack::narrow_guard::guard_narrow_decimal_plan` runs on the optimized plan in
  `StackQueryPlanner`, after `shield_try_cast_plan`.
  - It finds each `__repark_narrow_to_decimal_*` call whose argument is a signed integer, and wraps it as
    `CASE WHEN <condition> THEN <call> END`.
  - In a `Filter`, the condition is the conjunction of the earlier guarded conjuncts. Over inline `VALUES`, each
    earlier conjunct is written `IS NOT FALSE`.
  - In a nested `AND` the condition adds `left IS NOT FALSE`. In an `OR` it adds `left IS NOT TRUE`.
  - Volatile and subquery operands are never copied.
  - A plan with no such call is returned borrowed, and `TableScan` filters are not touched.

## Pruning and EXPLAIN

- **EXPLAIN equals main** for every query without an integer-to-decimal narrowing cast: Parquet and Iceberg, both modes.
- **Scan metrics equal the first head build** for every cast query. An overflowing-cast predicate reads 4
  of 4 row groups and 8 of 8 Iceberg files, which is Spark's read set: Spark pushes only `IsNotNull`.
- **A guarded conjunction keeps pruning on its other conjunct:** 2 of 4 row groups by `id IN (...)`, and the Iceberg
  predicate `id IN (...)`.
- **The guard's `CASE` survives the physical optimizer.** It is present in the physical plan of
  `prune/{pq,ice_part}/{conj_eq,conj_or}`, and with `datafusion.execution.parquet.pushdown_filters = true`, which the
  Spark door accepts. There the row filter runs (`pushdown_rows_pruned=3`) and the answers equal Spark's.

## Seat inventory

| seat | path (head) | role |
|---|---|---|
| DataFusion `unwrap_cast_in_comparison` | `datafusion-optimizer-54.1.0/src/simplify_expressions/unwrap_cast.rs:64` | moved the cast onto the literal: every silent and wrong filter cell on main |
| DataFusion `BinaryExpr` `AND` | `datafusion-physical-expr-54.1.0/src/expressions/binary.rs:739`, `:760` | evaluates the right side on the batch unless the left keeps 20% of rows or fewer |
| Cast shield | `crates/repark-core/src/session/df_guards/cast_shield.rs` (`shield_operand`), `cast_shield/udf.rs` (`is_integer_decimal_narrowing`) | hides `TRY_CAST` integer to decimal from the unwrap, on the Spark door |
| Door switch | `crates/repark-core/src/extension.rs` (`shields_integer_decimal_casts`), `crates/repark-spark/src/extension.rs` | gates the shield's new rule |
| Spark cast seam | `crates/repark-functions/src/cast_map/string_integral.rs` (`cast_udf_for`) | rewrites the user `CAST` to the narrowing kernel |
| Kernel | `crates/repark-functions/src/cast_map/leaf.rs` (`integral_to_decimal`) | NULL with ANSI off, Spark's condition with ANSI on |
| Run-once guard | `crates/repark-core/src/stack/narrow_guard.rs`, called from `stack/planner.rs` | evaluates the kernel only where Spark does |
| Statistics barrier | `crates/repark-core/src/session/df_guards/cast_shield/stats_barrier.rs` (`keeps_order`) | already barred these casts; unchanged |
| Empty-overwrite guard | `crates/repark-spark/src/insert_overwrite.rs` (`expr_has_unsafe_cast`) | judges the kernel call as the cast it replaced; unchanged |
| INSERT-OVERWRITE-CONST-CAST-1 guard | `crates/repark-spark/src/const_cast_guard.rs` on its own branch (PR #1431, parked) | not on main and not copied. If it lands, it meets these constants as kernel calls. |

## Scripts

- [probe_step0.py](probe_step0.py): the cells, the sources and the recorder.
  - Commands: `setup` (the Spark-written Iceberg source), `run` (one engine, one mode), `native`, `readback` (Spark
    reads an engine's writes), `fable` and `fixture`.
  - It is the same module as `python/repark/tests/types/_record_cast_shield_int_decimal_overflow_1.py`.
  - Fable's shapes are data in [cast_shield_int_decimal_overflow_1_fable_cells.json](cast_shield_int_decimal_overflow_1_fable_cells.json).
- [compare_step0.py](compare_step0.py): the table.
- [viewcheck.py](viewcheck.py): the temp-view finding.

Up: [../../map.md](../../map.md).
