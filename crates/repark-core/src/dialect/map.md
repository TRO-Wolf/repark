# map — repark-core/src/dialect

## Purpose

File-backed tests for the SQL dialect seam (`../dialect.rs`): `DataFusionDialect` passthrough
and the `EngineContext` construction contract.

## Contents

- `tests.rs` — passthrough + explicit-field-construction pins (`#[cfg(test)] mod tests;` in
  `../dialect.rs`).
- `../dialect.rs` — `SqlDialect::on_session_built` (default no-op) runs from
  `ReparkSessionBuilder::build` after extension `register`. AnsiDialect installs
  F-Y10-1 integer overflow there. pins: f-y10-1-int-overflow/C-003
  **ICE-DYN-OVERWRITE-1 round 2, ruling Q-20a-6 (2026-09-17):** `EngineContext`
  carries the overwrite intent; **ICE-OVERWRITE-MODE-1 (2026-09-19)** replaces the
  `force_static_overwrite` flag with `overwrite_intent: OverwriteIntent` (`Session` in `new`;
  the literals here set it explicitly). pins: ice-dyn-overwrite-1/L-001; ice-overwrite-mode-1/C-007
  **IPI-40 PR6 (2026-09-24):** the explicit-field literals in `tests.rs` set
  `temp_views: None` for the new `EngineContext` field. pins: ice-views-1/C-018
  **U7 PR2 slice-2 round 2 (2026-09-25, critic r4 V-001..V-007):** the literals set `source_by_name: false` for the
  V2 writer's by-name source flag. pins: u7-write-df-2/C-013
  **STRING-LITERAL-ESCAPE-1 re-verify fold (2026-09-30):** the literals set
  `verbatim_override: None`; `EngineContext::new` defaults it the same way.
  pins: string-literal-escape-1/C-011

- `tests.rs` — **REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 (2026-10-16, owner ruling 106 (a)):**
  `the_default_dialect_refuses_a_subquery_in_update_or_delete_where` runs `UPDATE` and
  `DELETE` with `IN`, correlated `EXISTS` and `NOT IN` subqueries through a default
  `ReparkSession` (no dialect installed) over a memory catalog: each refuses with the G3-E8
  text and the rows read back as seeded. Red on main, where the first `UPDATE … IN` wrote
  every row. Since the fix, `DataFusionDialect` intercepts this one statement class on purpose;
  the passthrough pin's wording predates that, and its query is untouched.
  pins: repark-sql-update-subquery-where-1/C-005
  **Owner ruling 131 (b), 2026-10-16:** `the_default_dialect_refuses_an_explained_subquery_dml`
  runs `EXPLAIN` and `EXPLAIN ANALYZE` (one `VERBOSE`) of a subquery-predicate `DELETE` and
  `UPDATE` through a default session: each refuses, the rows held.
  `the_unwrap_reaches_a_nested_explain` calls `refuse_dml_subquery_where` on `EXPLAIN ANALYZE
  EXPLAIN DELETE …`, which the planner refuses first at the door ("Nested EXPLAINs are not
  supported"), so only this test reaches the second level. Both red on `ff731179`.
  pins: repark-sql-update-subquery-where-1/C-013

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A statement the Spark door handled fails here | Phase-1 default is plain DataFusion (`DataFusionDialect`); Spark interception lives in `repark-spark`'s `SparkDialect`. |
| Downstream dialect crate can't build an `EngineContext` | `EngineContext` is `#[non_exhaustive]`; construct via `EngineContext::new` (pinned by `engine_context_new_is_the_downstream_constructor`). |

First checks: `cargo test -p repark-core dialect`. Escalate to: [../map.md#debug](../map.md).

**ICE-TT-RESOLVE-1 (2026-09-19):** the dialect context carries `session_time_zone` so time-travel
resolution sees the statement-time zone. pins: ice-tt-resolve-1/C-003
**ICE-TT-RESOLVE-1 round 2 (2026-09-19):** `new` is 3-arg again (zone defaults);
`new_with_time_zone` carries an explicit zone. pins: ice-tt-resolve-1/C-003
