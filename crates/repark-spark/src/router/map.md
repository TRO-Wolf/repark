# map — repark-spark/src/router

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

File-backed tests for the statement router (`../router.rs`): the TRUNCATE missing-table
class pin (`TABLE_OR_VIEW_NOT_FOUND`; DML-C), passthrough sanity, the BUG-010 ordering pin,
and the P11 read-only threading pin.
Location translation follows byte equality across owned and borrowed buffers.
The lib-root battery lives in `../tests/`
(`crate::tests`; see [../tests/map.md](../tests/map.md)).
**IPI-26/27 round 2 (2026-09-20):** the directory also holds pre-parse intercept
modules, which live here because `lib.rs` is at its re-export ceiling.

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../router.rs`.
  **MW-6:** the CALL dispatch covers all supported procedures, including `register_table`.
- `comment_on_table.rs` — **IPI-26/27 round 2 (2026-09-20):** the
  `COMMENT ON TABLE t IS 'lit'` / `IS NULL` intercept. The parser is a
  `DatabricksDialect` reader for `COMMENT ON TABLE <3-part> IS <literal|NULL>`;
  `COMMENT ON COLUMN` is not the shape and passes through. Execute sets or
  unsets the `comment` property with no reregister, like `SET TBLPROPERTIES`
  (cell `D-COMMENT-ON`). Unit pins are inline; door pins are
  [test_ice_ddl_clauses_1.py](../../../../python/repark/tests/test_ice_ddl_clauses_1.py).
- `hive_change_column.rs` — **IPI-26/27 round 2 (2026-09-20); rename refusal
  (2026-09-21):** the two-name Hive `ALTER TABLE t CHANGE [COLUMN] old new TYPE
  [COMMENT 'lit']` intercept. The type parses with `SparkSqlDialect` and maps at
  execute time, so non-primitive targets refuse with the session timestamp type
  applied. Parse refuses the rename form (`old` != `new`) right after the new
  name with `[PARSE_SYNTAX_ERROR]` / SQLSTATE 42601, as Spark does; execute then
  applies only `UpdateColumnType` plus an optional `UpdateColumnDoc` against the
  old name (cell `D-X-CHANGE-COLUMN-TYPE`). Unit pins are inline; door pins are
  [test_ice_change_column_rename_1.py](../../../../python/repark/tests/test_ice_change_column_rename_1.py)
  and
  [test_ice_ddl_clauses_1.py](../../../../python/repark/tests/test_ice_ddl_clauses_1.py)
  beside the `COMMENT ON` pins.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A routing regression on an intercepted form | The lib-root battery (`cargo test -p repark-spark tests::`) pins every arm end to end |
| A `COLLATE` spelling on CREATE TABLE was a generic column-option refuse | G15: `refuse_collation_in_statement` runs on the router parse before the CREATE arm |
| `CAST(x AS STRING COLLATE name)` was a generic parse error | G15 type-position scan on the executing-parse text in `spark_ast` |
| `RESET spark.sql.collation.*` skipped the valve | G15 `DfStatement::Reset` arm in `spark_ast` |

First checks: `cargo test -p repark-spark router::`. Escalate to: [../map.md#debug](../map.md).
