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

- `insert_positional.rs` — **U8 WRITE-SQL PR1 (2026-09-24):** `prepare_positional_insert`
  runs after U6's by-name routing check in `execute_insert_routed`. It aliases a later
  projection item whose DataFusion name repeats an earlier one (`__repark_col_<n>`; a cast
  keeps its input's name) in every top-level `SELECT` of the source, because a positional
  write never reads the names, then hands the statement to the PARTITION rewrite.
  `PreparedInsert.owned_append` tells the router to commit through the owned append. Round 3
  (2026-09-25): it hands the raw source to the PARTITION rewrite for arity naming. Round 4
  (2026-09-25, critic r3 V-003): beside a `*` it also aliases every column-named item (a
  column, a cast of one, or an alias), because the expansion's names are unknown before
  planning; a `SELECT *, CAST(id AS STRING)` no longer leaks DataFusion's `Projections require
  unique expression names`. Its two children are in [insert_positional/](insert_positional/map.md).
  pins: u8-write-sql/C-011, C-023
  **WO STORE-TS-TO-NUMERIC-1 (2026-09-28):** after that rewrite (`rewrite_positional_insert`)
  it runs `void_type::refuse_insert_source_types` on the prepared statement, so every
  positional INSERT door — append, owned append, OVERWRITE and VALUES — refuses a `-NULL`
  into a DATE, BOOLEAN or timestamp column and a STRING into FLOAT/DOUBLE with Spark's text.
  `router.rs` stays at its 1000-line ceiling. pins: store-ts-to-numeric-1/C-002, C-003
  **Fold 2026-09-29 (re-verify RT-1..RT-3, narrowing):** the gate judges `-NULL` only; STRING sources store
  as on base. pins: store-ts-to-numeric-1/C-006

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
- `table_props_ddl.rs` — **IPI-26/27 round 4 (2026-09-21, cell `D-SET-LOCATION`):**
  the `ALTER TABLE <cat.ns.tbl> SET LOCATION '<literal>'` intercept (the statement does
  not survive sqlparser). The parser is a `DatabricksDialect` reader that claims only
  after `SET LOCATION` — keywords case-insensitive, the quoted path verbatim — so
  `SET/UNSET TBLPROPERTIES`, branch/tag shapes and every other ALTER form pass through;
  a claimed-but-malformed tail (non-three-part name, missing or non-string path,
  trailing tokens) refuses loud naming the clause. Execute moves the metadata location
  through `repark_iceberg::write::set_location::set_table_location` behind the
  `parsed_ddl("ALTER TABLE")` write-options gate, then defensively invalidates the
  touched namespace as the schema-changing intercepts do — sibling convention, not a
  tested leg: the move changes no names, so no pin can observe it. Unit pins are
  inline; door pins are
  [../tests/ice_ddl_clauses_1.rs](../tests/ice_ddl_clauses_1.rs) and
  [test_ice_ddl_clauses_1.py](../../../../python/repark/tests/test_ice_ddl_clauses_1.py)
  (first linked from the `comment_on_table.rs` row above).

- `nested_ns.rs` — **ICE-TSNS-MERGE-WALL-1, the split (2026-10-09):**
  `refuse_nested_supply` is the one call that hands an `INSERT` (plain, `OVERWRITE`,
  `BY NAME`, `REPLACE WHERE`), `UPDATE` or `MERGE` statement and its Iceberg target to
  `repark_iceberg::write::nested_ns_gate::refuse_nested_ns_supply`. It costs one catalog load
  of the target. `insert_positional/replace_where.rs` gained the two accessors it reads.
  **Where the call sits (split fold 1, 2026-10-10):** in `router::execute_calibrated`, on the
  statement as the caller wrote it, before the metadata-table, changes, write-to-branch, WAP
  and time-travel rewrites. The first split called it at the top of `execute_inner`, which
  runs after `write_to_branch::apply_write_to_branch`; that function points a branch or WAP
  append at a temporary provider (`datafusion.public.<name>`), so the gate saw a target that
  was not an Iceberg table and ten routes stored the UTC wall. **Why no routed write can go
  around it:** every public entry of this module ends in `execute_in_session`, which either
  answers a temporary-view statement or calls `execute_calibrated`; `execute_inner` has two
  callers, the `CREATE VIEW` arm of `execute_calibrated` and `execute_time_travelled`, which
  only `execute_calibrated` calls after the gate. What re-enters with a statement of its own
  (the maintenance and partitioning `CALL`s) re-enters by the public `execute`, so it passes
  the gate again; a DataFrame writer enters by the public entry with the real name.
  **The target:** the written name, and also the name with a `branch_<x>` reference removed
  (`split_write_ref_parts`, the function the branch rewrite uses), so `t.branch_x` and
  `t.branch_main` decide on `t`; a `tag_<x>` reference is left to the door's own refusal.
  A session WAP setting does not change the written name.
  **A statement that carries a write (split fold 2, 2026-10-10):** the parsed statement is
  not matched against a list of write kinds with an arm that returns Ok for the rest; that
  arm let `EXPLAIN ANALYZE INSERT`, `PREPARE … AS INSERT` (run by `EXECUTE`) and
  `CREATE TABLE … AS INSERT …` execute a write past the gate. `executed_writes` answers every
  write the statement runs: an `EXPLAIN` is looked into when its analyze flag is set and
  answers nothing when it is not (a plain `EXPLAIN` executes nothing); a `PREPARE` is looked
  into; for any other statement the parser's `visit_statements` hands over each statement
  nested anywhere in it (a query body, a CTE, a subquery, a `CREATE TABLE AS` source, a
  procedural block), and each INSERT, UPDATE and MERGE found is decided as if written alone.
  `EXECUTE` carries a name only; its statement was decided at `PREPARE`.
  **A statement it cannot parse:** the function applies the two rewrites `execute_inner`
  applies before parsing (map casts and system functions, `WITH SCHEMA EVOLUTION`). If the
  statement still does not parse: one `EXPLAIN` layer is peeled by its tokens
  (`explained_statement`; the parser rejects a nested EXPLAIN, and the engine runs a doubled
  `EXPLAIN ANALYZE`), a peeled layer without ANALYZE answers Ok, one with it is decided on
  what remains; otherwise every `INSERT`, `UPDATE` or `MERGE` keyword in the statement gives
  a written target (`write_to_branch::written_targets`), and a table with such a leaf
  refuses (`NestedWrite::Unreadable`). The cost: a malformed write aimed at such a table
  reports the nested refusal, not the parser's error.
  A target that is not an Iceberg table passes through to the door, which answers as before.
  pins: ice-tsns-merge-wall-1/C-040, C-050, C-053

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
