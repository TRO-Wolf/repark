# task/wo/sql-setop-all-refuse-1/evidence/ — SQL-SETOP-ALL-REFUSE-1 measurement evidence

- [step0/](step0/map.md) — the table on main `a3f8c7d9` against Spark 4.1.2, the scripts and the
  provenance.
- [after/](after/map.md) — the same cells on the branch's release build.

## Step 0 on main

Cells: `INTERSECT ALL`, `EXCEPT ALL` and `MINUS ALL` in twelve shapes (top level, CTE, scalar
subquery, `IN` subquery, temporary view, persistent view, over Iceberg tables, `INSERT … SELECT`,
CTAS, `MERGE USING`, nested in `UNION ALL`, `EXPLAIN`), eight spellings at top level and as an
`INSERT` source, seven controls in four shapes, five text controls, the two `row_number()`
workarounds and, on the Spark door, the DataFrame and SQL-fragment cells.

| cells | Spark door | native door |
|---|---|---|
| `INTERSECT ALL` / `EXCEPT ALL`, 24 | 21 WRONG; 3 EQUAL (`INTERSECT ALL` in `IN`, where multiplicity is invisible, and the two `EXPLAIN`) | 15 WRONG, 3 EQUAL, 6 refused for other reasons (no Iceberg views, the native CTAS `register_table` refusal, `MERGE` does not parse) |
| `MINUS ALL`, 12 | refused by DataFusion (`MINUS ALL not implemented`) | the same |
| spellings, 16 | 12 WRONG, the 4 `MINUS ALL` refused by DataFusion | the same |
| controls, 28 | 24 EQUAL; the 4 `MINUS` cells refused by DataFusion (`MINUS not implemented`) | 18 EQUAL; `MINUS` refused (4); CTAS of `UNION ALL` refused (`register_table does not support tables with data`); CTAS of `UNION`, `INTERSECT`, `EXCEPT` and the two DISTINCT forms creates an empty table with no snapshot (5 WRONG) |
| text controls 5, workarounds 2 | all EQUAL | all EQUAL |
| DataFrame `intersectAll` / `exceptAll` | refused, `UnsupportedOperationException` (`octo C1-L-005` / `-006`) | n/a |
| `F.expr` scalar, `filter` string, `filter(F.expr)`, `selectExpr` | 4 WRONG | n/a |

The writes store the wrong multiset: `INSERT`, CTAS and `MERGE` on the Spark door and `INSERT` on the
native door store `1, 1, 1, 2, 2` for `INTERSECT ALL` (Spark `1, 2, 2`) and `4` for `EXCEPT ALL`
(Spark `1, 1, 4`). The `MINUS` control and the native CTAS controls diverge on main outside this unit;
the native CTAS data loss is reported for its own card, not fixed here.

## The seat

One statement seat for both doors: `ReparkSession::sql_with_write_options_inner`
(`crates/repark-core/src/session/write_options.rs`), the function every `sql` / `sql_with` /
`sql_built` / `sql_with_write_options` call reaches before the session's `SqlDialect` runs.
`spark.sql` (the `SparkDialect` session) and `repark.sql()` (the `LocalFsGuardedDialect` over
`DataFusionDialect`) both pass it, so the refusal fires before either door parses, plans or writes. It
calls `pre_execute::setop_all::refuse_in_session_sql`
(`crates/repark-core/src/pre_execute/setop_all.rs`), which tokenizes the text in the session's
`sql_parser.dialect` (Databricks on the Spark door, Generic on the native door) and refuses on an
unquoted `INTERSECT`, `EXCEPT` or `MINUS` followed by `ALL`. Two text-only entries that never reach
that seat call the same scanner (`refuse_multiset_set_operation`, Databricks dialect):
`repark_spark::refuse_sql_fragment` (the `F.expr`, `selectExpr` and `filter` fragments) and
`view_ddl::read::expand_view_body` (a stored Iceberg view body re-planned at read, which a foreign
writer or an older build may have stored).

## Red and green

- **Rust:** `cargo test -p repark-core --lib setop_all` (`pre_execute::setop_all::tests`), 5 tests. With the seat line removed, the two
  seat tests fail (`the_session_seat_refuses_before_the_door_and_lets_the_controls_through`,
  `a_refused_insert_or_ctas_writes_nothing`) and the three scanner tests pass; with it, 5 pass.
- **Facade:** `tests/sql/test_sql_setop_all_refuse_1.py`, 180 tests. On main's native module: 109
  failed (72 shape cells, 32 spelling cells, 4 fragment cells, the stored view) and 71 passed (56
  controls, 8 text controls, 4 workarounds, the 2 DataFrame refusals, the DataFrame controls). On the
  branch: 180 passed.
- **After table:** 72 shape cells and 32 spelling cells REFUSED on both doors; every refused write
  leaves the table state it found (no snapshot and no rows for `INSERT` and `MERGE`, no table for
  CTAS); the 40 Spark-door and 35 native-door control, text, workaround and DataFrame-control cells
  are byte-identical to step 0.

## Suites the refusal changes

Two struct set-operation suites ran `INTERSECT ALL` / `EXCEPT ALL` cells on single-row operands,
where the multiset is invisible. The statement seat now refuses them before the struct rule runs,
so both expect this unit's refusal and an unchanged table:
`python/repark/tests/sql/test_struct_setop_reorder_refusal_1.py` (`SETOP_ALL_REFUSED`) and the
`repark-sql` harness `crates/repark-sql/tests/setop_reorder_common/mod.rs` (`is_multiset`,
`check_multiset_refusal`). No other test in the tree runs the SQL operators on a RePark session.

Up: [../map.md](../map.md).
