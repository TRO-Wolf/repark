# ICE-NTZ-NESTED-WALL-1 progress

**Step:** 1 committed. `zone_store_frame` calls `nested_micro_ntz_wall_sql`.

**Walk commit:** `e926fbe89e3eec9e9eaaff6284b948a5c1f37d25`

**Red pin:** `evidence/pin-red-step1.txt`. On the parent, America/New_York and
Asia/Kolkata store `1767323045123456`. On the commit the three zones store the
session wall.

**Step 2, merge insert:** `1aa27854810ecaa157717faf718f8fa0c3c62963` calls the same walk from
`zone_wrapping_stream_sql`. Red on the parent: the column-list door stores
`1767323045123456` in America/New_York and Asia/Kolkata
(`evidence/pin-red-step2.txt`). Green on the commit.

**Step 2, store assignment:** `c3a63d17ab7df8f718583c36c97b34f047abf8b3` calls
the same walk from `store_assignment_cast_sql`. A caller that has a source
type passes it. `store_assignment_then_sql` passes `None` because it has the
SQL text and the target type only. The Spark fold passes the probed type for
a top-level struct and `None` when the probe does not plan. A path longer
than one step stays on `nested_leaf_cast_sql`. Red on the parent: MERGE
UPDATE from a column stores `1767323045123456` in America/New_York and
Asia/Kolkata (`evidence/pin-red-step2-store.txt`). Green on the commit: that
door and UPDATE from a sibling `TIMESTAMP` column store the session wall. A
wall literal stays `1767323045123456`. A field assignment stays the session
wall. A nested `TIMESTAMP` leaf stays the instant.

**Step 2, identity update:** `3a4d12c5b9a36b603df939860c31b3c9b7dff6f3` calls
the same walk from `update_projection_sql`. One `analyzed_types` of the SET
expressions, inside `collect_identity_update_rows`, supplies the source
types. A failed analysis passes `None`, and the expression stays main's.
Red without the call: the unit test's projection is `(s) AS `st``
(`evidence/pin-red-step2-update.txt`). Green with the call. The identity
facade stores the session wall, `1767323045123456` for a wall literal, and
the instant for a nested `TIMESTAMP`. Those stored values are already the
fold's, so this call does not move them.

**Step 2, insert analysis:** `26f8843a7b8ca0e9773f40065efde0ecdae94713` asks
`nested_micro_ntz_decision` from `after_analysis`. The SQL walk asks the same
decision and still emits the same SQL
(`the_four_sites_keep_the_sql_the_walk_emitted`). A selected instant leaf
becomes the NTZ kernel. A column with no selected leaf is the same
expression. A target with no nested microsecond `TIMESTAMP_NTZ` leaf keeps
the plan. Red on the parent: `INSERT … SELECT` of a column stores
`1767323045123456` in America/New_York and Asia/Kolkata
(`evidence/pin-red-step2-insert.txt`). Green on the commit: that door and
DataFrame `append` store the session wall. A literal stays
`1767323045123456`.

**Step 3:** release rebuilt. Head probe
`evidence/repark-head.jsonl` (1191 lines). `probe.py summary-head` wrote
`evidence/summary-head.md`: Spark 173 of 264, field 324 of 324, `ltz_nested`
396 of 396, `ntz_top` 396 of 396, UTC-wall remainder 22, controls moved 0.
R-011 is narrowed. C-003 stays open for those 22 and the doors that still
refuse. C-004 is proven. Gates: iceberg lib 1009 passed, spark lib 2696
passed, the three wall-door tests passed, pytest 569 passed, clippy 0,
panic-ban 0, fmt 0, map 0, ledger grammar 0, file size 0, comment ban 0,
maturin 0.

**Round 6, R8.1 null struct:** `emit_struct` guards null at every depth, the
same `CASE WHEN source IS NULL` shape `emit_sql` already had. Red on the
parent emitter: `INSERT … SELECT` of a null struct stored
`{'v': None, 'n': None}` (`evidence/pin-red-r8-null.txt`). Main copies the
column, so the stored value is null. Green on the commit: that door and
DataFrame `append`, plus a null inner struct at depth 2 on both doors, read
back as null.

**Round 6, R8.2 barrier:** `rand()` and `random()` are gone. A plain column is
read in place. Any other source is one inner `SELECT` (`__repark_ntz_src`) or
one inner projection (`__repark_ntz_src_N`). The logical pins pass. The
physical planner does not. `push_down_leaf_projections` fails with
`Schema contains duplicate qualified field name ice.ns.src.id` on DataFrame
append and `overwritePartitions`. The SQL bind fails with
`Physical plan does not support logical expression ScalarSubquery`. The
barrier is not put back. See `evidence/pin-red-r8-barrier.txt`. Halted here.

**Round 7, kernel:** `__repark_cast_nested_ntz__` rewrites selected zoned leaves
inside one struct value. A null struct keeps its validity bit. Untouched
children are the same arrays. Seven unit tests pass, including three zones
against the existing wall kernel.

**Round 7, emitters:** both emitters call `__repark_cast_nested_ntz__` inside
main's cast. The `get_field` rebuild, the bind, and `rand()` are gone. The
facade file is 18 passed. A 20-field volatile struct keeps one draw. `EXPLAIN`
has no `rand()`. Renamed and case-different members store NULL in the unmatched
leaf, which is main's cast. Reordered names store each session wall on its name.

**Round 7, cast peel:** a user `CAST` to `STRUCT<v: DATE, n: INT>` and a
`TRY_CAST` stay the original expression. The unit test passes.

**Round 7, Iceberg insert hook:** `after_analysis` wraps a nested microsecond
NTZ leaf only on `INSERT` into `IcebergTableProvider`. A fake table source and
`UPDATE` keep the plan. The facade file is 18 passed on the release build of
this gate.

**Round 7, paths pin:** `both_emitters_select_the_same_paths` reads the path
literal out of the emitted expression and out of the emitted SQL. `v`, `a,b`
and `i.v` match. A wall, a string, a date, an integer, a top-level timestamp
and a list emit no path from either side.

**Round 7, leaf table:** the nested leaf choice is nine predicate rows. Only
a zoned instant into a microsecond `TIMESTAMP_NTZ` wraps. The mirror row, a
microsecond `TIMESTAMP_NTZ` into a zoned timestamp, is present and does not
wrap. Seventeen `update_cast` tests pass.

**Round 7, raw-error doors:** `UPDATE` with no `WHERE` and
`INSERT OVERWRITE … VALUES` of a `TIMESTAMP` literal still raise main's
`PySparkException` and the step-0 first line, in UTC. Registry row
ICE-TSNS-SQL-1-R-019 is OPEN. R-011 records them as still an error, equal
to main. C-005 stays open.

**Round 8, Q7:** `store_assignment_then_sql` keeps passing `None`. The Spark
`WHEN MATCHED` and `WHEN NOT MATCHED BY SOURCE` clauses store the New York
session wall because `nested_assign` wraps first. The ANSI door's same two
clauses store the UTC wall `1767323045123456`. Those two are the hole.

**Round 8, R4:** the literal probe already uses the caller's session. The
Spark instant rule is a token rewrite before parse, not a parameter at the
call site. The attempt stops. Literal whole-struct cells stay on the UTC wall.

**Round 8, R5:** registry row R-020 records 88 NTZ-literal cells into a nested
`TIMESTAMP`. Depth 2 is pinned. A struct under a list keeps the UTC wall. A
map insert panics in Arrow before a store. The mirror row stays false.

**Round 8, R6:** the repair `UPDATE t SET st.v = c WHERE id >= 0` moves a
Parquet leaf from main's UTC wall `1767323045123456` to the New York session
wall `1767305045123456`.

**Round 8, R7:** the facade reads stored leaves from Parquet. DST gap and
overlap instants in America/New_York and Australia/Lord_Howe store the session
wall. The head matrix is 173/264 equal to Spark, field 324/324, ltz 396/396,
top 396/396, raw-error doors 162/162, and 22 UTC-wall cells, each mapped to
`SET *` or to the R4 literal. R-011 stays narrowed. The identity UPDATE pin
expects `arrow_cast` of `__repark_cast_nested_ntz__`. Clippy is clean.

**Round 9, F1:** not fixed here. Blocked on the struct coercion unit. The six
mixed statements are in ledger section 13. The wall row of the first statement
stores `1767305045123456` in America/New_York. The pin
`test_mixed_case_wall_row_stays_1767305045123456_until_coercion_unit_lands`
holds that tick and is green on this head. It must change when the coercion
unit lands. No provenance guard.

**Round 9, item 1:** `zone_store_frame` and `zone_wrapping_stream_sql` emit the
bare kernel. Members pair only when every name matches in order. Red before
the wheel: case-different BY NAME refused with `Unsupported CAST`, and a NOT
NULL member refused the same way. Green after: the six case-different
statements store main's positional UTC wall, and the 35 NOT NULL cells store.
In America/New_York a converted leaf is Spark's session wall. None of those
35 stay at the UTC wall. The six case-different statements do.

**Round 9, item 2:** a scalar subquery whose probed type would wrap is emitted
as main's cast, with no source type, so the kernel does not run. Red before:
the instant-struct UPDATE returned success and wrote nothing. Green after:
`PySparkException` and main's first line, and the rows stay. The equal-type
subquery stays the silent no-op. Registry row ICE-TSNS-SQL-1-R-021 is OPEN,
measured 2026-10-12.

**Round 9, item 3:** `probe_table_sql` drops the branch selector so the existing
walk sees the source type. Red before: the branch MERGE UPDATE stored
`1767323045123456`. Green after: `1767305045123456`, on `branch_dev` and on
`spark.wap.branch`. A member name containing `.` or `,` stays at main's UTC
wall. A matching leaf beside a renamed member stays main's row.

**Round 9, item 4:** recorded the cast text and EXPLAIN. Round 11 builds both.
EXPLAIN of a converted append `INSERT` and of a whole-struct `UPDATE` names
`__repark_cast_nested_ntz__`. A NOT NULL member's cast names the user's source
type. `INSERT OVERWRITE`, MERGE, `REPLACE WHERE`, `BY NAME`, and
`EXPLAIN ANALYZE INSERT` stay named holes.

**Round 9, item 5:** head matrix regenerated. Spark 173/264, field 324/324,
ltz 396/396, top 396/396, raw-error 162/162, UTC-wall cells 22, controls moved
0. Every UTC-wall cell is mapped to `SET *` or the R4 literal. Gates, each
unpiped: iceberg lib 0 (1012 passed), spark lib 0 (2710 passed, 5 ignored),
wall doors 0, functions lib 0 (928 passed, 1 ignored), clippy 0 after
collapsing the branch-probe `if`, panic-ban 0, fmt 0, crate-dag 0, map sync 0,
map-md 0, ledger grammar 0, file size 0, comment ban 0, pytest 0 (583 passed,
1 skipped), maturin 0.

**Round 10, halt:** F6 is not one site. `EXPLAIN <dml>` is planned by
`execute_passthrough` as `LogicalPlan::Explain`. The kernel is inserted later,
on a different path for INSERT (`after_analysis`), overwrite and by-name
(`zone_stores`), UPDATE (`refuse_cast_then_fold_nested`), and MERGE
(`fold_merge_clauses` / `zone_wrapping_stream_sql`). No code changed. F7's
sentence is DataFusion's `Unsupported CAST` in `expressions/cast.rs`. The
smallest option is to skip the wrap at the two cast emitters when a NOT NULL
member makes that cast fail, and leave the bare kernel sites as they are.

**Round 11, item 1:** EXPLAIN of a converted append `INSERT` and of a
whole-struct `UPDATE` names `__repark_cast_nested_ntz__`. The substitute
happens only when that rewrite's own output carries a kernel call the input
did not. `INSERT OVERWRITE`, the three `MERGE` clauses, `REPLACE WHERE`, and
`BY NAME` stay main (R-022 through R-027). `EXPLAIN ANALYZE INSERT` in
America/New_York writes `1767323045123456` (R-028) and is not changed.
Unconverted EXPLAIN text was recorded on `b6b20b3c` with the arm absent.

**Round 11, item 2:** `nested_micro_ntz_wall_sql` and `wrap_nested_ntz` skip
the wrap when a NOT NULL target member is nullable on the source. The bare
kernel sites stay wrapped. INSERT SELECT into that member raises main's
`Unsupported CAST` first line in UTC and America/New_York. The 35 NOT NULL
cells still store the `4b66decb` ticks.
