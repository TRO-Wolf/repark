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
