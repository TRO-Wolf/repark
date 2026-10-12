# Unit ledger — ICE-NTZ-NESTED-WALL-1 · a nested microsecond `TIMESTAMP_NTZ` field stores one wall on every door

**Date:** 2026-10-11 · **Branch:** `fix/ice-ntz-nested-wall-1` · **Base:** `2c33f289` ·
**Model:** Grok 4.7 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** step 0 only. **risk_tier: standard.** Target: v1.5.5.

**Order:** card
[ICE-NTZ-NESTED-WALL-1](../../roadmap/mid-term/ice-ntz-nested-wall-1-card-2026-10-10.md),
parity row
[ICE-TSNS-SQL-1-R-011](../../../docs/spark-sql-iceberg-parity.md).
The row was measured on 2026-10-09 and left open by
[ICE-TSNS-MERGE-WALL-1](ice-tsns-merge-wall-1-ledger.md) clause C-017.
**This ledger closes when the unit's pull request merges.**

**Why.** In a session whose zone is not UTC, an instant written into
`struct<v: TIMESTAMP_NTZ, n: INT>` stores the session wall through field
assignment and the UTC wall through every other door that answers. Spark 4.1.2
stores the session wall on every door it answers. This round measures that and
names the store sites. It changes no product code.

## PROPOSITION LEDGER — ICE-NTZ-NESTED-WALL-1 — 2026-10-11

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Main `2c33f289` is measured before any change and shows the split of the registry row. | The step-0 probe output. | **PROVEN** | §1. [repark-main.jsonl](../../wo/ice-ntz-nested-wall-1/evidence/repark-main.jsonl) and [summary.md](../../wo/ice-ntz-nested-wall-1/evidence/summary.md). Field assignment stores the session wall. The other answering doors store the UTC wall. pins: ice-ntz-nested-wall-1/C-001 |
| C-002 | Spark 4.1.2 with Iceberg 1.11 is measured on every nested door of the inventory. | The Spark probe output. | **PROVEN** | §2. [spark-4.1.2.jsonl](../../wo/ice-ntz-nested-wall-1/evidence/spark-4.1.2.jsonl). Spark stores the session wall on every door it answers. pins: ice-ntz-nested-wall-1/C-002 |
| C-003 | Every nested door stores the wall Spark stores, in three zones. | Head probe and the facade pins. | **OPEN** | Q5. Column instants on the wired doors store the session wall. The cells in §8 still store the UTC wall or still refuse. The row R-011 is narrowed, not closed. |
| C-004 | No control cell is worse than main. | Head probe. | **PROVEN** | [summary-head.md](../../wo/ice-ntz-nested-wall-1/evidence/summary-head.md): field-assignment 324 of 324, `ntz_top` 396 of 396, raw-error 162 of 162 identical to main; `ltz_nested` 382 of 396 with 14 moved, each now equal to Spark (the joined coercion unit localises the naive leaf on the INSERT and DataFrame doors). pins: ice-ntz-nested-wall-1/C-004 |
| C-005 | `UPDATE` with no `WHERE` and `INSERT OVERWRITE … VALUES` store the session wall. | Head probe against main. | **OPEN** | They still raise main's error class and first line. Pin `test_raw_error_doors_still_raise_mains_error`. Registry row R-019. Not a stored-value change. |

## 1. Main, measured before any change (C-001)

Commit `2c33f289`. Probe
[task/wo/ice-ntz-nested-wall-1/probe.py](../../wo/ice-ntz-nested-wall-1/probe.py).
Output
[repark-main.jsonl](../../wo/ice-ntz-nested-wall-1/evidence/repark-main.jsonl)
and the comparison
[summary.md](../../wo/ice-ntz-nested-wall-1/evidence/summary.md).
1188 cells: 3 schemas × 3 zones × 3 sources × 44 door runs. The 28 door names
are the nested matrix of ICE-TSNS-MERGE-WALL-1 §9.2 (the script `nx.py` that
produced the 660 cells). Sixteen of them are row-level and are also run
merge-on-read, which is how that matrix reached 660. Tables are Iceberg
format v2, as this unit's brief requires. Ticks are microseconds from
`toArrow()` cast to int64.

The source column holds the instant in every zone: `1767323045123456`.

Into `struct<v: TIMESTAMP_NTZ, n: INT>`, an instant (`ts_literal` or
`ts_column`) in America/New_York or Asia/Kolkata:

| Class | Cells | Which |
|---|---|---|
| session wall | 28 | field assignment that answers: `merge_update_field`, `merge_update_field_literal`, `update_field_where` of a literal, `update_field_col_where`, each in both row-level modes |
| UTC wall | 102 | every other door that answers: the INSERT family except `INSERT OVERWRITE … VALUES`, the six DataFrame writers, MERGE INSERT (three spellings), whole-struct MERGE UPDATE (three spellings), whole-struct `UPDATE … WHERE` of a literal, both modes where the door is row-level |
| refused | 46 | a scalar subquery inside `VALUES` or a bare `UPDATE` (the column source where the statement has no source relation); `INSERT OVERWRITE … VALUES`; `UPDATE` with no `WHERE` on the nested column |

New York session wall `1767305045123456`. Kolkata session wall
`1767342845123456`. UTC wall `1767323045123456`. In UTC the two walls are the
same number, and every answering door stores it. The NTZ literal stores
`1767323045123456` on every door that answers, in every zone.

The 28 and the 102 are the split in registry row R-011. The row's 106 counted
a `timestamptz_ns` instant as well as `TIMESTAMP`, on main `40fc916f`. This
inventory's instant sources are the `TIMESTAMP` literal and the `TIMESTAMP`
column. The door split is the same one: field assignment one wall, every other
answering door the other. It reproduced. The probe was not tuned toward the
registry's numbers.

A top-level `TIMESTAMP_NTZ` column stores the session wall on every door that
answers, in all three zones. That control did not show the split.

## 2. Spark 4.1.2 with Iceberg 1.11 (C-002)

Same inventory, same probe, interpreter and warehouse passed in.
Output
[spark-4.1.2.jsonl](../../wo/ice-ntz-nested-wall-1/evidence/spark-4.1.2.jsonl).
The source column holds the same instant, `1767323045123456`, in every zone.

Into `struct<v: TIMESTAMP_NTZ, n: INT>`, an instant in a non-UTC zone: Spark
stores the session wall on 172 cells and refuses 4. The 4 are a scalar
subquery inside `VALUES`
(`[UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES]`).
No answering door stores the UTC wall. Field assignment, whole-struct
assignment, MERGE INSERT, every INSERT form that answers, every DataFrame
writer, and `UPDATE` with no `WHERE` all store the session wall. The NTZ
literal stores `1767323045123456` on every door, including `UPDATE` with no
`WHERE`.

A top-level `TIMESTAMP_NTZ` column stores the session wall on every door Spark
answers. The same scalar subquery inside `VALUES` is refused. A scalar
subquery in `UPDATE` stores the session wall.

## 3. Where a nested leaf's stored value is built

Read at `2c33f289`. The 102 cells that store the UTC wall do not pass through
one site. Five sites build the value, and none of them runs the NTZ kernel on
a leaf inside a struct. The top-level conform runs only when the target column
itself is a timestamp the kernel names.

| Family | Where the leaf's value is built | Top-level conform on this column? |
|---|---|---|
| Field assignment (`SET t.st.v`, MERGE and `UPDATE`) | `repark-spark` `merge/nested_assign.rs` `leaf_sql` calls `assignment_cast_sql`, which calls `update_cast::nested_leaf_cast_sql` because the path is longer than one step. That calls `ntz_store::wall_cast_udf_name` and emits `__repark_cast_timestamp_ntz__`. Callers: `update_cast::refuse_cast_then_fold_nested` and `nested_assign::fold_merge_clauses`. | No. The leaf cast is this function. `store_assignment_cast_sql` is the top-level arm and is not used for a nested path. |
| Nested INSERT (`VALUES`, `SELECT`, `REPLACE WHERE`) and the DataFrame writers that render those statements (`append`, `insertInto`, `saveAsTable`, `overwrite(condition)`) | `repark-spark` `insert_timestamp_ns::after_analysis`. `ns_store_targets` keeps a column only when `timestamp_ns_target` matches, and that matches a top-level nanosecond timestamp only. A struct column is left to the plain cast. | The hook runs and does not rewrite the struct. |
| `INSERT … BY NAME`, `INSERT OVERWRITE`, and the DataFrame overwrite writers | `repark-iceberg` `ntz_store::zone_stores` / `zone_store_frame`. The match is a top-level Iceberg primitive timestamp. A struct is `None`, so the column is unchanged and the later cast is plain. Callers: `repark-spark` `insert_by_name.rs` and `insert_overwrite.rs`. | The conform runs and skips the struct. |
| MERGE INSERT (column list, `INSERT *`, literal) | `repark-iceberg` `merge/insert.rs` `zone_wrapping_stream_sql`. It wraps a column only when `store_kernel_udf_name` names one. A struct column is not wrapped. | The conform runs and returns `None` for a struct-only value column. |
| Whole-struct MERGE UPDATE (`SET t.st`, `SET *`, literal) | `merge/insert.rs` `store_assignment_then_sql` calls `store_assignment_cast_sql`. For a struct, `store_kernel_sql` is `None`, so `nested_leaf_cast_sql` emits `arrow_cast` of the whole struct. `wall_cast_sql` matches a timestamp, not a struct. | The conform function runs and casts the struct, not the leaf. |
| Whole-struct `UPDATE` | `predicate_dml/lineage.rs` `update_projection_sql` calls `store_kernel_sql` and, on `None`, inlines the expression. The batch is then cast. | The conform runs and does not wrap a struct. |

The 102 cells pass through the last five rows (INSERT, by-name and overwrite,
MERGE INSERT, whole-struct MERGE, whole-struct UPDATE). That is five sites.
Field assignment is a sixth site and already stores the session wall. The
DataFrame writers are not a seventh site: `writer_schema.append_statement`,
`replace_where_statement`, `overwritePartitions` and `insertInto` render INSERT
or INSERT OVERWRITE SQL, and `saveAsTable` appends through the same INSERT path.

`INSERT OVERWRITE … VALUES` and `UPDATE` with no `WHERE` do not store. They
raise Arrow `column types must match schema types` or `arguments need to have
the same data type`. Those are the raw nested refusals R-014 recorded. Spark
stores the session wall on both. A rewrite that reaches the value before that
cast is the same five sites; the re-measure says whether the raw error remains.

## 4. Why the earlier nested conform was removed

On this clone the removal is in `58bf67e339ae0b57e81747d9cc7b9f375a4b48f6`,
the squash of ICE-TSNS-MERGE-WALL-1 onto main. The ledger's pre-squash hash
`e8a862fb` is not an object here. Section 11.1 of that ledger is the reason.

Folds 1 and 2 of that unit built a nested conform (`holds_nested_ns_wall`,
`nested_wall_conform_sql`) so a nested `timestamp_ns` leaf stored the session
wall. The second re-verify of `c6d947a3` (2026-10-09, 72,077 cells) failed that
form again: a value cut to microseconds reached a nested leaf through an array
element, a SQL temporary view and a cached frame, and 700 cells answered with
a planner error that has no name. The owner had adopted amendment 3: if fold 2
failed on any nested shape, the pull request splits. The nested conform was
deleted, not disabled. The kernel's nested call, the pairing, the layout match,
the lineage guard and every site that emitted them are gone. A nested
`timestamp_ns` leaf is refused by name (`nested_ns_gate::refuse_nested_ns_supply`)
before a file is written. A nested microsecond leaf and a nested
`timestamptz_ns` leaf are not matched, which is why R-011 stayed as measured.

## 5. Controls, and what this round does not touch (C-004)

No file under `crates/` or `python/repark/src/` changed. The controls are
main's.

A top-level `TIMESTAMP_NTZ` column stores the session wall on every answering
door, on RePark and on Spark.

A nested `TIMESTAMP` leaf stores the instant (the UTC physical tick
`1767323045123456`) for a `TIMESTAMP` literal and a `TIMESTAMP` column, on
every door both engines answer. An `NTZ` literal into that leaf is not one
wall on RePark: field assignment stores the session-localised instant
(America/New_York `1767341045123456`, Asia/Kolkata `1767303245123456`) and the
other answering doors store `1767323045123456`. Spark stores the localised
instant on every door. That leaf is a control. This unit does not move it.

Not in this inventory, and not proposed here: a nested `timestamp_ns` leaf
(refused under ICE-TSNS-NESTED-1), nested `timestamptz_ns` localisation, and
the nanosecond narrowing guard (R-017).

## 6. Round 2 stopped before the walk (2026-10-11)

Q1 and Q2 were ruled yes: one walk, called from the five sites, wrapping a
microsecond `TIMESTAMP_NTZ` leaf below the top level only when the supply at
that leaf is an instant. The walk was not written. Two of the five sites cannot
tell an instant supply from a wall supply, and the type that would tell them
is not in their arguments.

`ntz_store::zone_store_frame` can tell. `analyzed_types` already returns each
column's source `DataType`, and a struct's leaf types are inside it. An instant
leaf is `Timestamp(_, Some(_))`. A wall leaf is `Timestamp(_, None)`.

`merge/insert.rs` `zone_wrapping_stream_sql` can tell with the same
`analyzed_types` call it already makes. Today that call runs only when a
top-level column is `is_ltz_instant_target`, so a struct column does not load
it. The function and the plan are already in the site.

`insert_timestamp_ns::after_analysis` can tell. `expr.get_type` already returns
the source `DataType`. `ns_store_targets` returns `None` unless a top-level
column is `timestamp_ns`, so a microsecond struct never reaches that line. The
type API is already in the function.

`update_cast::store_assignment_cast_sql(expr, target)` cannot tell. It is a
pure function of the SQL text and the target type. `named_struct('v', s.l, 'n', 1)`
and a column reference do not carry the leaf's type. A `TIMESTAMP` literal
does; the column supply in the measured 102 cells does not. The caller that
has the source type drops it: `repark-spark` `merge/nested_assign.rs`
`value_sql_for` probes `keyed.value_type` and, when the struct names match,
calls `assignment_cast_sql`, which calls `store_assignment_cast_sql` with the
target only. The merge rewriters `rewrite_column_with_maps` and
`not_matched_by_source::rewrite_else` also call `store_assignment_then_sql`
with the target type only.

`predicate_dml/lineage.rs` `update_projection_sql` cannot tell. Assignments are
`(column, sql)` pairs (`scalar_set_assignments`). The projection matches
`store_kernel_sql(expr_sql, field.data_type())` on the target field. The
caller `collect_identity_update_rows` has a `SessionContext` only after that
SQL is built.

Passing `source: &DataType` into those two functions, or walking the value in
`value_sql_for` instead, is either a new parameter seam or a sixth site. Both
are stop lines of this round. No product file was edited.

## Q. Questions for a ruling

- **Q1 (C-003, RULING).** Is the session wall the stored value on every nested
  `TIMESTAMP_NTZ` door Spark answers, including `UPDATE` with no `WHERE` and
  `INSERT OVERWRITE … VALUES`, where RePark still raises a raw Arrow error?
  *Premise:* §2. Spark stores the session wall on all 172 answering instant
  cells in a non-UTC zone, and on the NTZ literal everywhere. The 28 field
  assignment cells already store it. The 102 other answering cells store the
  UTC wall. *Lean:* yes. The four scalar-subquery `VALUES` cells stay a
  refusal on both engines and are not a door to retune.
- **Q2 (C-003, RULING).** Is the fix one nested NTZ walk called from the five
  sites in §3, in `repark-iceberg` and in `repark-spark`'s INSERT hook, leaving
  field assignment's leaf cast as it is? *Premise:* §3. No single site builds
  the 102 cells. The DataFrame writers render SQL those sites already run.
  *Lean:* yes. Five call sites, two crates. `repark-iceberg` holds
  `zone_store_frame`, `zone_wrapping_stream_sql`, `store_assignment_cast_sql`
  and `update_projection_sql`. `repark-spark` holds `insert_timestamp_ns`.
  The walk names `__repark_cast_timestamp_ntz__` for a microsecond NTZ leaf
  only, so a `timestamp_ns` leaf, a `timestamptz_ns` leaf and a top-level
  column stay on today's path.
- **Q3 (C-003, RULING). Round 2 stop.** May the one walk take
  `(expr_sql, source, target)` and may `store_assignment_cast_sql` and
  `update_projection_sql` gain a `source: &DataType` argument, filled by the
  caller that already has the leaf type (`value_sql_for`) or by the existing
  analyzer at `collect_identity_update_rows` before the projection is built?
  *Premise:* §6. Without the source type those two sites cannot wrap an instant
  leaf and leave a wall leaf as the same expression. Threading the type is a
  new parameter. Walking in `value_sql_for` instead is a sixth site, and it
  does not cover `update_projection_sql`. *Lean:* allow the parameter. It is
  the source type three of the five sites already hold, passed into the two
  that do not. The walk stays one function. No sixth site, no third crate.
- **Q4 (C-003, RULING). Round 5.** No renderer and no re-parse. One decision
  function, two emitters. Ruled and applied: `nested_micro_ntz_decision`.
- **Q5 (C-003, RULING). Round 5.** A source analyzed as a wall is not wrapped.
  Literal cells that stay on `1767323045123456` are equal to main and stay.
  The list is §8. The row is narrowed. C-003 stays open.

## 7. Callers of the two signature changes

`store_assignment_cast_sql(expr, source, target)`:

| Caller | Passes |
|---|---|
| `merge/insert.rs` `zone_wrapping_stream_sql` LTZ arm | the planned source from `analyzed_types` |
| `merge/insert.rs` `store_assignment_then_sql` | `None`. The caller has the SQL text and the target type only. |
| `merge/mod.rs` `rewrite_column_with_maps` | `None`, through `store_assignment_then_sql` |
| `merge/not_matched_by_source.rs` `rewrite_else` | `None`, through `store_assignment_then_sql` |
| `repark-spark` `nested_assign.rs` `assignment_cast_sql` | the probed type when `probe_type` returns one, else `None`. A path longer than one step calls `nested_leaf_cast_sql` and does not call this function. |

`update_projection_sql(schema, alias, assignments, sources)`:

| Caller | Passes |
|---|---|
| `predicate_dml.rs` `collect_identity_update_rows` | one `analyzed_types` of the SET expressions, or `None` when that analysis fails |

## 8. Head measurement

[repark-head.jsonl](../../wo/ice-ntz-nested-wall-1/evidence/repark-head.jsonl),
counted by `probe.py summary-head` into
[summary-head.md](../../wo/ice-ntz-nested-wall-1/evidence/summary-head.md).

- ntz_nested instant cells equal to Spark: 173 of 264.
- field-assignment cells identical to main: 324 of 324.
- `ltz_nested` identical to main: 382 of 396. The 14 moved cells are the
  INSERT and DataFrame doors in America/New_York and Asia/Kolkata writing a
  naive leaf into a nested instant target; each now stores Spark's instant
  (`1767341045123456`, `1767303245123456`) instead of main's wall
  (`1767323045123456`). They moved with the joined coercion unit, measured
  2026-10-12 on the round-12 head and unchanged by the item-2 revert.
- `ntz_top` identical to main: 396 of 396.
- raw-error doors identical to main: 162 of 162.
- instant cells still on the UTC wall outside UTC: 22. Each is mapped in
  [summary-head.md](../../wo/ice-ntz-nested-wall-1/evidence/summary-head.md):
  `merge_update_star` is `SET *` (`UpdateAll` copies the struct); the other
  14 are R4 literals. The head matrix reads `toArrow()` because `probe.py`
  `read_leaf` casts the query result. The facade pins read Parquet.

Those 22, by door and source: `insert_values` `ts_literal` 2;
`merge_update_star` `ts_column` 4; `merge_update_star` `ts_literal` 4;
`merge_update_whole` `ts_literal` 4; `merge_update_whole_literal` `ts_literal` 4;
`update_whole_where` `ts_literal` 4. They are equal to main. The bare-context
probe that types `TIMESTAMP '…+00:00'` as `Timestamp(Nanosecond, None)` is
`crates/repark-spark/src/merge/nested_assign/tests.rs`, and it is not fixed
here.

## Release note

Nested `TIMESTAMP_NTZ` values written by `INSERT` before the fix hold the UTC wall.

| Door | Before | After |
|---|---|---|
| `INSERT … SELECT`, `INSERT … BY NAME`, `INSERT OVERWRITE … SELECT`, `REPLACE WHERE` | UTC wall | session wall, column and literal |
| DataFrame `append`, `insertInto`, `saveAsTable`, `overwritePartitions`, `overwrite(condition)`, `insertInto(overwrite=True)` | UTC wall | session wall, column and literal |
| MERGE INSERT, `INSERT *`, literal INSERT | UTC wall | session wall, column and literal |
| MERGE UPDATE of a column, `UPDATE … WHERE` from a sibling column | UTC wall | session wall |
| Field assignment | session wall | session wall |
| MERGE `UPDATE SET *`, whole-struct MERGE UPDATE of a literal, `UPDATE … WHERE` of a literal (round 15, 2026-10-12) | UTC wall | session wall |
| `INSERT … VALUES` of a literal | UTC wall | UTC wall |
| `UPDATE` with no `WHERE`, `INSERT OVERWRITE … VALUES` | refusal | the same refusal |

Until round 15 a column and a literal of the same instant gave two answers on
whole-struct `MERGE UPDATE` and on `UPDATE … WHERE`. Measured 2026-10-12 on the
round 15 head, both store the session wall. `INSERT … VALUES` of a literal still
stores `1767323045123456` in every zone.

A wall inside a daylight-saving gap moves forward by the gap when it passes
through a `CASE`, `COALESCE` or `UNION ALL` that mixes it with an instant on its
way into a `TIMESTAMP_NTZ` field: in America/New_York `2026-03-08 02:30:00`
is stored as `03:30:00`. Spark stores the same value. Before, the wall was
stored unchanged.

Repair, for rows that hold that UTC wall: `UPDATE t SET st.v = c WHERE id >= 0`,
where `c` is a `TIMESTAMP` column holding the original instant. Field assignment
stores the session wall. Proved by `test_repair_moves_a_main_wall_to_the_session_wall`:
the Parquet leaf moves from `1767323045123456` to `1767305045123456` in
America/New_York, which is Spark's value for that instant.

## 9. The struct kernel (round 7)

The member-by-member rebuild cannot stand. Binding the source once still reaches
DataFusion 54.1's own leaf extraction: `push_down_leaf_projections` fails with
`Schema contains duplicate qualified field name ice.ns.src.id` on DataFrame
append and `overwritePartitions`. The scalar-subquery form fails with
`Physical plan does not support logical expression ScalarSubquery`. Evidence:
[pin-red-r8-barrier.txt](../../wo/ice-ntz-nested-wall-1/evidence/pin-red-r8-barrier.txt).
The round-7 architecture review, section 3, replaces that rebuild with one
struct kernel. Both emitters call it inside main's `arrow_cast`.

`__repark_cast_nested_ntz__` takes the struct once and rewrites only the
selected leaves. Validity, names, order, nullability and field metadata stay.
Other children are the same arrays. The map row is
`crates/repark-functions/src/map.md` `nested_ntz_cast.rs`.

These review items are settled by that kernel:

| Item | What the kernel settles | Pin |
|---|---|---|
| R8.2 | No `rand()` barrier. One struct value, so a volatile source is drawn once. | `test_a_volatile_struct_is_evaluated_once`: `f00` equals `f17`, differs from `f18`, and `EXPLAIN` has no `rand`. |
| R8.3 | An untouched child is the original array, not a field rebuilt through a wrapper. | `twenty_fields_pass_the_untouched_children_through`. |
| R8.5 | Members pair only when every name matches in order. A case-different, renamed, missing or extra member keeps main's expression. The bare sites then pair by position. | `a_misaligned_struct_stays_main_and_the_two_sites_can_emit_a_bare_kernel`. `test_case_different_members_store_mains_positional_utc_wall`. |
| R8.7 | The SQL emitter keeps main's `arrow_cast((…), '<target>')` around the kernel. | `an_instant_leaf_below_a_struct_is_wrapped_and_its_sibling_is_not` expects `arrow_cast((`. |

The leaf choice is the nine-row table in `update_cast.rs` `LEAF_ROWS`. Only a
zoned instant into a microsecond `TIMESTAMP_NTZ` wraps. The mirror row, a
microsecond `TIMESTAMP_NTZ` into a zoned timestamp, is in the table and does
not wrap. A later unit sets that row. It is not set here.

## 10. None site: `store_assignment_then_sql`

| File | Function | Callers | Reason | Follow-up |
|---|---|---|---|---|
| `merge/insert.rs` | `store_assignment_then_sql` | `merge/merge_sql.rs` `rewrite_column_with_maps`; `not_matched_by_source.rs` `rewrite_else` | No zoned value reaches this site; measured 2026-10-12. | None. Round 13 built the probe after the split and measured it neutral: the ANSI source is naive, Spark values arrive pre-wrapped, literals probe naive. The probe was reverted. |

The function passes `None`. `MERGE INSERT` does not reach it (`zone_wrapping_stream_sql` passes `analyzed_types`). `UPDATE SET *` is `UpdateAll` and does not call it. Identity `UPDATE` uses `update_projection_sql`.

One America/New_York cell, a `TIMESTAMP` column into `struct<v: TIMESTAMP_NTZ, n: INT>`. Session wall `1767305045123456`. UTC wall `1767323045123456`.

| Door and clause | Head | Main | Spark | What converts |
|---|---|---|---|---|
| Spark `WHEN MATCHED THEN UPDATE SET` | `1767305045123456` | `1767323045123456` (`merge_update_whole`, `ts_column`, copy-on-write) | `1767305045123456` | `nested_assign` probes the source and calls `store_assignment_cast_sql` before this function sees the text. Pin `test_merge_update_clauses_store_the_session_wall_before_the_none_site`. |
| Spark `WHEN NOT MATCHED BY SOURCE THEN UPDATE SET` | `1767305045123456` | not in the step-0 matrix | not in the step-0 matrix; the matched cell's Spark answer is the session wall | The same `nested_assign` fold. Same pin. |
| ANSI `WHEN MATCHED THEN UPDATE SET` | `1767323045123456` | not in the step-0 matrix | this door is not Spark | Correct as on main: the ANSI source is naive. ANSI DDL `TIMESTAMP(6) WITH TIME ZONE` creates a naive Iceberg `timestamp` column, so the UTC wall is the right store; measured 2026-10-12. Pin `ansi_matched_update_of_a_nested_ntz_keeps_the_utc_wall`. |
| ANSI `WHEN NOT MATCHED BY SOURCE THEN UPDATE SET` | `1767323045123456` | not in the step-0 matrix | this door is not Spark | Same: correct as on main, naive source. Pin `ansi_unmatched_source_update_of_a_nested_ntz_keeps_the_utc_wall`. |

## 11. The literal probe (R4, one attempt, not landed)

`probe_type` already plans `SELECT (expr) FROM {probe_from} LIMIT 0` on the
`SessionContext` the caller holds. That plans a `TIMESTAMP '…+00:00'` inside
`named_struct` as `Timestamp(Nanosecond, None)`. The Spark rule that types the
same text as an instant is the token rewrite in `spark_rewrites` on the Spark
door, before parse. It is not a parameter at the `probe_type` call site.
Teaching the probe to re-enter that rewrite is a design, so this attempt stops.
The literal cells stay as on main. A column and a literal of the same instant
therefore still give two answers on whole-struct `MERGE UPDATE` and on
`UPDATE … WHERE`: the column stores the session wall and the literal stores
`1767323045123456`.

## 12. Depth, the mirror, and list or map (R5)

Depth 2 is pinned on both emitters: `a_struct_inside_a_struct_wraps_only_the_inner_instant_leaf`
emits `i.v`, and `both_emitters_select_the_same_paths` reads `i.v` from the
expression and from the SQL. The mirror row in `LEAF_ROWS` stays `wrap: false`.
Registry row R-020 records the 88 step-0 cells. A struct under a list stores
the UTC wall (`test_a_struct_under_a_list_keeps_the_utc_wall`). A map of
structs panics in Arrow's list cast on insert; the walk does not enter it
(`a_list_or_map_is_left_as_it_is`). R-011 stays open for both.

## 13. Verify 1

F1 is blocked on the struct coercion unit. This PR does not merge before that
unit. No provenance guard is added. The engine unifies a `TIMESTAMP_NTZ` struct
with a `TIMESTAMP` struct by reading the wall as UTC. The kernel then localises
that coerced leaf into the session zone. The six statements, America/New_York:

1. `UPDATE t SET st = CASE WHEN id = 1 THEN named_struct('v', c, 'n', 100) ELSE st END WHERE id >= 0`
2. `UPDATE u SET st = coalesce(CASE WHEN c IS NOT NULL THEN named_struct('v', c, 'n', 100) END, st) WHERE id >= 0`
3. `INSERT INTO w SELECT id, st FROM zsrc UNION ALL SELECT id, st FROM nsrc` where `zsrc.st.v` is `TIMESTAMP` and `nsrc.st.v` is `TIMESTAMP_NTZ`
4. `MERGE INTO m t USING zsrc s ON t.id = s.id OR t.id = s.id + 1 WHEN MATCHED THEN UPDATE SET t.st = CASE WHEN t.id = 1 THEN s.st ELSE t.st END`
5. `INSERT INTO x SELECT id, CASE WHEN id = 1 THEN st ELSE (SELECT st FROM nsrc) END` from that same union
6. `INSERT INTO y BY NAME SELECT id, st FROM zsrc UNION ALL SELECT id, st FROM nsrc`

The wall row of statement 1 stores `1767305045123456` (the January wall minus
five hours). The instant row stores `1783149753654321`. The pin
`test_mixed_case_wall_row_stays_1767305045123456_until_coercion_unit_lands`
holds the wall row. The day the coercion unit lands, that pin goes red and is
rewritten. Disposition: blocked on the struct coercion unit.

Round 12 (2026-10-12): the coercion unit landed on this head. The pin went
red (wall row `1767323045123456`) and is rewritten as
`test_mixed_case_wall_row_stores_the_wall`. The eight-shape joined pin
`test_joined_write_shapes_store_the_wall_rows` holds the S1 wall rows in
America/New_York. Both pins were red on `ef721ee6` alone per the coercion
verdict's recorded cells and are green on this head; no file-level revert can
show that red, so the verdict's cells are the red evidence.

F2 is fixed at the two bare sites. `paired_field` is gone. A case-different
name is `Keep`, so `INSERT … BY NAME` and `INSERT OVERWRITE … SELECT` use
main's positional conform. The six statements, each under `caseSensitive` true
and false, in UTC and America/New_York, store main's rows. No refusal, no NULL
member, no swapped member. They stay at the UTC wall, a named hole equal to
main:

| Shape | `v` | `w` |
|---|---|---|
| `named_struct('V', l, 'W', l2)` | `1767323045123456` | `1783164153654321` |
| `named_struct('W', l2, 'V', l)` | `1783164153654321` | `1767323045123456` |
| `named_struct('W', l2, 'v', l)` | `1783164153654321` | `1767323045123456` |

Pin: `test_case_different_members_store_mains_positional_utc_wall`. Disposition: fixed, and the stored cells are a named hole equal to main.

F3 is fixed at the same two sites. The bare kernel keeps source nullability, and
main's later conform accepts NOT NULL members. The 35 cells (5 layouts by the
7 doors `by_name_table`, `by_name_named`, `overwrite_table`,
`merge_insert_table`, `merge_insert_star`, `merge_insert_named`,
`df_overwrite_partitions`) store. In UTC the ticks are the physical instants
`1767323045123456` and `1783164153654321`, which are Spark's and main's. In
America/New_York the names match, so the leaf is converted to Spark's session
wall `1767305045123456` and `1783149753654321`. None of these 35 cells stay at
the UTC wall in America/New_York. Plain `INSERT … SELECT` and DataFrame append
still go through a cast site. Pin:
`test_not_null_members_store_on_the_bare_kernel_doors`. Disposition: fixed.

F4 is fixed for the instant-into-NTZ shape. When the assignment value contains a
scalar subquery and the probed type would wrap, `leaf_sql` emits main's cast
with no source type, so the kernel does not run. `UPDATE t SET st = (SELECT st
FROM src WHERE id = 1) WHERE id >= 0`
raises `PySparkException` and the first line `datafusion engine error: Arrow
error: Invalid argument error: arguments need to have the same data type`. It
does not report success with no row changed. Pin:
`test_scalar_subquery_update_raises_mains_type_error`. The pre-existing silent
no-op, a subquery whose struct type equals the target, is registry row
ICE-TSNS-SQL-1-R-021 (OPEN, measured 2026-10-12). The same pin holds that row
at `(1, 1767323045123456, 77)`. Disposition: fixed for the instant source;
named hole, equal to main, for the equal-type source.

F5 is wired at the existing walk. `fold_merge_clauses` probed
`catalog.namespace.table.branch_dev`, which does not resolve, so the source
type was unknown and the walk kept main's expression. `probe_table_sql` drops
that branch selector and the existing walk emits the kernel. `MERGE INTO
b.branch_dev` and `spark.wap.branch` store the session wall
`1767305045123456`. Pin: `test_branch_merge_update_stores_the_session_wall`.
Disposition: fixed.

F6 is option (b), the two doors whose rewrite is side-effect free.
`after_analysis` descends a non-ANALYZE `LogicalPlan::Explain` to an append
`Dml` and substitutes only when `wrap_nested_ntz` changed an expression. The
`DfStatement::Explain` arm runs `refuse_cast_then_fold_nested` on an inner
`Update` and replaces assignments only when they name
`__repark_cast_nested_ntz__` more times than the original. Any other
statement, and any error from the fold, keeps the original value. `EXPLAIN`
of `INSERT OVERWRITE`, of the three `MERGE` clauses, of `REPLACE WHERE`, and
of `BY NAME` stays main (ICE-TSNS-SQL-1-R-022 through R-027). `EXPLAIN
ANALYZE INSERT` of an instant in America/New_York writes one row at the UTC
wall `1767323045123456` (R-028). Pin: `test_explain_pins`. The unconverted
texts were recorded on head `b6b20b3c` with the arm absent. Disposition:
fixed for append `INSERT` and whole-struct `UPDATE`; named holes for the
other doors and for `EXPLAIN ANALYZE`.

F7 skips the wrap at `nested_micro_ntz_wall_sql` and `wrap_nested_ntz` when a
NOT NULL target member is nullable on the source, so the existing cast names
the user's type. The bare kernel stays wrapped, and the 35 NOT NULL cells
still store the values they stored at `4b66decb`. Pin:
`test_not_null_insert_names_the_users_source_type` holds main's first line in
UTC and America/New_York, and `test_not_null_members_store_on_the_bare_kernel_doors`
holds the 35 stores. Disposition: fixed for the cast emitters; the bare
kernel doors are unchanged.

F8 is two named holes, both equal to main. A member whose name contains `.`
or `,` is not wrapped: ``a.b`` and ``c,d`` store `1767323045123456`. The
sibling `v` is still converted. A matching leaf beside a renamed member keeps
the whole struct: `INSERT … SELECT` of `named_struct('v', l, 'x', l2)` stores
the UTC wall in `v` and NULL in `w`. Pin:
`test_punctuation_names_and_a_renamed_sibling_stay_at_mains_values`.
Disposition: named hole.

## 14. Round 10 halt (2026-10-12)

The owner ruled that F6 and F7 are fixed in this unit. F6 does not fit one
site. `EXPLAIN <dml>` is the router fallthrough to
`spark_ast::execute_passthrough`. That plans `LogicalPlan::Explain`, whose
inner plan is the DML. `after_analysis` rewrites only `LogicalPlan::Dml` for
an Iceberg `INSERT`, so an `EXPLAIN` never receives that rewrite.

The kernel the statement runs is injected on a different path per door:

| Door | Where execution inserts the kernel | What EXPLAIN plans |
|---|---|---|
| `INSERT … SELECT` and DataFrame append | `after_analysis` / `wrap_nested_ntz` | `Explain` wrapping the unwrapped DML |
| `INSERT OVERWRITE`, `BY NAME`, `overwritePartitions` | `zone_stores` on the source frame inside the write | the unwrapped DML |
| `UPDATE` whole-struct | `refuse_cast_then_fold_nested` before `execute_passthrough` | the unfolded statement |
| `MERGE` insert and update | `fold_merge_clauses` and `zone_wrapping_stream_sql` inside `execute_merge` | the unfolded statement |

A change only in `after_analysis` would show the kernel for plain `INSERT`
and would leave the other doors on main's plan. Putting the same rewrite into
each of those paths is more than one site. No code was changed.

F7's sentence is built in DataFusion, not in this unit:
`datafusion-physical-expr-54.1.0` `expressions/cast.rs` formats
`Unsupported CAST from {expr_type} to {cast_type}` from the physical input
type. After the kernel's `retype`, that input is `Timestamp(us)` with no
zone. The walk still has the user's source type before the wrap. The smallest
option that does not reimplement the cast: at the two cast emitters
(`nested_micro_ntz_wall_sql` and `wrap_nested_ntz`), do not wrap when the
target struct has a NOT NULL member the arrow cast cannot accept. The
analyzer's existing cast then fails and names the user's type. The bare
kernel sites stay wrapped, so the 35 NOT NULL stores stay. Not done, pending
the F6 ruling.

## 15. Round 15: the second verification (2026-10-12)

The second verification failed the head `877feac5` on one S1 and named four
smaller findings. This section folds them. The verdict is not in the tree; its
numbers are quoted where they are compared.

### F1, the wall operand first

Cause, confirmed by measurement before the change. On `877feac5` in
America/New_York, `UPDATE t SET st = CASE WHEN id = 2 THEN st ELSE
named_struct('v', c, 'n', 100) END WHERE id >= 0` printed an `EXPLAIN` with
`__repark_localize_nested_ntz__` and without `__repark_cast_nested_ntz__`, and
stored the wall row at `1767341045123456` (the wall plus five hours). With the
two branches in the other order the `EXPLAIN` named both kernels and the wall
row stored `1767323045123456`. The fold read the type of the plan `ctx.sql`
returns. That plan is not analyzed: a `CASE` has the type of its first `THEN`
branch and a `UNION ALL` source the type of its left input. The value looked
naive, the decision was `Keep`, and main's cast then stored the instant the
analyzer had localised. The unit test
`a_wall_first_case_and_a_naive_first_union_resolve_to_the_zoned_struct` holds
both readings: the plan type is the naive struct and the analyzed type is the
zoned struct.

Fix. `nested_assign/probe.rs` `probe_type` returns the plan type and the
analyzed type. The plan type still drives the by-name struct checks and their
refusals, as on main. The analyzed type is the only source the store decision
sees. `UPDATE` (`refuse_cast_then_fold_nested`), `MERGE`
(`fold_merge_clauses`) and the `INSERT (…) VALUES (…)` rebuild call that one
probe. When the analysis fails, the assignment takes main's cast. The analysis
is `ntz_store::analyzed_types`, the pass the `INSERT`, `OVERWRITE` and `MERGE
INSERT` sites already run; it executes nothing.

After the change the same `EXPLAIN` names both kernels and the wall row stores
`1767323045123456`. The two statements that change no wall (the verdict's G
and H) leave both rows at `1767323045123456`: the wall is localised and then
stored in the same zone.

`UPDATE SET *` is not in either fold. Section 10 says `UPDATE SET *` does not
call `store_assignment_then_sql`; that is wrong. `expand_star_clauses` in
`repark-iceberg` `write/merge/mod.rs` turns the star into `source.column`
assignments, and those take main's cast. A star whose source is a mixed
`UNION ALL` stored the wall row shifted with either side first. The guard is
at that one place: the star assignment goes through
`nested_micro_ntz_wall_sql` with the source column's analyzed type. Only the
Spark door builds a star. `MERGE WITH SCHEMA EVOLUTION … UPDATE SET *` goes
through the same place: measured in America/New_York, the wall row stays
`1767323045123456` (main: the same; Spark refuses the statement).

Measured on the round 15 head, against main `2ac03089` and live Spark 4.1.2,
2026-10-12:

| Probe | Zones | Result |
|---|---|---|
| the verdict's eight statements (`s1_repro.py`) | America/New_York, Australia/Lord_Howe, UTC | 24 of 24 equal Spark |
| the verdict's 68 `MERGE` and `UPDATE` cells (`mu.py`, `mu2.py`) | America/New_York | 0 worse than main (was 24); 50 moved onto Spark, 18 as main |
| the same | Australia/Lord_Howe | 0 worse than main (was 24); 50 moved onto Spark, 18 as main |
| the same | UTC | 0 worse than main; 2 moved onto Spark, 66 as main |
| `wall_first_round15.py`, 12 shapes by 2 operand orders by 2 write modes | each of the three zones | 48 of 48 equal Spark, 0 rows worse than main (before: 24 of 48 and 28 rows worse in each non-UTC zone) |
| `probe.py`, 1188 cells | UTC, America/New_York, Asia/Kolkata | 0 worse than main; `ntz_nested` 100 moved onto Spark and 296 as main; `ltz_nested` 14 moved and 382 as main; `ntz_top` 396 as main |
| `join_round12.py` | America/New_York, Australia/Lord_Howe, UTC | 42 of 42 equal Spark; the three head files are byte-equal to the committed ones |

Main and Spark for `probe.py` are the verifier's runs of `2ac03089` and Spark
4.1.2, which it found equal to the committed evidence cell for cell. Every
other row was run again on all three engines.

The matrix moved 20 cells more than on `877feac5`, each onto Spark's tick, in
America/New_York and Asia/Kolkata, both write modes:

| Door | Source | Cells | Why |
|---|---|---|---|
| `merge_update_star` | `ts_column`, `ts_literal` | 8 | the star guard above |
| `merge_update_whole`, `merge_update_whole_literal`, `update_whole_where` | `ts_literal` | 12 | the analyzed type of `named_struct('v', TIMESTAMP '…+00:00', 'n', 1)` is the zoned struct, so the decision wraps it (by reading: the plan type is `Timestamp(ns)` and the analyzer types the literal as an instant) |

[summary-head.md](../../wo/ice-ntz-nested-wall-1/evidence/summary-head.md) is
regenerated: 193 of 264 `ntz_nested` instant cells equal Spark, and 2 instant
cells still hold the UTC wall, both `insert_values` `ts_literal`. Section 11
and the sentence of section 8 that lists 22 cells describe `877feac5`.
Two integration pins move with it, each onto the tick Spark recorded in
`spark-4.1.2.jsonl`: `timestamp_ns_wall_doors.rs` `NTZ_NESTED_DOORS` (`merge
set struct` now stores the session wall) and `timestamp_ns_nested_shapes.rs`
`a_nested_zoned_or_microsecond_leaf_is_not_guarded` (its `TIMESTAMP_NTZ` row,
a whole-struct `UPDATE … WHERE` of the literal, `1767305045123456` in
America/New_York).

Pins. `test_either_operand_order_stores_sparks_rows` replays the 12 shapes in
both orders and both write modes in America/New_York and Australia/Lord_Howe,
96 cells, against rows recorded from live Spark
([ice_ntz_nested_wall_1_wall_first.json](../../../python/repark/tests/ice_ntz_nested_wall_1_wall_first.json)).
`a_wall_first_case_and_a_naive_first_union_resolve_to_the_zoned_struct` and
`a_wall_first_case_folds_to_the_store_kernel` hold the decision in Rust.

Mutants, by hand, each restored:

| Mutant | Suite | Result |
|---|---|---|
| `probe.rs`: the stored type is the plan type again (the type decision reverted) | `repark-spark --lib` | killed by `a_wall_first_case_and_a_naive_first_union_resolve_to_the_zoned_struct` and `a_wall_first_case_folds_to_the_store_kernel` |
| the same | release module, the four facade files | killed: `test_either_operand_order_stores_sparks_rows` fails in both zones, 2 failed and 41 passed |
| `merge/mod.rs`: a star assignment keeps the bare `source.column` text | release module, the four facade files | killed: the same test fails in both zones on `merge-update-star`, both operand orders, 2 failed and 41 passed |
| `keep_update_store_casts` skips `UPDATE` | `repark-functions --lib`, `repark-spark --lib` | killed by `an_update_assignment_cast_keeps_mains_text_and_an_inner_coercion_is_localised` and by the Rust EXPLAIN pin |
| `analyzed_insert_stores_a_struct_wall` inverted | `repark-functions --lib` | killed by `an_analyzed_insert_into_a_struct_wall_keeps_mains_struct_cast` |
| the verdict's E1, E2, E3, E4 (the EXPLAIN INSERT and EXPLAIN UPDATE gates) | `repark-spark --lib` | each killed by `explain_names_the_store_kernel_on_the_converted_iceberg_doors_only` (they survived that suite on `877feac5`) |

### Q4, `EXPLAIN ANALYZE INSERT` of a mixed union

The cause is not F1's. `EXPLAIN ANALYZE` is excluded from the append descent
(R-028), so the write ran with the localising kernel and without the store
kernel. It now runs main's plan: `SparkLtzTimestampCast` does not take the
struct arm when the plan is `Analyze` over an `INSERT` into a table that holds
a struct with a naive microsecond leaf
(`nested_wall::analyzed_insert_stores_a_struct_wall`). Measured in
America/New_York: with the instant side first the wall rows store
`1767323045123456` and the instant rows `1783164153654321`, main's rows; with
the wall side first the statement raises main's error, `Internal error: cannot
convert text Timestamp(µs, "UTC") into byte Timestamp(µs).` R-028 is unchanged
and still names the instant row. Pins: `test_named_cells_equal_main` (cells
`explain-analyze-insert/…`) and
`an_analyzed_insert_into_a_struct_wall_keeps_mains_struct_cast`.

### Q2, a wall in a daylight-saving gap

Spark's moved gap wall is the target. A naive wall inside a gap that passes
through a mixed `CASE`, `COALESCE` or `UNION ALL` into an NTZ leaf is stored
moved forward by the gap (America/New_York `2026-03-08 02:30:00` becomes
`03:30:00`), as Spark stores it and as the top-level twin already did on main.
The second verification measured it (three walls per door in America/New_York,
two in Australia/Lord_Howe); it was not run again in this round. The release
note above carries the line. No code.

### F2, the depth-2 NOT NULL UPDATE cells

Named, not wired. In America/New_York a whole-struct `UPDATE … WHERE` of a
`TIMESTAMP` column into `STRUCT<i: STRUCT<v: TIMESTAMP_NTZ, n: INT> NOT NULL,
m: INT>` or into `STRUCT<i: STRUCT<v: TIMESTAMP_NTZ, n: INT>, m: INT NOT
NULL>`, bare or inside `CASE WHEN id = 1 THEN … ELSE st END`, stores `st.i.v =
1783164153654321`, the UTC wall, on head and on main. Spark stores
`1783149753654321`. Four cells. Registry row ICE-TSNS-SQL-1-R-031 (OPEN,
measured 2026-10-12). Pin: `test_named_cells_equal_main`, cells `not-null/…`.

### F3, the temp view after a zone change

Named, not wired. A temp view over a mixed frame, made in America/New_York
and inserted after the session zone is set to Asia/Kolkata, stores the wall
rows unchanged (`1767323045123456`), as main does. Spark stores
`1767360845123456`. The instant rows store Spark's Kolkata wall
`1783183953654321`. Three wall rows in two tables. Registry row
ICE-TSNS-SQL-1-R-032 (OPEN, measured 2026-10-12). Pin:
`test_named_cells_equal_main`, cells `zone-switch/…`.

### F4, `EXPLAIN UPDATE` of a zoned struct from a naive one

Choice: the `EXPLAIN` is main's text. It is the smaller change. The other
choice sets the mirror row of `LEAF_ROWS` and moves the R-020 cells of every
whole-struct door. The kernel was printed because DataFusion's `UPDATE`
planner casts the assigned value to the column type and the coercion rule
rewrote that cast. For a whole-struct assignment that cast does not run: the
fold replaces the value with `arrow_cast` text of the column type, so the
planner adds no cast. `nested_wall::keep_update_store_casts` runs on each
`Dml(Update)` node and takes the kernel back out of an assignment's own cast.
A coercion inside the assigned value keeps its kernel, because that one runs:
`UPDATE tl SET st = CASE WHEN id = 1 THEN ns ELSE st END WHERE id >= 0` still
stores Spark's `1767341045123456`.

One text is left. When the assigned value is a scalar subquery
(`UPDATE tl SET st = (SELECT st FROM n WHERE id = 1) WHERE id >= 0`), type
coercion puts the cast inside the subquery, and `EXPLAIN` prints the kernel
there. The statement is refused on main and on head, on `877feac5` as well,
and stores nothing; the first line of the refusal differs from main's (`UPDATE
operation on table` against main's `arguments need to have the same data
type`). Measured 2026-10-12 in America/New_York. Not changed in this round. Measured in
America/New_York: `EXPLAIN UPDATE tl SET st = named_struct('v', w, 'n', 1)
WHERE id >= 0` and `… SET st = ns WHERE id >= 0` print main's text, and the
`UPDATE` stores `1767323045123456`, main's value. The `UPDATE` does not move,
so R-020 is unchanged. Pins: `test_named_cells_equal_main` (cells
`explain-update/…`, text recorded on main `2ac03089`),
`an_update_assignment_cast_keeps_mains_text_and_an_inner_coercion_is_localised`,
and the last two assertions of the Rust pin below.

### F5, the EXPLAIN gates in Rust

`crates/repark-spark/src/tests/explain_nested_ntz.rs`
`explain_names_the_store_kernel_on_the_converted_iceberg_doors_only` plans
over an Iceberg provider with `common.rs` `setup_at_zone`, 72 lines. It kills
the four gate mutants of the verdict in the `repark-spark` lib suite (table
above).

### Gates

Run on the round 15 tree, each cargo, maturin and pytest command in the build
wrapper, 2026-10-12.

| Command | Exit | Result |
|---|---|---|
| `cargo fmt --all -- --check` | 0 | clean |
| `cargo clippy --locked --workspace --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 | clean |
| `make rust-panic-ban` | 0 | clean |
| `cargo test --locked -p repark-spark --lib` | 0 | 2713 passed, 5 ignored |
| `cargo test --locked -p repark-iceberg --lib` | 0 | 1013 passed |
| `cargo test --locked -p repark-functions --lib` | 0 | 946 passed, 1 ignored |
| `cargo test --locked -p repark-sql --lib` | 0 | 403 passed, 4 ignored |
| `cargo test --locked --no-fail-fast -p repark-spark --test timestamp_ns_wall_doors --test timestamp_ns_nested_shapes --test timestamptz_ns_wall_doors` | 0 | 7, 6 and 4 passed |
| `cargo test --locked --no-fail-fast -p repark-sql` | 0 | every target passed, `ansi_ntz_wall_cast` 9 passed |
| `uvx maturin@1.14.1 develop --release` | 0 | release module installed |
| `pytest -q` on `test_ice_ntz_nested_wall_1.py`, `test_ice_ntz_nested_wall_1_joined.py`, `test_ntz_struct_coerce_1.py`, `test_ice_ntz_nested_wall_1_wall_first.py` | 0 | 43 passed |
| `pytest -q python/repark/tests -k "ntz or timestamp or struct or union or coalesce or case_when or merge or update or tsns or explain"` | 0 | 3111 passed, 59 skipped, 10 xfailed, 0 failed |
| `make check-ledgers`, `check-ledger-grammar`, `check-docs-compaction`, `check-docs-links`, `check-rust-file-size`, `check-lib-py`, `check-python-conventions`, `check-docstring-presence`, `check-manifest`, `check-crate-dag`, `check-lib-rs`, `check-matrix-test-liveness`, `spell-check`, `toml-check` | 0 | clean |
| `python3 scripts/sync_map_md.py --check`, `ruff check .`, `ruff format --check .` | 0 | clean |
| the comment gate against `origin/main` | 0 | 0 hits |

The broad facade run, `cargo test -p repark-sql`, the `repark-functions` and
`repark-sql` lib suites and the three integration targets ran before the last
formatting pass. That pass changed line breaks in `write/merge/mod.rs` and in
the new Rust pin and no token; every other row ran after it.
