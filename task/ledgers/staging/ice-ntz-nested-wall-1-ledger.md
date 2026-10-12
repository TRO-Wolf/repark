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
| C-004 | Controls are identical to main. | Head probe. | **PROVEN** | [summary-head.md](../../wo/ice-ntz-nested-wall-1/evidence/summary-head.md): field-assignment 324 of 324, `ltz_nested` 396 of 396, `ntz_top` 396 of 396, moved 0. pins: ice-ntz-nested-wall-1/C-004 |
| C-005 | `UPDATE` with no `WHERE` and `INSERT OVERWRITE … VALUES` store the session wall. | Head probe against main. | **OPEN** | They still refuse with main's error (`update_whole_nowhere`, `update_col_nowhere`, `insert_overwrite_values`). Not a stored-value change. |

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
- `ltz_nested` identical to main: 396 of 396.
- `ntz_top` identical to main: 396 of 396.
- instant cells still on the UTC wall outside UTC: 22.

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
| `INSERT … VALUES` of a literal, MERGE `UPDATE SET *`, whole-struct literal MERGE and `UPDATE … WHERE` | UTC wall | UTC wall |
| `UPDATE` with no `WHERE`, `INSERT OVERWRITE … VALUES` | refusal | the same refusal |
