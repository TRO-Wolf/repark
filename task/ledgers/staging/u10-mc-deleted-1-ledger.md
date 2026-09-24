# Charter ledger — U10-MC-DELETED-1 · serve the `_deleted` metadata column (R-MC-DELETED)

**Date:** 2026-09-23 · **Branch:** `fix/u10-mc-deleted` · **Base:** `fd43f19` (`origin/main`) · **Model:** Devin SWE-2 (swe-2-high) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** no registry row touched.

**Retires:** in flight.

**Scope:** `SELECT id, _deleted FROM <cat>.<ns>.<t>` on a merge-on-read Iceberg table
answers what Spark 4.1.2 answers — every row of the scanned files, the deleted ones
included, with `_deleted = true` where a delete file removes the row and `false`
otherwise (recorded cell `R-MC-DELETED` = `[[1,true],[2,false],[3,false],[4,false]]`,
order-insensitive). `_deleted` joins `METADATA_COLUMN_NAMES` (4→5) and gains a
non-null Boolean field (`RESERVED_FIELD_ID_DELETED`) after `_partition` in
`crates/repark-iceberg/src/catalog/metadata_columns.rs`; the scan passthrough is
unchanged so the projected name reaches the pinned fork (rev `604edca`), which
activates include-deleted mode by itself. The unserved machinery
(`UNSERVED_METADATA_COLUMN_NAMES`, `canonical_unserved_token`,
`first_unserved_metadata_column`, `refuse_unserved`, the unserved branch in
`prepare_metadata_column_sql`, the `catalog/mod.rs` re-export) is deleted, and the
`refuse` message names all five served columns. mcdel-r4 adds the reserved-name
collision refusal in `crates/repark-core/src/metadata_columns.rs`
(`prepare_metadata_column_sql`) and stops the provider appending a reserved
field whose name the user schema already carries; it trues up the
`ICE-MC-FILEPOS-1` row in `docs/spark-sql-iceberg-parity.md` and the predecessor
`ice-metadata-cols-1` ledger (C-007, C-008, C-018, C-023). Tests in
`crates/repark-spark/src/tests/metadata_columns_deleted.rs` (the `_deleted`
cluster moved there when `metadata_columns.rs` reached the file-size ceiling)
and `python/repark/tests/test_ice_metadata_cols_1.py`; seven `map.md` files; this
ledger.
The fork, every `Cargo.toml`, `Cargo.lock`, `STATUS.md`, `time_travel.rs`, the
metadata-table paths, `describe_show.rs` and the lineage columns are untouched; no
code comment added anywhere.

## Measurements (decide-then-build evidence)

**M-1 — the oracle is recorded, not re-derived.** Cell `R-MC-DELETED`
(`/tmp/oc-worker/scoreboard/2026-09-23/cells_read.py:268-285`), measured on live
PySpark 4.1.2 + Iceberg 1.11.0: table `(id BIGINT, data STRING, cat STRING)`
`PARTITIONED BY (cat)`, `format-version=2`, `write.delete.mode=merge-on-read`;
`INSERT (1,'a','x'),(2,'b','y'),(4,'d','x')`; `INSERT (3,'c','x')`;
`DELETE WHERE id = 1`; `SELECT id, _deleted FROM t` =
`[[1,true],[2,false],[3,false],[4,false]]` (order-insensitive). The Rust seed
helper builds exactly this fixture.

**M-2 — the fork does the scan-mode work.** Fork pin `604edca`:
`crates/iceberg/src/arrow/reader.rs:555` enables include-deleted mode when the
projected field ids contain `RESERVED_FIELD_ID_DELETED`; `arrow/pos_apply.rs`
(`apply_pos_aware_batch` / `apply_pushdown_path_batch`, `include_deleted: bool`)
stops filtering deleted rows and appends a `_deleted` Boolean column;
`record_batch_transformer.rs` resolves the field as Boolean, constant `false`
where no delete verdict applies. Not projecting `_deleted` keeps today's filter.
RePark's `MetadataColumnsTableProvider` already hands projected names straight to
the fork scan, so serving is advertising plus not refusing — measured: after the
served-set change with no other scan edit, the marks-deleted pin answers the
recorded cell verbatim.

**M-3 — premise correction: a metadata column over a time-travel read raises the
planner's unresolved-column error, not `[ICE-MC-1]`.** The work order's near-miss
text claims `[ICE-MC-1]` "with the five-name message". Measured on the pre-change
head (probe, since removed): `SELECT id, _file FROM ice.ns.t VERSION AS OF <id>`
and `SELECT id, _deleted FROM ice.ns.t VERSION AS OF <id>` both raise
`[UNRESOLVED_COLUMN.WITH_SUGGESTION] … cannot be resolved`. Reason: the
time-travel rewrite runs before `prepare_metadata_column_sql` and replaces the
relation with `datafusion.public.__repark_tt_N`, which is not an Iceberg catalog
table, so no rewrite fires and the pinned static provider (which never advertises
metadata columns) answers unresolved. `prepare_metadata_column_sql`'s own
`version.is_none()` collector guard is unreachable through the Spark door — every
`AS OF` span is consumed upstream. The pin asserts the measured error for both a
previously-served column (`_file`) and the newly-served `_deleted`; the intent
("keep today's answer") holds verbatim. The mcdel-r2 ruling (orchestrator,
2026-09-23 18:04) measured that Spark 4.1.2 SERVES `_file` and `_deleted` over
`VERSION AS OF` (S16/S17 rows, `/tmp/xo-xo-opus58/probe/mcdq2-spark.log`) — the
refusal is therefore a KNOWN DIVERGENCE, not a pin of Spark behavior. RePark
refuses every metadata column over `VERSION AS OF` (the static provider advertises
none); Spark was measured only for `_file` and `_deleted`, so `_pos`, `_spec_id`
and `_partition` over time travel are UNMEASURED ON SPARK. The test
is renamed `metadata_column_over_time_travel_known_divergence` and asserts the
full message.

**M-4 — red-first.** Commit `2c108149` (tests only): six Rust legs fail with
`Analysis("Error during planning: [ICE-MC-1] metadata column _deleted is not yet
served; this layer serves (_file, _pos, _spec_id, _partition)")` —
`deleted_column_marks_merge_on_read_deleted_row`,
`deleted_column_on_copy_on_write_marks_all_rows_false`,
`deleted_name_folds_unquoted_but_quoted_upper_stays_unknown`,
`deleted_predicates_reapply_above_the_scan`,
`served_names_fold_and_composed_shapes_refuse`,
`served_spec_id_and_deleted_answer_together` — and the facade pin
`test_deleted_marks_merge_on_read_deleted_row` fails with the same text through
`AnalysisException`. The near-miss legs (`not_projecting…`, `select_star…`,
`metadata_column_over_time_travel…`, `test_not_projecting_deleted_still_filters`)
are green on the same commit — they pin unchanged behavior. (mcdel-r2 renamed
`deleted_name_folds_unquoted_but_quoted_upper_stays_unknown` to
`unquoted_upper_deleted_folds_to_served_name` plus
`quoted_upper_deleted_known_divergence`, and
`metadata_column_over_time_travel_keeps_todays_error` to
`metadata_column_over_time_travel_known_divergence`.)

**M-5 — mutations (mcdel-r1).** (a) Constant-false `_deleted`: `conform_batch`
synthesised a `BooleanArray` of `false` for the `_deleted` field instead of
projecting the fork's column → `deleted_column_marks_merge_on_read_deleted_row`
fails with left `[(1, false), (2, false), (3, false), (4, false)]`, and the three
pins that read a `true` verdict
(`deleted_predicates_reapply_above_the_scan`,
`deleted_name_folds_unquoted_but_quoted_upper_stays_unknown`) fail too; 16 pass.
Reverted. (b) `_deleted` re-added to the refusal (`[ICE-MC-1] … not yet served`
ahead of the rewrite) → the same six legs fail as in M-4; 14 pass. Reverted.

**M-6 — mcdel-r2 measurement sweep (probe on the PR head, since removed).**
The Spark answers S1–S14 are the orchestrator's live probe
`/tmp/xo-xo-opus58/probe/mcdq1-spark.log` and S15–S17 are
`/tmp/xo-xo-opus58/probe/mcdq2-spark.log` (Spark 4.1.2 / Iceberg 1.11, the recorded
seed). Every RePark answer in the r2 table was probed before pinning: S1
`[(1,true),(2,false),(3,false),(4,false)]`; S2 fields `id,data,cat` rows
`[(2,b,y),(3,c,x),(4,d,x)]`; S3 `[1]`; S4 `[2,3,4]`; S5 `[1,2,3,4]`; S6 `[3]`
(the `IS NOT NULL` leg prunes `_deleted`, matching Spark's fold); S7 `[3]`; S8
`[2,3,4]`; S9 `[2,3,4]` (pruned projection keeps the delete filter); S10
`[(1,D),(2,L),(3,L),(4,L)]`; S11 `[1]`; S12 `[2,3,4,1]`; S13
`[(false,3),(true,1)]`; S14 `[]`. All fourteen match Spark verbatim — no
production change was needed for V-001..V-003. S15 divergence check (R3):
quoted `` `_FILE` `` is refused with the same `[UNRESOLVED_COLUMN.WITH_SUGGESTION]`
as `` `_DELETED` `` — RePark's quoted-upper refusal is not `_deleted`-specific. Spark
was measured resolving quoted `` `_DELETED` `` only (S15); its answer for quoted
`` `_FILE` `` is UNMEASURED ON SPARK. Kept refused, full message pinned, KNOWN
DIVERGENCE for `` `_DELETED` ``. S16/S17 per R2: KNOWN DIVERGENCE for `_file` and
`_deleted` (the measured names), full message pinned.

**M-7 — mcdel-r2 mutations.** (a) `_deleted` filtered out of the names handed
to the fork in `MetadataColumnsTableProvider::scan` → the predicate-only pin
`deleted_predicates_reapply_above_the_scan` fails (`missing column '_deleted'`
internal error) together with the seven other `_deleted`-projecting pins; 4
pass. Reverted. (b) `conform_batch` synthesises a constant-`false` Boolean for
`_deleted` → `deleted_column_marks_merge_on_read_deleted_row` fails left
`[(1,false),…]` and `deleted_predicates_reapply_above_the_scan`,
`deleted_column_flows_through_subqueries`,
`deleted_column_in_expressions_order_and_group`,
`unquoted_upper_deleted_folds_to_served_name` fail; 7 pass. Reverted. (c)
`SQLSTATE: 42703` changed to `42704` in the `` `_DELETED` `` expected string →
`quoted_upper_deleted_known_divergence` fails alone; 11 pass. Reverted.

**M-8 — mcdel-r3 measurement (Sol critic r2 V-001..V-003).** Live Spark 4.1.2 /
Iceberg 1.11 answers on the same seed (orchestrator probe
`/tmp/xo-xo-opus62/probe/mcdq3-spark.log`, `c` copy-on-write, `t` merge-on-read): P1 `SELECT id, _spec_id, _deleted FROM c ORDER BY id` =
`[(2,0,false),(3,0,false),(4,0,false)]`; P2 the same on `t` =
`[(1,0,true),(2,0,false),(3,0,false),(4,0,false)]` (`_spec_id` int, `_deleted`
boolean NOT NULL); P3 `SELECT a.id, b.id FROM t a JOIN t b ON a.id = b.id + 1 WHERE
b._deleted ORDER BY 1` = `[(2,1)]`; P4 `SELECT a.id, b.id, b._deleted … ON a.id =
b.id + 1 ORDER BY 1` = `[(2,1,true),(3,2,false),(4,3,false)]`; P5 (S14) = `[]`; P6
`… WHERE NOT b._deleted ORDER BY 1` = `[(3,2),(4,3)]`; P7 `SELECT * FROM t ORDER BY
id` = `[(2,b,y),(3,c,x),(4,d,x)]`; P8 `… ORDER BY id DESC` =
`[(4,d,x),(3,c,x),(2,b,y)]`. RePark answered all eight verbatim on the first run —
no red commit and no production change; the join routes (no `classify_select`
refusal). The row helpers stopped sorting: every `ORDER BY` pin now asserts row
order, and the two unordered legs of `served_spec_id_and_deleted_answer_together`
compare a `sorted(..)` multiset. Planner trace (probe, since removed): DataFusion
calls the shared provider's `scan` for the join's right input `b` (projecting `id,
_deleted`) before the left `a` (projecting `id`).

**M-9 — mcdel-r3 mutations.** (a) `conform_batch` emits a constant-`1`
`Int32Array` for `_spec_id` whenever `_deleted` is also projected →
`served_spec_id_and_deleted_answer_together` fails alone, left
`[(2,1,false),(3,1,false),(4,1,false)]`; 24 pass. Reverted. (b) The provider
counts its `scan` calls; at `execute`, a self-join's first-planned scan (the
right input `b`, per M-8) drops `_deleted` from the names handed to the fork (so
the fork stays in filter mode) and `conform_batch` synthesises constant `false`
for it → `deleted_column_on_join_right_side_reaches_its_scan` fails on P3, left
`[]`; with the P3 leg removed it fails on P4, left `[(3,2,false),(4,3,false)]`;
S14 `deleted_column_in_self_join_answers_empty` stays green under the mutation,
which is the gap the critic named; 24 pass. Reverted. (c) `triples_i64` reverses
the collected rows → `select_star_keeps_user_columns_on_mor_table` fails, left
`[(4,d,x),(3,c,x),(2,b,y)]`; 24 pass. Reverted.

**M-10 — mcdel-r4 reserved-name measurement (Sol critic r3 V-003).** Orchestrator
probe, both engines on the same fixture: table `(id BIGINT, _deleted STRING)`,
copy-on-write and merge-on-read, `INSERT (1,'u1'),(2,'u2'),(3,'u3')`, `DELETE WHERE
id = 1` (`/tmp/xo-xo-opus62/probe/mcdcol-spark.log`,
`/tmp/xo-xo-opus62/probe/mcdcol-repark.log`). Spark 4.1.2 / Iceberg 1.11 refuses
`SELECT id, _deleted … ORDER BY id`, `SELECT id, _spec_id, _deleted … ORDER BY id`,
`SELECT t._deleted FROM T t ORDER BY 1` and `SELECT * … ORDER BY id` with
`Table column names conflict with names reserved for Iceberg metadata columns:
[_deleted]. Please, use ALTER TABLE statements to rename the conflicting table
columns.` (its copy-on-write `DELETE` refuses with the same text; its merge-on-read
`DELETE` is served, as on RePark), and `SELECT id
… WHERE _deleted = 'u2'` with `Invalid schema: multiple fields for name _deleted:
2 and 2147483644`. RePark at `0ace83da` leaked `Schema error: Schema contains
duplicate qualified field name __repark_mc_N._deleted` on the four
`_deleted`-naming shapes and served `SELECT *` = `[[2,'u2'],[3,'u3']]`. Orchestrator
ruling (2026-09-23 19:48): a query that names a served metadata column the table's
user schema also carries refuses with exactly Spark's first text, the bracket
listing the conflicting names. Built as one check in `prepare_metadata_column_sql`,
the one place that sees the referenced names (the canonical metadata tokens of
the statement) and each table's user schema; the error is `DataFusionError::Plan`,
the class the `[ICE-MC-1]` refusals use, so it reaches the user as
`Error during planning: Table column names conflict …` and in Python as
`AnalysisException`. Two findings of the build: a user `_file` column also leaked
when the query named only `_deleted`, because the provider appended a second `_file`
field, so the provider now skips any reserved field whose name the user schema
has; and a `DELETE` on a table with a user `_file` column refuses
`MERGE INTO cannot run against a table with a column named `_file` (reserved by
the merge executor)`. That refusal is pre-existing DML behavior, outside this unit,
so the near-miss `_file` fixture is seeded without the delete. The order of several
conflicting names is measured in M-12. The red commit `fab6745c` failed
`user_deleted_column_collision_refuses_like_spark`,
`every_served_metadata_name_collision_refuses` and
`reserved_name_near_misses_still_answer` with the `__repark_mc_` leak, and the
facade pin `test_user_column_named_deleted_refuses_like_spark` failed the same
way.

**M-11 — mcdel-r4 mutations.** (a) `_deleted` made nullable on merge-on-read
tables only (the field's nullability reads `write.delete.mode`) →
`served_spec_id_and_deleted_answer_together` fails at the new merge-on-read
`!is_nullable()` assert (its copy-on-write asserts pass), together with
`deleted_column_marks_merge_on_read_deleted_row`,
`deleted_column_on_join_right_side_reaches_its_scan` and
`deleted_predicates_reapply_above_the_scan`; 24 pass. Reverted. (b1) The collision
check skipped (`if false && …`) → `user_deleted_column_collision_refuses_like_spark`
and `every_served_metadata_name_collision_refuses` fail: `plan_error`'s
`a refused query must fail` fires because the query now answers the user column's
`Utf8` values. There is no leak because the provider's duplicate skip holds; 26
pass. (b2) The check skipped and the provider's duplicate skip reverted too → the
same two fail, plus `reserved_name_near_misses_still_answer`, with left `Schema
error: Schema contains duplicate qualified field name __repark_mc_N._deleted` /
`… __repark_mc_N._file`; 25 pass. Both reverted.

**M-12 — mcdel-r5 reserved-name measurement (Sol critic r4 V-001, V-002).**
Orchestrator probes on Spark 4.1.2 / Iceberg 1.11 and RePark `458fa042`, both rc 0:
`/tmp/xo-xo-opus62/probe/mcdjoin-spark.log` / `mcdjoin-repark.log` (J, O1–O5,
N1–N5), `mcdorder-spark.log` / `mcdorder-repark.log` (O6–O9) and
`mcdfile-spark.log` / `mcdfile-repark.log` (F1–F5, D1–D3). The bracket rule, EQUAL
on both engines: it lists the colliding names that the query references, in the
table's declaration order, whatever order the query names them — `(id, _pos,
_file)`: O1 `SELECT id, _file, _pos` and O3 `SELECT id, _pos, _file` both refuse
`[_pos, _file]`, O2 `SELECT id, _file` refuses `[_file]`; `(id, _file, _pos)`: O6
and O7 refuse `[_file, _pos]`; `(id, _spec_id, _deleted, _file)`: O8 `SELECT id,
_file, _deleted, _spec_id` refuses `[_spec_id, _deleted, _file]` and O9 `SELECT id,
_deleted, _file` refuses `[_deleted, _file]`. Spark does NOT refuse every scan of
such a table: a query that names no colliding column answers on both engines — O4
`SELECT id, _spec_id` = `[[1,0]]` and O5 `SELECT id` = `[[1]]` on `(id, _pos,
_file)`; N1–N5, one table `(id, c STRING)` per served name `c` with row
`(1,'u1')`, `SELECT id, _spec_id` = `[[1,0]]` (for `c = _spec_id`, `SELECT id,
_file` = one row whose path ends `.parquet`); on `(id, _file STRING)` with three
rows F1 `SELECT id, _deleted` = `[[1,false],[2,false],[3,false]]`, F2 `SELECT id,
_pos` = `[[1,0],[2,1],[3,2]]`, F3 `SELECT id, _partition` = three NULLs, F4
`WHERE _spec_id = 0` = `[[1],[2],[3]]`; on `(id, _deleted STRING)` D1 `SELECT id`,
D2 `count(*)` and D3 `_file IS NOT NULL` are EQUAL. Still divergent: F5 `SELECT *`
on `(id, _file)` — Spark refuses `[_file]`, RePark answers
`[[1,'u1'],[2,'u2'],[3,'u3']]` (the M-10 `SELECT *` shape again). Joins, `p (id,
data)` merge-on-read after `DELETE id = 1` and `uc (id, _deleted STRING)`: J1
`SELECT p.id, p._deleted FROM p JOIN uc u ON p.id = u.id ORDER BY p.id` — Spark
answers `[[1,true],[2,false],[3,false]]` (`_deleted` boolean NOT NULL), RePark
refuses `[_deleted]` because the check keys on the statement's tokens, not the
relation; J2 (`p._deleted, u._deleted`) and J3 (`u.id, u._deleted`) refuse
`[_deleted]` on both engines. The work order records that main refused J1 with
`[ICE-MC-1]` (`_deleted` unserved), so J1 is no regression; it is recorded as
residue candidate `R-MC-RESERVED-NAME-JOIN` and pinned, not fixed. RePark matched
every pinned row on the first run — no red commit, no production change.

**M-13 — mcdel-r5 mutation.** The provider's duplicate skip in
`append_metadata_fields` limited to `_file` → `reserved_name_near_misses_still_answer`
fails at the first other name, `Schema error: Schema contains duplicate qualified
field name __repark_mc_45._pos`; 28 pass. Four one-name variants (the skip removed
for exactly one of `_pos`, `_spec_id`, `_partition`, `_deleted`) each fail the same
test with the duplicate named for that column (`__repark_mc_45._pos`,
`__repark_mc_46._spec_id`, `__repark_mc_48._partition`,
`__repark_mc_49._deleted`); 28 pass each. All reverted.

## PROPOSITION LEDGER — U10-MC-DELETED-1 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `SELECT id, _deleted FROM t` on the recorded merge-on-read fixture answers `[(1,true),(2,false),(3,false),(4,false)]` and the `_deleted` field is non-null Boolean — the recorded `R-MC-DELETED` cell verbatim. | `deleted_column_marks_merge_on_read_deleted_row` (ordered under `ORDER BY id`) and `test_deleted_marks_merge_on_read_deleted_row` (sorted multiset, no `ORDER BY`) green. | **PROVEN** | Recorded cell replayed on both doors; Rust asserts `DataType::Boolean` + non-null, facade asserts `BooleanType`. pins: u10-mc-deleted-1/C-001 |
| C-002 | Not projecting `_deleted` keeps the delete filter on the same merge-on-read table: `SELECT id` = `[2,3,4]` and `count(*)` = 3. | `not_projecting_deleted_still_filters_mor_rows` and `test_not_projecting_deleted_still_filters` green. | **PROVEN** | The silent-wrong-answer pin; green before and after the change. pins: u10-mc-deleted-1/C-002 |
| C-003 | `SELECT * FROM t` on the merge-on-read table returns exactly the three user columns and the ordered rows `ORDER BY id` = `[(2,b,y),(3,c,x),(4,d,x)]` and `ORDER BY id DESC` = `[(4,d,x),(3,c,x),(2,b,y)]`; `_deleted` absent and no deleted row leaks. | `select_star_keeps_user_columns_on_mor_table` green — `field_names` plus unsorted `triples_i64` compared with `assert_eq!` for both orders. | **PROVEN** | mcdel-r2 V-002 added the rows; mcdel-r3 V-003: the helper no longer sorts and the DESC leg (Spark P8) discriminates order — M-9 (c). pins: u10-mc-deleted-1/C-003 |
| C-004 | A predicate on `_deleted` is re-applied above the scan (`Inexact` pushdown): `WHERE NOT _deleted` = `[(2,false),(3,false),(4,false)]`, `WHERE _deleted` = `[(1,true)]`; predicate-only legs match Spark S3–S6: `id WHERE _deleted` = `[1]`, `id WHERE NOT _deleted` = `[2,3,4]`, `id WHERE _deleted OR id > 0` = `[1,2,3,4]`, `count(*) WHERE _deleted IS NOT NULL` = `[3]` (the fold prunes `_deleted`). | `deleted_predicates_reapply_above_the_scan` green. | **PROVEN** | mcdel-r2 V-001: every leg asserts the full row list; the predicate-only legs prove `_deleted` reaches the scan even when it is not projected. pins: u10-mc-deleted-1/C-004 |
| C-005 | On the copy-on-write twin (no `write.delete.mode`), `SELECT id, _deleted` answers `[(2,false),(3,false),(4,false)]` — the deleted row stays filtered. | `deleted_column_on_copy_on_write_marks_all_rows_false` green. | **PROVEN** | No include-deleted verdict applies without a delete-file path; all live rows read `false`. pins: u10-mc-deleted-1/C-005 |
| C-006 | Unquoted `_DELETED` case-folds to `_deleted` and answers the merge-on-read cell `[(1,true),(2,false),(3,false),(4,false)]`. | `unquoted_upper_deleted_folds_to_served_name` green. | **PROVEN** | Fold parity with the other served names; mcdel-r2 split the quoted-mismatch leg into C-013. pins: u10-mc-deleted-1/C-006 |
| C-007 | A served `_spec_id` beside the newly-served `_deleted` answers Spark's rows: `SELECT id, _spec_id, _deleted … ORDER BY id` = `[(2,0,false),(3,0,false),(4,0,false)]` on the copy-on-write table and `[(1,0,true),(2,0,false),(3,0,false),(4,0,false)]` on the merge-on-read table; on each table the fields are `[id, _spec_id, _deleted]`, `_spec_id` Int32 and `_deleted` non-null Boolean. | `served_spec_id_and_deleted_answer_together` green. On each table (`ice.ns.t` copy-on-write, `ice.ns.m` merge-on-read) it asserts `field_names`, the unsorted `triples_i64_i32_bool` with `assert_eq!` (Spark P1, P2), `data_type` Int32 on field 1, `data_type` Boolean on field 2 and `!is_nullable()` on field 2. | **PROVEN** | mcdel-r3 V-001 replaced the row-count assertion with the full ordered rows (M-9 (a)); mcdel-r4 V-001 repeats the type and nullability asserts after the merge-on-read query (M-11 (a)). pins: u10-mc-deleted-1/C-007 |
| C-008 | A metadata column over a time-travel read keeps today's refusal — the full `[UNRESOLVED_COLUMN.WITH_SUGGESTION] … SQLSTATE: 42703` message, measured for `_file` and for the newly-served `_deleted`. | `metadata_column_over_time_travel_known_divergence` green. | **PROVEN** | KNOWN DIVERGENCE for the measured names `_file` and `_deleted` (RePark refuses every metadata column over time travel; Spark's answer for `_pos`, `_spec_id`, `_partition` is UNMEASURED ON SPARK): Spark answers S16/S17 rows (`_file LIKE '%.parquet'` = `[(2,true),(3,true),(4,true)]`; `_deleted` = `[(1,true),(2,false),(3,false),(4,false)]`) where RePark refuses; M-3 premise correction plus the r2 R2 ruling — kept out of scope, full message pinned. pins: u10-mc-deleted-1/C-008 |
| C-009 | `_deleted` through a subquery keeps the verdict: `id FROM (SELECT id, _deleted FROM t) s WHERE NOT s._deleted` = `[2,3,4]` (Spark S8) and the pruned `id FROM (SELECT id, _deleted AS d FROM t) s` = `[2,3,4]` (Spark S9 — deletes still filter). | `deleted_column_flows_through_subqueries` green. | **PROVEN** | mcdel-r2 V-001: nested and derived positions pin the full ordered row lists. pins: u10-mc-deleted-1/C-009 |
| C-010 | `_deleted` in expressions, ordering and grouping matches Spark S10–S13: `CASE WHEN _deleted THEN 'D' ELSE 'L' END` = `[(1,D),(2,L),(3,L),(4,L)]`; `sum(CAST(_deleted AS INT))` = `[1]`; `ORDER BY _deleted, id` = `[2,3,4,1]`; `GROUP BY _deleted` = `[(false,3),(true,1)]`. | `deleted_column_in_expressions_order_and_group` green. | **PROVEN** | mcdel-r2 V-001: ORDER BY alone brings the deleted row back, and the aggregate reads the true verdict. pins: u10-mc-deleted-1/C-010 |
| C-011 | A join's right-side `_deleted` reaches the right scan: `SELECT a.id, b.id FROM t a JOIN t b ON a.id = b.id + 1 WHERE b._deleted ORDER BY 1` = `[(2,1)]`, the projected `b._deleted` = `[(2,1,true),(3,2,false),(4,3,false)]` (non-null Boolean), `WHERE NOT b._deleted` = `[(3,2),(4,3)]`; and the equi self-join `ON a.id = b.id WHERE b._deleted` answers `[]` (Spark S14). | `deleted_column_on_join_right_side_reaches_its_scan` (P3/P4/P6, unsorted `assert_eq!` on the ordered rows) and `deleted_column_in_self_join_answers_empty` (S14) green. | **PROVEN** | mcdel-r3 V-002: S14 alone cannot tell whether the right scan served `_deleted`; M-9 (b) keeps S14 green and fails P3/P4. pins: u10-mc-deleted-1/C-011 |
| C-012 | The class sweep holds: every pin this PR adds or changes asserts the full row list — in order under `ORDER BY`, as a sorted multiset without it — or the full error string; no shape-only, count-only or sorted-under-`ORDER BY` pin remains. | The `metadata_columns_deleted.rs` row helpers do not sort; only `sorted(..)` wraps the two unordered legs of `served_spec_id_and_deleted_answer_together`; `served_names_fold_and_composed_shapes_refuse`'s two `[ICE-MC-1]` legs compare the full message and its backtick `` `_deleted` `` leg asserts the `false` values. | **PROVEN** | mcdel-r2 V-003 closed the refusal legs; mcdel-r3 closed the sorted-under-`ORDER BY` helpers and the count-only composed pin; sweep table in the mcdel-r3 hand-back. pins: u10-mc-deleted-1/C-012 |
| C-013 | Quoted `` `_DELETED` `` refuses the full `[UNRESOLVED_COLUMN.WITH_SUGGESTION] … SQLSTATE: 42703` message — and quoted `` `_FILE` `` refuses identically. | `quoted_upper_deleted_known_divergence` green. | **PROVEN** | KNOWN DIVERGENCE for `` `_DELETED` ``: Spark resolves the quoted upper name and answers S15 rows `[(1,true),(2,false),(3,false),(4,false)]` (field named `_DELETED`, `mcdq2-spark.log`). R3 measured on RePark that `` `_FILE` `` shares the refusal, so RePark's gap is not `_deleted`-specific; Spark's answer for quoted `` `_FILE` `` is UNMEASURED ON SPARK. pins: u10-mc-deleted-1/C-013 |
| C-014 | A query that names a served metadata column (`_file`, `_pos`, `_spec_id`, `_partition`, `_deleted`) which the table's own schema also carries refuses with the full text `Error during planning: Table column names conflict with names reserved for Iceberg metadata columns: [<names>]. Please, use ALTER TABLE statements to rename the conflicting table columns.` — Spark's message (M-10) after the planning prefix — and no `__repark_mc_` text reaches the user. On `(id BIGINT, _deleted STRING)`, copy-on-write and merge-on-read, `SELECT id, _deleted`, `SELECT id, _spec_id, _deleted` and `SELECT t._deleted FROM T t` refuse `[_deleted]` (M-10); each of the five names refuses `[<name>]` on its own table; the bracket lists the colliding names the query references in the table's declaration order, whatever the query order — `(id, _pos, _file)`: `SELECT id, _file, _pos` and `SELECT id, _pos, _file` refuse `[_pos, _file]`, `SELECT id, _file` refuses `[_file]`; `(id, _file, _pos)`: both query orders refuse `[_file, _pos]`; `(id, _spec_id, _deleted, _file)`: `SELECT id, _file, _deleted, _spec_id` refuses `[_spec_id, _deleted, _file]` and `SELECT id, _deleted, _file` refuses `[_deleted, _file]` — measured EQUAL on both engines (O1–O3, O6–O9, M-12). In a join, `p._deleted, u._deleted` and `u.id, u._deleted` over a table `uc` carrying `_deleted` refuse `[_deleted]` on both engines (J2, J3). | `user_deleted_column_collision_refuses_like_spark`, `every_served_metadata_name_collision_refuses` (iterates `METADATA_COLUMN_NAMES`, then the seven O-shapes), `reserved_name_collision_in_a_join` (J2, J3) and `test_user_column_named_deleted_refuses_like_spark` green, each refusal an `assert_eq!` on the full string. | **PROVEN** | mcdel-r4 V-003 (M-10, orchestrator ruling 2026-09-23 19:48); mcdel-r5 V-001 replaced the unmeasured "table-schema order" with the measured declaration-order rule (M-12). M-11 (b1)/(b2) red the refusals. The row-lineage names are served by `LineageColumnsTableProvider`, not this rewriter; this clause does not cover them. pins: u10-mc-deleted-1/C-014 |
| C-015 | KNOWN DIVERGENCE, residue `R-MC-RESERVED-NAME-SCAN`, limited to three measured shapes on a table whose schema carries a reserved metadata name: `SELECT * FROM T ORDER BY id` answers `[(2,u2),(3,u3)]` (fields `id, _deleted`) on copy-on-write and merge-on-read, and `[(1,u1),(2,u2),(3,u3)]` on `(id, _file STRING)`; the copy-on-write `DELETE FROM T WHERE id = 1` is served; `SELECT id FROM T WHERE _deleted = 'u2'` refuses with the C-014 text. Queries that name no colliding column answer and MATCH Spark: for each of the five names `c`, `(id, c STRING)` with row `(1,'u1')` answers `SELECT id, _spec_id` = `[(1,0)]` (fields `id, _spec_id`) — for `c = _spec_id`, `SELECT id, _file` answers one row, `id` 1, a path ending `.parquet` (N1–N5); `(id, _pos, _file)` answers `SELECT id, _spec_id` and `SELECT id` (O4, O5 shapes; three rows here, one in the probe); `(id, _file STRING)` answers `SELECT id, _deleted` = `[(1,false),(2,false),(3,false)]` (F1), `SELECT id, _pos` = `[(1,0),(2,1),(3,2)]` (F2) and `WHERE _spec_id = 0` = `[1,2,3]` (F4); a user column `deleted` beside `_deleted` answers `[(u1,true),(u2,false),(u3,false)]` on merge-on-read. | `user_deleted_column_collision_refuses_like_spark` (the `WHERE` leg plus `field_names` and `pairs_i64_str` of `SELECT *` on both tables, its `DELETE` seed served on both), `reserved_name_near_misses_still_answer` (the `deleted` leg, the loop over `METADATA_COLUMN_NAMES`, the O4/O5 shapes, F1/F2/F4 and the `SELECT *` row list) and the `SELECT *` leg of `test_user_column_named_deleted_refuses_like_spark` green. | **PROVEN** | KNOWN DIVERGENCE only where measured divergent: Spark refuses `SELECT *` with the C-014 text (`[_deleted]` M-10, `[_file]` F5 M-12) and the copy-on-write `DELETE` with the same text (M-10), and refuses the `WHERE` shape with `Invalid schema: multiple fields for name _deleted: 2 and 2147483644` (M-10). Spark serves the merge-on-read `DELETE` (M-10), like RePark. Every answering shape above is measured EQUAL (N1–N5, O4, O5, F1, F2, F4 in M-12). The user-`deleted` near miss is UNMEASURED ON SPARK and pinned as RePark's answer. Out of scope per the r4 ruling; the served values are pinned so the gap stays visible. pins: u10-mc-deleted-1/C-015 |
| C-016 | Residue candidate `R-MC-RESERVED-NAME-JOIN` (KNOWN DIVERGENCE): with `p (id, data)` merge-on-read after `DELETE id = 1` and `uc (id, _deleted STRING)`, `SELECT p.id, p._deleted FROM p JOIN uc u ON p.id = u.id ORDER BY p.id` refuses with the C-014 text `[_deleted]`, although only `uc` carries the user column and the query names `p._deleted`. | `reserved_name_collision_in_a_join` green, the J1 leg an `assert_eq!` on the full string. | **PROVEN** | KNOWN DIVERGENCE: Spark answers J1 `[(1,true),(2,false),(3,false)]` with `_deleted` boolean NOT NULL (M-12). The collision check keys on the statement's metadata tokens, not the relation qualifier; per the r5 work order, main refused J1 with `[ICE-MC-1]`, so there is no regression. Pinned so a later per-relation fix flips it deliberately; not fixed here (the 19:48 ruling's scope stands). pins: u10-mc-deleted-1/C-016 |

## Gates

| Command | Result |
|---|---|
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xo58-mcd origin/main HEAD` | exit 0 |
| `build-slot.sh cargo test -p repark-spark --lib metadata_columns` | exit 0 — 29 passed |
| `build-slot.sh cargo test -p repark-core --lib metadata_columns` | exit 0 — 9 passed |
| `build-slot.sh cargo test -p repark-iceberg --lib metadata_columns` | exit 0 — 0 tests (module compiles clean) |
| `build-slot.sh make rust-clippy` | exit 0 |
| `build-slot.sh make rust-panic-ban` | exit 0 |
| `build-slot.sh make check-rust-file-size` | exit 0 — 649 + 853 lines, both under the 1000 ceiling |
| `.venv/bin/python -m pytest -q python/repark/tests/test_ice_metadata_cols_1.py python/repark/tests/test_describe_table.py` | exit 0 — 23 passed, 1 skipped (native module rebuilt via `make develop`) |
| `bash scripts/check_map_md.sh --base origin/main` | exit 0 |
| `.venv/bin/python scripts/check_ledger_grammar.py` | exit 0 |

## COVERAGE_ATTESTATION

```
COVERAGE_ATTESTATION:
  pr_unit: u10-mc-deleted-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The answering clause replays the recorded cell on both doors — Rust pairs_i64_bool on the rendered batches, facade rows as a multiset plus the BooleanType field pin — and the composed clause asserts the full ordered (id, _spec_id, _deleted) rows on the copy-on-write and merge-on-read tables.
      artifacts: [crates/repark-spark/src/tests/metadata_columns_deleted.rs, python/repark/tests/test_ice_metadata_cols_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The seed is the recorded fixture itself — two appends plus one merge-on-read delete — and the COW twin re-runs the same inserts and delete without write.delete.mode; both pins sit mid-history after the DELETE commit.
      artifacts: [crates/repark-spark/src/tests/metadata_columns_deleted.rs, python/repark/tests/test_ice_metadata_cols_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The quoted-mismatch and time-travel legs pin the full planner message (class, sub-class, suggestion, SQLSTATE 42703) as KNOWN DIVERGENCEs; the composed refusal legs (non-query, wildcard-over-two-relations) compare the full five-name [ICE-MC-1] text; a served metadata name that collides with a user column refuses with Spark's full reserved-name text for all five names on both doors, and the near misses (a `deleted` column, an unreferenced `_file` column) still answer.
      artifacts: [crates/repark-spark/src/tests/metadata_columns_deleted.rs, crates/repark-spark/src/tests/metadata_columns.rs, crates/repark-core/src/metadata_columns.rs, python/repark/tests/test_ice_metadata_cols_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: Resolution runs per query through Session state; the provider registers one temp view per read with no shared mutable state — the same seam the lineage pins already use, unchanged here.
      artifacts: [crates/repark-core/src/metadata_columns.rs]
    - id: AT-5
      status: N/A
      justification: Read-only scans over the session's own catalog; no auth, secret, or injection surface added — the only new field is a Boolean constant the fork fills.
    - id: AT-6
      status: ATTACKED
      evidence: Every previously green behavior in scope is pinned unchanged at full strength: not-projected filtering, count(*), star user-columns AND ordered rows, COW answers, case folding, quoted unknown-column, and the time-travel unresolved-column error all hold on the new served set.
      artifacts: [crates/repark-spark/src/tests/metadata_columns_deleted.rs, crates/repark-spark/src/tests/metadata_columns.rs]
    - id: AT-7
      status: N/A
      justification: The scan streams through the existing provider and temp-view tail; the added field is one Boolean column per batch, no added materialization or hot-loop pattern.
    - id: AT-8
      status: ATTACKED
      evidence: The _deleted verdict is computed entirely by the pinned fork's include-deleted scan mode — RePark passes the projected name through unchanged; mutation (a) proves the pin dies if the verdict is synthesised.
      artifacts: [crates/repark-iceberg/src/catalog/metadata_columns.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The class of near-miss errors is pinned on the measured strings: the full [UNRESOLVED_COLUMN.WITH_SUGGESTION] text with SQLSTATE 42703 for the quoted-mismatch and time-travel legs, the full [ICE-MC-1] text for the non-query and wildcard-over-two-relations legs, and the full reserved-name collision text for every served name, which replaces the __repark_mc_ schema leak — no shape-only refusal pin remains.
      artifacts: [crates/repark-spark/src/tests/metadata_columns_deleted.rs, crates/repark-spark/src/tests/metadata_columns.rs]
    - id: AT-10
      status: ATTACKED
      evidence: One recorded cell replayed verbatim on both doors plus seventeen Rust pins in metadata_columns_deleted.rs, the strengthened fold/refusal legs in metadata_columns.rs, and three facade pins; every clause carries a pins: citation in the touched map.md rows and the facade docstrings; the schema leg asserts name, type and nullability.
      artifacts: [python/repark/tests/test_ice_metadata_cols_1.py, crates/repark-spark/src/tests/map.md, crates/repark-core/src/map.md, crates/repark-iceberg/src/catalog/map.md]
```

Every clause is PROVEN — the answering pins against the recorded PySpark 4.1.2
cell, the r2 S1–S14 table, the r3 P1–P8 table and the r4/r5 reserved-name probes, the near-miss and KNOWN DIVERGENCE pins against
measured pre-change behavior. No clause is
OPEN. Touched files per the Scope paragraph; the fork, `Cargo.toml`/`Cargo.lock`,
`STATUS.md`, `time_travel.rs`, the metadata-table paths, `describe_show.rs` and
the lineage columns are untouched. `make verify` and the full facade suite were
not run per the work order's gate list; the gates table above is the proof.
