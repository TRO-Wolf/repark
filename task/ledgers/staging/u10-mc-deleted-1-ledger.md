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
`ice-metadata-cols-1` ledger (C-007, C-008, C-018, C-023). mcdel-r6 moves the
collision refusal out of the rewrite into `MetadataColumnsTableProvider::scan`
(`crates/repark-iceberg/src/catalog/metadata_columns.rs`), so it keys on the
columns the scan reads. mcdel-r8 makes the rewrite expand `x.*` and a bare `*` under a
FROM alias to the relation's user columns (`qualified_rewrite`, `sole_relation_alias`
in `crates/repark-core/src/metadata_columns.rs`); mcdel-r9 scopes every wildcard decision
to its own SELECT (`rewrite_for_relation`, `select_relations`, `written_qualifier`;
`by_alias` removed). Tests in
`crates/repark-spark/src/tests/metadata_columns_deleted.rs` (the `_deleted`
cluster moved there when `metadata_columns.rs` reached the file-size ceiling),
`crates/repark-spark/src/tests/metadata_columns_reserved.rs` (the reserved-name
collision cluster, moved there in mcdel-r6 ahead of the same ceiling; it shares the
`pub(super)` helpers of `metadata_columns_deleted.rs`),
`crates/repark-spark/src/tests/metadata_columns_scope.rs` (the mcdel-r9 wildcard-scope
pins), `crates/repark-spark/src/tests/input_file_name.rs` (the mcdel-r12 comma-join
refusal pin), the module entries in `crates/repark-spark/src/tests/mod.rs`, and
`python/repark/tests/test_ice_metadata_cols_1.py`; seven `map.md` files; this
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
listing the conflicting names. Built in mcdel-r4 as one check in
`prepare_metadata_column_sql` that matched the statement's canonical metadata
tokens against each table's user schema. mcdel-r6 moved it to the scan's read
columns (M-14), because token matching also refused aliases. The error is `DataFusionError::Plan`,
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
`mcdfile-spark.log` / `mcdfile-repark.log` (F1–F5, D1–D3). The bracket rule, the same
refusal text on both engines (the class differs: Spark raises `org.apache.iceberg.exceptions.ValidationException` through `Py4JJavaError`, RePark `AnalysisException`; residue
`R-MC-RESERVED-NAME-CLASS`): it lists the colliding names that the query references, in the
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
`[_deleted]` on both engines with the same text; the class differs
(`R-MC-RESERVED-NAME-CLASS`). The work order records that main refused J1 with
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

**M-14 — mcdel-r6 alias measurement (Sol critic r5 V-002).** Orchestrator probe
`/tmp/xo-xo-opus62/probe/mcdalias.py`, logs
`/tmp/xo-xo-opus62/probe/mcdalias-spark.log` and `mcdalias-repark.log` (Spark
4.1.2 / Iceberg 1.11 vs RePark `dae592de`, both rc 0). Tables (named `D`, `F`, `P` here; the
probe script's own names are in the logs): `D (id, _deleted STRING)` rows
`(1,'u1'),(2,'u2')`, `F (id, _file STRING)` rows `(1,'f1'),(2,'f2')`, `P (id, v
STRING)` rows `(1,'a'),(2,'b')`. Spark answers A1 `SELECT id AS _deleted
FROM D ORDER BY id` = `[[1],[2]]` (field `_deleted`), A2 `SELECT id AS _file FROM
F` = `[[1],[2]]`, A3 `SELECT id, 7 AS _pos FROM D` = `[[1,7],[2,7]]`, A4 `… ORDER BY
_deleted` = `[[1],[2]]`, A5 `SELECT _deleted.id FROM D AS _deleted` = `[[1],[2]]`
(field `id`), A6 `WITH _deleted AS (SELECT id FROM D) SELECT id FROM _deleted` =
`[[1],[2]]`, A7 `SELECT id AS _deleted FROM P` = `[[1],[2]]`, A8 `SELECT id, v AS _file
FROM P` = `[[1,'a'],[2,'b']]`, A10 `SELECT x AS _deleted FROM (SELECT id AS x FROM D)`
= `[[1],[2]]`; A9 `SELECT id, _deleted AS d FROM D` refuses with the reserved-name
text at `collect` (`SparkSchemaUtil.validateMetadataColumnReferences` from
`SparkScan.<init>`). RePark at `dae592de` refused A1, A2, A4, A5, A6, A10 because
the check matched SQL tokens. Spark's rule is the scan's read schema: it refuses
when the scan reads a user column whose name is a reserved metadata name. mcdel-r6
took the ruling's PRIMARY: `MetadataColumnsTableProvider::scan` collects the
projected user fields whose names are in `METADATA_COLUMN_NAMES` (a `None`
projection reads every field), sorted into the provider schema's field order, which
is the table's declaration order, and returns `DataFusionError::Plan` with the
unchanged text. `refuse_reserved_name_collision` moved to the iceberg crate, and the
core block is gone. `referenced_metadata_names` stays as routing only. The Rust
error string and the Python `AnalysisException` class and text are unchanged: every
existing refusal pin and `test_user_column_named_deleted_refuses_like_spark` stay
green. The error is raised from `scan`, which DataFusion calls during physical
planning. No pin asserts which step (`sql` or `collect`) raises, because
`plan_error` and the facade's `pytest.raises` block cover both.
The red commit `2749aecf` failed `reserved_word_outside_a_user_column_read_answers`
(A1 refused) and `reserved_name_collision_in_a_join` (J1 refused), both with
`Analysis("Error during planning: Table column names conflict … [_deleted] …")`.
After `ad2787be` all pins are green, and the full `repark-spark` lib suite passes
(1821 passed, 5 ignored). J1 now answers Spark's rows, closing residue candidate
`R-MC-RESERVED-NAME-JOIN` (C-016).

**M-15 — mcdel-r6 class-N probe.** `/tmp/xo58-mcd-r6probe/mcdclassn.py`, a copy of
`mcdalias.py` with rows added; logs `/tmp/xo58-mcd-r6probe/mcdclassn-spark.log`
(Spark 4.1.2 through `jvm-lock.sh`) and `mcdclassn-repark.log` (RePark on the r6
wheel), both rc 0. RePark on r6 answers A1–A10 as Spark does. B-rows on `D`:
B1 `GROUP BY 1` alias = `[[1,1],[2,1]]` EQUAL; B2 `SELECT id AS _deleted, count(*)
… GROUP BY _deleted` errors on both engines, because both resolve the GROUP BY name
to the user column: Spark `[MISSING_AGGREGATION] …`, RePark `Error during planning:
Column in SELECT must be in GROUP BY or an aggregate function: …` — DIVERGENT text,
not a collision refusal; B3 derived column-alias list `t(_deleted)` = `[[1],[2]]`
EQUAL; B4 backticked alias = `[[1],[2]]` EQUAL; B5 string literal `'_deleted'` =
`[[1,'_deleted'],[2,'_deleted']]` EQUAL; B6 `_deleted(id)` refuses
`[UNRESOLVED_ROUTINE] Cannot resolve routine `_deleted` …` with identical text,
EQUAL; B7 struct field `s._deleted` = `[[1],[2]]` EQUAL rows, but RePark names the
field `s[_deleted]` where Spark prints `_deleted`. That is general DataFusion
struct-access naming (`s.x` → `s[x]` on a plain table,
`/tmp/xo58-mcd-r6probe/b7name-repark.log`), not a metadata-column effect. B8 `SELECT
id AS _file FROM D` = `[[1],[2]]` EQUAL; B9 `SELECT _deleted FROM D AS t` refuses
`[_deleted]` on both engines with the same text, class differs
(`R-MC-RESERVED-NAME-CLASS`); B10 `count(*) … WHERE id > 0 AND _spec_id = 0` = `[[2]]` EQUAL.
Re-checks on r6: R1 `SELECT * FROM F` — Spark refuses `[_file]`, RePark answers
(unrouted, still divergent); R2 `WHERE _deleted = 'u2'` — Spark `Invalid schema:
multiple fields …`, RePark the reserved-name text (still divergent); R3 `SELECT *,
_spec_id FROM D` refuses `[_deleted]` on both with the same text, class differs
(`R-MC-RESERVED-NAME-CLASS`; routed, and the expanded star reads the user column); R4 the copy-on-write `DELETE FROM uc WHERE id = 1` — Spark
refuses, RePark serves (still divergent); J1 answers
`[[1,true],[2,false],[3,false]]` on both (EQUAL).

**M-16 — mcdel-r6 mutations.** (a) The scan check disabled (`if false && …`) →
`reserved_word_outside_a_user_column_read_answers` (A9),
`every_served_metadata_name_collision_refuses` (the five-name loop and the O-rows),
`reserved_name_collision_in_a_join` (J2/J3), `reserved_word_positions_follow_spark`
(B9/R3) and `user_deleted_column_collision_refuses_like_spark` fail at `plan_error`'s
`a refused query must fail`; 26 pass. Reverted. (b) The token check restored in
`prepare_metadata_column_sql` (refuse when the statement text names a colliding
reserved word) → `reserved_word_outside_a_user_column_read_answers`,
`reserved_name_collision_in_a_join` (J1) and `reserved_word_positions_follow_spark`
fail with `Analysis("Error during planning: Table column names conflict …")`; 28
pass. A per-row report under the same mutation (a temporary edit, reverted) shows A1,
A2, A4, A5, A6 and A10 each refuse and A7 answers. Both reverted.

**M-17 — mcdel-r7 join and query-position probe (Sol critic r6 V-003, class N).**
`/tmp/xo58-mcd-r7probe/mcdjoinpos.py`, a copy of `mcdalias.py` with rows added; logs
`/tmp/xo58-mcd-r7probe/mcdjoinpos-spark.log` (Spark 4.1.2 through `jvm-lock.sh`) and
`mcdjoinpos-repark.log` (RePark on a wheel rebuilt at `6079369e`), both rc 0. Tables
(labelled here): `D (id, _deleted STRING)` rows `(1,'u1'),(2,'u2')`, `D2` same schema
rows `(2,'u2'),(3,'u3')`, `P (id, v STRING)` rows `(1,'a'),(2,'b')`. Joins: J4 `SELECT
id FROM D JOIN D2 USING (_deleted)` — both engines raise `[AMBIGUOUS_REFERENCE]` for
`id`, with different candidate text (Spark three-part names plus `line 1 pos 7`, RePark
the rewrite aliases) — DIVERGENT text. J4B `SELECT D.id … USING (_deleted)` — Spark
`Invalid schema: multiple fields for name _deleted: 2 and 2147483644`, RePark the
reserved-name text `[_deleted]`: both refuse, DIVERGENT text (the M-10 `WHERE`
shape). J5 `SELECT id FROM D JOIN P USING (id)` = `[[1],[2]]` EQUAL. J6 `SELECT * FROM
D JOIN P USING (id)` — Spark refuses `[_deleted]`, RePark (unrouted) answers
`[[1,'u1','a'],[2,'u2','b']]` — DIVERGENT, the `SELECT *` shape of
`R-MC-RESERVED-NAME-SCAN`. J7 `SELECT id FROM D NATURAL JOIN D2` and J7B `SELECT D.id
… NATURAL JOIN` — Spark `Invalid schema: …`, RePark (unrouted) answers `[[2]]` —
DIVERGENT, unrouted. J8 `SELECT P.id FROM P JOIN D ON P.id = D.id` = `[[1],[2]]` EQUAL.
J9 `LEFT SEMI JOIN` = `[[1],[2]]` EQUAL (both parse it). J8M `SELECT P.id, D._spec_id
… ON` = `[[1,0],[2,0]]` EQUAL. J5M `SELECT id, _spec_id … USING (id)` — Spark
`[UNRESOLVED_COLUMN.WITH_SUGGESTION]` (metadata columns are not visible through a
`USING` join's output), RePark `[AMBIGUOUS_REFERENCE] Reference `_spec_id` is
ambiguous …` — DIVERGENT. J6M `SELECT *, D._spec_id … USING (id)` — Spark
`[UNRESOLVED_COLUMN.WITH_SUGGESTION]`, RePark the pre-existing `[ICE-MC-1] … over a
wildcard over more than one relation` — DIVERGENT. Positions reading `D._deleted`,
all refusing `[_deleted]` on both engines with the same text, the class differing
(`R-MC-RESERVED-NAME-CLASS`): N1 `GROUP BY _deleted`, N2
`HAVING max(_deleted)`, N3 `ORDER BY _deleted`, N5 window `PARTITION BY _deleted`, N6
window `ORDER BY _deleted`, N7 `IN (SELECT _deleted …)`, N9 scalar subquery
`max(_deleted)`, N10 `UNION ALL` branch. N4 `JOIN … ON D._deleted = P.v` and N8
`EXISTS (… WHERE D._deleted = 'u1')` refuse on both, Spark with `Invalid schema: …`,
so DIVERGENT text. N11 `LATERAL VIEW explode(array(_deleted))` — Spark refuses
`[_deleted]`, RePark `This feature is not implemented: LATERAL VIEWS` (a pre-existing
feature gap) — DIVERGENT. N12 `SELECT t.* FROM D t` — Spark refuses, RePark
(unrouted) answers `[[1,'u1'],[2,'u2']]` — DIVERGENT, unrouted. N13 `SELECT t.*,
t._spec_id FROM D t` — Spark refuses `[_deleted]`, RePark `Projections require unique
expression names …` — DIVERGENT (see M-18; since mcdel-r8 RePark refuses `[_deleted]`
with Spark's text but not its class, M-20). N14 (`WHERE`), N15 (`SELECT *`) and N16
(`SELECT *, _spec_id`) repeat R2, R1 and R3 (M-15). Positions not reading it (EQUAL):
N17 `IN (SELECT id FROM D)` = `[[1],[2]]`, N18 `UNION ALL` of ids = `[[1],[1],[2],[2]]`,
N19 `count(*) OVER (PARTITION BY id) … WHERE _spec_id = 0` = `[[1,1],[2,1]]`. N19's
field name is RePark's `count(*) PARTITION BY [ndel.id] ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING` against Spark's `count(1) OVER (PARTITION BY id ROWS BETWEEN
UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)` — a general window-naming divergence.
Every RePark answer is pinned as served; the Rust pins use the table names `ndel`,
`ndel2` and `pl`, so the pinned candidate lists name those.

**M-18 — mcdel-r7 qualified-wildcard finding (DIVERGENT, a silent wrong answer).**
Probes `/tmp/xo58-mcd-r7probe/mcdqualstar.py` and `mcdqualmor.py`, logs
`mcdqualstar-{spark,repark}.log` and `mcdqualmor-{spark,repark}.log`, all rc 0.
When a statement names any metadata column, a qualified wildcard `x.*` whose
qualifier is an alias different from the table's name is not expanded by the
rewrite to the user columns. DataFusion then expands it over every provider field,
including all five metadata columns. On the recorded merge-on-read seed, `SELECT x.*,
x._pos AS p FROM t x` (X1), `SELECT x.*, _spec_id AS s FROM t x` (X2) and `SELECT x.*
FROM t x WHERE x._spec_id = 0` (X3) serve fields `id, data, cat, _file, _pos, _spec_id,
_partition, _deleted` (plus `p` / `s`). Because `_deleted` is then projected, the fork's
include-deleted mode returns the deleted row 1 with `_deleted = true` — ids
`[1,2,3,4]`, where Spark answers user columns only and ids `[2,3,4]`. With the alias
equal to the table name (`t.*` over `t`) the rewrite expands correctly and matches
Spark (W1–W4). On a plain table the same expansion makes `SELECT t.*, t._spec_id FROM
P t` fail with `Projections require unique expression names …` (Q1, Q2; N13), where
Spark answers. The extra metadata columns in `x.*` are inferred, from the code
path, to predate this PR (main's provider already carried four); the deleted-row
resurrection needs `_deleted` in the provider and so is new with this PR. main was
not built or measured here. Per the r7 work order, no product code changed: X1–X3 and
N13 are pinned as RePark's current answer (C-018) and raised in the hand-back.
Fixed in mcdel-r8 (M-20, C-018 FIXED).

**M-19 — mcdel-r7 mutation.** The scan check limited to `projection.is_none()` (only
full-schema scans refuse) → seven tests fail, each at `plan_error`'s `a refused query
must fail` because a refusal became an answer:
`every_served_metadata_name_collision_refuses`, `reserved_name_join_positions` (at J4B,
its first reserved-name refusal), `reserved_name_collision_in_a_join`,
`reserved_name_query_positions`, `reserved_word_positions_follow_spark`,
`user_deleted_column_collision_refuses_like_spark`,
`reserved_word_outside_a_user_column_read_answers`; 28 pass. Reverted. The answer-only `reserved_name_query_positions_that_answer` stays green, as expected.

**M-20 — mcdel-r8 qualified-wildcard fix, measured.** Probe
`/tmp/xo58-mcd-r8probe/mcdqualfix.py` on the r7 seed (merge-on-read `t`, `P (id, v)`
rows `(1,'a'),(2,'b'),(3,'c')`, `D (id, _deleted STRING)`, a v3 table, and a
DataFrame temp view `tv (id, v)` rows `(2,'b'),(3,'c')`). Logs:
`/tmp/xo58-mcd-r8probe/mcdqualfix-spark.log` (Spark 4.1.2),
`/tmp/xo58-mcd-r8probe/mcdqualfix-repark-before.log` (RePark at `780d4f01`, the
rebased form of the pre-rebase `ee655264`) and
`/tmp/xo58-mcd-r8probe/mcdqualfix-repark.log` (RePark after the fix, wheel rebuilt).
Before the fix, X1–X3, K1 and P1 widened the wildcard to every provider field and
returned the deleted row 1; P2 widened `q.*` to `P`'s provider fields
(`mcdqualfix-repark-before.log`). B1/B2 (bare `*` under an alias) failed with
`UNRESOLVED_COLUMN` because the expansion qualified by the table name. Q1/Q2/N13
failed on the duplicate `_spec_id`. After the fix these rows are EQUAL with Spark:
X1–X3, B1, B2, K1, W1–W5, D1, C1, P1–P3 and Q1, Q2. N13 is not EQUAL: same refusal text,
class differs (Spark `ValidationException` via `Py4JJavaError`, RePark `AnalysisException`;
IPI-51), residue `R-MC-RESERVED-NAME-CLASS`. Since mcdel-r12 `reserved_name_query_positions`
asserts N13's full string and its class `ErrorClass::Analysis`, the class the Python door
maps to `AnalysisException`. M1 and M2 error on both engines with different text (pre-existing). The lineage
rewriter's `x.*` shapes over a v3 table (L2–L4) are EQUAL before and after — no alias
gap there. Mutation evidence, saved at the current code: `R4` in
`/tmp/xo58-mcd-r11probe/mutations-r11.json` restores the pre-r8 resolution (the qualifier matched
against the rewrites' own aliases only). It reds
`qualified_wildcard_under_another_alias_serves_user_columns` (first at X3),
`qualified_wildcard_near_misses_keep_their_answers` (first at M2),
`reserved_name_query_positions`, `wildcard_over_a_plain_relation_keeps_its_own_columns`
and both core legs (logs `/tmp/xo58-mcd-r11probe/logs/R4-*.log`). The r8 run's own counts
and per-row report were not saved and are not claimed. The red commit `e5c606a7` (the
rebased form of the pre-rebase `8573a684`) also removes a duplicate N11/N13 loop left
by mcdel-r7 and moves N12 into `reserved_name_query_positions_that_answer`, where C-015
already placed it.

**M-21 — mcdel-r9 wildcard scope (Sol critic r7 V-001), measured.** Probe
`/tmp/xo58-mcd-r9probe/mcdscope.py` on the r8 seed (merge-on-read `t`, a v3 table, and a
DataFrame temp view `tv (id, data, cat, extra)` rows `(2,'b','y','e2'),(3,'c','x','e3')`).
Logs: `/tmp/xo58-mcd-r9probe/mcdscope-spark.log` (Spark 4.1.2),
`/tmp/xo58-mcd-r9probe/mcdscope-repark-before.log` (RePark at `0863b0b4`, the rebased
form of the pre-rebase `66f7b647`) and `/tmp/xo58-mcd-r9probe/mcdscope-repark.log`
(after the fix, wheel rebuilt). Before the
fix, S1 (`*` over `tv t` beside an Iceberg `t` in an `EXISTS` subquery) and S4B (`t.*`
over a CTE `t`) served the Iceberg table's three columns and dropped `extra` (a silent
wrong answer). S3B (`t.*` over a derived table `t`) failed `UNRESOLVED_COLUMN`, and S6
(`*` over `tv t JOIN tv u`) refused `[ICE-MC-1]`. S2, S3, S3C, S4, S5, S5B, S7 and S8 were
already EQUAL. After the fix every S-row is EQUAL. The r8 probe re-run on the fixed
wheel (`/tmp/xo58-mcd-r9probe/mcdqualfix-repark-r9.log`) keeps every r8 row as it was, except M2's RePark
text: `Invalid qualifier t` now, where r8 printed the `UNRESOLVED_COLUMN` suggestion
list. Both still refuse, with a different text from Spark's
`[CANNOT_RESOLVE_STAR_EXPAND]`, so the pin is updated. Lineage (`lineage_columns.rs`,
untouched): the collision shapes L1–L3 refuse `[V3-ROWID-2] lineage projection over
subqueries / CTEs is not yet served` on RePark where Spark answers `tv`'s rows. That is
the pre-existing lineage refusal, not a silent leak. U1, a one-part `t` with a nested
`WITH t` in an `EXISTS` after `USE sc.db`, is EQUAL (`[[2,0],[3,0],[4,0]]`), so the
statement-global CTE-name check does not mis-scope that shape. Mutation evidence,
saved at the current code in `/tmp/xo58-mcd-r11probe/mutations-r11.json` (logs
`/tmp/xo58-mcd-r11probe/logs/R1-*.log` … `R3-*.log`). `R1` re-adds the alias fallback in
`sole_rewritten_relation` and reds `wildcard_over_a_plain_relation_keeps_its_own_columns`
(at S1) and the core leg `a_wildcard_resolves_only_against_its_own_select`. `R2` re-adds
it in `qualified_rewrite` and reds `wildcard_over_a_plain_relation_keeps_its_own_columns`
(at S2), `qualified_wildcard_near_misses_keep_their_answers` (at M2) and the core leg.
`R3` re-adds it in `select_touches_rewrite` and reds both scope tests and the core leg,
each with the `[ICE-MC-1]` wildcard refusal. The r9 run's own outputs were not saved and
are not claimed.

**M-22 — mcdel-r10/r11 unreachable-arm sweep (Sol critic r8 V-001, r9 V-001, class T).**
Every sentence here is backed by a file under `/tmp/xo58-mcd-r11probe/` or
`/tmp/xo58-mcd-r10probe/`. The script `/tmp/xo58-mcd-r11probe/mutate_all.py` applies
one mutation at a time to `crates/repark-core/src/metadata_columns.rs`, runs repark-core
`--lib metadata_columns`, repark-spark `--lib metadata_columns` and repark-spark
`--lib input_file_name` through the build slot, and restores the file with a `git diff
--quiet` check. Results are in `/tmp/xo58-mcd-r11probe/mutations-r11.json` (per id: the before/after strings,
each command, its exit code, the failing tests and the raw log
`/tmp/xo58-mcd-r11probe/logs/<id>-<suite>.log`); the digest is
`/tmp/xo58-mcd-r11probe/mutations-r11-summary.md`. `BASELINE` (no mutation) passes
11/38/19. Each live arm is mutated by one id and reds at least one test: `rewrite_for_relation`
(M01), `select_relations` (M02), `written_qualifier` (M03, M04), `qualified_rewrite`
(M05, M06, M07), `sole_relation_alias` (M08), the bare-`*` qualifier fallback (M09),
`sole_relation_qualifier` (M10, M11), `sole_rewritten_relation` (M12, M13),
`select_touches_rewrite` (M14), `expand_wildcards` (M15, M16, M17),
`rewrite_input_file_names` (M18–M21) and `sole_input_file_name_relation` (M22–M24). One
arm was a gap: `M22-before-fix` (`sole_input_file_name_relation`'s `from.len() != 1`
guard removed) redded nothing. `input_file_name()` over a comma join is the
`[UNRESOLVED_ROUTINE]` residue on RePark while Spark answers
(`/tmp/xo58-mcd-r11probe/mcdifncomma-{spark,repark}.log` I1): cell I1, residue
`R-MC-IFN-COMMA-JOIN` (Residues, item 3). The core leg
`input_file_name_over_a_comma_join_is_left_unresolved` asserts only the rewritten SQL
(`input_file_name()` left in place), not the refusal. mcdel-r12 adds the Spark-door leg
`input_file_name_over_a_v3_comma_join_is_an_unresolved_routine`
(`crates/repark-spark/src/tests/input_file_name.rs`): I1's SQL over a v3 table joined to
itself by comma, asserting `ErrorClass::Analysis` and the full string
`[UNRESOLVED_ROUTINE] Cannot resolve routine `input_file_name` on search path
[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. SQLSTATE: 42883; line 1
pos 7`. The r12 re-run of `M22` alone (`/tmp/xo58-mcd-r12probe/mutate_all.py`, the r11 script
with its output paths moved; results `/tmp/xo58-mcd-r12probe/mutations-r12.json`, logs
`/tmp/xo58-mcd-r12probe/logs/M22-*.log`) reds both legs:
`input_file_name_over_a_comma_join_is_left_unresolved` (core, 11 passed, 1 failed) and
`input_file_name_over_a_v3_comma_join_is_an_unresolved_routine` (input_file_name, 19
passed, 1 failed, at `refusal`'s `a refused query must fail`: the mutated rewrite answers
`true` on every row, Spark's answer). The metadata_columns suite stays green (38 passed).
Three arms that r9 carried are removed; `A1`–`A3` re-add each one at the
current code, and every suite stays green (12/38/19 pass, exit 0): the replacement-name
match in `rewrite_for_relation` (`A1`: `pre_visit_select` decides every wildcard before
`pre_visit_table_factor` renames that SELECT's relations), `written_qualifier`'s
Derived-alias arm (`A2`), and `qualified_rewrite`'s unaliased `entry.alias` branch
(`A3`). A mixed-case table routes only in its fully quoted form, where the two branches
yield the same quoted ident (`/tmp/xo58-mcd-r10probe/mcdcase-{spark,repark}.log` T2
EQUAL); the unquoted T1/T3 never route (a pre-existing divergence). The code without
the three arms passes the full repark-core lib (753 passed,
`/tmp/xo58-mcd-r11probe/logs/HEAD-repark-core-lib.log`), the full repark-spark lib (1891
passed, `HEAD-repark-spark-lib.log`) and the facade pytest (23 passed, 1 skipped,
`HEAD-pytest.log`). The repark probes are identical, row for row, to the r9 logs
(`/tmp/xo58-mcd-r11probe/probe-compare.txt`: mcdscope 18 rows, mcdqualfix 35 rows). The
`QualifiedWildcard` `Expr(_)` arm stays, because the match must be exhaustive: an
expression-qualified wildcard is a `ParseException` on both engines, with different text
(`/tmp/xo58-mcd-r10probe/mcdexprstar-{spark,repark}.log` E1, E2). Structural arms (`let
TableFactor::Table … else`, `_ => None`) are not mutated.

## Residues (declared)

1. **`R-MC-RESERVED-NAME-SCAN`** (KNOWN DIVERGENCE, C-015) — on a table whose schema
   carries a reserved metadata name, the shapes RePark never routes to the metadata-column
   scan (`SELECT *`, `SELECT t.*`, `SELECT *` over a `USING` join, `NATURAL JOIN`, the
   copy-on-write `DELETE`) answer where Spark refuses. The filter-bound reads (`WHERE`,
   `JOIN … ON`, `JOIN … USING (_deleted)`, `EXISTS`: M-10, R2, N4, N8, N14, J4B) refuse on
   both engines, with the reserved-name text on RePark and `Invalid schema: multiple fields
   for name _deleted: 2 and 2147483644` on Spark. The class differs there too, as in item 2.
2. **`R-MC-RESERVED-NAME-CLASS`** (KNOWN DIVERGENCE, IPI-51; cell N13 and every other
   reserved-name refusal) — same refusal text, class differs. Spark raises
   `org.apache.iceberg.exceptions.ValidationException` through `Py4JJavaError`
   (`SparkSchemaUtil.validateMetadataColumnReferences`). RePark raises `AnalysisException`
   (Rust `ErrorClass::Analysis`) with the same text after its `Error during planning: `
   prefix. Measured cells, each Spark row printing `Py4JJavaError …
   org.apache.iceberg.exceptions.ValidationException: Table column names conflict …`:
   - N13 `SELECT t.*, t._spec_id FROM D t` (`/tmp/xo58-mcd-r8probe/mcdqualfix-spark.log`,
     RePark `/tmp/xo58-mcd-r9probe/mcdqualfix-repark-r9.log`);
   - the M-10 shapes `SELECT id, _deleted`, `SELECT id, _spec_id, _deleted` and
     `SELECT t._deleted` on both tables (`/tmp/xo-xo-opus62/probe/mcdcol-spark.log`);
   - O1–O3, J2, J3 (`mcdjoin-spark.log`) and O6–O9 (`mcdorder-spark.log`);
   - A9, B9, R3 (`/tmp/xo58-mcd-r6probe/mcdclassn-spark.log`);
   - N1–N3, N5–N7, N9, N10, N16 (`/tmp/xo58-mcd-r7probe/mcdjoinpos-spark.log`).
   The RePark class of each is `AnalysisException` in its saved log. The exceptions are the
   M-10 shapes, whose saved RePark log predates the r4 refusal; the r4 facade pin holds
   their class. Pinned for class and full string: N13 in
   `reserved_name_query_positions` (`ErrorClass::Analysis`), and the M-10 `SELECT id,
   _deleted` shape in `test_user_column_named_deleted_refuses_like_spark`
   (`pytest.raises(AnalysisException)`). The other refusal legs assert the full string only.
   The `…_like_spark` test names refer to the text. The precedent is the class gap in
   `test_drop_table_purge_gc_disabled_refuses` (`python/repark/tests/test_ice_small_parser_1.py`).
3. **`R-MC-IFN-COMMA-JOIN`** (KNOWN DIVERGENCE, cell I1, C-020) — `SELECT input_file_name()
   LIKE '%.parquet' AS f FROM t a, t b`. Spark answers `[[true],[true],[true],[true]]` on
   a v2 table (`/tmp/xo58-mcd-r11probe/mcdifncomma-spark.log`) and on a v3 table
   (`/tmp/xo58-mcd-r12probe/mcdifncomma3-spark.log`, field `f` boolean NOT NULL). RePark raises
   `AnalysisException` `[UNRESOLVED_ROUTINE] Cannot resolve routine `input_file_name` on
   search path [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`].
   SQLSTATE: 42883; line 1 pos 7` (`/tmp/xo58-mcd-r11probe/mcdifncomma-repark.log`), the
   fall-through that registry row `ICE-MC-IFN-1` lists for a self join.
   `sole_input_file_name_relation` rewrites only a SELECT with one FROM item and no joins.

## PROPOSITION LEDGER — U10-MC-DELETED-1 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `SELECT id, _deleted FROM t` on the recorded merge-on-read fixture answers `[(1,true),(2,false),(3,false),(4,false)]` and the `_deleted` field is non-null Boolean — the recorded `R-MC-DELETED` cell verbatim. | `deleted_column_marks_merge_on_read_deleted_row` (ordered under `ORDER BY id`) and `test_deleted_marks_merge_on_read_deleted_row` (sorted multiset, no `ORDER BY`) green. | **PROVEN** | Recorded cell replayed on both doors; Rust asserts `DataType::Boolean` + non-null, facade asserts `BooleanType`. pins: u10-mc-deleted-1/C-001 |
| C-002 | Not projecting `_deleted` keeps the delete filter on the same merge-on-read table: `SELECT id` = `[2,3,4]` and `count(*)` = 3. | `not_projecting_deleted_still_filters_mor_rows` and `test_not_projecting_deleted_still_filters` green. | **PROVEN** | The silent-wrong-answer pin; green before and after the change. pins: u10-mc-deleted-1/C-002 |
| C-003 | `SELECT * FROM t` on the merge-on-read table returns exactly the three user columns and the ordered rows `ORDER BY id` = `[(2,b,y),(3,c,x),(4,d,x)]` and `ORDER BY id DESC` = `[(4,d,x),(3,c,x),(2,b,y)]`; `_deleted` absent and no deleted row leaks. | `select_star_keeps_user_columns_on_mor_table` green — `field_names` plus unsorted `triples_i64` compared with `assert_eq!` for both orders. | **PROVEN** | mcdel-r2 V-002 added the rows; mcdel-r3 V-003: the helper no longer sorts and the DESC leg (Spark P8) discriminates order — M-9 (c). pins: u10-mc-deleted-1/C-003 |
| C-004 | A predicate on `_deleted` is re-applied above the scan (`Inexact` pushdown): `WHERE NOT _deleted` = `[(2,false),(3,false),(4,false)]`, `WHERE _deleted` = `[(1,true)]`; predicate-only legs match Spark S3–S6: `id WHERE _deleted` = `[1]`, `id WHERE NOT _deleted` = `[2,3,4]`, `id WHERE _deleted OR id > 0` = `[1,2,3,4]`, `count(*) WHERE _deleted IS NOT NULL` = `[3]` (the fold prunes `_deleted`). | `deleted_predicates_reapply_above_the_scan` green. | **PROVEN** | mcdel-r2 V-001: every leg asserts the full row list; the predicate-only legs prove `_deleted` reaches the scan even when it is not projected. pins: u10-mc-deleted-1/C-004 |
| C-005 | On the copy-on-write twin (no `write.delete.mode`), `SELECT id, _deleted` answers `[(2,false),(3,false),(4,false)]` — the deleted row stays filtered. | `deleted_column_on_copy_on_write_marks_all_rows_false` green. | **PROVEN** | No include-deleted verdict applies without a delete-file path; all live rows read `false`. pins: u10-mc-deleted-1/C-005 |
| C-006 | Unquoted `_DELETED` case-folds to `_deleted` and answers the merge-on-read cell `[(1,true),(2,false),(3,false),(4,false)]`, the field named `_deleted`. | `unquoted_upper_deleted_folds_to_served_name` green (`field_names` = `[id, _deleted]` plus the unsorted rows). | **PROVEN** | Fold parity with the other served names; mcdel-r2 split the quoted-mismatch leg into C-013. Rows EQUAL to Spark (mcdq2); field-name KNOWN DIVERGENCE: Spark names the field `_DELETED` (mcdq2-spark.log), RePark folds it to `_deleted` — general unquoted-identifier folding, pinned as served. pins: u10-mc-deleted-1/C-006 |
| C-007 | A served `_spec_id` beside the newly-served `_deleted` answers Spark's rows: `SELECT id, _spec_id, _deleted … ORDER BY id` = `[(2,0,false),(3,0,false),(4,0,false)]` on the copy-on-write table and `[(1,0,true),(2,0,false),(3,0,false),(4,0,false)]` on the merge-on-read table; on each table the fields are `[id, _spec_id, _deleted]`, `_spec_id` Int32 and `_deleted` non-null Boolean. | `served_spec_id_and_deleted_answer_together` green. On each table (`ice.ns.t` copy-on-write, `ice.ns.m` merge-on-read) it asserts `field_names`, the unsorted `triples_i64_i32_bool` with `assert_eq!` (Spark P1, P2), `data_type` Int32 on field 1, `data_type` Boolean on field 2 and `!is_nullable()` on field 2. | **PROVEN** | mcdel-r3 V-001 replaced the row-count assertion with the full ordered rows (M-9 (a)); mcdel-r4 V-001 repeats the type and nullability asserts after the merge-on-read query (M-11 (a)). pins: u10-mc-deleted-1/C-007 |
| C-008 | A metadata column over a time-travel read keeps today's refusal — the full `[UNRESOLVED_COLUMN.WITH_SUGGESTION] … SQLSTATE: 42703` message, measured for `_file` and for the newly-served `_deleted`. | `metadata_column_over_time_travel_known_divergence` green. | **PROVEN** | KNOWN DIVERGENCE for the measured names `_file` and `_deleted` (RePark refuses every metadata column over time travel; Spark's answer for `_pos`, `_spec_id`, `_partition` is UNMEASURED ON SPARK): Spark answers S16/S17 rows (`_file LIKE '%.parquet'` = `[(2,true),(3,true),(4,true)]`; `_deleted` = `[(1,true),(2,false),(3,false),(4,false)]`) where RePark refuses; M-3 premise correction plus the r2 R2 ruling — kept out of scope, full message pinned. pins: u10-mc-deleted-1/C-008 |
| C-009 | `_deleted` through a subquery keeps the verdict: `id FROM (SELECT id, _deleted FROM t) s WHERE NOT s._deleted` = `[2,3,4]` (Spark S8) and the pruned `id FROM (SELECT id, _deleted AS d FROM t) s` = `[2,3,4]` (Spark S9 — deletes still filter). | `deleted_column_flows_through_subqueries` green. | **PROVEN** | mcdel-r2 V-001: nested and derived positions pin the full ordered row lists. pins: u10-mc-deleted-1/C-009 |
| C-010 | `_deleted` in expressions, ordering and grouping matches Spark S10–S13: `CASE WHEN _deleted THEN 'D' ELSE 'L' END` = `[(1,D),(2,L),(3,L),(4,L)]`; `sum(CAST(_deleted AS INT))` = `[1]`; `ORDER BY _deleted, id` = `[2,3,4,1]`; `GROUP BY _deleted` = `[(false,3),(true,1)]`. | `deleted_column_in_expressions_order_and_group` green. | **PROVEN** | mcdel-r2 V-001: ORDER BY alone brings the deleted row back, and the aggregate reads the true verdict. pins: u10-mc-deleted-1/C-010 |
| C-011 | A join's right-side `_deleted` reaches the right scan: `SELECT a.id, b.id FROM t a JOIN t b ON a.id = b.id + 1 WHERE b._deleted ORDER BY 1` = `[(2,1)]`, the projected `b._deleted` = `[(2,1,true),(3,2,false),(4,3,false)]` (non-null Boolean), `WHERE NOT b._deleted` = `[(3,2),(4,3)]`; and the equi self-join `ON a.id = b.id WHERE b._deleted` answers `[]` (Spark S14). | `deleted_column_on_join_right_side_reaches_its_scan` (P3/P4/P6, unsorted `assert_eq!` on the ordered rows) and `deleted_column_in_self_join_answers_empty` (S14) green. | **PROVEN** | mcdel-r3 V-002: S14 alone cannot tell whether the right scan served `_deleted`; M-9 (b) keeps S14 green and fails P3/P4. pins: u10-mc-deleted-1/C-011 |
| C-012 | The class sweep holds per leg. Every answer leg of every test this PR adds or changes asserts its field names, as RePark serves them, and its full row list: in order under `ORDER BY`, as a sorted multiset without it, with the id column wherever the query projects one. Every refusal leg asserts the full error string. The one exception is the three X legs of C-018, which assert field names, the id column and the `_deleted` column but not the `_file` paths. The `_file` value in the `_spec_id` near-miss leg equals, as a full string, the single data file listed from that table's warehouse directory. | Every `batches(..)` leg in `metadata_columns_deleted.rs`, `metadata_columns_reserved.rs` and the changed `served_names_fold_and_composed_shapes_refuse` (`metadata_columns.rs`) is followed by `assert_eq!(field_names(..), ..)`; the empty S14 leg reads names from the frame schema (`frame_field_names`). The row helpers do not sort; only `sorted(..)` wraps the two unordered legs of `served_spec_id_and_deleted_answer_together`. The backticked leg is `SELECT id, `_deleted` … ORDER BY id` = `[(2,false),(3,false),(4,false)]` and the alias leg `SELECT x.id, x._pos … ORDER BY id` = `[(2,0),(3,0),(4,0)]`. The facade legs assert `[f.name for f in schema.fields]`. `reserved_name_near_misses_still_answer` asserts `strs(&rows, 1) == parquet_files_under(<wh>/ns/n2)`. | **PROVEN** | mcdel-r2 V-003 closed the refusal legs; mcdel-r3 closed the sorted-under-`ORDER BY` helpers and the count-only composed pin; mcdel-r6 V-001 replaced the `.parquet`-suffix check with the full-path equality. mcdel-r7 V-001/V-002 added ids and full rows to the backticked `_deleted` and `x._pos` legs and field names to every answer leg (the class-F per-leg table is in the mcdel-r7 hand-back). Field names are RePark's, and where Spark's differ (measured: `count(*)` vs `count(1)`, `CASE WHEN t._deleted THEN Utf8("D") ELSE Utf8("L") END` vs `CASE WHEN _deleted THEN D ELSE L END`, `sum(t._deleted)` vs `sum(CAST(_deleted AS INT))` in mcdq1; the unquoted `_DELETED` fold in mcdq2; the N19 window name in M-17) that is a general naming divergence pinned as served. Spark's field names for unquoted `_POS` and backticked `_pos` are UNMEASURED ON SPARK. pins: u10-mc-deleted-1/C-012 |
| C-013 | Quoted `` `_DELETED` `` refuses the full `[UNRESOLVED_COLUMN.WITH_SUGGESTION] … SQLSTATE: 42703` message — and quoted `` `_FILE` `` refuses identically. | `quoted_upper_deleted_known_divergence` green. | **PROVEN** | KNOWN DIVERGENCE for `` `_DELETED` ``: Spark resolves the quoted upper name and answers S15 rows `[(1,true),(2,false),(3,false),(4,false)]` (field named `_DELETED`, `mcdq2-spark.log`). R3 measured on RePark that `` `_FILE` `` shares the refusal, so RePark's gap is not `_deleted`-specific; Spark's answer for quoted `` `_FILE` `` is UNMEASURED ON SPARK. pins: u10-mc-deleted-1/C-013 |
| C-014 | A query whose scan reads a user column named like a served metadata column (`_file`, `_pos`, `_spec_id`, `_partition`, `_deleted`) refuses with the full text `Error during planning: Table column names conflict with names reserved for Iceberg metadata columns: [<names>]. Please, use ALTER TABLE statements to rename the conflicting table columns.` — Spark's message (M-10) after the planning prefix — and no `__repark_mc_` text reaches the user. On `(id BIGINT, _deleted STRING)`, copy-on-write and merge-on-read, `SELECT id, _deleted`, `SELECT id, _spec_id, _deleted`, `SELECT t._deleted FROM T t` and `WHERE _deleted = 'u2'` refuse `[_deleted]` (M-10); `SELECT id, _deleted AS d` (A9), `SELECT _deleted FROM T AS t` (B9) and `SELECT *, _spec_id` (R3) refuse `[_deleted]`; each of the five names refuses `[<name>]` on its own table. The bracket lists the colliding user columns the scan reads, in the table's declaration order, whatever the query order — `(id, _pos, _file)`: `SELECT id, _file, _pos` and `SELECT id, _pos, _file` refuse `[_pos, _file]`, `SELECT id, _file` refuses `[_file]`; `(id, _file, _pos)`: both query orders refuse `[_file, _pos]`; `(id, _spec_id, _deleted, _file)`: `SELECT id, _file, _deleted, _spec_id` refuses `[_spec_id, _deleted, _file]` and `SELECT id, _deleted, _file` refuses `[_deleted, _file]` — measured with the same text on both engines (O1–O3, O6–O9, M-12). In a join, `p._deleted, u._deleted` and `u.id, u._deleted` over a table `uc` carrying `_deleted` refuse `[_deleted]` on both engines (J2, J3). Each position that reads the user column refuses `[_deleted]` on both engines: `GROUP BY` (N1), `HAVING` (N2), `ORDER BY` (N3), window `PARTITION BY` (N5), window `ORDER BY` (N6), an `IN`-subquery select item (N7), a scalar subquery (N9) and a `UNION ALL` branch (N10), each with the same text on both engines (M-17); `SELECT t.*, t._spec_id FROM D t` (N13) refuses `[_deleted]` too (M-20). `JOIN … USING (_deleted)` with a qualified select (J4B), a `JOIN … ON D._deleted = …` (N4) and an `EXISTS` subquery filter (N8) refuse with this text on RePark, where Spark refuses with `Invalid schema: multiple fields for name _deleted: 2 and 2147483644` — DIVERGENT text, the M-10 `WHERE` shape. The Python door raises `AnalysisException` with the same text; the pin puts `sql` and `collect` inside one `pytest.raises` block, so it does not assert which step raises. The class is not Spark's: on every refusal above Spark raises `ValidationException` through `Py4JJavaError` and RePark `AnalysisException` (residue `R-MC-RESERVED-NAME-CLASS`, IPI-51). | `user_deleted_column_collision_refuses_like_spark`, `every_served_metadata_name_collision_refuses` (iterates `METADATA_COLUMN_NAMES`, then the seven O-shapes), `reserved_name_collision_in_a_join` (J2, J3), `reserved_word_outside_a_user_column_read_answers` (A9), `reserved_word_positions_follow_spark` (B9, R3), `reserved_name_join_positions` (J4B), `reserved_name_query_positions` (N1–N10, N13), all in `metadata_columns_reserved.rs`, and `test_user_column_named_deleted_refuses_like_spark` green, each refusal an `assert_eq!` on the full string. The N13 leg also asserts `ErrorClass::Analysis` (`refusal` maps a `collect` error through `repark_core::engine_err`, as the Python binding does), and the facade pin asserts `AnalysisException`. | **PROVEN** | mcdel-r7 V-003 measured the join and query positions (M-17); M-19 reds them. mcdel-r4 V-003 (M-10, orchestrator ruling 2026-09-23 19:48); mcdel-r5 V-001 measured the declaration-order rule (M-12); mcdel-r6 V-002 moved the check to the scan's read columns (M-14), the measured Spark rule. M-16 (a) reds the refusals. The row-lineage names are served by `LineageColumnsTableProvider`, not this provider; this clause does not cover them. pins: u10-mc-deleted-1/C-014 |
| C-015 | KNOWN DIVERGENCE, residue `R-MC-RESERVED-NAME-SCAN`, limited to three measured shapes on a table whose schema carries a reserved metadata name, all outside the metadata-column scan. `SELECT * FROM T ORDER BY id` with no metadata name in the statement is not routed and answers `[(2,u2),(3,u3)]` (fields `id, _deleted`) on copy-on-write and merge-on-read, and `[(1,u1),(2,u2),(3,u3)]` on `(id, _file STRING)`. The same unrouted shape covers `SELECT * FROM D JOIN P USING (id)` = `[(1,u1,a),(2,u2,b)]`, fields `id, _deleted, v` (J6), `SELECT t.* FROM D t` = `[(1,u1),(2,u2)]`, fields `id, _deleted` (N12), and `NATURAL JOIN` of two such tables = `[2]`, field `id`, for `SELECT id` (J7) and `SELECT D.id` (J7B). The copy-on-write `DELETE FROM T WHERE id = 1` is served. `SELECT id FROM T WHERE _deleted = 'u2'` refuses with the C-014 text. Queries whose scans read no colliding user column answer and MATCH Spark: for each of the five names `c`, `(id, c STRING)` with row `(1,'u1')` answers `SELECT id, _spec_id` = `[(1,0)]` (fields `id, _spec_id`); for `c = _spec_id`, `SELECT id, _file` answers id 1 with the table's one data file path (N1–N5). `(id, _pos, _file)` answers `SELECT id, _spec_id` and `SELECT id` (O4, O5 shapes; three rows here, one in the probe). `(id, _file STRING)` answers `SELECT id, _deleted` = `[(1,false),(2,false),(3,false)]` (F1), `SELECT id, _pos` = `[(1,0),(2,1),(3,2)]` (F2) and `WHERE _spec_id = 0` = `[1,2,3]` (F4). A user column `deleted` beside `_deleted` answers `[(u1,true),(u2,false),(u3,false)]` on merge-on-read. | `user_deleted_column_collision_refuses_like_spark` (the `WHERE` leg plus `field_names` and `pairs_i64_str` of `SELECT *` on both tables; its `DELETE` seed is served on both), `reserved_name_near_misses_still_answer` (the `deleted` leg, the loop over `METADATA_COLUMN_NAMES`, the O4/O5 shapes, F1/F2/F4 and the `SELECT *` row list) the J6/J7/J7B legs of `reserved_name_join_positions`, the N12 leg of `reserved_name_query_positions_that_answer` and the `SELECT *` leg of `test_user_column_named_deleted_refuses_like_spark` green. | **PROVEN** | KNOWN DIVERGENCE only where measured divergent and re-checked on r6 (M-15 R1, R2, R4), plus J6 and N12 (Spark refuses `[_deleted]`) and J7/J7B (Spark `Invalid schema: …`) in M-17: Spark refuses `SELECT *` with the C-014 text (`[_deleted]` M-10, `[_file]` F5 M-12 / R1) and the copy-on-write `DELETE` with the same text (M-10, R4); it refuses the `WHERE` shape with `Invalid schema: multiple fields for name _deleted: 2 and 2147483644` (M-10, R2). Spark serves the merge-on-read `DELETE` (M-10), like RePark. Every answering shape above is measured EQUAL (N1–N5, O4, O5, F1, F2, F4 in M-12). The user-`deleted` near miss is UNMEASURED ON SPARK and pinned as RePark's answer. A `SELECT *` that also names a metadata column is routed, reads the user column and refuses on both engines (R3, C-014). pins: u10-mc-deleted-1/C-015 |
| C-016 | A join whose scans read no colliding user column answers like Spark. With `p (id, data)` merge-on-read after `DELETE id = 1` and `uc (id, _deleted STRING)`, `SELECT p.id, p._deleted FROM p JOIN uc u ON p.id = u.id ORDER BY p.id` answers `[(1,true),(2,false),(3,false)]`, fields `id, _deleted`, `_deleted` non-null Boolean: the `uc` scan reads only `id`. | `reserved_name_collision_in_a_join` green — the J1 leg asserts `field_names`, the unsorted `pairs_i64_bool` rows, `data_type` Boolean and `!is_nullable()` on field 1. | **PROVEN** | J1 measured on Spark: `[[1,True],[2,False],[3,False]]`, `_deleted` boolean NOT NULL (M-12; re-measured EQUAL on r6, M-15). The residue candidate `R-MC-RESERVED-NAME-JOIN` recorded in mcdel-r5 is closed by the mcdel-r6 scan-level check (M-14); `R5` in `/tmp/xo58-mcd-r11probe/mutations-r11.json` (the pre-r6 token check restored) reds `reserved_name_collision_in_a_join` at this J1 leg (`/tmp/xo58-mcd-r11probe/logs/R5-spark-metadata_columns.log`). pins: u10-mc-deleted-1/C-016 |
| C-017 | A reserved metadata name outside a read of the table's user column is not treated as one. On `(id BIGINT, _deleted STRING)` / `(id BIGINT, _file STRING)` / `(id BIGINT, v STRING)` with two rows, these answer `[1, 2]` under the named field: select-item alias (A1 `_deleted`, A2 `_file`, A7 on the plain table, B8 `_file` on the `_deleted` table), the alias referenced in `ORDER BY` (A4), a table alias (A5, field `id`), a CTE name (A6, field `id`), a derived-table output alias (A10), a derived column-alias list `t(_deleted)` (B3) and a backticked alias (B4). A literal alias `7 AS _pos` answers `[(1,7),(2,7)]` (A3); `v AS _file` on the plain table answers `[(1,a),(2,b)]` (A8); `GROUP BY 1` over the alias answers `[(1,1),(2,1)]` (B1); a string literal `'_deleted'` answers `[(1,_deleted),(2,_deleted)]` (B5); a struct field `s._deleted` answers `[1, 2]` under the field name `s[_deleted]` (B7); `count(*) … WHERE id > 0 AND _spec_id = 0` answers `[2]` under the field name `count(*)` (B10). Positions whose scans do not read the user column answer: `JOIN P USING (id)` selecting `id` = `[1, 2]` (J5), `JOIN … ON` = `[1, 2]` (J8), `LEFT SEMI JOIN` = `[1, 2]` (J9), all field `id`; `SELECT P.id, D._spec_id … ON` = `[(1,0),(2,0)]`, fields `id, _spec_id` (J8M); `IN (SELECT id FROM D)` = `[1, 2]` (N17); `UNION ALL` of ids = `[1, 1, 2, 2]` (N18); `count(*) OVER (PARTITION BY id) … WHERE _spec_id = 0` = `[(1,1),(2,1)]` under the fields `id` and `count(*) PARTITION BY [ndel.id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` (N19). RePark's errors pinned as served: `JOIN D2 USING (_deleted)` selecting bare `id` → `Error during planning: [AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: [`ndel`.`id`, `ndel2`.`id`]. SQLSTATE: 42704` (J4); `SELECT id, _spec_id … USING (id)` → `… Reference `_spec_id` is ambiguous, could be: [`ndel`.`_spec_id`, `pl`.`_spec_id`] …` (J5M); `SELECT *, D._spec_id … USING (id)` → the `[ICE-MC-1] … over a wildcard over more than one relation` text (J6M); `LATERAL VIEW explode(array(_deleted))` → `This feature is not implemented: LATERAL VIEWS` (N11). An unknown call `_deleted(id)` refuses `[UNRESOLVED_ROUTINE] Cannot resolve routine `_deleted` on search path […]. SQLSTATE: 42883; line 1 pos 7` (B6). `GROUP BY _deleted` over the alias refuses `Error during planning: Column in SELECT must be in GROUP BY or an aggregate function: …` (B2). | `reserved_word_outside_a_user_column_read_answers` (A1–A8, A10), `reserved_word_positions_follow_spark` (B1–B8, B10), `reserved_name_join_positions` (J4, J5, J5M, J6M, J8, J8M, J9) and `reserved_name_query_positions` (N11) and `reserved_name_query_positions_that_answer` (N17–N19) green, each an `assert_eq!` on `field_names` and the full row list or on the full error string. | **PROVEN** | Measured on both engines (M-14, M-15). EQUAL rows: A1–A8, A10, B1, B3–B6, B8, B10 and the B7 rows. KNOWN DIVERGENCE by row id, pinned as RePark's current answer: B2, where both engines error because both resolve `GROUP BY _deleted` to the user column, but Spark's text is `[MISSING_AGGREGATION] …` (DataFusion's aggregate-validation text, not a collision); and B7's field name, `s[_deleted]` against Spark's `_deleted`, which is general DataFusion struct-access naming, also seen for `s.x` on a plain table (M-15). B10's field name `count(*)` against Spark's `count(1)` (mcdalias B10, mcdclassn B10), a general aggregate-naming divergence. From M-17: J5, J8, J9, J8M, N17, N18 EQUAL, and N19 with EQUAL rows and a divergent window field name. J4 errors on both engines with different candidate text. J5M and J6M: Spark `[UNRESOLVED_COLUMN.WITH_SUGGESTION]`, RePark as pinned. N11: Spark refuses `[_deleted]`, RePark reports the LATERAL VIEW gap. N13 (`SELECT t.*, t._spec_id FROM D t`) refuses `[_deleted]` on both engines since mcdel-r8, with the same text but a different class: Spark `ValidationException` via `Py4JJavaError`, RePark `AnalysisException` (IPI-51, residue `R-MC-RESERVED-NAME-CLASS`; C-014, M-20). `R5` in `/tmp/xo58-mcd-r11probe/mutations-r11.json` (the pre-r6 token check restored) reds `reserved_word_outside_a_user_column_read_answers` and `reserved_word_positions_follow_spark` (`/tmp/xo58-mcd-r11probe/logs/R5-spark-metadata_columns.log`). pins: u10-mc-deleted-1/C-017 |
| C-018 | FIXED, EQUAL with Spark (mcdel-r8) on the shapes pinned below: in a statement that names a metadata column, a qualified wildcard whose qualifier names a rewritten Iceberg relation of the same SELECT — by its FROM alias or, unaliased, by its table name — expands to that relation's user columns, and a bare `*` over one aliased rewritten relation qualifies by that alias. On the recorded merge-on-read seed, `SELECT x.* FROM t x WHERE x._spec_id = 0` (X3), `SELECT * FROM t x WHERE x._spec_id = 0` (B1), `SELECT X.* …` (K1), `SELECT t.* FROM t t …` (W2) and `SELECT t.* FROM t t` (W4) answer fields `id, data, cat` and rows `[(2,b,y),(3,c,x),(4,d,x)]`. `SELECT x.*, x._pos AS p FROM t x` (X1), `SELECT *, x._pos AS p FROM t x` (B2), `SELECT t.*, t._pos AS p FROM t t` (W1) and the unaliased `FROM t` form (W5) add `p` = `[0, 0, 1]`. `SELECT x.*, _spec_id AS s FROM t x` (X2) and `t.*` over `t t` (W3) add `s` = `[0, 0, 0]`. On `P (id, v)` with rows `(1,a),(2,b),(3,c)`, `SELECT t.*, t._spec_id FROM P t` (Q1) and `SELECT t.*, _spec_id FROM P t` (Q2) answer fields `id, v, _spec_id` and rows `[(1,a,0),(2,b,0),(3,c,0)]`. Near misses keep their answers: a derived table `x` (D1) and a CTE `x` (C1) answer `[2, 3, 4]` under the field `id`; `x.*, q.v` over `t x JOIN P q` (P1) answers fields `id, data, cat, v` and rows `[(2,b,y,b),(3,c,x,c)]`; `q.*, x.id AS xid` over the rewritten `P q` (P2) and over the non-Iceberg view `tv q` (P3) answer fields `id, v, xid` and rows `[(2,b,2),(3,c,3)]`. A qualifier naming no relation refuses `Error during planning: Invalid qualifier y` (M1), and the table name used while the table is aliased (`SELECT t.* FROM t x …`) refuses `Error during planning: Invalid qualifier t` (M2; the pre-r9 text was the `UNRESOLVED_COLUMN` suggestion list naming the metadata fields, M-21). | `qualified_wildcard_under_another_alias_serves_user_columns` (X1–X3, B1, B2, K1, W1–W5, Q1, Q2) and `qualified_wildcard_near_misses_keep_their_answers` (D1, C1, P1–P3, M1, M2) in `metadata_columns_reserved.rs` green. Every answer leg asserts `field_names` and the full rows (`triples_i64` plus the fourth column), and every refusal leg asserts the full string. The core leg `a_qualified_wildcard_under_a_from_alias_expands_to_user_columns` (`crates/repark-core/src/metadata_columns.rs`) asserts the full rewritten SQL for `x.*`, `X.*`, the bare `*` under an alias, `t.*` over `t t`, `t.*` unaliased, a rewritten join partner `q.*`, a non-rewritten partner `q.*`, a derived table, a CTE and an unknown qualifier. | **PROVEN** | mcdel-r8 fixed it under the orchestrator ruling (2026-09-24 00:05, Q1 lean adopted): `RewriteMetadataColumns::qualified_rewrite` resolves the qualifier against this SELECT's FROM relations (joins included) only — the statement-global `by_alias` fallback it had in r8 was removed in mcdel-r9 (C-019); `sole_relation_alias` qualifies the bare `*`. Measured EQUAL on both engines (M-20, `/tmp/xo58-mcd-r8probe/mcdqualfix-{spark,repark}.log`). Before the fix it was a silent wrong answer — the deleted row returned (M-18) — and B1/B2 failed with `UNRESOLVED_COLUMN`. M1/M2 error on both engines with different text (Spark `[CANNOT_RESOLVE_STAR_EXPAND] …`), pre-existing, pinned as served. `R4` in `/tmp/xo58-mcd-r11probe/mutations-r11.json` (the pre-r8 resolution restored) reds `qualified_wildcard_under_another_alias_serves_user_columns` (first at X3), `qualified_wildcard_near_misses_keep_their_answers` (first at M2), `reserved_name_query_positions` and both core legs. The residue candidate `R-MC-QUALIFIED-WILDCARD` is withdrawn. pins: u10-mc-deleted-1/C-018 |
| C-019 | FIXED, EQUAL with Spark (mcdel-r9): a wildcard is expanded only when a relation in its own SELECT's FROM or JOIN list is itself a rewrite target — a `TableFactor::Table` whose name is the rewrite's original (the wildcard is decided in `pre_visit_select`, before `pre_visit_table_factor` renames that SELECT's relations, so the original name is the only one that can be seen; the replacement-name match r9 also carried was unreachable and was removed in mcdel-r10, M-22). A plain relation, CTE or derived table that merely shares the Iceberg table's alias in another scope keeps its own columns, and the Iceberg relation keeps its user columns when a plain namesake sits in a subquery. With `tv (id, data, cat, extra)` rows `(2,b,y,e2),(3,c,x,e3)` and the merge-on-read `t`: `SELECT * FROM tv t WHERE EXISTS (SELECT 1 FROM t WHERE _spec_id = 0)` (S1), `SELECT t.* FROM tv t …` (S2), `WITH t AS (SELECT id, data, cat, extra FROM tv) SELECT * FROM t …` (S4) and `… SELECT t.* FROM t …` (S4B) answer fields `id, data, cat, extra` and rows `[(2,b,y,e2),(3,c,x,e3)]`; `SELECT x.* FROM (SELECT id, data, 'e' AS extra FROM tv) x WHERE EXISTS (SELECT 1 FROM t x WHERE x._spec_id = 0)` (S3), the same derived table aliased `t` under `t.*` (S3B) and under `*` (S3C) answer fields `id, data, extra` and rows `[(2,b,e),(3,c,e)]`; `SELECT * FROM tv t JOIN tv u ON t.id = u.id WHERE EXISTS (…)` (S6) answers eight fields `id, data, cat, extra, id, data, cat, extra` and both rows; `SELECT * FROM t t WHERE t._spec_id = 0 AND EXISTS (SELECT * FROM tv t WHERE t.id = 2)` (S5) and its `t.*` form (S5B) answer fields `id, data, cat` and rows `[(2,b,y),(3,c,x),(4,d,x)]` — no deleted row. | `wildcard_over_a_plain_relation_keeps_its_own_columns` (S1 `crates/repark-spark/src/tests/metadata_columns_scope.rs:57`, S2 :61, S4 :65, S4B :72, S3 :90, S3B :96, S3C :103, S6 :131) and `wildcard_over_the_iceberg_relation_ignores_a_plain_namesake` (S5 :148, S5B :153, the near misses S7 :171 and S8 :175) green, each leg asserting `field_names` and the full rows. The core leg `a_wildcard_resolves_only_against_its_own_select` (`crates/repark-core/src/metadata_columns.rs:935`) asserts the full rewritten SQL for five plain-relation shapes (left untouched) and the Iceberg-outer shape. | **PROVEN** | Sol critic r7 V-001, ruling tick 53. Before the fix, S1 and S4B dropped `extra` (a silent wrong answer), S3B failed `UNRESOLVED_COLUMN`, and S6 refused `[ICE-MC-1] … wildcard over more than one relation`: `sole_rewritten_relation`, `qualified_rewrite` and `select_touches_rewrite` fell back to the statement-global `by_alias` (M-21). The fix is `rewrite_for_relation` plus `select_relations`/`written_qualifier`, and `by_alias` is gone. Measured EQUAL on both engines (M-21, `/tmp/xo58-mcd-r9probe/mcdscope-{spark,repark-before,repark}.log`), and every r8 row stays EQUAL (`/tmp/xo58-mcd-r9probe/mcdqualfix-repark-r9.log`). In `/tmp/xo58-mcd-r11probe/mutations-r11.json`, `R1`, `R2` and `R3` re-add the r9 alias fallback in `sole_rewritten_relation`, `qualified_rewrite` and `select_touches_rewrite`. They red S1, then S2 and M2, then both scope tests with `[ICE-MC-1]`, and each also reds the core leg. pins: u10-mc-deleted-1/C-019 |
| C-020 | KNOWN DIVERGENCE, residue `R-MC-IFN-COMMA-JOIN`: `input_file_name()` over a comma join of an Iceberg table with itself is not rewritten and refuses. On a format-v3 table `ice.ns.t3 (id BIGINT, data STRING)` with rows `(1,a),(2,b)`, `SELECT input_file_name() LIKE '%.parquet' AS f FROM ice.ns.t3 a, ice.ns.t3 b` raises class `ErrorClass::Analysis` (`AnalysisException` at the Python door) with the full text `[UNRESOLVED_ROUTINE] Cannot resolve routine `input_file_name` on search path [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. SQLSTATE: 42883; line 1 pos 7`. | `input_file_name_over_a_v3_comma_join_is_an_unresolved_routine` (`crates/repark-spark/src/tests/input_file_name.rs`) green, one `assert_eq!` on the (class, full string) pair; the core leg `input_file_name_over_a_comma_join_is_left_unresolved` (`crates/repark-core/src/metadata_columns.rs`) asserts the rewritten SQL keeps `input_file_name()`. | **PROVEN** | Sol critic r10 V-002. Cell I1 (M-22): Spark answers `[[true],[true],[true],[true]]` on a v2 table (`/tmp/xo58-mcd-r11probe/mcdifncomma-spark.log`) and on a v3 table (`/tmp/xo58-mcd-r12probe/mcdifncomma3-spark.log`); the RePark text and class equal `/tmp/xo58-mcd-r11probe/mcdifncomma-repark.log` I1. `M22` in `/tmp/xo58-mcd-r12probe/mutations-r12.json` (the `from.len() != 1` guard removed) reds both pins (M-22). pins: u10-mc-deleted-1/C-020 |

## Gates

mcdel-r12 run, logs under `/tmp/xo58-mcd-r12probe/logs/gate-*.log`.

| Command | Result |
|---|---|
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xo58-mcd origin/main HEAD` | exit 0 — hits=0 |
| `build-slot.sh cargo test -p repark-spark --lib metadata_columns` | exit 0 — 38 passed |
| `build-slot.sh cargo test -p repark-spark --lib input_file_name` | exit 0 — 20 passed |
| `build-slot.sh cargo test -p repark-core --lib metadata_columns` | exit 0 — 12 passed |
| `build-slot.sh cargo test -p repark-core --lib lineage_columns` | exit 0 — 0 tests (`lineage_columns.rs` carries no unit tests) |
| `build-slot.sh make rust-clippy` | exit 0 |
| `build-slot.sh make rust-panic-ban` | exit 0 |
| `build-slot.sh make check-rust-file-size` | exit 0 — 991 (`repark-core` `metadata_columns.rs`, unchanged) + 665 + 689 + 934 + 181 + 648 lines (`metadata_columns.rs`, `metadata_columns_deleted.rs`, `metadata_columns_reserved.rs`, `metadata_columns_scope.rs`, `input_file_name.rs` under `repark-spark/src/tests`), all under the 1000 ceiling |
| `build-slot.sh make develop` then `.venv/bin/python -m pytest -q python/repark/tests/test_ice_metadata_cols_1.py` | exit 0 — 15 passed |
| `bash scripts/check_map_md.sh --base origin/main` | exit 0 |
| `python3 scripts/check_ledger_grammar.py` | exit 0 |

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
      evidence: The quoted-mismatch and time-travel legs pin the full planner message (class, sub-class, suggestion, SQLSTATE 42703) as KNOWN DIVERGENCEs; the composed refusal legs (non-query, wildcard-over-two-relations) compare the full five-name [ICE-MC-1] text; a served metadata name that collides with a user column refuses with Spark's full reserved-name text for all five names on both doors (Spark's class is ValidationException, RePark's AnalysisException: residue R-MC-RESERVED-NAME-CLASS; the N13 leg and the facade pin assert RePark's class), and the near misses (a `deleted` column, an unread colliding column, a reserved word as an alias, CTE name, table alias, literal or struct field) still answer; the check keys on the columns the scan reads.
      artifacts: [crates/repark-spark/src/tests/metadata_columns_deleted.rs, crates/repark-spark/src/tests/metadata_columns_reserved.rs, crates/repark-spark/src/tests/metadata_columns.rs, crates/repark-iceberg/src/catalog/metadata_columns.rs, python/repark/tests/test_ice_metadata_cols_1.py]
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
      artifacts: [crates/repark-spark/src/tests/metadata_columns_deleted.rs, crates/repark-spark/src/tests/metadata_columns_reserved.rs, crates/repark-spark/src/tests/metadata_columns.rs]
    - id: AT-10
      status: ATTACKED
      evidence: One recorded cell replayed verbatim on both doors plus thirteen Rust pins in metadata_columns_deleted.rs, eleven in metadata_columns_reserved.rs, two in metadata_columns_scope.rs, one in input_file_name.rs (the comma-join class and text), the strengthened fold/refusal legs in metadata_columns.rs, and three facade pins; every clause carries a pins: citation in the touched map.md rows and the facade docstrings; the schema leg asserts name, type and nullability.
      artifacts: [python/repark/tests/test_ice_metadata_cols_1.py, crates/repark-spark/src/tests/map.md, crates/repark-core/src/map.md, crates/repark-iceberg/src/catalog/map.md, crates/repark-spark/src/tests/input_file_name.rs]
```

Every clause is PROVEN — the answering pins against the recorded PySpark 4.1.2
cell, the r2 S1–S14 table, the r3 P1–P8 table and the r4/r5/r6/r7 reserved-name probes, the near-miss and KNOWN DIVERGENCE pins against
measured pre-change behavior. No clause is
OPEN. Touched files per the Scope paragraph; the fork, `Cargo.toml`/`Cargo.lock`,
`STATUS.md`, `time_travel.rs`, the metadata-table paths, `describe_show.rs` and
the lineage columns are untouched. `make verify` and the full facade suite were
not run per the work order's gate list; the gates table above is the proof.
