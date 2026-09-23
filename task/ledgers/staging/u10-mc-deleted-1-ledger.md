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
`refuse` message names all five served columns. Tests in
`crates/repark-spark/src/tests/metadata_columns_deleted.rs` (the `_deleted`
cluster moved there when `metadata_columns.rs` reached the file-size ceiling)
and `python/repark/tests/test_ice_metadata_cols_1.py`; five `map.md` files; this
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
`VERSION AS OF` (S16/S17 rows) — the refusal is therefore a KNOWN DIVERGENCE,
pre-existing for every metadata column, not a pin of Spark behavior; the test
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
Every live-Spark answer in the r2 table was probed before pinning: S1
`[(1,true),(2,false),(3,false),(4,false)]`; S2 fields `id,data,cat` rows
`[(2,b,y),(3,c,x),(4,d,x)]`; S3 `[1]`; S4 `[2,3,4]`; S5 `[1,2,3,4]`; S6 `[3]`
(the `IS NOT NULL` leg prunes `_deleted`, matching Spark's fold); S7 `[3]`; S8
`[2,3,4]`; S9 `[2,3,4]` (pruned projection keeps the delete filter); S10
`[(1,D),(2,L),(3,L),(4,L)]`; S11 `[1]`; S12 `[2,3,4,1]`; S13
`[(false,3),(true,1)]`; S14 `[]`. All fourteen match Spark verbatim — no
production change was needed for V-001..V-003. S15 divergence check (R3):
quoted `` `_FILE` `` is refused with the same `[UNRESOLVED_COLUMN.WITH_SUGGESTION]`
as `` `_DELETED` ``, so the quoted-upper gap is pre-existing for all metadata
columns — kept refused, full message pinned, KNOWN DIVERGENCE. S16/S17 per R2:
KNOWN DIVERGENCE (pre-existing, all metadata columns), full message pinned.

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

## PROPOSITION LEDGER — U10-MC-DELETED-1 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `SELECT id, _deleted FROM t` on the recorded merge-on-read fixture answers `[(1,true),(2,false),(3,false),(4,false)]` and the `_deleted` field is non-null Boolean — the recorded `R-MC-DELETED` cell verbatim. | `deleted_column_marks_merge_on_read_deleted_row` and `test_deleted_marks_merge_on_read_deleted_row` green. | **PROVEN** | Recorded cell replayed on both doors; Rust asserts `DataType::Boolean` + non-null, facade asserts `BooleanType`. pins: u10-mc-deleted-1/C-001 |
| C-002 | Not projecting `_deleted` keeps the delete filter on the same merge-on-read table: `SELECT id` = `[2,3,4]` and `count(*)` = 3. | `not_projecting_deleted_still_filters_mor_rows` and `test_not_projecting_deleted_still_filters` green. | **PROVEN** | The silent-wrong-answer pin; green before and after the change. pins: u10-mc-deleted-1/C-002 |
| C-003 | `SELECT * FROM t` on the merge-on-read table returns exactly the three user columns and the ordered rows `[(2,b,y),(3,c,x),(4,d,x)]`; `_deleted` absent and no deleted row leaks. | `select_star_keeps_user_columns_on_mor_table` green. | **PROVEN** | mcdel-r2 V-002: the pin now asserts fields AND the full ordered triples (Spark S2), so a leaked deleted row fails. pins: u10-mc-deleted-1/C-003 |
| C-004 | A predicate on `_deleted` is re-applied above the scan (`Inexact` pushdown): `WHERE NOT _deleted` = `[(2,false),(3,false),(4,false)]`, `WHERE _deleted` = `[(1,true)]`; predicate-only legs match Spark S3–S6: `id WHERE _deleted` = `[1]`, `id WHERE NOT _deleted` = `[2,3,4]`, `id WHERE _deleted OR id > 0` = `[1,2,3,4]`, `count(*) WHERE _deleted IS NOT NULL` = `[3]` (the fold prunes `_deleted`). | `deleted_predicates_reapply_above_the_scan` green. | **PROVEN** | mcdel-r2 V-001: every leg asserts the full row list; the predicate-only legs prove `_deleted` reaches the scan even when it is not projected. pins: u10-mc-deleted-1/C-004 |
| C-005 | On the copy-on-write twin (no `write.delete.mode`), `SELECT id, _deleted` answers `[(2,false),(3,false),(4,false)]` — the deleted row stays filtered. | `deleted_column_on_copy_on_write_marks_all_rows_false` green. | **PROVEN** | No include-deleted verdict applies without a delete-file path; all live rows read `false`. pins: u10-mc-deleted-1/C-005 |
| C-006 | Unquoted `_DELETED` case-folds to `_deleted` and answers the merge-on-read cell `[(1,true),(2,false),(3,false),(4,false)]`. | `unquoted_upper_deleted_folds_to_served_name` green. | **PROVEN** | Fold parity with the other served names; mcdel-r2 split the quoted-mismatch leg into C-013. pins: u10-mc-deleted-1/C-006 |
| C-007 | A served `_spec_id` beside the newly-served `_deleted` answers instead of refusing: `SELECT id, _spec_id, _deleted` serves `[id, _spec_id, _deleted]` over the live rows. | `served_spec_id_and_deleted_answer_together` green. | **PROVEN** | The old composed-refusal pin flipped to a composed-answer pin. pins: u10-mc-deleted-1/C-007 |
| C-008 | A metadata column over a time-travel read keeps today's refusal — the full `[UNRESOLVED_COLUMN.WITH_SUGGESTION] … SQLSTATE: 42703` message, measured for `_file` and for the newly-served `_deleted`. | `metadata_column_over_time_travel_known_divergence` green. | **PROVEN** | KNOWN DIVERGENCE (pre-existing, all metadata columns): Spark answers S16/S17 rows (`_file LIKE '%.parquet'` = `[(2,true),(3,true),(4,true)]`; `_deleted` = `[(1,true),(2,false),(3,false),(4,false)]`) where RePark refuses; M-3 premise correction plus the r2 R2 ruling — kept out of scope, full message pinned. pins: u10-mc-deleted-1/C-008 |
| C-009 | `_deleted` through a subquery keeps the verdict: `id FROM (SELECT id, _deleted FROM t) s WHERE NOT s._deleted` = `[2,3,4]` (Spark S8) and the pruned `id FROM (SELECT id, _deleted AS d FROM t) s` = `[2,3,4]` (Spark S9 — deletes still filter). | `deleted_column_flows_through_subqueries` green. | **PROVEN** | mcdel-r2 V-001: nested and derived positions pin the full ordered row lists. pins: u10-mc-deleted-1/C-009 |
| C-010 | `_deleted` in expressions, ordering and grouping matches Spark S10–S13: `CASE WHEN _deleted THEN 'D' ELSE 'L' END` = `[(1,D),(2,L),(3,L),(4,L)]`; `sum(CAST(_deleted AS INT))` = `[1]`; `ORDER BY _deleted, id` = `[2,3,4,1]`; `GROUP BY _deleted` = `[(false,3),(true,1)]`. | `deleted_column_in_expressions_order_and_group` green. | **PROVEN** | mcdel-r2 V-001: ORDER BY alone brings the deleted row back, and the aggregate reads the true verdict. pins: u10-mc-deleted-1/C-010 |
| C-011 | `SELECT a.id FROM t a JOIN t b ON a.id = b.id WHERE b._deleted` answers `[]` (Spark S14). | `deleted_column_in_self_join_answers_empty` green. | **PROVEN** | mcdel-r2 V-001: the join's right-side `_deleted` predicate reaches the scan and yields no rows. pins: u10-mc-deleted-1/C-011 |
| C-012 | The class sweep holds: every pin this PR adds or changes asserts the full ordered row list (or sorted multiset) or the full error string — no shape-only pin remains. | `served_names_fold_and_composed_shapes_refuse`'s two `[ICE-MC-1]` legs now compare the full message; the backtick `` `_deleted` `` leg asserts the `false` row values. | **PROVEN** | mcdel-r2 V-003: refusal pins compare class + sub-class + suggestion + `SQLSTATE: 42703` (or the full `[ICE-MC-1]` text); sweep table is in the mcdel-r2 hand-back. pins: u10-mc-deleted-1/C-012 |
| C-013 | Quoted `` `_DELETED` `` refuses the full `[UNRESOLVED_COLUMN.WITH_SUGGESTION] … SQLSTATE: 42703` message — and quoted `` `_FILE` `` refuses identically. | `quoted_upper_deleted_known_divergence` green. | **PROVEN** | KNOWN DIVERGENCE (pre-existing, all metadata columns): Spark resolves the quoted upper name and answers S15 rows `[(1,true),(2,false),(3,false),(4,false)]` (field named `_DELETED`); R3 measured `` `_FILE` `` shares the refusal, so the gap is not `_deleted`-specific. pins: u10-mc-deleted-1/C-013 |

## Gates

| Command | Result |
|---|---|
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xo58-mcd origin/main HEAD` | exit 0 |
| `build-slot.sh cargo test -p repark-spark --lib metadata_columns` | exit 0 — 24 passed |
| `build-slot.sh cargo test -p repark-core --lib metadata_columns` | exit 0 — 0 tests (module compiles clean) |
| `build-slot.sh cargo test -p repark-iceberg --lib metadata_columns` | exit 0 — 0 tests (module compiles clean) |
| `build-slot.sh make rust-clippy` | exit 0 |
| `build-slot.sh make rust-panic-ban` | exit 0 |
| `build-slot.sh make check-rust-file-size` | exit 0 — 633 + 497 lines, both under the 1000 ceiling |
| `.venv/bin/python -m pytest -q python/repark/tests/test_ice_metadata_cols_1.py python/repark/tests/test_describe_table.py` | exit 0 — 22 passed, 1 skipped (native module rebuilt via `maturin develop`) |
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
      evidence: The answering clause replays the recorded cell on both doors — Rust pairs_i64_bool on the rendered batches, facade rows as a multiset plus the BooleanType field pin — and the composed clause reads _spec_id beside _deleted on the live rows.
      artifacts: [crates/repark-spark/src/tests/metadata_columns_deleted.rs, python/repark/tests/test_ice_metadata_cols_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The seed is the recorded fixture itself — two appends plus one merge-on-read delete — and the COW twin re-runs the same inserts and delete without write.delete.mode; both pins sit mid-history after the DELETE commit.
      artifacts: [crates/repark-spark/src/tests/metadata_columns_deleted.rs, python/repark/tests/test_ice_metadata_cols_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The quoted-mismatch and time-travel legs pin the full planner message (class, sub-class, suggestion, SQLSTATE 42703) as KNOWN DIVERGENCEs; the composed refusal legs (non-query, wildcard-over-two-relations) compare the full five-name [ICE-MC-1] text.
      artifacts: [crates/repark-spark/src/tests/metadata_columns_deleted.rs, crates/repark-spark/src/tests/metadata_columns.rs]
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
      evidence: The class of near-miss errors is pinned on the measured strings: the full [UNRESOLVED_COLUMN.WITH_SUGGESTION] text with SQLSTATE 42703 for the quoted-mismatch and time-travel legs, and the full [ICE-MC-1] text for the non-query and wildcard-over-two-relations legs — no shape-only refusal pin remains.
      artifacts: [crates/repark-spark/src/tests/metadata_columns_deleted.rs, crates/repark-spark/src/tests/metadata_columns.rs]
    - id: AT-10
      status: ATTACKED
      evidence: One recorded cell replayed verbatim on both doors plus twelve Rust pins in metadata_columns_deleted.rs, the strengthened fold/refusal legs in metadata_columns.rs, and two facade pins; every clause carries a pins: citation in the touched map.md rows and the facade docstrings; the schema leg asserts name, type and nullability.
      artifacts: [python/repark/tests/test_ice_metadata_cols_1.py, crates/repark-spark/src/tests/map.md, crates/repark-core/src/map.md, crates/repark-iceberg/src/catalog/map.md]
```

Every clause is PROVEN — the answering pins against the recorded PySpark 4.1.2
cell and the r2 S1–S14 table, the near-miss and KNOWN DIVERGENCE pins against
measured pre-change behavior. No clause is
OPEN. Touched files per the Scope paragraph; the fork, `Cargo.toml`/`Cargo.lock`,
`STATUS.md`, `time_travel.rs`, the metadata-table paths, `describe_show.rs` and
the lineage columns are untouched. `make verify` and the full facade suite were
not run per the work order's gate list; the gates table above is the proof.
