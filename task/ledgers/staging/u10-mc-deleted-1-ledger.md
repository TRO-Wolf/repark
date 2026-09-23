# Charter ledger — U10-MC-DELETED-1 · serve the `_deleted` metadata column (R-MC-DELETED)

**Date:** 2026-09-23 · **Branch:** `fix/u10-mc-deleted` · **Base:** `0002a7f2` (`origin/main`) · **Model:** Devin SWE-2 (swe-2-high) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
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
`crates/repark-spark/src/tests/metadata_columns.rs` and
`python/repark/tests/test_ice_metadata_cols_1.py`; five `map.md` files; this ledger.
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
("keep today's answer") holds verbatim.

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
are green on the same commit — they pin unchanged behavior.

**M-5 — mutations.** (a) Constant-false `_deleted`: `conform_batch` synthesised a
`BooleanArray` of `false` for the `_deleted` field instead of projecting the
fork's column → `deleted_column_marks_merge_on_read_deleted_row` fails with left
`[(1, false), (2, false), (3, false), (4, false)]`, and the three pins that read a
`true` verdict (`deleted_predicates_reapply_above_the_scan`,
`deleted_name_folds_unquoted_but_quoted_upper_stays_unknown`) fail too; 16 pass.
Reverted. (b) `_deleted` re-added to the refusal (`[ICE-MC-1] … not yet served`
ahead of the rewrite) → the same six legs fail as in M-4; 14 pass. Reverted.

## PROPOSITION LEDGER — U10-MC-DELETED-1 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `SELECT id, _deleted FROM t` on the recorded merge-on-read fixture answers `[(1,true),(2,false),(3,false),(4,false)]` and the `_deleted` field is non-null Boolean — the recorded `R-MC-DELETED` cell verbatim. | `deleted_column_marks_merge_on_read_deleted_row` and `test_deleted_marks_merge_on_read_deleted_row` green. | **PROVEN** | Recorded cell replayed on both doors; Rust asserts `DataType::Boolean` + non-null, facade asserts `BooleanType`. pins: u10-mc-deleted-1/C-001 |
| C-002 | Not projecting `_deleted` keeps the delete filter on the same merge-on-read table: `SELECT id` = `[2,3,4]` and `count(*)` = 3. | `not_projecting_deleted_still_filters_mor_rows` and `test_not_projecting_deleted_still_filters` green. | **PROVEN** | The silent-wrong-answer pin; green before and after the change. pins: u10-mc-deleted-1/C-002 |
| C-003 | `SELECT * FROM t` on the merge-on-read table returns exactly the three user columns; `_deleted` absent. | `select_star_keeps_user_columns_on_mor_table` green. | **PROVEN** | Load-bearing regression pin — `*` expands to user columns only on the new served set. pins: u10-mc-deleted-1/C-003 |
| C-004 | A predicate on `_deleted` is re-applied above the scan (`Inexact` pushdown): `WHERE NOT _deleted` = `[(2,false),(3,false),(4,false)]`, `WHERE _deleted` = `[(1,true)]`. | `deleted_predicates_reapply_above_the_scan` green. | **PROVEN** | RePark-internal consistency pin — UNMEASURED vs Spark, not claimed as an oracle cell. pins: u10-mc-deleted-1/C-004 |
| C-005 | On the copy-on-write twin (no `write.delete.mode`), `SELECT id, _deleted` answers `[(2,false),(3,false),(4,false)]` — the deleted row stays filtered. | `deleted_column_on_copy_on_write_marks_all_rows_false` green. | **PROVEN** | No include-deleted verdict applies without a delete-file path; all live rows read `false`. pins: u10-mc-deleted-1/C-005 |
| C-006 | Unquoted `_DELETED` case-folds to `_deleted` and answers the merge-on-read cell; quoted `` `_DELETED` `` keeps today's unknown-column error (`[UNRESOLVED_COLUMN]`). | `deleted_name_folds_unquoted_but_quoted_upper_stays_unknown` green. | **PROVEN** | Fold parity with the other served names; the quoted-mismatch leg never reaches the rewrite. pins: u10-mc-deleted-1/C-006 |
| C-007 | A served `_spec_id` beside the newly-served `_deleted` answers instead of refusing: `SELECT id, _spec_id, _deleted` serves `[id, _spec_id, _deleted]` over the live rows. | `served_spec_id_and_deleted_answer_together` green. | **PROVEN** | The old composed-refusal pin flipped to a composed-answer pin. pins: u10-mc-deleted-1/C-007 |
| C-008 | A metadata column over a time-travel read keeps today's error — the planner's `[UNRESOLVED_COLUMN]`, measured for `_file` and for the newly-served `_deleted`. | `metadata_column_over_time_travel_keeps_todays_error` green. | **PROVEN** | M-3 premise correction: the work order's `[ICE-MC-1]` claim is measured wrong; the pin holds today's real answer for both column states. pins: u10-mc-deleted-1/C-008 |

## Gates

| Command | Result |
|---|---|
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xo58-mcd origin/main HEAD` | exit 0 |
| `build-slot.sh cargo test -p repark-spark --lib metadata_columns` | exit 0 — 20 passed |
| `build-slot.sh cargo test -p repark-core --lib metadata_columns` | exit 0 — 0 tests (module compiles clean) |
| `build-slot.sh cargo test -p repark-iceberg --lib metadata_columns` | exit 0 — 0 tests (module compiles clean) |
| `build-slot.sh make rust-clippy` | exit 0 |
| `make rust-panic-ban` | exit 0 |
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
      artifacts: [crates/repark-spark/src/tests/metadata_columns.rs, python/repark/tests/test_ice_metadata_cols_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The seed is the recorded fixture itself — two appends plus one merge-on-read delete — and the COW twin re-runs the same inserts and delete without write.delete.mode; both pins sit mid-history after the DELETE commit.
      artifacts: [crates/repark-spark/src/tests/metadata_columns.rs, python/repark/tests/test_ice_metadata_cols_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The quoted-mismatch leg pins the planner's own unknown-column error and asserts no [ICE-MC-1] leak; the composed refusal message (DELETE / wildcard-over-two-relations legs, pre-existing pins) now names all five served columns.
      artifacts: [crates/repark-spark/src/tests/metadata_columns.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Resolution runs per query through Session state; the provider registers one temp view per read with no shared mutable state — the same seam the lineage pins already use, unchanged here.
      artifacts: [crates/repark-core/src/metadata_columns.rs]
    - id: AT-5
      status: N/A
      justification: Read-only scans over the session's own catalog; no auth, secret, or injection surface added — the only new field is a Boolean constant the fork fills.
    - id: AT-6
      status: ATTACKED
      evidence: Every previously green behavior in scope is pinned unchanged: not-projected filtering, count(*), star user-columns, COW answers, case folding, quoted unknown-column, and the time-travel unresolved-column error all hold on the new served set.
      artifacts: [crates/repark-spark/src/tests/metadata_columns.rs]
    - id: AT-7
      status: N/A
      justification: The scan streams through the existing provider and temp-view tail; the added field is one Boolean column per batch, no added materialization or hot-loop pattern.
    - id: AT-8
      status: ATTACKED
      evidence: The _deleted verdict is computed entirely by the pinned fork's include-deleted scan mode — RePark passes the projected name through unchanged; mutation (a) proves the pin dies if the verdict is synthesised.
      artifacts: [crates/repark-iceberg/src/catalog/metadata_columns.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The class of near-miss errors is pinned on the measured strings: [UNRESOLVED_COLUMN] naming the column for the quoted-mismatch and time-travel legs, and no [ICE-MC-1] where the claim would be wrong; the served-set refusal text (non-query, wildcard-over-two-relations) keeps its five-name enumeration.
      artifacts: [crates/repark-spark/src/tests/metadata_columns.rs]
    - id: AT-10
      status: ATTACKED
      evidence: One recorded cell replayed verbatim on both doors plus eight Rust pins and two facade pins; every clause carries a pins: citation in the touched map.md rows and the facade docstrings; the schema leg asserts name, type and nullability.
      artifacts: [python/repark/tests/test_ice_metadata_cols_1.py, crates/repark-spark/src/tests/map.md, crates/repark-core/src/map.md, crates/repark-iceberg/src/catalog/map.md]
```

Every clause is PROVEN — the answering pins against the recorded PySpark 4.1.2
cell, the near-miss pins against measured pre-change behavior. No clause is
OPEN. Touched files per the Scope paragraph; the fork, `Cargo.toml`/`Cargo.lock`,
`STATUS.md`, `time_travel.rs`, the metadata-table paths, `describe_show.rs` and
the lineage columns are untouched. `make verify` and the full facade suite were
not run per the work order's gate list; the gates table above is the proof.
