# Unit ledger — SILVER-S0 · contract and storage feasibility

**Date:** 2026-09-12 · **Branch:** `docs/silver-s0` · **Base:** `d3a20d53`
**Model:** grok-4.6 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** READING. **risk_tier: standard.**
**Reading path:** this is a READING unit under R-10 — no RePark product code.
Evidence is fork pin `3ebf7d36` (`fork/<path>:<line>`), RePark source at this
branch's base, and probe runs on `fork/` branch `probe/silver-s0`.

**Why now.** Epic
[deterministic-silver-layer-compiler-2026-09-12.md](../../roadmap/epic-term/deterministic-silver-layer-compiler-2026-09-12.md)
§17 S-0: settle MVP types and prove stage / validate / conditional-commit can
be implemented in the correct owner. The ten SIL decisions of §18 stay open
for the owner; this ledger files recommendations, never rulings.

**Retires:** this ledger moves to `../completed/` when the unit's last commit
lands.

**Not in this unit:** product code in RePark or the fork; `STATUS.md`;
`briefs/next-sequence.md`; `.github/`; dependency files; Docker; JVM; network
catalogs; `aws`; push.

## PROPOSITION LEDGER — SILVER-S0 — 2026-09-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | At pin `3ebf7d36`, `Transaction` exposes the listed actions; snapshot-producing `commit` emits `UuidMatch` plus `RefSnapshotIdMatch` for the target branch against the *refreshed* table; a caller cannot supply the expected-base snapshot as the CAS requirement independently of refresh. `validate_from_snapshot` only pins the conflict-validation walk. | Read `fork/crates/iceberg/src/transaction/mod.rs`, `snapshot.rs`, `overwrite_files.rs`; probe emitted requirements. | **PROVEN** | Factory methods: `upgrade_table_version`, `update_table_properties`, `fast_append`, `merge_append`, `delete_files`, `overwrite_files`, `replace_partitions`, `rewrite_files`, `rewrite_manifests`, `row_delta`, `replace_sort_order`, `manage_snapshots`, `expire_snapshots`, `cherry_pick`, `update_partition_spec`, `update_schema`, `update_location`, `update_statistics`, `update_partition_statistics` (`fork/crates/iceberg/src/transaction/mod.rs:212-374`). `Transaction::new` captures `starting_snapshot_id` from the loaded table (`mod.rs:158-171`). `do_commit` always `load_table`s and rebases when the pointer moved (`mod.rs:512-527`). Snapshot producer requirements: `UuidMatch { uuid: table.metadata().uuid() }` plus, unless `stage_only`, `RefSnapshotIdMatch { ref: target_branch, snapshot_id: current_snapshot_id() or named-ref head }` computed from the table the action ran against — the refreshed base (`fork/crates/iceberg/src/transaction/snapshot.rs:1540-1556`). `OverwriteFilesAction::validate_from_snapshot` documents that it does not enable validation and only sets the walk start (`overwrite_files.rs:260-266`). Update-properties emits no requirements (`update_properties.rs:94-103`). Probe `fast_append_commit_emits_uuid_and_ref_snapshot_requirements` (`fork/crates/iceberg/src/transaction/silver_s0_probe.rs:401`) asserts both requirement variants equal the loaded table's UUID and current snapshot id. |
| C-002 | Data files can be written (parquet + `DataFileWriter`) with no catalog commit; the table's current snapshot stays unchanged; the returned `DataFile`s can be handed to a later commit. | PROBE on the memory catalog. | **PROVEN** | Writer `close()` returns `DataFile`s with no catalog call (`fork/crates/iceberg/src/writer/map.md`; `writer/mod.rs:89-90`). RePark already stages then commits (`crates/repark-iceberg/src/write/overwrite.rs:57-79`). Probe `parquet_data_file_write_does_not_advance_snapshot_and_later_commit_reuses_files` (`silver_s0_probe.rs:137`): write one parquet via `DataFileWriter` on a fresh memory-catalog table; `load_table` current snapshot id equals the pre-write id (`None`); `file_path` ends with `.parquet` and `record_count==1`; a later `fast_append` of those same `DataFile`s advances the snapshot and summary `added-data-files=1`. |
| C-003 | When another commit advances the table between stage and commit, the fork refreshes and rebases by default (a silent rebase of full-table overwrite, violating epic §12 if used as the silver publication). `validate_no_conflicting_data` refuses with non-retryable `DataInvalid`. `commit.retry.num-retries=0` does not disable the first-attempt rebase. Both orders (concurrent then stage; stage then concurrent) rebase unless validation is armed. | PROBE both orders. | **PROVEN** | Source: `do_commit` rebases then re-applies (`mod.rs:524-527, 546-554`); retry gate is `e.retryable() && kind != CommitStateUnknown` (`mod.rs:409`); default `commit.retry.num-retries` is 4 (`fork/crates/iceberg/src/spec/table_properties.rs:112-114`). Overwrite conflict validation is opt-in and non-retryable (`overwrite_files.rs:231-238, 418-422`). Probe `overwrite_without_conflict_validation_rebases_over_concurrent_append` (`silver_s0_probe.rs:163`): concurrent append then overwrite-by-alwaysTrue without validation commits `Operation::Overwrite` with `deleted-data-files=2` (base + concurrent) and `added-data-files=1` — the concurrent writer's file is gone. Probe `overwrite_with_validate_no_conflicting_data_refuses_concurrent_append` (`silver_s0_probe.rs:213`): same setup with `.validate_no_conflicting_data()` returns `ErrorKind::DataInvalid`, `retryable()==false`, and the catalog head stays the concurrent snapshot. Probe `concurrent_append_before_staging_then_overwrite_without_validation_still_rebases` (`silver_s0_probe.rs:249`): the other order, `deleted-data-files=2`. Probe `zero_commit_retries_still_rebases_overwrite_over_concurrent_append` (`silver_s0_probe.rs:285`): table property `commit.retry.num-retries=0`, still `deleted-data-files=2`. |
| C-004 | A run key can be written into the same snapshot's summary properties via `set_snapshot_properties` on the snapshot-producing action; after commit the snapshot is findable from table metadata alone by walking `metadata.snapshots()`. Table properties can share the same `Transaction`. | PROBE: commit with a marker, find the snapshot by marker from metadata. | **PROVEN** | Source: `OverwriteFilesAction::set_snapshot_properties` (`overwrite_files.rs:225-228`); producer copies them into snapshot summary (`append.rs:229-241`). Multiple actions on one `Transaction` (`action.rs:101-106`). Probe `run_key_in_snapshot_summary_is_findable_from_table_metadata` (`silver_s0_probe.rs:327`): overwrite commit with `repark.silver.run-key=plan-abc/bronze-snap-1`; `snapshot_id_for_run_key` over `metadata.snapshots()` equals `current_snapshot_id` on the committed table and again after `load_table`. |
| C-005 | A later process resolves "did run K publish" by walking `metadata.snapshots()` summary properties (or `history()` / refs). `expire_snapshots` is metadata-only and removes expired snapshots, so a summary-only marker disappears; that bounds SIL-6 windows. | Source plus PROBE expire of a marked snapshot. | **PROVEN** | Source: `TableMetadata::snapshots` / `snapshot_by_id` / `history` / `snapshot_for_ref` (`fork/crates/iceberg/src/spec/table_metadata.rs:312-358`). `ExpireSnapshotsAction` never deletes files; it emits `RemoveSnapshots` (`expire_snapshots.rs:18-26, 110-116`). Probe `expire_snapshot_id_drops_run_key_from_metadata` (`silver_s0_probe.rs:356`): after a marked overwrite and a successor append, `expire_snapshot_id(marked_id)` commits; the run key is gone from every remaining snapshot, `snapshot_by_id(marked_id)` is `None`, and `current_snapshot_id` is still the successor. SIL-6 consequence: a retry/replay window longer than snapshot retention cannot resolve an ambiguous commit from metadata alone. |
| C-006 | Commit-outcome taxonomy at the pin, per catalog RePark uses: conflict = `CatalogCommitConflicts` (retryable); requirement/validation failure = `DataInvalid` (non-retryable); lost response = `CommitStateUnknown` (never auto-retried; absent-after-refresh is not failure). Ambiguous commit is distinguishable from a refused one. REST noted. | Read `ErrorKind`, Memory/Glue/S3 Tables/REST mapping, `commit_status.rs`. | **PROVEN** | `ErrorKind` (`fork/crates/iceberg/src/error.rs:30-100`): `CatalogCommitConflicts`, `CommitStateUnknown`, `DataInvalid`, `PreconditionFailed`, `Unexpected`, `FeatureUnsupported`, plus existence kinds. Memory CAS: retryable `CatalogCommitConflicts` (`fork/crates/iceberg/src/catalog/memory/catalog.rs:254-261`). Glue: `ConcurrentModificationException` → retryable `CatalogCommitConflicts`; timeout / 5xx / maybe-sent transport → `CommitStateUnknown`; never-sent → `Unexpected`; `EntityNotFoundException` → `TableNotFound` (`fork/crates/catalog/glue/src/commit_transport.rs:287-347`). S3 Tables: the same split (`fork/crates/catalog/s3tables/src/commit_transport.rs:303-344`). REST: 409 → `CatalogCommitConflicts`; 500/502/503/504 → `CommitStateUnknown` (`fork/crates/catalog/rest/src/catalog.rs:1165-1220`). `Transaction::commit` never retries `CommitStateUnknown`; it reconciles by searching attempted snapshot ids in the reloaded snapshot *set*; strict Failure is converted to the original unknown (`mod.rs:376-496`). Metadata-only commits have no snapshot evidence and stay unknown (`mod.rs:438-448`). Requirement failures are checked in `Transaction::apply` via `requirement.check` (`mod.rs:200-202`) before `update_table`. Probe C-003's refuse path is `DataInvalid` not `CommitStateUnknown`. |
| C-007 | A single-snapshot full replacement of a table's data (all old files removed, new classified files added) is `OverwriteFilesAction::overwrite_by_row_filter(AlwaysTrue).add_files(...)`, operation `Overwrite`, with the snapshot-producer requirements of C-001. `ReplacePartitions` on an unpartitioned table is the dynamic-partition analogue. RePark already calls this in `commit_overwrite_replace_all_to`. | Source plus PROBE. | **PROVEN** | Source: unpartitioned `AlwaysTrue` deletes every live data file (`overwrite_files.rs:40-42, 183-190`). RePark `crates/repark-iceberg/src/write/overwrite_commit.rs:17-42` (`overwrite_by_row_filter(AlwaysTrue)` + isolation validations + `set_snapshot_properties` for `engine.operation-id`). Default RePark overwrite isolation is **snapshot** (`overwrite.rs:21-31, 36-42`): `validate_no_conflicting_deletes` only; concurrent *appends* are not refused unless the table property is `serializable`. `replace_partitions` on unpartitioned = full replace (`mod.rs:259-263`). Probe `overwrite_by_always_true_replaces_all_live_files_in_one_snapshot` (`silver_s0_probe.rs:429`): two live files replaced by one; `Operation::Overwrite`; `deleted-data-files=2`, `added-data-files=1`, `deleted-records=2`, `added-records=4`. |
| C-008 | What is missing for SIL-4 (stage → validate → conditional commit with marker → resolve), named as fork-side `F-SILVER-*` gaps versus RePark-side adapter work, with the smallest owning boundary. | Split from C-001..C-007. | **PROVEN** | SIL-4 is implementable at this pin **if** the adapter holds the expected-base `Table`, stages via `DataFileWriter`, runs quality on those `DataFile`s, then `overwrite_files().overwrite_by_row_filter(AlwaysTrue).add_files(staged).set_snapshot_properties(run_key).validate_no_conflicting_data().validate_from_snapshot(expected_base)` and treats `CommitStateUnknown` as "walk snapshots for the run key; do not retry a replace." Default overwrite isolation (snapshot) is **not** sufficient (C-003, C-007). Fork-side candidate cards (not required if the adapter opts in correctly): `F-SILVER-PIN-BASE` — first-class expected-base that is not rewritten on rebase (`fork/crates/iceberg/src/transaction/mod.rs` `do_commit` + `snapshot.rs` requirements); `F-SILVER-NO-REBASE` — `commit.retry.num-retries=0` still rebases on the first attempt (`mod.rs:512-527`); `F-SILVER-MARKER-RETENTION` — `expire_snapshots` has no run-key awareness (`expire_snapshots.rs`). RePark-side: a publish function next to `crates/repark-iceberg/src/write/overwrite_commit.rs` (smallest owning boundary: the existing write adapter). Plan/compile of the silver contract belongs in `crates/repark-core` (crate-DAG tier 2; may depend on `repark-iceberg` tier 1). Do not create `repark-exec` / `repark-io` for this. |
| C-009 | SIL-1 finite MVP matrix, measured: Iceberg↔Arrow round-trip for int32/int64/utf8/bool/date/timestamp-micros UTC/decimal; keys and source-order types DataFusion orders totally; five §8 ops mapped to an existing expression or MISSING. | Read `fork/crates/iceberg/src/arrow/schema.rs` and RePark function dispatch; type-round-trip probe. | **PROVEN** | Probe `mvp_iceberg_arrow_types_round_trip` (`silver_s0_probe.rs:474`) plus `type_to_arrow_type` (`fork/crates/iceberg/src/arrow/schema.rs:876-916`): Int→Int32, Long→Int64, String→Utf8, Boolean→Boolean, Date→Date32, Timestamptz→`Timestamp(Microsecond, Some("UTC"))`, Decimal(10,2)→Decimal128(10,2); each pair round-trips `arrow_type_to_type`. Iceberg String is Utf8, not Utf8View; RePark write conform already maps view strings (`crates/repark-iceberg/src/write/map.md` store-assign). Keys/order: Int32/Int64/Utf8/Boolean/Date32/Timestamp(µs)/Decimal128 have Arrow total comparison; DataFusion `SortExpr` uses those kernels; Utf8 is bytewise (matches epic §5). Null ordering is `SortOptions` (`nulls_first` / `nulls_last`), not a type property. Collation-aware keys are unsupported — fail at plan compile. §8 ops: `Trim` → DataFusion `btrim` / RePark `trim` (`crates/repark-python/src/column/function_dispatch.rs:69-71`); a U+0020-only trim is `btrim(col, " ")`, not the default whitespace class. `EmptyToNull` → `nullif(col, '')` (DataFusion `nullif`, already used at `crates/repark-functions/src/analyzer.rs:7,258`). `ParseTimestamp` → format-less `to_timestamp` exists (`crates/repark-functions/src/instant_ts.rs:57-60`); `to_timestamp(format=...)` is refused (`python/repark/src/repark/spark/functions_expr.py:548-558`); `try_to_timestamp` is refused (`python/repark/tests/test_examples_functions_b.py:12-15`); `iso8601_seconds_offset_v1` plus a parse-success flag distinct from null is **MISSING**. `RequireNonNull` → `isnotnull`; row disposition is plan-level, not a kernel. `CheckAllowedValues` → DataFusion `in_list` (`crates/repark-spark/src/call/rewrite_where.rs:66`). |
| C-010 | Recommendations SIL-1..SIL-10: evidence clauses, a RECOMMENDED resolution, and what the OWNER must still decide. Never "Ruled". | One row per SIL id, below. | **PROVEN** | See "Recommendations SIL-1..SIL-10" below. No SIL id is marked Ruled. |
| C-011 | Named gates green; probe command and fork log `3ebf7d36..HEAD` pasted. | `make check-ledger-grammar`, `make check-docs-links`, `make check-map-sync`, `uvx typos@1.47.2` on this ledger; fork `cargo test -p iceberg --lib silver_s0_probe`. | **PROVEN** | RePark 2026-09-12: `make check-ledger-grammar` → `ledger-grammar: 117 live ledgers clean (746 clauses, 1365 pinned clause ids, 2 exception rows)` exit 0; `make check-docs-links` → `docs-links: 796 files, 5105 links checked — clean` exit 0; `make check-map-sync` → `map-sync: 246 maps clean (strict=off)` exit 0; `uvx typos@1.47.2 task/ledgers/staging/silver-s0-ledger.md` exit 0. Fork: `CARGO_BUILD_JOBS=8 cargo test -p iceberg --lib silver_s0_probe` → `ok. 10 passed; 0 failed; 0 ignored; 0 measured; 3680 filtered out; finished in 0.05s`. `git -C fork log --oneline 3ebf7d36..HEAD` → `ac2d6ab8 test: SILVER-S0 probes for stage, rebase, run marker, replace`. |

VERDICT: 11 clauses, 11 PROVEN, 0 OPEN, 0 REJECTED.

## Recommendations SIL-1..SIL-10

These are recommendations. The owner decides. Nothing here is Ruled.

| ID | Evidence | Recommendation | Owner decides |
|---|---|---|---|
| SIL-1 | C-009 | First classified table uses the measured Iceberg↔Arrow set: int32, int64, utf8, bool, date32, timestamp-micros UTC, decimal128. Keys and source-order: int32/int64 (numeric) and utf8 (bytewise). The five §8 ops: `Trim` (explicit `" "`), `EmptyToNull` (`nullif`), `RequireNonNull` (`isnotnull` + classify), `CheckAllowedValues` (`in_list`). Treat `ParseTimestamp` `iso8601_seconds_offset_v1` as out of the first matrix until a kernel with a parse-success flag exists. | Which bronze table; whether decimal precision/scale is capped at Decimal128 (38,s); whether `ParseTimestamp` waits for S-2. |
| SIL-2 | epic §10 (no storage probe) | Adopt latest-source-version: an invalid latest version quarantines; do not fall back to an older valid row. | Whether that default is accepted. |
| SIL-3 | epic §10 (no storage probe) | Fail the run on conflicting greatest-version ties and on unorderable keyed history. Identical payload repeats: lowest bronze record id. | Canonical payload equality (which fields, hash vs exact). |
| SIL-4 | C-001, C-002, C-003, C-004, C-006, C-007, C-008 | Implement publication in `repark-iceberg` write adapter with existing fork primitives: stage `DataFile`s (C-002), quality on those files (no second rewrite), then overwrite-by-alwaysTrue with `validate_no_conflicting_data` + `validate_from_snapshot(expected_base)` + `set_snapshot_properties(run_key)` (C-003/C-004/C-007). Hold the expected-base `Table`; do not `load_table` immediately before commit. On `CommitStateUnknown`, walk snapshots for the run key (C-006); do not treat absence as failure. Do not ship a fork change for SIL-4 unless the owner wants `F-SILVER-PIN-BASE`. Default RePark overwrite isolation (snapshot) must not be used for silver publication. | Whether opt-in serializable validation is enough, or `F-SILVER-PIN-BASE` is required; the run-key property name. |
| SIL-5 | epic §11, C-007 | One classified physical table; one snapshot is the publication point. Snapshot-bound read handles from the committed snapshot id. | How ordinary SQL consumers bind a run (snapshot id vs branch vs filter on run key). |
| SIL-6 | C-005 | A summary-only run marker dies when that snapshot expires. Set `history.expire.min-snapshots-to-keep` / max-snapshot-age (or `expire_snapshot_id` policy) to cover the retry/replay window, **or** dual-write the run key into table properties in the same transaction. Physical file cleanup is a separate `ExpireSnapshotsCleanup` step. | Replay/retry windows, quarantine access, whether table-property dual-write is wanted. |
| SIL-7 | epic §11 (no storage probe) | Quality gates run on staged files before commit (C-002 makes that possible). A failed gate leaves the last accepted snapshot current. | Denominators, thresholds, empty-input behavior, whether evidence-loss blocks publication. |
| SIL-8 | C-008, crate DAG | Plan/policy/compile in `crates/repark-core` (tier 2). Publish adapter in `crates/repark-iceberg/src/write/` next to `overwrite_commit.rs` (tier 1). Python remains a thin authoring door. Do not create `repark-exec` or `repark-io` for this. | Whether silver is a module under `repark-core` or waits for a later crate split. |
| SIL-9 | epic §14 (not this unit) | No speed target is asserted here. Set hardware/data methodology before any implementation trial. | Budgets and the 190-column corpus. |
| SIL-10 | epic §13 (not this unit) | Fixed initial output contract per accepted plan. Breaking changes need a versioned target or a reviewed migration. | Migration vs versioned target. |

## Fork-side candidate cards (not opened)

| Card | One line | File it would touch |
|---|---|---|
| `F-SILVER-PIN-BASE` | First-class expected-base snapshot that `do_commit` does not rewrite into the live head. | `fork/crates/iceberg/src/transaction/mod.rs`, `snapshot.rs` |
| `F-SILVER-NO-REBASE` | A commit option that skips refresh-and-re-apply; `commit.retry.num-retries=0` is not that option. | `fork/crates/iceberg/src/transaction/mod.rs` |
| `F-SILVER-MARKER-RETENTION` | Expire does not know about run keys; prefer RePark retention policy (SIL-6) over a fork change. | `fork/crates/iceberg/src/transaction/expire_snapshots.rs` |

## Probe run (C-002..C-005, C-007, C-009)

Command: `CARGO_BUILD_JOBS=8 cargo test -p iceberg --lib silver_s0_probe`

```
running 10 tests
test transaction::silver_s0_probe::mvp_iceberg_arrow_types_round_trip ... ok
test transaction::silver_s0_probe::parquet_data_file_write_does_not_advance_snapshot_and_later_commit_reuses_files ... ok
test transaction::silver_s0_probe::fast_append_commit_emits_uuid_and_ref_snapshot_requirements ... ok
test transaction::silver_s0_probe::overwrite_by_always_true_replaces_all_live_files_in_one_snapshot ... ok
test transaction::silver_s0_probe::overwrite_with_validate_no_conflicting_data_refuses_concurrent_append ... ok
test transaction::silver_s0_probe::run_key_in_snapshot_summary_is_findable_from_table_metadata ... ok
test transaction::silver_s0_probe::expire_snapshot_id_drops_run_key_from_metadata ... ok
test transaction::silver_s0_probe::concurrent_append_before_staging_then_overwrite_without_validation_still_rebases ... ok
test transaction::silver_s0_probe::overwrite_without_conflict_validation_rebases_over_concurrent_append ... ok
test transaction::silver_s0_probe::zero_commit_retries_still_rebases_overwrite_over_concurrent_append ... ok

test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 3680 filtered out; finished in 0.05s
```

## Red first

This is a READING unit. The falsifiable checks are the ten probe tests in
`fork/crates/iceberg/src/transaction/silver_s0_probe.rs` (green on the memory
catalog at pin `3ebf7d36`) and the line citations against that pin.

A mutant that dropped `validate_no_conflicting_data` would turn the refuse
probe green-path; a mutant that skipped the parquet `close()`-without-commit
check would fail C-002; a mutant that treated `CommitStateUnknown` as a
retryable conflict would contradict C-006's source.

## Evidence notes

**Expected base is not a CAS argument.** The only snapshot id a snapshot
producer puts in `RefSnapshotIdMatch` is the head of the table it ran
against after refresh. `validate_from_snapshot` is a different knob (the
start of the conflict walk). SIL-4 therefore pins the expected base by
keeping the `Table` loaded at that snapshot and arming
`validate_no_conflicting_data`, not by injecting a requirement.

**WAP `stage_only` is not SIL-4 staging.** `stage_only()` adds a snapshot
without moving `main`. SIL-4 staging is writing parquet `DataFile`s with
no catalog mutation (C-002).

**Default overwrite isolation is a trap.** RePark
`commit_overwrite_replace_all_to` uses snapshot isolation unless the table
property is `serializable`. Snapshot isolation does not refuse a concurrent
append. A silver publisher that reused that default would silently rebase
(C-003).

```
COVERAGE_ATTESTATION:
  pr_unit: silver-s0
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every SILVER-S0 clause was walked against fork pin 3ebf7d36 and RePark write-adapter source. C-001 lists every Transaction factory and the requirements snapshot producers emit. C-008 names F-SILVER-* versus the repark-iceberg write adapter. C-010 files SIL-1..SIL-10 as Recommendation / Owner decides, never Ruled.
      artifacts: [task/ledgers/staging/silver-s0-ledger.md, task/roadmap/epic-term/deterministic-silver-layer-compiler-2026-09-12.md]
    - id: AT-2
      status: ATTACKED
      evidence: Probes cover staging without commit, both concurrent-commit orders, retry-count-zero still rebasing, armed validate_no_conflicting_data refusal, run-key lookup, expire dropping the marker, always-true replace, requirement emission, and the seven-type Arrow round-trip. Memory catalog only; Glue/S3/REST taxonomy is source-read at the pin.
      artifacts: [fork/crates/iceberg/src/transaction/silver_s0_probe.rs]
    - id: AT-3
      status: ATTACKED
      evidence: C-006 distinguishes CatalogCommitConflicts (refused, retryable CAS), DataInvalid (refused, non-retryable validation), and CommitStateUnknown (ambiguous; absent-after-refresh is not failure). C-003 probe pins the refuse path as DataInvalid with retryable false.
      artifacts: [fork/crates/iceberg/src/error.rs, fork/crates/iceberg/src/transaction/mod.rs, fork/crates/catalog/glue/src/commit_transport.rs, fork/crates/catalog/s3tables/src/commit_transport.rs]
    - id: AT-4
      status: ATTACKED
      evidence: C-003 probes both orders of concurrent append versus overwrite and shows the default path silently rebases (deleted-data-files=2). The armed-validation path refuses. retry num-retries=0 does not skip the first-attempt rebase.
      artifacts: [fork/crates/iceberg/src/transaction/silver_s0_probe.rs, fork/crates/iceberg/src/transaction/mod.rs]
    - id: AT-5
      status: N/A
      justification: Reading unit. No AWS, no IAM, no catalog mutation outside the in-process memory catalog used by fork tests.
    - id: AT-6
      status: ATTACKED
      evidence: SIL-4 recommendation requires holding the expected-base Table and opting into validate_no_conflicting_data. Default RePark overwrite isolation (snapshot) is named as insufficient. No public RePark signature is changed.
      artifacts: [crates/repark-iceberg/src/write/overwrite_commit.rs, crates/repark-iceberg/src/write/overwrite.rs]
    - id: AT-7
      status: N/A
      justification: No performance claim. SIL-9 is left as owner-decides with no speed target.
    - id: AT-8
      status: ATTACKED
      evidence: Probe citations and ErrorKind/requirement line numbers were read from the pin and from the rustfmt'd probe file after the tests went green. A wrong requirement claim would fail fast_append_commit_emits_uuid_and_ref_snapshot_requirements.
      artifacts: [fork/crates/iceberg/src/transaction/snapshot.rs, fork/crates/iceberg/src/transaction/silver_s0_probe.rs]
    - id: AT-9
      status: N/A
      justification: Nothing runs in production from this unit.
    - id: AT-10
      status: ATTACKED
      evidence: Ten probe tests failed closed on the named risks (silent rebase, marker expiry, staging visibility) and passed on the memory catalog. Gates in C-011.
      artifacts: [fork/crates/iceberg/src/transaction/silver_s0_probe.rs, task/ledgers/staging/silver-s0-ledger.md]
  complete: true
```
