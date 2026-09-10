# Unit ledger — AP-1 step 1 (plan) · `CALL plan_partitioning()`

**Unit:** AP-1 step 1 (plan) · **Date:** 2026-09-10 · **Branch:** `feat/ap-1` · **Base:** `origin/main`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** AP-0 measured P-2/P-3 from manifest bounds and filed AP-0-R-001 (byte
projection 76–88 % high) and AP-0-R-002 (file-count column untested). This step builds the
plan half: `CALL <catalog>.system.plan_partitioning(table => …, target_file_size_bytes => …)`
returning the D-1 frame, P-2 candidates scored with exactly P-3, the R-001 caveat visible in
the output, P-5 branch refusal plus the multi-spec note. No apply, no guide section (step 2).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this step:** `apply_partitioning` (AP-2), the `adaptive_partitioning` hook (AP-3),
any release-build run over the AP-0 tables, `docs/perf/adapt-part-ap1-*.md`, the
`docs/guide/maintenance-policy.md` section, any dependency file, `STATUS.md`,
`briefs/next-sequence.md`.

**Decision (no data scan).** D-2 allows no data scan in this card, while AP-0 used a bounded
head sample for the P-2 distinct-count branch. The procedure therefore derives the
identity/bucket branch from the union of manifest bound endpoints per column (≤ 1000 → identity,
else buckets): manifest-only, no data read. Recorded here so step 2 reads the same rule.

## PROPOSITION LEDGER — AP-1 step 1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `plan_days_ts_wins`: on a synthetic table whose `ts` spans 90 days at about the target per day, `days(ts)` ranks first. | CALL the procedure on the 9-file fixture; row 0 candidate is `days(ts)`. | **PROVEN** | `plan_days_ts_ranks_first_on_ninety_day_fixture`: 9 ten-day files, target = total/90; row 0 `days(ts)` at 0.0 over 90 partitions, ddl carries `days(ts)`, frame sorted best-first, winner `projected_files_at_target` > 0, `calls` chains `rewrite_data_files`. |
| C-002 | `plan_frame_shape`: the frame carries exactly `candidate`, `score`, `projected_partitions`, `projected_files_at_target`, `ddl`, `calls`, `plan_id`, `notes` with Arrow types Utf8, Float64, Int64, Int64, Utf8, Utf8, Utf8, Utf8. | Schema assertion on the collected batch. | **PROVEN** | `plan_frame_carries_d1_columns_with_arrow_types` plus the shared `plan_rows` helper asserting names and Arrow types on every collected frame. |
| C-003 | `plan_boundless_column_excluded`: a column with no readable bounds is not a candidate and the last row's `notes` names it. | Fixture `note` column (all NULL); no candidate mentions it; last-row notes contain `note`. | **PROVEN** | `plan_boundless_column_is_excluded_and_named_on_last_row`: no candidate contains `note`; last-row notes name it. |
| C-004 | `plan_unpartitioned_present`: `unpartitioned` is a candidate row. | Frame contains a row with candidate `unpartitioned`. | **PROVEN** | `plan_unpartitioned_is_a_candidate`. |
| C-005 | `plan_id_stable`: `plan_id` repeats for the same (snapshot, candidate) across two CALLs and differs across candidates. | Two CALLs, per-candidate comparison plus intra-frame uniqueness. | **PROVEN** | `plan_id_repeats_per_candidate_and_differs_across_candidates`: two CALLs agree per candidate, ids non-empty, all distinct within the frame; in-module `plan_id_is_stable_per_pair_and_unique_per_candidate`. |
| C-006 | `plan_r001_note_everywhere`: every row's `notes` names AP-0-R-001 and states the pre-rewrite-byte caveat. | Substring assertion on all rows. | **PROVEN** | `plan_every_row_carries_the_r001_file_count_caveat`: every row's notes contain `AP-0-R-001` and `projected_files_at_target`. |
| C-007 | `plan_branch_refuses`: a table with a branch besides `main` refuses the CALL. | CREATE BRANCH then CALL errors naming the branch. | **PROVEN** | `plan_branch_besides_main_refuses`: `CREATE BRANCH feat` then the CALL errors naming `feat`, `branch`, `main`. |
| C-008 | `plan_multispec_noted`: a table with more than one partition spec plans with a note that apply would rewrite to one spec. | ADD PARTITION FIELD then CALL succeeds; notes contain `one spec`. | **PROVEN** | `plan_multi_spec_table_reports_the_single_spec_rewrite`: after `ADD PARTITION FIELD days(ts)` the CALL succeeds and a row's notes contain `one spec` (spec count read from table metadata, not from files, so a new spec with no files yet still reports). |
| C-009 | `plan_identity_wins`: on a 4-region table at ~2x target per region, `identity(region)` ranks first with score 0. | CALL on the region fixture; row 0 is `identity(region)` at 0.0. | **PROVEN** | `plan_identity_region_ranks_first_at_twice_target`: 4 single-region files, target = total/8; row 0 `identity(region)` at 0.0 over 4 partitions. |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

All 9 pins were written before the procedure existed and run against the base tree.
They fail because there is nothing to run — the honest red:

```text
test result: FAILED. 0 passed; 9 failed; 0 ignored; 0 measured; 895 filtered out; finished in 0.17s
plan_partitioning runs: NotImplemented("CALL system.plan_partitioning is not supported. Supported procedures: expire_snapshots, register_table, rewrite_data_files, rewrite_manifests, remove_orphan_files, rewrite_position_delete_files, rollback_to_snapshot, run_maintenance.")
```

Two fixture corrections landed before green, both in test code only: the first red
also caught a weak C-007 pin (DataFusion renders `NotImplemented` as "This feature is
not implemented", which matched the `feat`/`main` substrings), so the pin now requires
the word `branch`; and the door exposes no SQL `date_add`, so the 90-day fixture renders
its days as `TIMESTAMP '…'` literals in Rust instead of date arithmetic in SQL. No
production file was edited to make the red pass except adding the procedure the pins name.

## Gates

- `cargo test -p repark-spark --lib plan_partitioning`: green (23 passed).
- `make verify`: green.
- Comment fence (`git diff --cached` grep for added `//`/`#` lines): prints nothing.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ap-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause was re-derived from the pins rather than read off the card — the 9 pins failed red on the base tree (0 passed, exact NotImplemented text pasted above) and pass green after, 23 tests total including the red-caught C-007 strengthening.
      artifacts: [crates/repark-spark/src/tests/plan_partitioning.rs, crates/repark-spark/src/call/plan_partitioning.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Both temporal and identity winners pinned end to end (90-day ts table, 4-region table), plus the boundless-column exclusion, the unpartitioned candidate, and the branch/multi-spec tables; bucket and pair paths pinned at unit level (even N split, 50-cell cross product) since no fixture spans them.
      artifacts: [crates/repark-spark/src/tests/plan_partitioning.rs, crates/repark-spark/src/call/plan_partitioning_score.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Branch refusal, missing/zero/negative target and unknown-key refusals all pinned loud with the key named; the multi-spec case plans with a note instead of refusing, pinned too.
      artifacts: [crates/repark-spark/src/tests/plan_partitioning.rs]
    - id: AT-4
      status: N/A
      justification: No shared or mutable state, no concurrency: one CALL collects its metadata batches, scores them single-threaded, and answers one frame; the fixtures run in isolated memory catalogs.
    - id: AT-5
      status: N/A
      justification: No privileged action, no credential, no network. All reads are local memory-catalog metadata tables.
    - id: AT-6
      status: ATTACKED
      evidence: The P-1 integrity claim (manifests first, no data scan) was attacked by construction and by pin: the procedure issues SQL only against the `files` and `refs` metadata tables and reads the schema from loaded table metadata, and the all-NULL column test proves a boundless column is excluded without any data read.
      artifacts: [crates/repark-spark/src/call/plan_partitioning.rs]
    - id: AT-7
      status: ATTACKED
      evidence: The work scales with the manifest row count plus the truncated-range expansion, both bounded by the table's own file layout; the whole battery (a 9-commit and a 4-commit fixture plus eight small tables) runs in under two seconds.
      artifacts: [crates/repark-spark/src/tests/plan_partitioning.rs]
    - id: AT-8
      status: ATTACKED
      evidence: The two upstream shapes the procedure could have presumed were checked against the fork source, not presumed: `refs` type strings (`BRANCH`/`TAG` in `inspect/refs.rs`) and the `files` columns (`content`, `file_size_in_bytes`, `readable_metrics` in `inspect/data_file.rs`). The first multi-spec attempt read spec ids from `files` and missed a spec with no files yet; detection moved to table metadata, pinned by C-008.
      artifacts: [crates/repark-spark/src/call/plan_partitioning.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal names the table, the key and the offending value; every plan row carries its DDL, its follow-up CALLs, its plan id and the R-001 caveat, and the last row names each skipped column with its reason.
      artifacts: [crates/repark-spark/src/call/plan_partitioning.rs]
    - id: AT-10
      status: ATTACKED
      evidence: The pins are the reproduce record — 9 of 9 failed red before the procedure existed and pass after, with the red output pasted verbatim above; adequacy was attacked once already (the weak C-007 substring the base tree satisfied for the wrong reason) and the pin was strengthened to require the word `branch`.
      artifacts: [task/ledgers/staging/ap-1-ledger.md, crates/repark-spark/src/tests/plan_partitioning.rs]
  complete: true
```
