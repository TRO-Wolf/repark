# Unit ledger — ICE-MERGE-APPEND-1 · an INSERT commits through `merge_append`, as Spark's `newAppend()` does

**Date:** 2026-09-19 · **Branch:** `fix/ice-merge-append-1` · **Base:** `main` `859c6506` ·
**Model:** claude-opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Campaign:** the 1-to-1 Spark–Iceberg parity campaign, IPI-11 (merge-on-commit).
**Fork pin:** `44834673` (`MergeAppendAction` present since the fork landed
`crates/iceberg/src/transaction/merge_append.rs`).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Clause numbering.** The brief's clauses `C-1`..`C-8` are this ledger's `C-001`..`C-008`,
one-to-one and in order (the repo's ledger-grammar gate parses `C-NNN`). `C-009` and `C-010`
are added by this unit: the fork-side INSERT-path finding and the no-dependency-move guard.

**Why now.** Recorded 2026-09-19, 120 sequential single-row `INSERT`s into one v2 table:
Spark 4.1.2 + Iceberg 1.11.0 merges the manifest list on commit
(`/tmp/oc-worker/qa/merge_append_truth.json` — defaults collapse 99 manifests into 1 at the
100th append, `commit.manifest.min-count-to-merge=5` keeps the table between 1 and 4
manifests, `commit.manifest-merge.enabled=false` never merges), while RePark at `main`
produces one manifest per append in ALL THREE variants (120 → 120) and honours neither
property. Manifest count per append is the read-planning cost of every subsequent scan.

**Not in this unit:** the `manifests-created` / `-kept` / `-replaced` snapshot summary keys
(fork ask, see C-006); the fork pin bump; manifest compaction procedures
(`rewrite_manifests`); delete-manifest merging (the fork's named deviation); `STATUS.md`
lifecycle rows.

## 1. The mapping — which RePark commit site is Spark's `newAppend()` (C-001)

**Evidence basis.** Bytecode of the artifact the truth file was recorded against,
`org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`
(`/tmp/oc-worker/ice-rating/scratch/.ivy2/jars/`), disassembled with `javap -p -c`. Java's
`Table.newAppend()` is the MERGING producer and `newFastAppend()` the non-merging one; the
fork's `merge_append.rs` module doc states the same contract.

| Spark write op (class) | producer in bytecode | merges? |
|---|---|---|
| `SparkWrite$BatchAppend.commit` | `Table.newAppend()` | **yes** |
| `SparkWrite$StreamingAppend.commit` | `Table.newFastAppend()` | no |
| `SparkWrite$DynamicOverwrite.commit` | `Table.newReplacePartitions()` | n/a |
| `SparkWrite$OverwriteByFilter.commit` | `Table.newOverwrite()` | n/a |
| `SparkWrite$CopyOnWriteOperation.commit` | `Table.newOverwrite()` | n/a |
| `SparkWrite$StreamingOverwrite.commit` | `Table.newOverwrite()` | n/a |
| `SparkPositionDeltaWrite$PositionDeltaBatchWrite.commit` | `Table.newRowDelta()` | n/a |

CTAS/RTAS writes through `StagedSparkTable`, whose `Table` is
`BaseTransaction$TransactionTable`; that class's `newAppend()` forwards to
`BaseTransaction.newAppend()`, which the bytecode shows constructing
`org.apache.iceberg.MergeAppend` (and `newFastAppend()` constructing
`org.apache.iceberg.FastAppend`). **So a CTAS append is a merging append too.**

So: **every batch append in Spark merges; only the structured-streaming micro-batch append
does not.** RePark has no streaming writer (streaming is v1.6.0), so RePark has **no**
`newFastAppend()` site — every one of its append commit sites is a `newAppend()` site.

| RePark commit site | reached by | Spark counterpart | disposition |
|---|---|---|---|
| `crates/repark-iceberg/src/write/append.rs::commit_append` | public `write::append()`, native-door CTAS (`repark-sql/src/create_table.rs:452`), Spark-door CTAS without write options (`repark-spark/src/ctas.rs:548`) | `BatchAppend` (via `TransactionTable.newAppend()` for CTAS) | **route to `merge_append`** |
| `crates/repark-iceberg/src/write/commit_target.rs::commit_append_to` | `INSERT INTO … BY NAME` (`repark-spark/src/insert_by_name.rs:188`), branch-targeted | `BatchAppend` | **route to `merge_append`** |
| `crates/repark-iceberg/src/write/write_options.rs::commit_append_with_summary` | `INSERT` with statement write options (`repark-spark/src/append_with_options.rs:77`), `append_with_statement_options`, CTAS with options (`repark-spark/src/ctas.rs:581`) | `BatchAppend` | **route to `merge_append`** |
| `crates/repark-spark/src/ctas.rs:337` (staged-table append) | CTAS/RTAS through a staged table | `StagedSparkTable` + `BatchAppend` → `BaseTransaction.newAppend()` | **route to `merge_append`** |
| `overwrite_files()` sites (`commit_replace_write*`, `commit_overwrite_replace_all*`, `partition_overwrite`) | `INSERT OVERWRITE`, RTAS | `OverwriteByFilter` / `DynamicOverwrite` | leave — not an append producer |
| `write/merge/snapshot_commit.rs` (MERGE/UPDATE/DELETE) | `MERGE INTO`, `UPDATE`, `DELETE` | `PositionDeltaBatchWrite` / `CopyOnWriteOperation` | leave — not an append producer |

**The finding (C-009).** The commit site that produced the measured RePark series is **not**
in this repository. A bare `INSERT INTO cat.ns.t VALUES (…)` on either SQL door falls through
to DataFusion (`repark-spark/src/router.rs:279` → `passthrough_after_p11`), and DataFusion
plans it on the fork's provider. Measured on this tree (`EXPLAIN INSERT INTO …`):

```
physical_plan  IcebergCommitExec: table=ns.t
                 IcebergWriteExec: table=ns.t
```

`IcebergCommitExec`'s `InsertOp::Append` arm commits through `tx.fast_append()`
(fork `crates/integrations/datafusion/src/physical_plan/commit.rs:362`), and both
`IcebergWriteExec` and `IcebergCommitExec` are `pub(crate)` in the fork — RePark cannot
reuse or replace them. The facade's `df.write.insertInto(…)` /
`saveAsTable(mode="append")` lower to the same bare `INSERT INTO`, so they share the gap.
This half is a fork ask; see C-009 for its registry row and xfail.

## PROPOSITION LEDGER — ICE-MERGE-APPEND-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The mapping in §1 is right: every RePark append commit site is Spark's `newAppend()` (merging); RePark has no `newFastAppend()` site; no overwrite/row-delta site is an append. | `javap -p -c` over the 1.11.0 runtime jar for every `SparkWrite$*` / `SparkPositionDeltaWrite$*` commit method plus `BaseTransaction`; `grep` over `crates/` for every `fast_append()` call. | **PROVEN** | §1. Bytecode: `BatchAppend` → `Table.newAppend`; `StreamingAppend` → `Table.newFastAppend`; `DynamicOverwrite` → `newReplacePartitions`; `OverwriteByFilter` / `CopyOnWriteOperation` / `StreamingOverwrite` → `newOverwrite`; `PositionDeltaBatchWrite` → `newRowDelta`. `BaseTransaction.newAppend()` constructs `org.apache.iceberg.MergeAppend` and `newFastAppend()` constructs `FastAppend`, so the CTAS path through `StagedSparkTable` → `BaseTransaction$TransactionTable.newAppend()` is merging too. `grep` found five `fast_append()` call sites in `crates/`: the four routed here, and none left over — the fifth (`ctas.rs`) is the staged append. RePark has no streaming writer (v1.6.0), so it has no `newFastAppend()` site at all. |
| C-002 | With defaults, the RePark-owned append path reproduces Spark's `defaults` series at every probe point (1, 5, 20, 50, 99, **100 → ONE manifest**, 101, 110, 120) for manifest count and data-file count. | Series pins replaying the fixture. | **PROVEN** | Red-first on this tree with RePark's measured numbers (100 → 100 manifests, 120 → 120); green after the routing. `merge_append_series.rs::defaults_series_matches_spark` compares the whole nine-point series as one value, so a single wrong probe reds it. End to end: `test_by_name_append_series_matches_spark[defaults]` and `test_defaults_collapse_to_one_manifest_at_the_hundredth_append` (1 manifest, 100 files, 100 rows). |
| C-003 | `commit.manifest.min-count-to-merge=5` is honoured: the `min_count_5` series matches Spark at every probe point (1, 1, 4, 2, 3, 4, 1, 2, 4). | Series pins. | **PROVEN** | Red-first (120 → 120), green after. `min_count_to_merge_series_matches_spark` + `test_by_name_append_series_matches_spark[min_count_5]`. Mutation M3 (drop the property from the table) reds this pin and nothing else. |
| C-004 | `commit.manifest-merge.enabled=false` reproduces the `merge_disabled` series (120 appends → 120 manifests) — what `main` does today. | Series pins. | **PROVEN** | Green BEFORE and AFTER the routing change: the escape hatch is real, and this is the one clause the change must not move. `merge_disabled_series_matches_spark` + `test_by_name_append_series_matches_spark[merge_disabled]` + `test_merge_disabled_is_a_real_escape_hatch` (100 appends → 100 manifests, 100 files). Mutation M2 reds exactly this pin. |
| C-005 | A merging commit changes nothing else: delete manifests carry forward untouched on a MoR table, sequence numbers and row lineage survive a merge, multi-spec tables never merge across spec ids, branch writes still target their ref, and the concurrency pins still hold. | The named regression checks. | **PROVEN** | Five checks, all green. **Delete manifests:** `delete_manifests_carry_forward_through_a_merge` — a `row_delta` position-delete manifest's path is compared before and after 119 further merging appends and is byte-identical, while the DATA manifests merge beside it. **Sequence numbers:** `sequence_numbers_and_file_provenance_survive_a_merge` — after the hundredth append collapses the list to ONE manifest, every entry in it still carries the sequence number of the append that added it (`step-N.parquet` ↔ N, all 100). **Multi-spec:** `merge_never_mixes_partition_spec_ids` — 60 appends at spec 0, a spec evolution, 60 at spec 1; both spec ids survive, all 120 files live, and the list still shrank. **Branch:** `branch_merging_append_moves_only_the_branch` — the merge fires on the BRANCH's list (the carried seed plus 99 branch appends = the hundredth manifest → 1) and main's pointer never moves. **Row lineage:** `test_v3_row_lineage_survives_a_merging_append` — a v3 table's `_row_id` set is still 0..99 and the rows intact after the merging hundredth append. **Concurrency:** `cargo test -p repark-iceberg` 591 passed / 0 failed, including `write/merge/tests/occ*.rs`; `test_ice_occ_scoped_1.py` green in the 7-file batch (162 passed). |
| C-006 | The `manifests-created` / `-kept` / `-replaced` summary keys are DECLARED, not faked: a registry row naming the fork ask and a strict xfail that flips when the fork lands. | Registry row + strict xfail. | **PROVEN** | Registry `ICE-MERGE-APPEND-SUMMARY-1` (BACKLOG 2026-09-19, TRIGGER fork #322) in [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7. Pin `test_merging_commit_stamps_the_manifests_summary_keys` asserts Spark's `1 / 0 / 4` on the `min_count_5` merge under `pytest.mark.xfail(strict=True)`, so it errors the moment the fork stamps them. Nothing is synthesised engine-side. **Flipped at RP-40 (2026-09-20):** fork #322 stamps the three keys on every operation, the xfail became a plain assertion (five appends at `min-count-to-merge=5` stamp `1 / 0 / 4`), and the registry row is FIXED. |
| C-007 | The cost of merging is measured: 120 sequential appends timed before and after on the same head, both numbers recorded. | The timing pair. | **PROVEN** | §2. Merging is **cheaper**, not dearer: 4.25 s → 3.03 s of process CPU for 120 sequential appends (−29 %; −22 % after normalising by the `merge_disabled` control). |
| C-008 | The pins are mutation-proof: routing back to `fast_append` reds C-002 and C-003; ignoring `commit.manifest-merge.enabled` reds C-004. | Two recorded, reverted mutations. | **PROVEN** | §3. Three mutations, each recorded and reverted: M1 (route all four sites back to `fast_append`) reds 6 of 7 Rust pins and leaves the `merge_disabled` pin green; M2 (drop `commit.manifest-merge.enabled` from the table) reds exactly `merge_disabled_series_matches_spark`; M3 (drop `commit.manifest.min-count-to-merge`) reds exactly `min_count_to_merge_series_matches_spark`. `git status` clean after each. |
| C-009 | The bare `INSERT INTO` path commits fork-side and cannot be routed from this repository at pin `44834673`; the gap is DECLARED with a registry row naming the fork ask. | The `EXPLAIN` measurement, the fork source, the `pub(crate)` visibility, the registry row. | **PROVEN** | §1 "The finding". Measured, not inferred: `EXPLAIN INSERT INTO …` on this tree plans `IcebergCommitExec → IcebergWriteExec`. Fork `crates/integrations/datafusion/src/physical_plan/commit.rs:362` commits `InsertOp::Append` through `tx.fast_append()`; `physical_plan/mod.rs` declares `pub(crate) mod commit;` and `pub(crate) mod write;`, and the crate root re-exports neither exec, so the plan cannot be rebuilt or replaced from RePark. Registry `ICE-MERGE-APPEND-INSERT-1` (BACKLOG 2026-09-19, TRIGGER fork ask `ICE-MERGE-APPEND-COMMITEXEC`), pinned both ways: today's 100 manifests plus the plan node, and a strict xfail on Spark's 1. |
| C-010 | No dependency moves: `git diff main -- Cargo.toml Cargo.lock` is empty at hand-back. | The diff. | **PROVEN** | Empty. The fork pin stays `44834673`; this unit consumes `Transaction::merge_append()`, which that pin already carries. |

VERDICT: 10 clauses, 10 PROVEN, 0 OPEN, 0 REJECTED.

## 2. The cost of merging (C-007)

120 sequential single-row appends through the RePark-owned append door
(`INSERT INTO … BY NAME`), default release profile, same head, same harness
(`one_run.py`: a fresh process, a fresh memory catalog and a fresh warehouse per run,
7 runs per cell). **Before** = the four sites on `fast_append`; **after** = on `merge_append`.

The box is shared with other agents and its load average sat between 20 and 25 during both
runs, so wall clock is not a usable statistic here (the same cell ranged 4.9 s to 78 s).
Process CPU time is, because it does not count the time the process spent descheduled; the
median of 7 is the headline and the `merge_disabled` cell is the control — it does no merging
in either build, so it prices the noise between the two builds.

| cell | before (CPU s, median of 7) | after (CPU s, median of 7) | manifests at 120 |
|---|---|---|---|
| `defaults` | **4.25** (min 4.04, max 5.25) | **3.03** (min 2.77, max 3.27) | 120 → **21** |
| `merge_disabled` (control) | 3.91 (min 3.79, max 4.36) | 3.58 (min 3.51, max 3.89) | 120 → 120 |

**Merging ordinary appends is cheaper, not dearer: −29 % of process CPU, −22 % after
normalising by the control** (before 4.25/3.91 = 1.087; after 3.03/3.58 = 0.846). The reason is
visible in the same table: a merging commit does read and rewrite manifests, but it keeps the
manifest list short, and every *subsequent* commit then carries a list of 21 entries instead of
120. The merge pays for itself well before the hundredth append. Wall clock agreed in direction
(min-of-7: 9.82 s before, 4.93 s after) but is reported only as a direction.

Scope of the claim: single-row appends into an unpartitioned v2 table on local FS. A workload
whose manifests are large enough to reach `commit.manifest.target-size-bytes` (8 MB) would
bin-pack differently and is not measured here.

## 3. Mutations (C-008)

Each applied, run, recorded and reverted; `git status` clean after each.

| # | Mutation | Expected | Measured |
|---|---|---|---|
| M1 | All four commit sites routed back to `.fast_append()` | C-002 and C-003 red | **6 of 7 Rust pins red**: `defaults_series`, `min_count_to_merge_series`, `merge_never_mixes_partition_spec_ids`, `delete_manifests_carry_forward`, `sequence_numbers_survive`, `branch_merging_append`. `merge_disabled_series` stayed **green** — correct: C-004 asserts today's behaviour. |
| M2 | `commit.manifest-merge.enabled=false` no longer set on the table (the property ignored) | C-004 red | **Exactly `merge_disabled_series_matches_spark` red**, 6 green. The series collapsed to the `defaults` shape, which is what "the property is ignored" looks like. |
| M3 | `commit.manifest.min-count-to-merge=5` no longer set on the table | C-003 red | **Exactly `min_count_to_merge_series_matches_spark` red**, 6 green. |

M2 and M3 are applied at the property RePark sets on the table rather than at a reading site,
because the three `commit.manifest*` properties are read fork-side, in
`MergeSettings::from_table`. That is the point this repository controls, and the mutation
answers the same question: if the property did not reach the merge, would the pin notice?

## 4. What was run

| gate / suite | result |
|---|---|
| `cargo test -p repark-iceberg` | 591 passed, 0 failed (584 before this unit's 7 pins) |
| `cargo test -p repark-spark` | 1297 passed, 0 failed, 5 ignored (`ctas.rs` changed) |
| `python/repark/tests/test_ice_merge_append_1.py` | 9 passed, 2 xfailed |
| 15 further Iceberg pytest files (OCC storms, RTAS by name, write options ×2, rowid order, branch ops, sorted insert, array insert, evo DML, v3 write default, dyn overwrite ×2, overwrite mode, hadoop vN, spark table) | 661 passed, 33 skipped, 5 xfailed, 0 failed |
| `cargo fmt --all` / `cargo clippy -p repark-iceberg --all-targets -- -D warnings` | clean |
| `comment_ban.py` | `comment-ban hits=0` |
| `make ci` guards run by the repo's pre-commit hook | clean on every commit |

`make verify`, `make preflight`, a whole-workspace `cargo test`, the full Python facade suite
and the parity harness were **not** run — out of this unit's sanctioned surface.

## COVERAGE ATTESTATION

```
COVERAGE_ATTESTATION:
  pr_unit: ice-merge-append-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against its pins; the three recorded series are compared as whole nine-point values, and the headline cell (100 default appends -> ONE manifest, 100 files, 100 rows) is pinned separately end to end.
      artifacts: [crates/repark-iceberg/src/tests/merge_append_series.rs, python/repark/tests/test_ice_merge_append_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The merge boundary itself is the edge case - probes sit at 99, 100 and 101 in every variant; a table with two partition specs, a table carrying a DELETE manifest, a v3 table with row lineage, and a branch whose list merges at a different append than main's are each pinned.
      artifacts: [crates/repark-iceberg/src/tests/merge_append_series.rs, python/repark/tests/test_ice_merge_append_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: No error contract changes - merge_append mirrors fast_append's public surface and its commit errors fold through the same commit_result/commit_err path, including the CommitStateUnknown arm. The two declared gaps fail loudly (strict xfail) rather than silently.
      artifacts: [crates/repark-iceberg/src/write/commit_error.rs, python/repark/tests/test_ice_merge_append_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: Manifest merging rewrites the manifest list at commit, which is exactly what a concurrent appender races. The repo's storm pins were run - cargo test -p repark-iceberg 591/0 including write/merge/tests/occ*.rs, and test_ice_occ_scoped_1.py green - and the stale-handle append pin (append_commit_seam_racing_append_both_land) still lands through refresh-and-re-apply.
      artifacts: [crates/repark-iceberg/src/write/merge/tests, python/repark/tests/test_ice_occ_scoped_1.py]
    - id: AT-5
      status: N/A
      justification: No auth, secret, deserialization or path handling. The change swaps one transaction action for another on the same catalog handle; the merged manifests are written by the fork's own writer to the table's own location.
    - id: AT-6
      status: ATTACKED
      evidence: The Spark-visible behaviour is the whole unit and is pinned against a recorded 4.1.2 oracle, not a hand-computed expectation; the producer mapping is disassembled from the same artifact. Registry rows filed with dates, unit id and named fork asks; every touched map.md updated in lockstep.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/ice_merge_append_1_truth.json]
    - id: AT-7
      status: ATTACKED
      evidence: A merging commit reads and rewrites manifests, so the cost was measured rather than assumed - 120 sequential appends before and after on the same head, 7 runs per cell, with a merge_disabled control to price the cross-build noise. Result: -29 percent process CPU (-22 percent normalised), because the shorter manifest list makes every later commit cheaper.
      artifacts: [task/ledgers/staging/ice-merge-append-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: Live-recorded Spark 4.1.2 + Iceberg 1.11.0 is the honoured contract on every cell. Zero dependency movement - git diff main -- Cargo.toml Cargo.lock empty, fork pin unchanged at 44834673. The escape hatch (commit.manifest-merge.enabled=false) is pinned to behave exactly as main does today.
      artifacts: [python/repark/tests/ice_merge_append_1_truth.json, docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: N/A
      justification: Synchronous library path with no ops surface; a failed merging commit reaches the caller as the same typed DataFusion error a failed fast append did, with the same operation-id stamp for reconciliation.
    - id: AT-10
      status: ATTACKED
      evidence: Red-first battery (red on this tree with RePark's measured numbers, green after the routing); mutations M1/M2/M3 each red their named subset and nothing else, and M1 deliberately leaves the merge_disabled pin green.
      artifacts: [crates/repark-iceberg/src/tests/merge_append_series.rs]
  reattested: []
  complete: true
```
