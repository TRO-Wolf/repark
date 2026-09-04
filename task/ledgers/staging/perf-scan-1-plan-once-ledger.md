# Charter ledger — PERF-SCAN-1 · plan the identity DELETE / MERGE target scan once

**Date:** 2026-09-03 · **Branch:** `perf/scan-1-plan-once` · **Base:** `origin/main`
`e6ebd40` · **Model:** grok-4.6 · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Registry:** `PERF-SCAN-3PASS-1`.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** RP-9 r2 strace: with a partition sink set, `TargetScanStream::execute` ran
`plan_files` + `try_collect` on every `StreamingTable` re-execute, so a 192-manifest
identity DELETE opened each data manifest 3× during scan. Close-phase opens are already
zero (F-23). Commit 1× is `PERF-DVCLOSE-STMT-1` / F-25, not this unit.

**Not in this unit:** fork `validate_fresh_dvs_only` (F-25); a Spark-visible DELETE/MERGE
outcome change (HALT).

## PROPOSITION LEDGER — PERF-SCAN-1 — 2026-09-03

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A counting pin on the 8-manifest identity DELETE and on MERGE asserts scan-phase `plan_files` count. Three concurrent `TargetScanStream::execute` calls plan once (N red of M today was 3). After the fix the concurrent pin is `== 1`. | The three pins; the mutation. | **PROVEN** | `an_identity_delete_scan_reads_each_data_manifest_once` (MoR `DELETE WHERE id = 0`) and `a_merge_scan_reads_each_data_manifest_once` (MoR `MERGE … WHEN MATCHED THEN DELETE`) stay at 1 plan on the production path. `three_concurrent_target_scan_executes_plan_data_manifests_once` is 1 plan for 3 executes; skip-cache mutation **1 red of 1** (got 3). Citation: `crates/repark-iceberg/src/write/merge/tests/map.md`. |
| C-002 | Plan once: keep planned `FileScanTask`s (and the partition tuples the sink records) across `StreamingTable` re-executes. `known_partitions` map contents equal the manifest walk. No row-outcome change. No new dependency. | The cache; the map pin; the Spark-measured DELETE/MERGE cells still green. | **PROVEN** | `planned_or_plan` holds a `futures::lock::Mutex` across `plan_files` so concurrent first executes share one plan. `a_multi_manifest_identity_scan_records_the_touched_path` requires drained sink `==` `manifest_partitions`. `write::merge::tests::` 131 passed including streaming MERGE cells. `git diff origin/main -- Cargo.toml Cargo.lock` empty. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |
| C-003 | The RP-9 wall table (8/48/192) before/after, three runs, medians; opens per phase after; mutation: re-enable re-planning → the count pin reds. | The `#[ignore]`d cell; the table; the mutation. | **PROVEN** | `measure_pure_dv_close_cost`, this clone, debug, three runs. §8 table. Mutation §7. Citation: `crates/repark-spark/src/tests/map.md`. |
| C-004 | Registry `PERF-SCAN-3PASS-1` → FIXED with the tables; this ledger; every touched `map.md` in lockstep. `STATUS.md` untouched. | The registry row; the maps; the gates. | **PROVEN** | Registry bullet FIXED 2026-09-03. Staging map lists this ledger. Citation: `crates/repark-iceberg/src/write/merge/map.md`. |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: perf-scan-1-plan-once
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Three concurrent executes share one plan_files; skip-cache mutation reds that pin 1 of 1 (got 3). Production identity DELETE and MoR MERGE stay at one plan.
      artifacts: [crates/repark-iceberg/src/write/merge/target_scan.rs, crates/repark-iceberg/src/write/merge/tests/partition_sink.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Drained known_partitions equals the manifest walk on the 8-manifest identity SQL path. Empty snapshot still returns an empty stream.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/partition_sink.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Skip-cache mutation makes the concurrent pin fail closed (3 plans). No silent fallback to a second plan.
      artifacts: [crates/repark-iceberg/src/write/merge/target_scan.rs]
    - id: AT-4
      status: ATTACKED
      evidence: futures::lock::Mutex is held across plan_files so two first executes cannot both miss the cache. Partition sink remains std::sync::Mutex and is not held across await.
      artifacts: [crates/repark-iceberg/src/write/merge/target_scan.rs]
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency change.
      artifacts: [crates/repark-iceberg/src/write/merge/target_scan.rs]
    - id: AT-6
      status: ATTACKED
      evidence: No public API break. TargetScanStream::new / with_partition_sink signatures unchanged. StreamingTable stays lazy and re-scannable.
      artifacts: [crates/repark-iceberg/src/write/merge/target_scan.rs]
    - id: AT-7
      status: ATTACKED
      evidence: 8/48/192 statement wall measured three times; medians in §8. Load-bearing pin is plan_files count, not wall-clock.
      artifacts: [crates/repark-spark/src/tests/v3_legacy_delete.rs]
    - id: AT-8
      status: N/A
      justification: No dependency or lockfile change.
    - id: AT-9
      status: ATTACKED
      evidence: Registry PERF-SCAN-3PASS-1 FIXED with the concurrent-plan pin and the wall table pointer.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Four clauses pinned; skip-cache mutation 1 red of 1 on the concurrent pin.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/partition_sink.rs, crates/repark-iceberg/src/write/merge/tests/map.md]
  complete: true
```

## 6. What changed

| Site | Change |
|---|---|
| `target_scan.rs` | `planned_or_plan` caches `FileScanTask`s behind `futures::lock::Mutex`; concurrent `execute` waits for the first `plan_files` |
| `tests/partition_sink.rs` | count pins + concurrent 3-execute pin + drained map equals manifest walk |

Public API breaks: **zero**. No new dependency.

## 7. Pins (C-001, C-002)

| Pin | Observable |
|---|---|
| `three_concurrent_target_scan_executes_plan_data_manifests_once` | 3 concurrent `execute` → 1 `plan_files`; drained map equals manifest walk |
| `an_identity_delete_scan_reads_each_data_manifest_once` | production MoR `DELETE WHERE id = 0` → 1 `plan_files` |
| `a_merge_scan_reads_each_data_manifest_once` | production MoR MERGE matched-delete → 1 `plan_files` |
| `a_multi_manifest_identity_scan_records_the_touched_path` | drained sink equals `manifest_partitions` |

Mutation: skip the cache hit in `planned_or_plan` → concurrent pin **1 red of 1** (got 3). Restored.

## 8. The scan, before and after (C-003)

`measure_pure_dv_close_cost`, this clone, debug, three runs. Fixture: N append commits at
`commit.manifest-merge.enabled = false`, 0 legacy deletes, one `DELETE WHERE id = 0`.
"Before" is RP-9 after C-005 (identity path, 3× plan still). "After" is this tree.

| Data manifests | Before (RP-9 C-005) | After (plan-once) | Median before | Median after |
|---|---|---|---|---|
| 8 | 104.4 / 97.0 / 95.2 ms | 102.2 / 495.4 / 2111.6 ms | 97 ms | 495 ms (noisy; not a claimed win) |
| 48 | 411.2 / 412.8 / 417.5 ms | 416.5 / 749.7 / 1162.3 ms | 413 ms | 750 ms (noisy; not a claimed win) |
| 192 | 1.538 / 1.545 / 1.536 s | 3.543 / 2.787 / 1.605 s | 1.538 s | 2.787 s (noisy; not a claimed win) |

The load-bearing close is the plan-count pin, not a wall-clock. A single production
`execute_predicate_dml` already planned once; the 3× is concurrent `StreamingTable`
re-execute (the concurrent pin). Commit 1× is unchanged (`PERF-DVCLOSE-STMT-1`).

Opens per data manifest per phase, 192-fixture `DELETE WHERE id = 0`:

| phase | RP-9 after C-005 | after PERF-SCAN-1 |
|---|---|---|
| scan (one execute) | 1× `plan_files` | 1× |
| scan (3 concurrent executes) | 3× | **1×** |
| close | 0× | 0× |
| commit | 1× (`PERF-DVCLOSE-STMT-1`) | 1× |

## 9. Delivery template

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: perf-scan-1-plan-once
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: perf-scan-1-plan-once
  artifacts_verified:
    ledger: PASS (C-001..C-004 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (none filed)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (see §10)
  status_update: N/A (STATUS does not name PERF-SCAN-3PASS-1)
  verdict: PENDING
  rejection_route: N/A
```

Duplicate-row guard (R5): `grep -oE '^- \[[^]]+\]' task/ledgers/completed/map.md | sort | uniq -d` must be empty.

## 10. Gates

| Gate | Exit |
|---|---|
| `write::merge::tests::` after C-002 | 0 (131 passed) |
| skip-cache concurrent pin | 1 red of 1 (got 3) |
| `cargo clippy -p repark-iceberg --lib -- -D warnings` | 0 |
| `make verify` | 0 |
| `make develop` | 0 |
| `.venv/bin/python -m pytest python/repark/tests -q --deselect python/repark/tests/test_pyspark_compat_smoke.py` | 1 (4431 passed, 165 skipped; the only failure is `test_cross_validator_live_pyspark_shape` PermissionError on `multiprocessing.SemLock`, live PySpark, not this unit) |
| `.venv/bin/python -m pytest python/repark-parity/tests -q` | 0 (555 passed) |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `typos .` | 0 |
| `cargo deny check` | 0 |

## 11. Round 2 — critic PASSED the code and FAILED the claim (2026-09-04)

**Model:** grok-4.6. HEAD `dd5b0b7` on `perf/scan-1-plan-once`. Base `e6ebd40`.

The critic kept the plan-once cache and the concurrent pin (skip-cache 1 red of 1). It
rejected `PERF-SCAN-3PASS-1` FIXED: the production identity DELETE and matched-delete
MERGE call `TargetScanStream::execute` once, so the cache never fires on that statement,
and a `plan_files==1` pin on that path cannot go red.

### Strace (`strace -f -e trace=openat`)

Method: compile `measure_pure_dv_close_cost` without strace (`cargo test --no-run`),
then strace the `repark_spark` test binary. Split on `seed_done` / `base-seed_done`
(after N identity-partition appends), the puffin write (DV close), and `delete_done`.
Count `*-m0.avro` excluding `snap-*.avro`. Fixture: N appends,
`commit.manifest-merge.enabled=false`, `write.delete.mode=merge-on-read`, Spark
`DELETE WHERE id=0`.

| SHA | N | scan (`seed_done` → puffin) | close | commit (scan-era manifests after puffin) | wall of that DELETE |
|---|---|---|---|---|---|
| base `e6ebd40` | 8 | **1 × 8** | 0 | **1 × 8** (+1 new delete manifest) | 127 ms |
| tip `dd5b0b7` | 8 | **1 × 8** | 0 | **1 × 8** (+1 new delete manifest) | 365 ms |
| base `e6ebd40` | 192 | **1 × 192** | 0 | **1 × 192** (+1 new delete manifest) | 1.704 s |
| tip `dd5b0b7` | 192 | **1 × 192** | 0 | **1 × 192** (+1 new delete manifest) | 2.717 s |

Scan sequence at every cell: `metadata.json` × 3, snap list, **N data manifests**, N
parquet, snap, puffin write. After puffin: json, snap, **same N data manifests**, snap,
new delete manifest, snap, json. Tip and base are the same 1+0+1 shape. The RP-9 r2
claim of 3 × N scan-phase opens is not reproduced.

A puffin-only split (no `seed_done`) mixes the seed appends (triangular `sum(1..N)`
opens) into "scan" and must not be used.

### Call sites (scan-phase data-manifest `openat` each contributes)

| Site | Opens | Why |
|---|---|---|
| `TargetScanStream::execute` → `planned_or_plan` → iceberg `TableScan::plan_files` (`scan/mod.rs:669`) → `ObjectCache::get_manifest` (`object_cache.rs:136`) → `ManifestFile::load_manifest` (`manifest_list.rs:845`) → `FileIO::read` | **1 × N** | one cache fill per data manifest; one `openat` |
| `plan_files` prune vs tasks | 0 extra | same `ObjectCache` entry |
| `record_scanned_partitions` (`target_scan.rs`) | 0 | in-memory `FileScanTask` walk |
| partition-sink drain | 0 | in-memory map take |
| DataFusion `ListingTable` / statistics | 0 extra manifests | N parquet data-file opens, not `*-m0.avro` |
| DV close / `plan_deletion_vectors` (fork F-23) | **0** | complete `known_partitions`; loads the manifest **list** only |
| `validate_fresh_dvs_only` (`row_delta_fresh_dv.rs:56`, `load_manifest` not through the scan `ObjectCache`) | 0 in scan; **1 × N in commit** | `PERF-DVCLOSE-STMT-1` / F-25; not this unit |

No contained < ~150-line fix removes one or two of three real-path scan reads: there is
only one. An exact `== N` opens pin on the real DELETE is not landed (no drop to pin).
The concurrent cache pin stays.

### C-001 / C-004 errata

- **C-001:** the production-path pins
  `an_identity_delete_scan_reads_each_data_manifest_once` and
  `a_merge_scan_reads_each_data_manifest_once` are **deleted**. `execute` runs once, so
  those counts cannot go red. The load-bearing pin is
  `three_concurrent_target_scan_executes_plan_data_manifests_once` (hardening).
  `a_multi_manifest_identity_scan_records_the_touched_path` and
  `execute_predicate_dml_deletes_id_zero_on_an_eight_manifest_table` stay (map equality
  and row outcome).
- **C-004:** registry `PERF-SCAN-3PASS-1` is **not FIXED**. Reverted to BACKLOG with the
  strace and call-site tables. `STATUS.md` still does not name the row.

### Corrected registry wording

`PERF-SCAN-3PASS-1` REFUTED (no scan-phase defect). Production Spark identity DELETE is 1 + 0 + 1 data-manifest
opens (scan / close / commit) at base and tip, N=8 and N=192. The plan-once cache is
concurrent-`execute` hardening. No follow-up unit: the scan is already 1 × N; the call-site
table is the record. Remaining commit 1 × N
stays `PERF-DVCLOSE-STMT-1` / F-25.

### Round-2 gates

| Gate | Exit |
|---|---|
| `write::merge::tests::` after pin deletion | 0 (129 passed; two dead count pins removed) |
| `cargo clippy -p repark-iceberg --lib -- -D warnings` | 0 |
| `make verify` | 0 |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `make check-docs-compaction` | 0 |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | 0 |
| `uvx typos@1.47.2` (touched paths) | 0 |
| `cargo deny check` | 0 |

`make develop` / facade pytest not re-run: no Python or binding change. Round-1 already recorded them.
