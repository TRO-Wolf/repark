# Unit ledger — ICE-INTEGRITY-PINS-1 · pin K, a writer killed between its data files and its commit

**Date:** 2026-10-22 · **Branch:** `feat/ice-integrity-pins-1-k` · **Base:** `18626942` (`main`)
**Model:** claude-opus-5-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Release:** 1.5.6.
**Sketch:** [../../wo/ice-integrity-pins-1/sketch.md](../../wo/ice-integrity-pins-1/sketch.md)
(§3, the owner's rulings 79 to 88 of 2026-10-15). **Plan row:** U-9 of the Iceberg closeout.

**This round builds pin K only** (sketch §3): the local Rust pin, option K-A, test code only
(ruling 79), the orphan count under ruling 82's ceiling, the local catalog's commit rule as
ruling 83 states it. Pins U and X (live Glue and S3 Tables, the cross-release leg) await the
owner's workflow wiring (#1070, ruling 86); their rows below stay OPEN.

**Fork:** `d80a2884` (`Cargo.lock`). Every fork line below is on that rev.

## Orchestrator rulings (2026-10-22, under the owner's overnight grant)

Recorded as "orchestrator ruling K-n, 2026-10-22, under the owner's grant"; the owner reads them
in the morning.

- **K-1 (step-0 Q-A, K-7's answer).** K-7 stays (18 cells). On the local catalog the CTAS commit
  point is the staged metadata write inside the fork's `StagedTableTransaction::commit`
  (`crates/iceberg/src/transaction/staged_table.rs:271-306`), reached before
  `publish_create_table`, under ruling 83's rule (the newest metadata file is the commit on the
  local catalog). A kill at the hold therefore reads back as the created table with every row
  and no orphan. H-K2 does not fire: this is the declared local rule, not a defect, and no
  earlier point is reachable without product code (ruling 79). **The answer is claimed for the
  local catalog only.** On Glue and S3 Tables the catalog pointer governs, so a CTAS kill there
  is pin U's cell (expected "no table"), unmeasured here.
- **K-2 (H-K1).** Not a halt. `publish_create_table` is a catalog call the test decorator reaches
  with no product code, the second hook point sketch §7 anticipated; it is K-7's hold point.
- **K-3 (step-0 Q-B, the orphan assertion).** Assert ruling 82's ceiling (every orphan is in the
  attempted snapshot's new-file set, read from the held commit's `take_updates()`), the measured
  per-kind counts per door, and visible rows equal to the pre-crash rows.
- **K-4 (step-0 Q-C, the reopen).** A fresh session adopts the newest metadata file through
  `CALL ice.system.register_table`.

## Corrections to the sketch's premises (re-read on `18626942` and fork `d80a2884`)

- Fork lines moved: `Transaction::commit` `mod.rs:373` (sketch `:379`), `do_commit` `:495`
  (`:501`), `catalog.update_table` `:574` (`:582`), `reconcile_unknown_commit_outcome` `:424`
  (`:430`). The memory catalog's `update_table` (`memory/catalog.rs:548`) and its metadata write
  (`:584`) did not move.
- The `Catalog` trait gained `publish_create_table` and `publish_replace_table`
  (`catalog/mod.rs:180`, `:193`) beside defaulted `name`, `properties`, `invalidate_*` and view
  methods. The test decorator forwards every method, modelled on `EncryptionGuardCatalog`
  (`crates/repark-iceberg/src/catalog/encryption_guard.rs:326-497`); a defaulted `properties()`
  would break the CTAS `FileIO` and a defaulted `publish_replace_table` refuses. The decorators
  of `describe_view_routing.rs` forward neither, so `register_catalog` (which takes a `TempDir`)
  is mirrored by a path-taking `register_hold` beside the pin.
- `TableCommit` lends no view of its updates and its builder is `pub(crate)`. The hold never
  forwards, so it consumes the commit (`take_updates()`); no fork change.
- `type=hadoop` maps to the memory catalog with Hadoop naming (`crates/repark-core/src/catalog_kind.rs:42-52`),
  not to plain memory; M1's `00002-<uuid>` names are uuid naming. The pin runs on the test
  catalog's uuid naming.
- Not acted on, flagged for pin U: the fork's `do_commit` already checks
  `commit_status::rejected_attempt_landed` before re-basing (`mod.rs:497`,
  `commit_status.rs:273`), which reads as the first half of ruling 84's fork card. Glue and
  S3 Tables lines moved too (`glue/commit_transport.rs:385`, `:414`; the S3 Tables SDK call
  `:160`).

## PROPOSITION LEDGER — ICE-INTEGRITY-PINS-1 (pin K) — 2026-10-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Each kill cell's child reaches its hold point and dies of SIGKILL there: `update_table` for K-1 to K-6 (`INSERT … SELECT`, the same with a session write key, `INSERT OVERWRITE`, `MERGE` copy-on-write, `MERGE` merge-on-read, `DELETE` copy-on-write), `publish_create_table` for K-7 (CTAS), on format v2 and v3; the marker names one attempted snapshot. | `tests::kill_mid_write::k*_v2`, `k*_v3` (14 cells). | PROVEN | Red first below; green in step 2 (§Green). |
| C-002 | On reopen (a fresh session adopting the newest metadata file through `CALL ice.system.register_table`), K-1 to K-6 read the pre-state: the newest metadata file, the current snapshot id, the snapshot id set, and the rows with their Arrow types equal the pre-crash table; the attempted snapshot id is absent. | The 12 cells K-1 to K-6. | PROVEN | Red first below. Green in step 2 (§Green). |
| C-003 | The orphans (files under the table location that no snapshot of the reopened table reaches) are within ruling 82's ceiling (each one is a file the crashed commit wrote) and equal that set; per kind (data, deletes, manifests, lists, metadata): K-1 and K-2 1/0/1/1/0, K-3, K-4 and K-6 1/0/2/1/0, K-5 1/1/2/1/0 (a parquet position-delete file on v2, a puffin deletion vector on v3); each cell prints `K-n v<f> orphans data=… deletes=… manifests=… lists=… metadata=…`. | The 12 cells K-1 to K-6. | PROVEN | Measured at step 0 on the hook's entry; asserted in step 2. Green in step 2 (§Green). |
| C-004 | With every file time set two days back, `CALL ice.system.remove_orphan_files(dry_run => true, older_than => now - 25 h)` lists exactly the counted set. | All 18 cells. | PROVEN | Green in step 2 (§Green). |
| C-005 | The reopened table is writable: one more `INSERT` commits and reads back with the reopened rows unchanged. | All 18 cells. | PROVEN | Green in step 2 (§Green). |
| C-006 | K-7 on the local catalog (orchestrator ruling K-1): the staged metadata file is the commit, so the reopen reads the created table, the attempted snapshot current and alone, every CTAS row, and no orphan. Claimed for the local catalog only; Glue and S3 Tables follow their pointer (pin U's cell, unmeasured). | `k7_ctas_killed_at_publish_reads_created_on_the_local_catalog_v2`, `_v3`. | PROVEN | Green in step 2 (§Green). |
| C-007 | Controls: K-C0 (the hook forwards at once) commits, the child exits 0, the reopen reads the statement's rows once, orphans 0; K-C1 (the hook forwards, then holds and is killed) reads back as committed from the metadata file written inside `update_table` (`memory/catalog.rs:584`): ruling 83's local rule. | `kc0_*_v2`, `_v3`, `kc1_*_v2`, `_v3`. | PROVEN | Green in step 2 (§Green). |
| C-008 | K-2 is the routed append: its attempted snapshot carries the session snapshot property `k` (`router.rs:452-459` → `commit_append_with_summary`, `append_with_options.rs:90`); no other cell's does. | All 18 cells assert the key's presence equals the cell's session key. | PROVEN | Green in step 2 (§Green). |
| C-009 | Every assertion family fails for its reason: M1 (the hook forwards before it holds) reds the 12 K-1 to K-6 cells; M2 (the decorator not installed) reds all 18; M3 (the orphan count skips manifests) reds the 12 K-1 to K-6 cells. | §Red first, §Revert check. | PROVEN | §Revert check (step 3). |
| C-010 | No flake: the 18 kill cells pass 20 runs in a row under the 40 G cap; per-cell run time recorded. | §No flake. | PROVEN | 20 of 20 (§No flake, step 3). |
| C-011 | Records: the registry row beside ICE-COMMIT-UNKNOWN-1 names the local catalog's commit rule (ruling 83); production status row K3 carries the dated pin-K line; the sketch §3 carries a dated "built" line; the work-order map and the 1.5.6 notes carry the unit. | The diffs of the records commit. | PROVEN | §Records (step 4). |
| C-012 | Pin U: an unknown commit outcome drilled live on Glue and S3 Tables and resolved on reopen. | Sketch §4. | OPEN (awaits #1070) | Not this round: the owner's workflow wiring (ruling 86) comes first. |
| C-013 | Pin X: the v1.5.4 wheel and the head read each other's tables on both catalogs and both format versions. | Sketch §5. | OPEN (awaits #1070) | Not this round, as C-012. |

## Red first (step 1, 2026-10-22)

The pins and the decorator were written in the working tree on `18626942` and run before any
commit. The red runs are the decorator taken away (M2) and the hook made to forward before it
holds (M1, sketch §9's first mutant); each mutation was reverted after its run. Command:
`cargo test --locked -p repark-spark --lib kill_mid_write -- --test-threads=4` under
`systemd-run --user --scope -p MemoryMax=40G`, `taskset -c 0-25`. Temporary paths are shown as
`<tmp>`.

**M2, no decorator** (the child registers the plain session catalog as `fault`): 18 of 18 red.

```
test tests::kill_mid_write::k1_insert_select_killed_at_the_commit_v2 ... FAILED
… (all 18 cells FAILED)
test result: FAILED. 1 passed; 18 failed; 0 ignored; 0 measured; 2784 filtered out; finished in 0.95s
K-1 v2: the child exited (exit status: 0) before the hold
K-7 v3: the child exited (exit status: 0) before the hold
```

The one passing test is the child entry, which returns at once outside a child.

**M1, the hook forwards before it holds:** the 12 K-1 to K-6 cells red, K-7 and the controls
green (K-7's answer is the created table either way, ruling K-1).

```
test result: FAILED. 7 passed; 12 failed; 0 ignored; 0 measured; 2784 filtered out; finished in 1.67s
thread 'tests::kill_mid_write::k1_insert_select_killed_at_the_commit_v3' panicked at crates/repark-spark/src/tests/kill_mid_write.rs:480:13:
  left: "<tmp>/warehouse/sales/t/metadata/00002-f78133ce-73af-4410-97bc-ea8989052474.metadata.json"
 right: "<tmp>/warehouse/sales/t/metadata/00001-933aea55-ddce-435b-b9c3-27d357c19054.metadata.json"
```

The same `00002` against `00001` line for each of K-1 to K-6 on v2 and v3: the reopen read the
committed table.

## Green (step 2, 2026-10-22)

`crates/repark-spark/src/tests/kill_mid_write.rs` (the cells, the child entry, the reopen and
the checks) and `kill_mid_write_hold.rs` (`HoldCatalog`, its marker, the file sets), both
`#[cfg(unix)]` in the repark-spark lib test binary; `common.rs` gains `setup_v3_at` (a session
over a warehouse path). No product file, no fork file, no `Cargo.toml`, no new test binary.
The decorator holds by awaiting a future that never completes after the marker is written
(renamed into place); the parent waits for the marker under a 60 s deadline, sends SIGKILL and
asserts signal 9. Command: `cargo test --locked -p repark-spark --lib kill_mid_write --
--nocapture --test-threads=1` under the 40 G scope.

```
K-1 v2 orphans data=1 deletes=0 manifests=1 lists=1 metadata=0
K-1 v3 orphans data=1 deletes=0 manifests=1 lists=1 metadata=0
K-2 v2 orphans data=1 deletes=0 manifests=1 lists=1 metadata=0
K-2 v3 orphans data=1 deletes=0 manifests=1 lists=1 metadata=0
K-3 v2 orphans data=1 deletes=0 manifests=2 lists=1 metadata=0
K-3 v3 orphans data=1 deletes=0 manifests=2 lists=1 metadata=0
K-4 v2 orphans data=1 deletes=0 manifests=2 lists=1 metadata=0
K-4 v3 orphans data=1 deletes=0 manifests=2 lists=1 metadata=0
K-5 v2 orphans data=1 deletes=1 manifests=2 lists=1 metadata=0
K-5 v3 orphans data=1 deletes=1 manifests=2 lists=1 metadata=0
K-6 v2 orphans data=1 deletes=0 manifests=2 lists=1 metadata=0
K-6 v3 orphans data=1 deletes=0 manifests=2 lists=1 metadata=0
K-7 v2 orphans data=0 deletes=0 manifests=0 lists=0 metadata=0
K-7 v3 orphans data=0 deletes=0 manifests=0 lists=0 metadata=0
K-C0 v2 orphans data=0 deletes=0 manifests=0 lists=0 metadata=0
K-C0 v3 orphans data=0 deletes=0 manifests=0 lists=0 metadata=0
K-C1 v2 orphans data=0 deletes=0 manifests=0 lists=0 metadata=0
K-C1 v3 orphans data=0 deletes=0 manifests=0 lists=0 metadata=0
test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured; 2784 filtered out; finished in 30.33s
```

The 19th test is the child entry. K-5's delete file is a parquet position-delete file on v2 and
a puffin deletion vector on v3 (step-0 probe on the hook's entry). Run times: §No flake.

## Revert check (step 3, 2026-10-22)

On the committed pins (`55b25bc7`), each mutation applied alone to the working tree, run, and
reverted (the tree clean after each): `cargo test --locked -p repark-spark --lib kill_mid_write
-- --test-threads=4` under the 40 G scope.

| Mutation | Red cells | First failing assertion |
|---|---|---|
| M1, the hook forwards before it holds (`update_table` → `forward_commit`, which then holds) | 12: K-1 to K-6, v2 and v3 | `K-1 v3: the newest metadata file is the pre-state's`, left `…/metadata/00002-….metadata.json`, right `…/metadata/00001-….metadata.json` |
| M2, the decorator disabled (the child registers the plain session catalog as `fault`) | 18 of 18 | `K-2 v3: the child exited (exit status: 0) before the hold` |
| M3, the orphan count skips manifests | 12: K-1 to K-6, v2 and v3 | `K-2 v3: orphans per kind`, left `[1, 0, 0, 1, 0]`, right `[1, 0, 1, 1, 0]` |
| M4, the child drops the session write key | 2: K-2 v2 and v3 | `K-2 v2: the session snapshot property marks the routed append`, left `false`, right `true` |

K-7 and the controls stay green under M1 and M3 by design: K-7's answer is the created table
whichever way the hook forwards (ruling K-1), and the controls have no orphan.

## No flake (step 3, 2026-10-22)

The committed pins, the test binary run directly 20 times in a row
(`<binary> kill_mid_write --nocapture`, default threads) under
`systemd-run --user --scope -p MemoryMax=40G` and `taskset -c 0-25`: **20 of 20 runs passed, 18
of 18 cells each run.** Another agent's build shared the machine (load average 27 to 38), so the
per-cell times below are a loaded reading; a quiet reading of the same cells, taken before step
2 split `kill_cell` into helpers (no assertion changed), sits beside it (load average about 3,
also 20 of 20).

| Cell | Loaded: median ms (min to max), 20 runs | Quiet: median ms (min to max), 20 runs |
|---|---|---|
| K-1 v2 | 2114 (1157 to 5045) | 415 (387 to 544) |
| K-1 v3 | 2111 (1108 to 5360) | 407 (386 to 561) |
| K-2 v2 | 2128 (1118 to 5042) | 409 (377 to 549) |
| K-2 v3 | 2096 (1192 to 5042) | 411 (391 to 499) |
| K-3 v2 | 2132 (1154 to 5045) | 431 (401 to 562) |
| K-3 v3 | 2110 (1152 to 5057) | 433 (411 to 622) |
| K-4 v2 | 2252 (1192 to 5355) | 529 (493 to 691) |
| K-4 v3 | 2240 (1194 to 5357) | 549 (506 to 712) |
| K-5 v2 | 2368 (1460 to 5426) | 539 (499 to 717) |
| K-5 v3 | 2317 (1480 to 5452) | 546 (513 to 760) |
| K-6 v2 | 2115 (1196 to 5057) | 444 (412 to 618) |
| K-6 v3 | 2127 (1153 to 5034) | 447 (419 to 638) |
| K-7 v2 | 1657 (636 to 3894) | 291 (276 to 404) |
| K-7 v3 | 1456 (768 to 3907) | 290 (276 to 388) |
| K-C0 v2 | 2174 (1259 to 5357) | 530 (480 to 677) |
| K-C0 v3 | 2324 (1232 to 5361) | 540 (490 to 701) |
| K-C1 v2 | 2323 (1235 to 5355) | 526 (494 to 676) |
| K-C1 v3 | 2321 (1232 to 5355) | 524 (489 to 709) |

Whole filter, wall clock per run: loaded 1.6 to 5.6 s; quiet 0.60 to 0.84 s. One serial run
(`--test-threads=1`): loaded 37.2 s, quiet 7.9 s. Each cell spawns one child (start about 30 ms,
step 0); the sketch's §6.4 estimate for the local pins ("under 1 min") holds.

## Records (step 4, 2026-10-22)

- Registry: `docs/spark-sql-iceberg-parity.md` gains **ICE-LOCAL-COMMIT-POINT-1** beside
  ICE-COMMIT-UNKNOWN-1, DECLARED, in ruling 83's words (the local catalog's rule only; Glue and
  REST follow the catalog pointer, and the AWS drill pins that), with the K-7 CTAS answer
  (ruling K-1) and the orphan ceiling (ruling 82). Pin: `kill_mid_write.rs`.
- Production status row K3 (`docs/cutover/production-iceberg-status-2026-09-14.md`) gains the
  dated line "2026-10-22: kill pin K PROVEN locally …; live drill awaits #1070". Its rating stays
  "PROVEN offline; no live drill".
- The sketch's §3 carries the dated "built" line with the premises corrected; the work-order map
  says pin K is built; the 1.5.6 notes draft gains an "Integrity under failure" line (no stored
  value or behaviour change), and the mid-term map's row for the draft says so.
- Not touched: `STATUS.md`, `.github/`, the fork, any product file, `Cargo.toml`.

## Verifier round 1 (2026-10-22): FAIL, and fix round 1

A fresh Opus 5.5 verifier read PR #1142 at `4a439694` and returned FAIL: one S1, four S2, six
S3 and a §7 halt (ruling 83 on the `type=hadoop` path read). The fix round's orders came from the
orchestrator on 2026-10-22. Each finding gets one dated row below, with its disposition. Rows
land one per commit, and the earlier clauses stand unedited.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-014 | (Verifier S1-1, 2026-10-22.) A held child that loses its parent ends its hold by itself and exits non-zero without committing. The hold polls `getppid()` every 20 ms against the parent recorded at the child's start, and it is also bounded at 120 s (`HOLD_LIMIT`). On either release the child calls `std::process::exit(75)`, so no commit is made. | `tests::kill_mid_write::a_held_child_whose_parent_is_gone_exits_without_committing`: an intermediate process (`kill_mid_write_orphaning_parent`) spawns the K-1 v2 child, waits for its marker, records its pid and exits 0 without killing it; the test then requires the child gone within 10 s and no metadata file past the pre-state's. | PROVEN | **Red that motivated it:** the verifier sent SIGTERM to a K-4 v3 parent while its child held. The parent exited 143, and the child was re-parented to the systemd user manager, still `Sl` 23 s later, with its temp directory leaked (`u9v` `r1/sigterm.sh`). **Red of the pin:** under the old bare `std::future::pending()` hold, the new test fails with `a held child outlived its parent by 10 s (the hold limit is 120s)`, and the test kills the stranded child itself. **Green:** 21 of 21 (`kill_mid_write`). **Residual:** a parent killed from outside still leaves its own `TempDir`, as any killed test does; only the child's hold is fixed. |
| C-015 | (Verifier §7 halt, 2026-10-22.) Ruling 83 does not hold for the `type=hadoop` path read in the window between `v(N+1).metadata.json` (fork `memory/catalog.rs:584`) and the `version-hint.text` advance (`:598-600`). RePark's path read (`crates/repark-core/src/iceberg_path.rs:99-108`) answers the hinted version, while `register_table`, Spark's `HadoopCatalog` and Spark's path load answer the newer one. | Card [ICE-HADOOP-HINT-WINDOW-1](../../roadmap/mid-term/ice-hadoop-hint-window-1-card-2026-10-22.md). | OPEN (halt recorded, owner decision pending) | Halt recorded, owner decision pending, card ICE-HADOOP-HINT-WINDOW-1. This is existing product behaviour; pin K never takes the path. Not fixed in this unit. The owner's options: walk past the hint as Java's `HadoopTableOperations.refresh` does, or narrow ruling 83 to `register_table`. |
| C-016 | (Verifier S2-1, 2026-10-22.) Registry row ICE-LOCAL-COMMIT-POINT-1 claims only what is measured: the reopen that follows ruling 83 is `CALL <catalog>.system.register_table` on the newest metadata file. "Or a path read" is gone, and the row points at the card. | The diff of `docs/spark-sql-iceberg-parity.md`; the reopen in every cell is `register_table` (`kill_mid_write.rs`, `kill_cell`). | PROVEN | Fix round 1. |
| C-017 | (Verifier S3, 2026-10-22.) The registry row no longer names `type=memory` as a door. The facade refuses a bare `type=memory` without the extensions key (`catalog_config/refusal.rs:63-71`); the row now says the in-process memory catalog, reached by `type=hadoop` with Hadoop file names. | The same diff. | PROVEN | Fix round 1. |
| C-018 | (Verifier S2-2, 2026-10-22.) The 1.5.6 notes line says what the ledger says: a writer killed "before the metadata file is written" leaves the table as it was (reopened through `register_table`), and one killed inside the same commit call after that write reads back as committed (K-C1). | The diff of `task/roadmap/mid-term/v1-5-6-release-notes-draft.md`; cells `kc1_*` (killed inside `update_table` after the write, reads committed). | PROVEN | Fix round 1. |
| C-019 | (Verifier S2-3, 2026-10-22.) C-003's delete-file format is pinned. The marker records each attempted delete file's manifest `file_format`, and every K-5 delete orphan must be `Parquet` (a position delete) on v2 and `Puffin` (a deletion vector) on v3. | `k5_merge_on_read_killed_at_the_commit_v2`, `_v3`. | PROVEN | Mutant (expected formats swapped): both K-5 cells red, `K-5 v2: the delete file …/data/pos-del-00000-….parquet is a Puffin file` (left `Some("Parquet")`) and `K-5 v3: the delete file …/data/dv-00000-….puffin is a Parquet file` (left `Some("Puffin")`); 19 others green. Reverted, then 21 of 21 green. |
| C-020 | (Verifier S2-4, 2026-10-22.) C-001's "dies of SIGKILL there", stated honestly. The parent reads status 9 from its own kill, and `try_wait()` returning `None` immediately before the kill proves only that the child had not exited by then. A child that aborts after writing its marker (the verifier's V13 mutant, with a piped core dump interrupted by the kill) is indistinguishable by status, and neither check catches it. What catches a wrong outcome is the pre-state, newest-metadata and orphan checks (C-002, C-003), which bind whatever the death point (M1; V5 + M1). | The 18 cells; the `try_wait()` assertion in `run_child_to_the_hold`. | PROVEN | Fix round 1. This row corrects C-001's wording; C-001 stands as written above. |
| C-021 | (Verifier S3, 2026-10-22.) K-7 compares Arrow types, as K-1 to K-6 do. The reopened CTAS table reads `[Int32, Utf8]`. | `k7_ctas_killed_at_publish_reads_created_on_the_local_catalog_v2`, `_v3`. | PROVEN | Mutant (expected `Int64` for `id`): both K-7 cells red, 19 others green. Reverted, then 21 of 21 green. |
| C-022 | (Verifier S3, 2026-10-22.) Production status row K3's dated line no longer says every cell "leaves the table as it was". That holds on the 12 cells killed before the metadata file is written. K-7 and the two controls read back as committed. | The diff of `docs/cutover/production-iceberg-status-2026-09-14.md`. | PROVEN | The rating is unchanged ("PROVEN offline; no live drill"). |
| C-023 | (Verifier S3, 2026-10-22.) C-009's mutation set is four, not three. M4 (the child drops the session write key) reds K-2 on v2 and v3, as §Revert check records. | §Revert check, M4 row; `k2_*_v2`, `_v3`. | PROVEN | This row completes C-009's sentence; C-009 stands as written. |
| C-024 | (Verifier S3, 2026-10-22.) `publish_replace_table` is forwarded unheld. None of the seven pinned doors calls it (the verifier's call trace), so RTAS and `CREATE OR REPLACE TABLE … AS SELECT` are unpinned kill doors. | A replace-door cell. | OPEN (carried to pin U/X or a later card) | Not this round. Closing question: which unit pins the replace doors (hold `publish_replace_table`), and on which catalogs? |
| C-025 | Pin K runs the live `remove_orphan_files` (deletion), not only its dry run. | None in this unit. | REJECTED (out of scope, sketch §0: orphan deletion is out; only the count is in scope) | The verifier's probe ran the live call on all 18 cells. On K-1 to K-6 it removed exactly the marker set, leaving rows and metadata unchanged; on K-7 and the controls it removed nothing. |

## Verifier round 2 (2026-10-22): FAIL, and fix round 2

A fresh Opus 5.5 verifier re-read PR #1142 at `e1a163c5` and returned FAIL: one S1 and one S2, no
new halt.

- **What held.** Measured from outside with a subreaper, the bounded hold released with exit 75
  and no commit in 12 of 12 cases. The fix-round-1 records check out. The original 18 cells keep
  their meaning.
- **What failed:**
  - **S1-1.** The 120 s deadline is unpinned (mutant F2, deadline removed: 0 of 21 red). The child
    recorded `getppid()` 100 to 300 ms after it started. A parent that died before then left the
    child recording the reaper, so only the deadline could end the hold, and under F2 the hold was
    for ever. The verifier's `prestart_orphan.py` reproduced this window.
  - **S2-1.** The parent-gone pin could not see how the child ended. Mutants F3 (exit 0), F5 (the
    intermediate kills and reaps the child) and F5b (kills without reaping) all survived.

The orchestrator ordered fix round 2 on 2026-10-22. The rows below are appended; C-014 stands as
written.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-026 | (Verifier round 2 S1-1, 2026-10-22; corrects C-014's proof.) The parent-check window is closed, and the hold limit is pinned. (a) The parent passes its own pid to the child in `REPARK_KILL_MID_WRITE_PARENT`, and the hold compares `getppid()` against that pid, so a parent gone before the child records anything is seen at once. (b) The hold limit can be overridden by the test-only key `REPARK_KILL_MID_WRITE_HOLD_LIMIT_MS` (default 120 s). A live parent that never kills its child sees the child leave the hold at the limit with exit 75 and no commit. | Three tests, now in `kill_mid_write_release.rs` (the parent-gone pin moved there from `kill_mid_write.rs`, its name unchanged): `a_child_whose_parent_is_gone_before_it_starts_releases_at_the_hold_without_committing`, `a_held_child_whose_live_parent_never_kills_it_is_released_at_the_hold_limit`, and `a_held_child_whose_parent_is_gone_exits_without_committing`. | PROVEN | Each mutant red under its pin: F2 (deadline removed) reds the hold-limit pin, 1 of 24, `still holding after 10 s`; F8 (the child records `getppid()` at its start, the round-1 code) reds the pre-start pin, 1 of 24, `outlived its parent by 10 s`. Green: 24 of 24 (`kill_mid_write`). **Why the pre-start shape cannot red F2:** with (a) a pre-start orphan is released by the parent check, not the deadline. The deadline is the backstop for a parent that is alive but never kills, which is what the hold-limit pin builds, with the test itself as that parent and a 1 s limit. |
| C-027 | (Verifier round 2 S2-1, 2026-10-22.) The release pins see how the child ended. Just before `std::process::exit(code)` the hold writes `released <reason> <code>` beside its marker, with reason `parent` or `deadline` and code 75. The parent-gone and pre-start pins assert `released parent 75`. The hold-limit pin asserts `released deadline 75` and also reads the child's real exit status (75), because there the test is the child's own parent. | The three tests of `kill_mid_write_release.rs`. | PROVEN | Mutant counts, out of 24 tests: F3 (`exit(0)`, file unchanged): 1 red, the hold-limit pin reading the real status. F3b (code 0 in the file and the exit): 3 red, all three release pins. F5 (the intermediate kills and reaps the child): 1 red, the parent-gone pin, with no release file. F5b (kills, no reap): 1 red, the same pin. Re-checked on this head: F2 still 1 red (the hold-limit pin), F8 still 1 red (the pre-start pin). Green: 24 of 24. **Limit of the file pins:** only the child's own parent can read its real exit status, so F3 with the file left at 75 is caught by the hold-limit pin alone. |

## Verifier round 3 (2026-10-22): PASS, with one records S2

A fresh Opus 5.5 verifier re-read PR #1142 at `223e34be` and returned PASS: no S1, one S2, no new
halt.

- **Round 2's findings are fixed.** The window measured closed 25 times from outside; the
  deadline is pinned; the release file is asserted byte for byte on all three paths.
- **Counts reproduced, out of 24 tests:** F2 1, F8 1, F3 1, F3b 3, F5 1, F5b 1.
- **F3c survives**, 0 of 24 red. F3c is `exit(if reason == "parent" { 0 } else { code })`.
- **V13 and V14 now red 3 each.** The release pins see the abort, or the missing release file.

The S2 is that C-027 overstates what a pin can read. The records-only commit adds C-028; C-027
stands as written.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-028 | (Verifier round 3 S2-1, 2026-10-22; narrows C-027.) **Unpinned:** the parent-path exit code (an orphaned child's release). Nothing consumes an orphan's exit status: the reaper, the systemd user manager, discards it. C-027's "only the child's own parent can read its real exit status" is wrong: a `PR_SET_CHILD_SUBREAPER` process becomes the orphan's parent and reads it. **Pinned on the parent path:** no commit, the release reason (`released parent 75` in the release file), and the bounded hold. **Pinned on the deadline path:** the hold-limit pin, as the child's own parent, reads the real exit status. | The three tests of `kill_mid_write_release.rs`. | PROVEN | The verifier's subreaper probe found that at head the parent path exits 75 in 23 of 23 releases. Under F3c the subreaper reads exit 0, with `released parent 75` and no commit, while the 24 tests stay green. The verifier's other fix, a subreaper helper under the parent-gone pin, is not built in this round. |
| C-029 | (Owner ruling 2026-10-22, morning read of the overnight watch; closes the decision C-015 left open.) Option 1 adopted: the path read walks past `version-hint.text` as Java's `HadoopTableOperations.refresh` does, in its own Opus unit after ZONE-SEAM-1; ruling 83 stands as written; registry row ICE-LOCAL-COMMIT-POINT-1 widens again when that unit lands. | Card [ICE-HADOOP-HINT-WINDOW-1](../../roadmap/mid-term/ice-hadoop-hint-window-1-card-2026-10-22.md), status paragraph. | OPEN | Ruled, not yet proven: nothing in this unit changes, and C-015 stays OPEN until the fix unit's ledger proves the path read. |
