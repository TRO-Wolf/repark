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

## Pins U and X (2026-10-22): branch `feat/ice-integrity-pins-1-ux`, base `a2de82aa`

Lane 2 of the owner's two-Opus-lane ruling of 2026-10-22, model claude-opus-5-5. This round
builds pins U and X as tests, helpers and records only (H-6). The fork is `d80a2884`, and every
fork line below is on that rev. The orchestrator's rulings on the step-0 questions (2026-10-22)
are quoted where a row rests on them. Rows are appended one per built piece; C-012 and C-013
stand as written above.

**Premise moved (step 0, recorded at the orchestrator's order).** Fork #368 (`409ff330`) landed
both halves of ruling 84's card. It refreshes and checks for the attempt's snapshot before every
re-base (`crates/iceberg/src/transaction/mod.rs:498`, `commit_status.rs:273`). It also turns the
SDK retry off on the two commit calls: Glue `crates/catalog/glue/src/commit_transport.rs:137`,
S3 Tables `crates/catalog/s3tables/src/commit_transport.rs:168`, both `RetryConfig::disabled()`.
The only re-send left is the fork's own after a throttling reply (`refused_before_processing`,
Glue `:76`, S3 Tables `:77`; at most `THROTTLE_ATTEMPTS = 3`). The SDK re-send that sketch cell U-5
drilled therefore cannot happen, and U-5 as written collapses into U-1. Orchestrator ruling Q-1:
keep U-5, now pinning that fact (exactly one commit request at the gate, rows once), and add
U-5r. In U-5r the gate forwards the commit, answers the client a throttle in place of the real
reply, and forwards the fork's re-send, which AWS rejects as stale. The leg grows to 96
statements, and a double append in U-5r is still H-U5.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-030 | The offline twin of pin U: on the memory catalog, through a test-only `GateCatalog`, the 24 cells U-1 to U-4 × the plain `INSERT … SELECT`, the session-write-key append and `MERGE` copy-on-write × v2 and v3 answer as sketch §4.4 says. U-1 is `Ok`. U-2, U-3 and U-4 raise `CommitStateUnknown` through `engine_err`, with `operation_id` `None` on the plain door and the attempted snapshot's `engine.operation-id` on the other two. There is one commit call and no re-apply. The cut cells fail exactly the 2 refreshes of the status-check budget (`num-retries=1`). A fresh session adopting the newest metadata file resolves U-1 and U-2 as committed (one snapshot by operation id and by snapshot id, rows once) and U-3 and U-4 as not committed (the pre-state's snapshots and rows). | `tests::commit_unknown_doors::u{1,2,3,4}_*_v{2,3}` (24 cells). | PROVEN | 24 of 24 green on `a2de82aa` + the step-1 tree (2.05 s, `--test-threads=4`, load about 18). Red through five mutants (§Pin U offline twin, red first). |
| C-031 | Pin U, cell U-5 after fork #368: on Glue and S3 Tables the SDK no longer re-sends a commit whose reply was lost. The gate sees exactly one commit request, the statement succeeds by reconciliation, and the rows land once. | `test_aws_acceptance_commit_unknown.py`, U-5 cells (12 statements). | OPEN | Built; first live run pending #1070. The collapse and its fork lines are recorded in the paragraph above. |

### Pin U offline twin, red first (step 1, 2026-10-22)

`cargo test --locked -p repark-spark --lib commit_unknown -- --test-threads=4` under
`systemd-run --user --scope -p MemoryMax=40G` and `taskset -c 0-25`, with the load average about
18 (lane 1 building). Main has no twin, so the red proof is the mutants. Each was applied alone,
run, and reverted, and the tree was byte-identical afterwards (`cmp`).

| Mutant | Red cells (of 24) | First failing assertion |
|---|---|---|
| M-U1, the gate forwards in U-3 and U-4 | 12: U-3, U-4 | `U-3 Merge v2: commits that reached the catalog`, left 1, right 0 |
| M-U2, reads never cut | 12: U-2, U-3 | `U-2 Merge v2: refreshes cut inside the status-check budget`, left 0, right 2 |
| M-U3, the gate answers a plain failure in place of unknown | 24 | `U-1 Plain v3: reconciliation finds the landed snapshot, got Err(External(Unexpected => the gate lost the commit reply))` |
| M-U4a, the status-check properties dropped | 24 | `the drill table carries commit.status-check.num-retries before the statement`, left `None` |
| M-U4b, dropped with the presence check removed | 12: U-2, U-3 (run 34.5 s against 2 s) | `U-2 Plain v2: refreshes cut inside the status-check budget`, left 4, right 2 |
| M-U5, the plain door expected to carry an id | 6: U-2, U-3, U-4 plain | `U-2 Plain v2: the error's operation id`, left `None`, right `Some("…")` |

### The gate, the signer, the runner and the previous release (step 2, 2026-10-22)

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-032 | The commit gate behaves as its cells need, with no AWS. It recognises the commit request on both catalogs (Glue `X-Amz-Target: AWSGlue.UpdateTable`; S3 Tables `PUT …/metadata-location`, never the `GET`). It refuses another database or namespace, another table bucket, and every delete, unforwarded. It forwards only to `{glue,s3tables}.<region>.amazonaws.com`. Each script answers its cell's sequence: forward; land then reset; land then cut; drop then reset; drop then cut; land then forward the re-send; land then answer a throttle and forward the re-send; hold unforwarded then reset. It signs every forwarded request again with its own credentials, never forwarding the client's signature or token, and logs no header or credential. A RePark session whose Glue `uri` names the gate lists the scratch namespace through it. | `python/repark/tests/parity/test_acceptance_commit_gate.py` (66 tests). | PROVEN | 66 passed in 17.3 s. Mutants, each applied alone and reverted (`cmp` clean): the gate forwards a dropped commit (4 red, U-3 and U-4 on both catalogs); the client's headers pass through unsigned (3 red: both re-sign tests and the RePark session); the scratch refusal off (2 red). |
| C-033 | The gate's SigV4 signer equals AWS's on the published vectors and on botocore. It matches 26 vectors of the AWS suite: canonical request, string to sign and signature, and for `double-encode-path` the double-encoded canonical URI that the S3 Tables ARN path needs. It also equals botocore 1.43.109's `SigV4Auth` on one Glue `UpdateTable` and one S3 Tables `UpdateTableMetadataLocation` request at a fixed time with a session token (ruling 88's one cross-check). | `test_signer_matches_the_published_aws_vector[*]` (26); the cross-check below. | PROVEN | Mutant single encoding: 1 red (`double-encode-path`). **Cross-check, once (ruling 88):** no moto venv existed (step 0), so per orchestrator ruling Q-3 a throwaway venv was built in the lane's scratch directory with `uv pip install --offline moto boto3` from the uv cache (moto 5.2.3, boto3 and botocore 1.43.109, nothing fetched, nothing committed). Glue: canonical request equal, signature equal (`d3bcea1a…`). S3 Tables: canonical request equal, signature equal (`f01be59c…`). |
| C-034 | `previous_release_requirements.txt` pins `repark==1.5.5` and its dependencies by hash (orchestrator ruling: the previous release at the 1.5.6 label is 1.5.5, moved by ruling 81's checklist line). Its five `repark` hashes equal PyPI's `repark/1.5.5` digests (five `cp312-abi3` wheels, none yanked, uploaded 2026-10-21). `uv pip install --only-binary :all: --require-hashes` into a fresh 3.12 venv imports `repark.__version__ == '1.5.5'`. | The file; the install check. | PROVEN | Step 0 read of PyPI JSON; install in 3.7 s; H-X2 clear. |

### The legs and the local cross-release control (step 3, 2026-10-22)

**C-012 and C-013, 2026-10-22: built; first live run pending #1070.** Leg U is
`test_aws_acceptance_commit_unknown.py` (96 statements under orchestrator ruling Q-1) and leg X
is `test_aws_acceptance_cross_release.py` (40 cells). Both rows stay OPEN as written above. C-013's
"v1.5.4" reads 1.5.5 under the orchestrator's ruling of 2026-10-22.

**Read differently from the sketch, stated.**
- §5.3 item 3 ("the current snapshot's `total-records` equals the rows each side read") holds for
  X-P, X-T and X-E only. On the merge-on-read X-D table, `total-records` counts the records in the
  live data files: 1,050 against the 900 rows read, on both releases. So X-D asserts that the two
  sides read one snapshot id, one `total-records` and one delete-file list, and that the writer
  wrote at least one delete file.
- The head still reports `1.5.5` until the 1.5.6 bump. So "the right releases ran" is version plus
  install origin: `index-wheel` (no `direct_url.json`) or `local-build`.
- The map column is `map(concat('k', …), CAST(id AS INT))`, because 1.5.5 answers `map('k', <a
  column>)` with "map requires key and value lists to have the same length". MAP-COLUMN-PANIC-1
  (#1131) fixed that after the tag.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-035 | X-L, the local control (orchestrator ruling Q-2): on a `type=hadoop` warehouse, the hash-pinned 1.5.5 wheel (`index-wheel`) and the head (`local-build`) each read the other's table equal for X-P, X-T, X-D and X-E on v2 and v3, in both directions (16 cells). Equal means the shared schema and row-set helpers, exact Arrow types, one current snapshot id, one `total-records` and one delete-file list. In the 4 mixed-writer cells the other release appends 3 rows and the writer reads them back equal. X-NS (v3, `timestamp_ns` and `timestamptz_ns`) is printed, not asserted. | `test_cross_release_local.py::test_x_l_*` with `REPARK_PREVIOUS_RELEASE_PYTHON` set. | PROVEN | 20 of 20 EQUAL in 42.6 s (load about 18 to 27), every row printing its two identities. X-NS, both directions: `tns timestamp[ns]` and `tzns timestamp[ns, tz=UTC]`, value `2026-01-01 00:00:00.123456789` on both sides. |
| C-036 | X-C0 and X-C1 hold on every pull request. X-C0: the head writes and reads through the runner and reads 1,000 rows equal, so the runner adds nothing. X-C1: the runner refuses with exit 3 and writes nothing when its version and install origin are not its role's, and a cell whose two sides would run one identity fails before running. | `test_x_c0_*`, `test_x_c1_*` (3 tests). | PROVEN | Green in the same run. |
| C-037 | Leg U is built as orchestrator ruling Q-1 states: 96 statements, U-1 to U-5, U-5r, U-C0 and U-C1 × the plain door, the session-key door and `MERGE` × v2 and v3 × Glue and S3 Tables. Each drill table carries and asserts the four `commit.status-check.*` properties. The gate's commit-request count is asserted (1, or 2 on U-5r). "Unresolved" is never guessed: two reads 2 s apart must agree within 60 s, or the cell fails. The attempted snapshot and its id are read from the held metadata file through `pyarrow.fs` (ruling Q-5). A duplicate is reported as H-U5. | First live run. | OPEN | Built; first live run pending #1070 (C-012). Collected and skipped here. |
| C-038 | Leg X is built: 40 live cells (32 one-way, 8 mixed-writer) on Glue and S3 Tables, with S3 Tables v3 recorded through `classify_v3_create_outcome` if the service refuses format 3 at create. | First live run. | OPEN | Built; first live run pending #1070 (C-013). |
| C-039 | The legs' gating. Without `REPARK_AWS_ACCEPTANCE=1` both live modules collect and skip, 136 of 136. With the gate on and `REPARK_PREVIOUS_RELEASE_PYTHON` unset, leg X fails at its fixture with the message naming the variable, before any AWS call. | The two collections. | PROVEN | 136 skipped in 0.52 s; `ERROR at setup … REPARK_PREVIOUS_RELEASE_PYTHON is unset with REPARK_AWS_ACCEPTANCE=1`. |

### Proof (step 4, 2026-10-22)

The live cells cannot run here (no AWS credentials on this box, and tier 2 runs merged code
only). So the step-4 mutants of sketch §9 are aimed at the AWS-free cells that hold each rule on
every pull request: the gate's unit test, the resolver's unit test, X's comparison, X-C1, X-L, and
the offline twin. Each mutant was applied alone, run and reverted (`cmp` clean). Run over
`test_acceptance_commit_gate.py`, `test_acceptance_commit_resolver.py` and
`test_cross_release_local.py` with `REPARK_PREVIOUS_RELEASE_PYTHON` set: 99 tests green, load
about 13 to 25.

| Mutant (sketch §9 step 4) | Red cells | Note |
|---|---|---|
| The gate forwards in U-3 | 4: `test_each_script_answers_as_its_cell_says[U-3-*]`, `[U-4-*]` | The twin's M-U1 reds U-3 and U-4 (12 of 24, step 1). |
| The resolver answers "not committed" on a failed read | 1: `test_a_failed_read_is_unresolved_never_not_committed` | |
| The status-check properties dropped | 1: `test_a_missing_status_check_property_is_named` | The twin's M-U4a reds 24 and M-U4b 12 (4 cut refreshes against 2, 34.5 s against 2 s, step 1). |
| The runner runs the head as the previous release (X-C1) | a) the runner never refuses: 1, `test_x_c1_the_runner_refuses_a_release_its_role_does_not_name`; b) `REPARK_PREVIOUS_RELEASE_PYTHON` names the head: 20, every X-L cell (`the previous runner failed`) | The head reports 1.5.5 as well, so the install origin carries the identity. |
| The comparison ignores types | 1: `test_x_compare_sees_an_ntz_column_read_as_an_instant` | `test_x_compare_sees_a_decimal_scale` stays green: the row set already tells `1.00` from `1.000`. |

**Local run times (load average beside each):**
- the twin: 24 cells in 2.03 s (load about 18);
- the gate's unit test: 66 in 17.3 s (about 31 falling to 20);
- the resolver's unit test and the comparison tests: 11 in 3.1 s;
- X-L, X-NS, X-C0 and X-C1: 25 in 42.6 s (18 to 27).

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-040 | Leg U's resolver never guesses. Two agreeing snapshot-id reads 2 s apart resolve, and a late landing is waited for. A failed read raises `UnresolvedError`, never "not committed". Reads that never agree are unresolved at the 60 s deadline. The held metadata file must name exactly one attempt. A missing status-check property is named. | `test_acceptance_commit_resolver.py` (6 tests); leg U calls these helpers. | PROVEN | Red under the "not committed on a failed read" and "presence check dropped" mutants (table above). |
| C-041 | X's comparison sees types. An NTZ column read back as an instant, the same values with `timestamp[us]` against `timestamp[us, tz=UTC]`, is a difference, and so is a decimal scale. | `test_x_compare_*` (2 tests). | PROVEN | Red under "the comparison ignores types" (1 cell). |

### Records (step 5, 2026-10-22)

- Registry row ICE-COMMIT-UNKNOWN-1 (`docs/spark-sql-iceberg-parity.md`) gains a "Pins U and X"
  bullet. It names the offline twin and the two live legs, built and awaiting #1070, and records
  that fork #368 collapsed the SDK re-send, so U-5 asserts one commit request and U-5r drills the
  fork's own re-send.
- Production status row K3 gains a dated line. Its rating is unchanged ("PROVEN offline; no live
  drill").
- The sketch's §4 and §5 each carry a dated "built" line.
- The work-order map names the built pieces and the #1070 refresh proposal.
- The 1.5.6 notes draft's integrity section gains one line, and the mid-term map's row for the
  draft says so.
- Not touched: `STATUS.md`, `.github/`, the fork, any product file, `Cargo.toml`,
  `pyproject.toml`, `uv.lock`.
- `make verify` ran once, on `bc8b670f`. Nextest passed 7,822 of 7,822 tests (26 slow, 97
  skipped) in 559.6 s, and the doctests passed too. The load average was 27 to 47 throughout:
  lane 1, the ZONE-SEAM-1 fix round, kept it above 10. Nothing failed, so no single test needed a
  rerun.
- PR #1157 is open: "Verifier: pending", owner merges.

## Verifier round 1 (2026-10-22): FAIL, and fix round 1

A fresh Opus 5.5 verifier read PR #1157 at `c6d74054` and returned FAIL with four S1 findings,
all in test code and records. No halt fired, and every measured cell was green. The orchestrator
ordered fix round 1 the same day: one commit per finding, nothing amended, and the unit does not
grow. The rows below are appended, and the earlier clauses stand as written.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-042 | (Verifier S1-1, 2026-10-22; corrects C-032's "refuses another database or namespace … and every delete".) The gate forwards only an allow list of the operations the drills send, and refuses everything else before signing. Glue: `GetDatabases`, and `GetDatabase` (`Name`), `GetTables`, `GetTable`, `CreateTable` and `UpdateTable` (`DatabaseName`) naming the scratch database, with no `CatalogId` and a body that is one JSON object with unique keys. S3 Tables, by method and path shape: `GET /get-table` (only `tableBucketARN`, `namespace`, `name`, each once, the configured bucket and the scratch namespace); `GET` and `PUT …/{bucket}/{scratch}/{name}/metadata-location`; `GET /tables/{bucket}?namespace={scratch}`; `PUT /tables/{bucket}/{scratch}`; `GET /namespaces/{bucket}` and `/{bucket}/{scratch}`; `PUT /namespaces/{bucket}` with the body `{"namespace": [scratch]}`. | `test_glue_refuses_every_request_the_drills_do_not_send` (10), `test_s3tables_refuses_every_request_the_drills_do_not_send` (7), and the two `forwards_the_operations_the_drills_send` tests (6 + 8). | PROVEN | **Red on `c6d74054`:** 14 of the 17 refusal cases were forwarded by the old gate. On Glue: `GetDatabase` on `prod`, `UpdateDatabase` naming `prod`, a foreign `CatalogId`, `DeleteColumnStatisticsForTable`, `DeleteTableOptimizer`, `DeleteConnection`, `SearchTables`, a repeated `DatabaseName` key, an unparsable body. On S3 Tables: `CreateNamespace` naming `prod` in its body, `RenameTable` to `prod`, `ListTables` with no namespace, the bucket-wide maintenance `PUT`, `get-table` by `tableArn`. The other three were already refused. **Green:** the gate suite gives 97 passed. Every allowed drill operation is forwarded, and the RePark session through the gate still lists the scratch namespace. |
| C-043 | (Verifier S1-2, 2026-10-22; corrects C-033's "26 vectors" and the map's reason for the omissions.) The signer's payload hash is pinned. The vector set is 28: the 26 plus `post-x-www-form-urlencoded` and `-parameters`, the suite's only vectors with a body, with their `X-Amz-Content-Sha256` given as an input header as the suite's context signs it. Their canonical request, string to sign and signature equal the suite's. `get-space-normalized` stays out because it hides no gate path. | `test_signer_matches_the_published_aws_vector[post-x-www-form-urlencoded*]`; `test_the_vector_set_covers_the_gate_paths` (28). | PROVEN | **Red:** the empty-payload mutant (`hashlib.sha256(body)` → `hashlib.sha256(b"")`), which left 72 of 72 green on `c6d74054`, now reds 2 (the two body vectors; 103 others green). Reverted, `cmp` clean. **Green:** 29 vector tests pass. |
| C-044 | (Verifier S1-3, 2026-10-22; narrows C-012's obligation, sketch §4.4 U-3 "orphans counted as in K".) Leg U counts U-3's orphans live as pin K does (data, deletes, manifests, lists, metadata) and asserts ruling 82's ceiling live. | Leg U's U-3 cells. | REJECTED (narrowed in the build; stated here) | The built U-3 checks only `metadata=1 lists=1`: through `pyarrow.fs`, the held metadata file and the attempt's manifest list exist on storage. The data files and manifests are not counted, and the ceiling is not asserted live. pyarrow cannot read the Avro manifest list, `register_table` is refused on S3 Tables (R126), and pin K counts and asserts the ceiling on the local catalog (C-003). C-012 stays OPEN for the live drill. Sketch §4.4, the registry bullet and K3 now say so. |
| C-045 | (Verifier S1-4, 2026-10-22; pins the value half of C-040 and the "exact Arrow types" of C-035.) Leg U's status-check precondition compares the four values, not only the keys: a drill table with `commit.status-check.num-retries = 3` fails it. X's comparison reports a `string` column read back as `large_string`, a difference `compare_schemas` does not see, through the exact type check alone. | `test_a_missing_status_check_property_is_named` (now with a wrong value); `test_x_compare_sees_a_string_read_as_a_large_string`. | PROVEN | **Red:** V-P3 (a key-presence check) reds `test_a_missing_status_check_property_is_named`, 1 of 34. B-X3-narrow (`exact_type_differences` dropped from `compare_tables`) reds `test_x_compare_sees_a_string_read_as_a_large_string`, 1 of 34. Both had survived on `c6d74054`. Reverted, `cmp` clean. **Green:** 34 of 34 (resolver and `test_cross_release_local.py` with the 1.5.5 interpreter set, X-L 20 of 20 again EQUAL). |

**S3 wording, fix round 1 (2026-10-22).** The leg X fixes are in test code; the other two are
record wording:
- Leg X masks account ids when it re-raises a runner failure. Leg U masks them when it re-raises
  a create failure and in its gate-refusal message.
- The registry's "U-5r … rows once" now reads as the expected answer, unmeasured until the first
  live run.
- The 1.5.6 notes line names the head (the pre-bump build that still reports 1.5.5), not 1.5.6.

## First live run (2026-10-23): FAIL, and fix round 2

Run 39936752073 on main `75b9e43b` was dispatched by the orchestrator with purpose "ICE-INTEGRITY-PINS-1 legs U/X first run". The integrity legs gave 98 failed and 38 passed. The run's four findings were the gate's refusal of read-only Glue calls outside the scratch namespace (F1), the legs' MERGE source in a column-alias form Spark itself refuses at parse, which RePark fails at resolution instead (F2), the silent 900 s runner cut-off (F3) and the expired token (F4). Legs U and X stay OPEN until a dispatched run is green.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-046 | (F1; narrows C-042's Glue list.) Read-only Glue operations forward for any database; `CreateTable` and `UpdateTable` still name the scratch namespace. | the twin cells of item 1 | PROVEN | Revert check: the old gate file reds 3 (the `default` cell and the two moved cells), 6 passed on the forwards selection; restored: 9 passed. Full suite 101 passed. |
| C-047 | (F2.) The legs' MERGE and INSERT source is `(SELECT * FROM VALUES … AS v(id, name)) AS s`; the `(VALUES …) AS s(id, name)` form, which Spark 4.1.2 refuses at parse (`[COLUMN_ALIASES_NOT_ALLOWED]`, measured on the oracle with the Iceberg runtime) and RePark fails at resolution, is carded as VALUES-ALIAS-COLUMNS-1 for the refusal text. | the local pin of item 2 | PROVEN | Red on the old text (`UNRESOLVED_COLUMN` on `id`, suggesting `column1`/`column2`), green on the new (7 of 7 resolver suite). |
| C-048 | (F3.) A runner cut off at 300 s fails its cell with exit 124 and its stderr tail. | the pin of item 3 | PROVEN | Red on the old driver (the timeout escapes `run_side`), green on the new (the pin passes in about 1 s with exit 124 and the tail); 29 collected. |
| C-049 | (F4, and the run.) The first dispatched run did not prove legs U or X: 36 U cells and the plain Glue v2 X pair passed; every other cell fell to F1, F2, F3 or the expired token. | the next dispatched run | OPEN | run 39936752073; credentials re-minted before the legs by the orchestrator in the workflow |

## Second live run (2026-10-23): FAIL, and fix round 3

Run 39965367559 on main `80e60ebb` was dispatched by the orchestrator with purpose "ICE-INTEGRITY-PINS-1 legs U/X second run". The integrity legs gave 68 failed and 68 passed. The run proved leg U 48/48 on S3 Tables, the Glue control U-C1 6/6, and 14 leg X cells. Its four findings were the gate's unnamed no-target refusal (G1), the partitioned table's 168 files under `days(ts)` (G2, measured 8 under `months(ts)`), the cross-read step's local-only metadata read (G3) and the expired token (G4); legs U and X stay OPEN until a dispatched run is green.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-050 | (G1.) A gate refusal of a request with no X-Amz-Target names the method, path and header names, and every refusal is an exchange. | `test_acceptance_commit_gate.py`: the no-target `GET /` cell and the refused-exchange cell. | PROVEN | **Red** on the pre-fix gate file: both cells fail. **Green:** 103 of 103 in the gate suite. |
| C-051 | (G2.) The partitioned cross-release table is `months(ts), bucket(4, id)`: 8 data files in 8 partitions, not 168. `ts` steps one hour per row (`make_interval`'s fifth argument), so `days(ts)` over 1,000 rows was 42 days × 4 buckets, not the 1,000 the orchestrator's first reading assumed. | `test_cross_release_local.py`: the head/head partitioned v2 cell counts `<table>.files` (at most 160) and reads the spec back. | PROVEN | **Red** on `days(ts)`: 168 files, `assert 168 <= 160`. **Green:** 8 files, spec names `months(ts)` and `bucket(4, id)`; 8 passed, 22 skipped (no 1.5.5 interpreter on the build box; X-L itself unchanged). |
| C-052 | (G3.) The cross-read step reads a metadata location through the filesystem its URI names. | `test_acceptance_cross_read_local.py`: four cells (`file://`, a bare absolute path, a relative path, an `s3://` URI through a patched `FileSystem.from_uri` that asserts the two slashes). | PROVEN | **Red** on the old read: the `s3://` cell fails `FileNotFoundError` on `s3:/bucket/…`. **Green:** 4 passed, 4 skipped (live flag off). |
| C-053 | (G4, and the run.) The second dispatched run proved leg U on S3 Tables (48/48) and 14 leg X cells; the 42 gated Glue cells landed and failed only on an unnamed refusal; the legs step ran 65 min and the trailing 22 cross-release cells and the cross-read step fell to an expired token or G3. | the next dispatched run | OPEN | the orchestrator splits the legs into two steps with a credential mint each in the workflow |

## Third live run (2026-10-23): leg X proven, and fix round 4

Run 39995251302 on main `1aeb1445` was dispatched by the orchestrator. Leg U gave 42 failed and 54 passed in 24 min, leg X gave 40 passed in 529 s, and the cross-read step gave 2 failed, both at the 1200 s timeout. No token expired. H1 is the gated Glue cells' S3 metadata read routed through the gate, fixed by naming the session's S3 endpoint, and H2 is the cross-read `metadata_log_entries` hang, carried with the tokio experiment. The orchestrator's read-only local probes of the Glue acceptance table's `history`, `snapshots` and `metadata_log_entries` returned in under a second on the same build at 8, 2 and 1 cores.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-054 | (H1.) The gated Glue session sets `s3.endpoint` to the regional S3 endpoint, so the fork's Glue catalog (iceberg-rust `d80a2884`, `crates/catalog/glue/src/catalog.rs`, the `S3_ENDPOINT` insert when none is set) does not route the catalog's file I/O through the gate; the S3 Tables session is unchanged. | `test_acceptance_commit_resolver.py`: `gate_session_extra` for a Glue target (region `us-east-2`) yields the gate `uri` and `s3.endpoint = https://s3.us-east-2.amazonaws.com`, for an S3 Tables target only its `endpoint_url`. | PROVEN | **Red** on the pre-fix leg module (the function is absent; collection error). **Green:** 9 of 9 in the resolver suite. |
| C-055 | (Closes C-038's obligation.) Leg X holds live: 40 of 40 cells on Glue and S3 Tables, v2 and v3, plain, partitioned, deletes and evolved, one-way and mixed-writer, in 529 s. | run 39995251302, step "integrity leg X" | PROVEN | `40 passed in 528.91s`; the partitioned cells took seconds at 8 files. |
| C-056 | (H2.) The cross-read step's `metadata_log_entries` read returns on the runner. | the next dispatched run, on tokio 1.53.3 | OPEN | Hung 1200 s on both acceptance tables in run 39995251302 (tokio 1.54.1); returned in seconds in run 39965367559 (1.53.2) and in the orchestrator's local probes on 1.54.1. The lock pins 1.53.3 for the next run as the experiment. |
| C-057 | (The run.) Leg U on S3 Tables held again (48 of 48); the 42 gated Glue cells landed and failed only on H1. | the next dispatched run | OPEN | run 39995251302 |

## Fourth live run (2026-10-23): legs U and X proven, the cross-read hang stays open

Run 40025222296 on main `2d069e8b` (fix round 4, #1170, with tokio pinned to 1.53.3 as the C-056 experiment) was dispatched by the orchestrator under the owner's word of 2026-10-23. Leg U gave 96 passed in 1,933 s and leg X 40 passed in 513 s; the acceptance module and the dbt gold step passed; no token expired. The cross-read module failed its two tests at the new 600 s cap, this time inside `DataFrame.to_arrow()` of a table read (`_acceptance_cross_read.py` `cross_read_table_pair`), not in `metadata_log_entries` as in run 3, so the hang is a RePark collect over an S3-backed table inside the cross-read process, and tokio 1.54 is exonerated. The rows below close the legs and carry the hang.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-058 | (Closes C-037's and C-012's live obligation; supersedes the OPEN state of C-049, C-053 and C-057.) Leg U holds live: 96 of 96 cells, U-1 to U-5, U-5r, U-C0 and U-C1 × the plain door, the session-key door and `MERGE` × v2 and v3 × Glue (48) and S3 Tables (48), with the gate armed, the four status-check properties asserted, the commit-request count asserted (1, or 2 on U-5r) and every unknown outcome resolved by snapshot id on reopen. | run 40025222296, step "integrity leg U" | PROVEN | `96 passed in 1933.42s (0:32:13)`; Glue 48 PASSED, S3 Tables 48 PASSED in the step log; the gated Glue session names `s3.endpoint` itself (C-054), so no request reached the gate without a target. |
| C-059 | (H2, continued; narrows C-056.) The cross-read step's RePark reads of the acceptance tables return on the runner. | the first run whose cross-read step carries the native stack dump (the nightly of 2026-10-24 or a dispatch) | OPEN | Run 3 (tokio 1.54.1) hung in `metadata_log_entries`; run 4 (tokio 1.53.3) hung in `to_arrow()` of a table read; run 2 (1.53.2, before #1167) returned in seconds and failed later on the `s3:/` path. Tokio is exonerated and the lock returns to 1.54.1. Leg U and leg X read the same catalogs in the same job without a hang; the cross-read process is the one that also starts the PySpark JVM. The workflow step now runs `py-spy dump --subprocesses --native` on the pytest process 420 s in, before the 600 s cap, so the next run names the blocked native frame. |

## Run-4 review correction and diagnostic plan (2026-10-23)

The paragraph and C-059 above misclassified the S3 Tables timeout. Glue stopped in RePark row collection at `_acceptance_cross_read.py:294`. S3 Tables completed that collection and stopped in PyArrow `open_input_file` at line 235. Run 4 establishes failures on 1.53.3; it does not establish a cause or exonerate 1.54.1. The lock restoration follows the handoff's experimental sequence, not a causal finding. C-044's live orphan limit is unchanged. C-058 proves the checked outcomes, not an independently captured gate-request transcript; its last inference is not separate network evidence.

Diagnostic correction plan:
1. Install one pinned binary profiler before the first credential mint.
2. Target the Python test process directly and sample throughout both catalog tests.
3. Bound each dump, report capture failures, and reap the watchdog without changing pytest's status.
4. Exercise the real workflow shell locally with harmless success, failure, delayed and failed-profiler processes; require independent review and exact-head CI before any main-only dispatch.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-060 | (Corrects C-059's diagnosis.) Both cross-read catalog tests complete on the runner; native samples distinguish RePark collection from PyArrow metadata I/O if either stalls. | A merged diagnostic run with both test outcomes and native captures. | OPEN | Run 40025222296: Glue row collection timeout; S3 Tables metadata file-open timeout. Neither cause is established. |
| C-061 | The diagnostic shell targets the Python process, samples both test phases, preserves pytest status and reaps its delay process; binary profiler installation precedes credentials. | Execute the workflow shell with controlled child processes and revert/mutation probes. | PROVEN | `test_cross_read_diagnostics.py`: 8/8 passed. The original workflow fails 8/8; wrong-wrapper PID, single sample, masked test exit and omitted watchdog stop mutants each fail. Existing bench workflow tests also pass (22 combined). A real py-spy 0.4.1 direct-Python probe captures libc sleep frames locally; hosted sudo/ptrace behavior remains a live-run check. |

Validation on 2026-10-23: `make py-lint py-format-check workflows-lint` exits 0. The local native probe grants tracing only on its harmless target process; no host ptrace policy changes. No AWS run was dispatched for this unmerged change.

## Active-dump shutdown review correction (2026-10-23)

C-061's original process tests did not cover pytest finishing during a dump. That shutdown
could suppress the failure warning and leave its output group open. A delay killed without
`wait` also made the fast-completion pin intermittently fail. The watchdog now requests a stop,
reaps an active delay synchronously, clears a reaped delay PID and finishes reporting an active
bounded dump. This corrects C-061's cleanup claim without changing the live C-060 obligation.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-062 | Diagnostic shutdown preserves native output, capture-failure warnings and group closure when pytest exits during a dump; the delay is reaped synchronously and its stale PID is not signalled. The timeout command executes. | Execute the workflow shell with delayed and timed-out profilers; discard-output, old-trap, stale-delay and removed-timeout mutants. | PROVEN | 11 diagnostic tests and 14 existing bench workflow tests pass. The delayed-failure pin fails on the prior workflow. Discard-output fails 9 tests; old-trap fails 4; stale-delay fails 1; removed timeout plus a decoy string fails 2. The timeout fixture validates the actual 30 s / 5 s arguments and scales them to 0.5 s / 0.1 s for execution. Fast completion passes 21 independent invocations after synchronous reap. Hosted ptrace and cross-read completion remain unmeasured. |


## Fifth live run, attempt 1 (2026-10-24): job cap, native capture proven

Run 40040855187 on merged main `bc30f341` ended CANCELLED at its two-hour job cap.
Acceptance 11, dbt 1, U 96 and X 40 passed. Cross-read started nine minutes before cancellation
and produced no final pytest outcome. Four native samples succeeded: PyArrow S3 HeadObject,
Spark Arrow socket receive twice, then RePark collection, all within the Glue test. The harness
iterates tables without intermediate identity reporting; a permanent stall or a cause is not
established. This supplies C-061/C-062's hosted capture evidence while C-060 stays OPEN.
The [live-run record](../../wo/ice-integrity-pins-1/live-runs-2026-10-23.md) carries exact timings
and the single cap-cancellation rerun under the October 22 owner ruling. Attempt 2 is pending.


## Cross-read progress delivery (2026-10-24)

The run-5 snapshots cannot identify the current table. The first diagnostic change streams
Python progress before pytest finishes. A blocked harmless test executes through the actual
workflow shell. Default capture and tee-sys without unbuffered Python both fail the visibility
pin; unbuffered Python plus tee-sys passes while preserving success and failure exits.

| Clause | Proposition | Pin / evidence | Status | Result |
|---|---|---|---|---|
| C-063 | Cross-read Python progress reaches the job output before a blocked pytest test returns, while pytest success and failure remain the step's exit status. | `test_cross_read_diagnostics.py::test_progress_is_visible_before_pytest_finishes` executes the workflow shell with real pytest and both final outcomes. | PROVEN | Both cells fail before releasing the blocked test on default capture and on tee-sys alone. Both pass with Python `-u` and tee-sys; 27 workflow tests pass. This is local process evidence, not live C-060 completion. |
| C-064 | Cross-read reports the discovered count and each table before metadata lookup and engine reads, then reports completion status and elapsed time. Selection, call order, all outcomes and the final assertion remain intact. | `test_cross_read_progress.py` executes exact source function ASTs with controlled external calls and flushed-output observations. | PROVEN | Five cases fail on baseline and pass on the candidate. Delayed metadata/pair progress, skipped later tables, suppressed final assertion, unflushed output and unsorted discovery mutants each fail. Live table count and C-060 completion remain unmeasured. |

## Run-5 retry result and run-6 observation (2026-10-24, 05:33 UTC)

The pending run-5 attempt-2 line above is historical. Attempt 2 failed both cross-read tests
at their 600 s whole-test deadlines after acceptance, dbt, U and X passed. Its nine native
samples moved between stages and establish neither a cause nor one operation's duration.
Run 6 attempt 1 reached the job cap without any acceptance pytest output or outcome; the
later legs were not reached. It supplies no cross-read diagnostic measurement.

The [live-run record](../../wo/ice-integrity-pins-1/live-runs-2026-10-23.md) holds the measured
counts, timings and frames. It also records the orchestrator's single run-6 cap retry on
unchanged `e765f897`, observed in progress at 05:33 UTC. That retry's outcome is unmeasured.
Run 5 and run 6 each consumed their own one retry. C-060 stays OPEN; no clause closes here.
