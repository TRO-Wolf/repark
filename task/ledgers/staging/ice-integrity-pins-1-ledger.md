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

