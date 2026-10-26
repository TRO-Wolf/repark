# ICE-RESOURCE-BOUND-1 — the measured and ruled resource bound (2026-10-26)

Unit ICE-RESOURCE-BOUND-1 (U-11, release 1.5.6). Order: [order](../../task/wo/ice-resource-bound-1/order.md). Ledger: [ice-resource-bound-1-ledger](../../task/ledgers/staging/ice-resource-bound-1-ledger.md). Step-0 measurement: [step-0-bound-table-2026-10-26.md](../../task/wo/ice-resource-bound-1/evidence/step-0-bound-table-2026-10-26.md), 261 cells on format v2 and v3 (the JSON sits beside it).

**Status, 2026-10-26.** Step 0 measured; the owner ruled Q1 to Q5 the same day; the sketch and the orchestrator rulings O-1 to O-13 and Q-B1 and Q-B2 are in the order. Steps 1 to 5 built the bound, the refusals, the detector walk and the pins on format v2 and v3. This file records the outcome. The scoped verifier rounds are open (ledger C-012).

## What is bounded

Three paths hold their input outside the session memory pool. Above a stated fraction of the live pool, the statement refuses before any data or delete file is staged. Each path has one key.

| Path | Statement | Key | Default | Bound at the 8 GiB default pool |
|---|---|---|---|---|
| PP-01 | ordered write: the input drained before the sort | `repark.write.ordered.max-input-fraction` | 0.10 | 819 MiB |
| PP-02 | merge-on-read MERGE: updated rows, matched and NOT MATCHED BY SOURCE | `repark.merge.mor.max-updated-fraction` | 0.05 | 410 MiB |
| PA-11 | changelog read: the source window | `repark.changelog.max-source-fraction` | 0.0625 | 512 MiB |

Rules, as ruled (order Q1, Q2 and O-5):

- The bound is the fraction times the live pool. The pool is read at each statement, so `SET datafusion.runtime.memory_limit` moves it.
- A fraction of 0 means no bound on that path. A pool with no finite limit means no bound on any path.
- The input is not charged to the pool. The bound is a separate limit beside the pool (Q2).
- The three keys are build-time only. A runtime `conf.set` of one refuses with an `IllegalArgumentException` that names the builder door. A value that is not a number, is negative or is not finite refuses at build, as `IllegalArgumentException`, and names the key. Fractions above 1 are accepted.
- The refusal has the pool's own class: DataFusion `ResourcesExhausted`, which surfaces as `PySparkException` (Q1). It names the statement kind, the estimate, the fraction times the pool and the key. It names no data-deleting action (ruling 76(a)). Its remedies are raising the key or the pool.

The ruled text, with `{...}` filled at the crossing:

> REPARK: the {statement kind} holds {what} in memory outside the memory pool; {what} reached at least {estimate} bytes, above the bound {fraction} x pool {pool} bytes = {limit} bytes. Raise `{key}` (0 = no bound) or raise the memory pool.

A measured refusal, the ordered write at a 32 MiB pool: `Resources exhausted: REPARK: the ordered write holds its input before the sort in memory outside the memory pool; its input before the sort reached at least 3743744 bytes, above the bound 0.1 x pool 33554432 bytes = 3355443 bytes. Raise repark.write.ordered.max-input-fraction (0 = no bound) or raise the memory pool.` The facade reads it with DataFusion's `datafusion engine error: ` prefix. The Arrow export of a changelog read adds the pool hint of the export path.

The refusal leaves no staged file. The PP-01 drain precedes every writer, so no data file exists at the refusal. The PP-02 matched work runs before the staging starts. The PA-11 read stages nothing. On the parallel ordered arm one shared counter and a drain-end barrier stop the sibling workers from writing after a refusal (O-6).

## Step-0 proposals, as measured

The three proposal lines of the step-0 table, copied from [the table](../../task/wo/ice-resource-bound-1/evidence/step-0-bound-table-2026-10-26.md):

- PP-01 proposed bound: ordered input Arrow bytes <= 0.10 x pool. Basis: statement RSS growth / input <= 4.68 on every ordered cell at 4M and 16M rows (3.22 to 4.08 at 16M), unordered 0.54 to 0.57 at 16M; allowance = pool / 0.6; at the bound a full pool plus 4.68 x 0.10 x pool = 1.47 x pool, margin 1.14 to the allowance on the worst cell. At the 8 GiB default pool: 819 MiB.
- PP-02 proposed bound: updated-row Arrow bytes (matched plus NOT MATCHED BY SOURCE) <= 0.05 x pool. Basis: growth minus pool >= 8.90 x target bytes (4M, 10% + NMBS, pool 2048 MiB) and 6.14 x (16M, 100%, pool 8192 MiB); at the bound a full pool plus 8.90 x 0.05 x pool = 1.45 x pool, margin 1.15. At the 8 GiB default pool: 410 MiB. Below 512 MiB pools the in-pool sort refuses first on every 200k and 1M target (snapshots unchanged).
- PA-11 proposed bound: changelog source Arrow bytes in the window <= 0.0625 x pool. Basis: growth / source bytes 7.38 to 7.83 on every cell, the same with LIMIT 10 and with a one-column projection, pool 32 MiB never refused; at the bound a full pool plus 7.83 x 0.0625 x pool = 1.49 x pool, margin 1.12. At the 8 GiB default pool: 512 MiB.

The owner ruled these bounds, and the defaults, as proposed. Each proposal's margin is the bound's headroom against the allowance (the pool divided by 0.6, the detector's own default share): PP-01 1.14, PP-02 1.15, PA-11 1.12.

## Reproduction

- The step-0 table, re-run by the orchestrator on one 4M-row cell per path: PP-01 statement growth 1,008 MiB against the lane's 1,006; PP-02 3,983 against 3,986; PA-11 1,700 against 1,707. All three within 1 percent.
- The head module, one ordered write at 400k rows and a 32 MiB pool: the ruled text, zero snapshots and zero rows. The same write with the key at 0 commits 400,000 rows in one snapshot.

## Counted bytes against the table's bytes (O-4)

The seam counts the slice-aware Arrow size of the batches it holds. The step-0 table counts the eight source columns (57.125 bytes a row). The ratio of the two was measured per path by bisection over the fraction at a 2048 MiB pool. The ratio must stay at or below 1.4; the owner hears before the pools move if it does not.

| path | format | rows | counted bytes | table bytes | ratio |
|---|---|---:|---:|---:|---:|
| PP-01, ordered INSERT | v2 | 100k | about 5,714,454 | 5,712,500 | 1.000 |
| PP-01, ordered INSERT | v3 | 100k | about 5,714,454 | 5,712,500 | 1.000 |
| PP-02, MoR MERGE 100 % | v2 | 200k | about 11,426,760 | 11,425,000 | 1.000 |
| PP-02, MoR MERGE 100 % | v3 | 200k | about 14,650,133 | 11,425,000 | 1.282 (two lineage columns, 16 bytes a row) |
| PA-11, `count(id)` | v2 | 100k | about 7,913,477 | 5,712,500 | 1.385 (three changelog metadata columns) |
| PA-11, `count(id)` | v3 | 100k | about 7,913,477 | 5,712,500 | 1.385 |

Every ratio is at most 1.4. PA-11 sits closest, at 1.385, and is the only ratio above 1.282.

## Pins on format v2 and v3

Every refusal cell asserts the error class, the ruled text with none of the verbs `delete`, `drop`, `truncate`, `expire`, `remove` or `purge`, and the snapshot, the rows, the Arrow types, the file set and zero orphans, all unchanged. Every control asserts a commit with values, types and per-file order equal to the expected answer.

| cell | rows | pool | key | main (red run) | head |
|---|---|---|---|---|---|
| PP-01-R1, ordered INSERT overwrite, unpartitioned and 32 partitions, writers 1 and 4 | 100k | 32 MiB | default 0.10 | commits | refuses |
| PP-01-C1, the same | 100k | 128 MiB | default | commits | commits |
| PP-01-R2, unpartitioned writers 1; 32 partitions writers 4 | 1M | 128 MiB | default | commits | refuses |
| PP-01-C2, the same | 1M | 2048 MiB | default | commits | commits |
| PP-01-K0, R1 with the key at 0 | 100k | 32 MiB | 0 | commits | commits |
| PP-01-D, D2, D3, D5 and D10 (`UPDATE ... WHERE`) | 100k | 32 / 128 MiB | default | commits | refuses / commits |
| PP-01-M, D8 copy-on-write MERGE on an ordered table | 100k | 128 MiB | 0.01 / default | commits | refuses / commits |
| PP-01-U, unordered table, the R2 shape | 100k | 32 MiB | default | commits | commits (seam not reached) |
| PP-02-R1, MoR MERGE 100 %, 32 files | 200k | 512 MiB | 0.01 | commits | refuses |
| PP-02-R2, 10 % plus NOT MATCHED BY SOURCE | 200k | 512 MiB | 0.01 | commits | refuses; the matched leg alone commits |
| PP-02-C1, MoR MERGE 100 % | 200k | 512 MiB | default 0.05 | commits | commits |
| PP-02-C2, 10 % plus NOT MATCHED BY SOURCE | 200k | 2048 MiB | 0.01 | commits | commits |
| PP-02-O8, ordered MoR target | 200k | 512 MiB | 0.01 and ordered 0.01 | commits | refuses with the PP-02 text |
| PP-02-P, the 32 MiB pool | 200k | 32 MiB | default | the in-pool sort refuses | the same, green |
| PA-11-R1, `LIMIT 10` and `count(id)` | 100k over 10 snapshots | 32 MiB | default 0.0625 | answers | refuses |
| PA-11-C1, the same | 100k | 2048 MiB | default | answers | answers |

The red run on the base product code, before the steps 3 to 5 change: 20 refusal cells failed; every control and PP-02-P stayed green. The D10 cell first used the bare `UPDATE ... SET`, which takes the fork's plain path and never reaches the seam; step 5 re-spelled it with `WHERE id >= 0`. The head run of `tests::resource_bound tests::kill_mid_write` passed 61 of 61 on the step-5 head.

The facade file, on format v2 and v3, passed 24 cells with 18 scale cells skipped. On the base release, 18 facade refusal cells were red and 6 controls were green. At a 512 MiB pool the facade's MoR MERGE refuses on the base through DataFusion's in-pool sort, so the facade PR cells use a 2048 MiB pool for the MoR control.

## Kill against refusal (pin K shape)

- **KR-1, ordered INSERT (D2).** The refusal side, in process at the default key: the process lives, the snapshot is unchanged, the rows and files are unchanged and orphans are zero. The kill side, a child with the key at 0 under the hold mode: the child is killed with signal 9 at the commit, no `REPARK: the` text appears in its output, and on reopen the newest metadata is the pre-state's, with the snapshot set and rows unchanged. Orphans are exactly the marker's attempted files: data 1, deletes 0, manifests 1, lists 1, metadata 0, on v2 and v3.
- **KR-2, MoR MERGE 100 %, 32 files.** The same pair, with the refusal at key 0.01 and the kill at key 0. The killed commit on v2 leaves data 1, position-delete files 56, manifests 2, lists 1 and metadata 0. On v3 it leaves data 1, one deletion vector, manifests 2, lists 1 and metadata 0.

Both pairs run on Unix only. PA-11 has no kill pair: it is a read.

## Scale cells and the local red-on-main run (O-12, Q4)

The 1x grid runs in the pull request. The 4x and 16x cells run only in the scale file, behind `REPARK_RESOURCE_BOUND_SCALE=1`.

- **Red on main, once, local.** The base release wheel, under a 40 GiB memory scope: 12 failed (`DID NOT RAISE`) and 6 passed, in 192 s. The 16M MERGE cells took 35 to 37 s each.
- **Head, once, local (the ci build).** 18 passed. The 16M MERGE refusals took 22 to 27 s.
- **The scheduled job.** Workflow `resource-bound-scale` (file `.github/workflows/resource-bound-scale.yml`), job `scale`. It runs on manual dispatch only; the owner adds the schedule (O-12), as #1070 was wired. No CI run id exists for these local runs.
- **A first head attempt.** The scaled appends first ran without the snapshot-property option. That spelling is D1 (the fork's insert path, with an in-pool sort), which refuses at 32 MiB with DataFusion's external-sorter text, not the bound. The cells now pass the option, so they reach D2.

Peak process memory of the 16M MoR cells on the base reached 11 to 14 GiB (step 0). The largest head-side control is the 4M MoR MERGE at 8192 MiB.

## Cost gate (write set below the bound)

**Protocol.** Base: the branch base `d114a2cf`, built once as a release wheel. Head: product code at `b41aa85e`, the same build. Each cell runs in its own process on up to 26 cores (the owner's worker cap), with base and head order alternated per round. Twelve rounds. Each cell's median is taken over 30 runs per round, after one warm-up run; each timed run is followed by an untimed rollback to the snapshot. Start loads were 2.99 to 8.18. The gate is a ratio of head to base of at most 1.05 on each cell; the bar is the one ICE-STRUCT-PAIRING-1's order records from the owner's 2026-10-13 ruling. Whether it binds here is the owner's call.

The table is the probe's output, copied from the run (`probe_cost.py`, [order](../../task/wo/ice-resource-bound-1/order.md) directory):

| cell | base ms | head ms | ratio | n | head slower |
|---|---:|---:|---:|---:|---:|
| ordered INSERT unpartitioned w1 | 84.85 | 84.37 | 0.994 | 12 | 6 |
| ordered INSERT 32 partitions w4 | 94.68 | 94.27 | 0.996 | 12 | 3 |
| unordered INSERT unpartitioned w1 | 75.46 | 75.59 | 1.002 | 12 | 7 |
| unordered INSERT 32 partitions w4 | 90.41 | 90.99 | 1.006 | 12 | 8 |
| MoR MERGE 10% | 274.58 | 223.21 | 0.813 | 12 | 5 |
| MoR MERGE 10% + NOT MATCHED BY SOURCE | 472.28 | 255.83 | 0.542 | 12 | 2 |
| changelog count(id) 1M | 383.84 | 387.67 | 1.010 | 12 | 9 |
| ordered UPDATE copy-on-write | 152.09 | 152.03 | 1.000 | 12 | 6 |

Start loads 2.99 to 8.18.

Every ratio is within 1.05. The two MoR MERGE cells are noise-dominated on both sides, so their ratios below 1 measure noise and not a speed-up. Per-round medians (the lowest and highest of the twelve rounds):

| cell | side | lowest per-round median ms | highest |
|---|---|---:|---:|
| MoR MERGE 10 % | base | 70.8 | 330.1 |
| MoR MERGE 10 % | head | 88.3 | 329.7 |
| MoR MERGE 10 % + NOT MATCHED BY SOURCE | base | 91.4 | 606.9 |
| MoR MERGE 10 % + NOT MATCHED BY SOURCE | head | 72.2 | 482.7 |
| changelog count(id), 1M | base | 378.5 | 392.5 |
| changelog count(id), 1M | head | 384.2 | 404.2 |
| ordered INSERT, 32 partitions, writers 4 | base | 91.5 | 97.3 |
| ordered INSERT, 32 partitions, writers 4 | head | 91.6 | 96.6 |
| ordered INSERT, unpartitioned, writers 1 | base | 83.0 | 86.1 |
| ordered INSERT, unpartitioned, writers 1 | head | 79.6 | 87.7 |
| ordered UPDATE copy-on-write | base | 148.2 | 157.8 |
| ordered UPDATE copy-on-write | head | 146.5 | 154.7 |
| unordered INSERT, 32 partitions, writers 4 | base | 87.4 | 92.7 |
| unordered INSERT, 32 partitions, writers 4 | head | 88.5 | 91.9 |
| unordered INSERT, unpartitioned, writers 1 | base | 68.7 | 78.0 |
| unordered INSERT, unpartitioned, writers 1 | head | 70.6 | 76.7 |

The change on the MoR path is one slice-size sum per pushed batch. The changelog cell is the one most likely to move, because its drain replaces `collect` with one merged stream; it stayed at 1.010.

## Declared rows, amendments and the release line

- The PA-10 and PA-11 dated rows, and the PP-01 and PP-02 amendments, are in the audit documents (see the production audit and the performance audit of 2026-10-11).
- The declared rows (the ctx-less public Rust APIs, the CTAS plan writer, and the three platform rows of PA-10) are in the parity registry, as ICE-RESOURCE-BOUND-1 dated rows.
- The 1.5.6 release-notes draft carries one line for the stated bound.

## Out of scope, observed

- The static door classifier answers D8 for `MERGE ... UPDATE SET t.a = s.a`, and the runtime record answers D9. Carded as WRITE-DOOR-CLASSIFIER-D8-D9-1.
- In-pool sorts refuse rather than spill under 32 to 512 MiB pools on this host. A 1M-row `ORDER BY` at 32 MiB refuses. The MoR MERGE target sort needs about 10 to 40 times the target bytes of pool.
- The unordered 32-partition write grows 407 to 448 MiB at 1M rows. That is the PP-06 fan-out buffer, not this bound.
- The MoR delete state (`seen_pair`, `pair_indices`, path interning) stays uncounted. The ruled bound covers updated rows only.
- `sort_batches_by_default_order` still sorts under `TaskContext::default()`, an unbounded pool. Spilling it is out of scope.
- A bare `UPDATE t SET ...` with no `WHERE` on a copy-on-write table takes the fork's plain path, whose sort is charged to the pool. It does not reach the PP-01 seam. `UPDATE ... WHERE` does.
- The CTAS plan writer resolves the bound per writer partition, so N writer partitions admit N times the bound. No door reaches it today: a CTAS table carries no write order.
- The bounds ride a plain SessionConfig extension, not a SET-able option. A context built from a fresh SessionConfig falls back to the ruled defaults. None was observed.

## Pointers

- The census of the doors that reach each path, and the D10 spellings: the order, step 6.
- The ledger: [ice-resource-bound-1-ledger](../../task/ledgers/staging/ice-resource-bound-1-ledger.md).
- Up: [map](map.md).
