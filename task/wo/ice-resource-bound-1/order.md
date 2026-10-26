# ICE-RESOURCE-BOUND-1 (U-11) — a measured, written resource bound; exceeded means refused     grade: B   engine band: Claude Opus 5.5 measure and build, one scoped Opus 5.5 verifier   release: 1.5.6

**Status:** order, **awaits the owner's adopt of the 2026-10-20 plan**. Not dispatched; opens
only after U-9 (ICE-INTEGRITY-PINS-1) merges with pin K. Written by Claude (claude-opus-5-5),
clerk, on 2026-10-20, from card
[ICE-RESOURCE-BOUND-1](../../roadmap/mid-term/ice-resource-bound-1-card-2026-10-20.md), the
closeout plan's U-11 row, the 10-17 replan's U-11 text (unmerged branch, not adopted), audit
rows PP-01, PP-02, PA-10 and PA-11, and the U-9 sketch §0. Paths read on main `08b65ff1`; nothing
was run.

**Step 0 outcome (2026-10-26).** The measurement is done and halts for the owner's rulings. No
product code changed. The bound table is [evidence/step-0-bound-table-2026-10-26.md](evidence/step-0-bound-table-2026-10-26.md),
with its JSON beside it: 261 cells on format v2 and v3, every cell under the 40 GiB scope, the
largest process peak 14.1 GiB, and no cell reached the cap. The probe is
[probe_bound.py](probe_bound.py).

**Proposed bounds**, as the table's proposal lines state them (allowance = pool / 0.6; margins 1.14 / 1.15 / 1.12):

- PP-01: ordered input Arrow bytes <= 0.10 x pool. Basis: growth/input <= 4.68 (4M rows, v2, 32 partitions, writers 1). At the 8 GiB default pool, 819 MiB.
- PP-02: updated-row bytes (matched plus NOT MATCHED BY SOURCE) <= 0.05 x pool. Basis: growth minus pool >= 8.90 x target bytes (4M rows, v3, 10 % plus NOT MATCHED BY SOURCE, pool 2048 MiB). At the 8 GiB default pool, 410 MiB.
- PA-11: changelog source Arrow bytes <= 0.0625 x pool. Basis: growth/source 7.38 to 7.83 on every cell. At the 8 GiB default pool, 512 MiB.

**Orchestrator reproduction (2026-10-26).** One 4M-row cell per path: PP-01 statement growth 1,008 MiB against the lane's 1,006; PP-02 3,983 against 3,986; PA-11 1,700 against 1,707. All three are within 1 percent.

**Census row (owner ruling 2026-10-22).** PP-01 is reached by D2, D3, D4, D5, D10 and D8 (copy-on-write). It is not reached by D1, whose fork sort is charged to the pool and refuses, nor by D6. PP-02 cells record D9 at runtime. Doors D14 to D21 are not reached from Python, per the notes. The WRITE-SPINE-1 ledger (C-009) names D14 as the bare-session `repark.sql()` door, so the owner should confirm that reading.

**Spark 4.1.2 control (1 GiB driver, 4M rows).** The ordered write spills (304 MiB memory, 151 MiB disk) and the MERGE spills (736 MiB memory, 109 MiB disk). The changelog `LIMIT 10` needed no spill.

**Questions for the owner (await the owner's ruling before step 2).** Premises and leans are the hand-back's.

- **Q1, refusal error class and text.** Premise: order section 1, "not ruled". Lean: the same class as today's pool refusal (DataFusion `ResourcesExhausted`, which surfaces as `PySparkException`); the text names the statement kind, the estimate, F x pool and the key; no data-deleting action.
- **Q2, session key and default.** Premise: not ruled. Lean: one fraction key per path, `repark.write.ordered.max-input-fraction` = 0.10, `repark.merge.mor.max-updated-fraction` = 0.05, `repark.changelog.max-source-fraction` = 0.0625; 0 means no bound. The alternative charges the drained input to the pool (PP-01 input <= ~0.24 x pool) with no new key.
- **Q3, PA-10 close or declare.** Premise: measured in this nested scope, the root `memory.max` is absent, the leaf scope has 40 GiB, and the detector reads `/proc/meminfo` (125 GiB) and sets the 8 GiB pool. Lean: close for cgroup v2 (walk the leaf and its ancestors, take the minimum); declare for v1, macOS and Windows, with an explicit budget required.
- **Q4, sizes multiple.** Premise: the closeout says 1x, 4x and 16x; the replan says the audit grids. Lean: 1x, 4x and 16x of the audit grids, as measured; no series reached the 40 GiB cap.
- **Q5, pool peak reader for step 1 pins.** Premise: no session surface exposes `MemoryPool` reserved bytes or a high-water mark. Lean: the step 1 sketch names a test-visible reader, or the pins use refusal or commit at a chosen pool instead.

**Owner rulings (2026-10-26, "adopt all of it, go ahead with the U-11 sketch").** Step 1 opens on Claude Opus 5.5.

- **Q1 RULED:** the same class as today's pool refusal (DataFusion `ResourcesExhausted`, surfacing as `PySparkException`). The text names the statement kind, the estimate, the fraction times the pool and the key. It names no data-deleting action (ruling 76(a)).
- **Q2 RULED:** one fraction key per path with the measured defaults: `repark.write.ordered.max-input-fraction` = 0.10, `repark.merge.mor.max-updated-fraction` = 0.05, `repark.changelog.max-source-fraction` = 0.0625; 0 means no bound. The input is not charged to the pool: that changes write-path accounting, a wider change than a stated bound.
- **Q3 RULED:** close PA-10 for cgroup v2 by walking the leaf and its ancestors and taking the minimum `memory.max`. cgroup v1, macOS and Windows are declared as dated rows that require an explicit pool budget.
- **Q4 RULED:** 1x of the audit grids is pinned in the pull-request tests; 4x and 16x run in the acceptance nightly only.
- **Q5 RULED:** no pool-peak reader. The pins assert refusal or commit at a chosen pool size.

**Out of scope, observed (from the hand-back):**

- The static door classifier answers D8 for `MERGE ... UPDATE SET t.a = s.a`, and the runtime record answers D9. Carded as [WRITE-DOOR-CLASSIFIER-D8-D9-1](../../roadmap/mid-term/write-door-classifier-d8-d9-1-card-2026-10-26.md).
- In-pool sorts refuse rather than spill under 32 to 512 MiB pools on this host. A 1M-row `ORDER BY` at 32 MiB refuses; the MoR MERGE target sort needs about 10 to 40 times the target bytes of pool.
- The unordered 32-partition write grows 407 to 448 MiB at 1M rows (the PP-06 fan-out buffers).
- The brief names `crates/repark-sql` for the door enum; the enum lives in `crates/repark-spark/src/write_door.rs`.

## 0. Why, and what is out of scope

The v2 list asks for a stated resource bound (closeout plan, U-11): the ordered write (PP-01)
and the merge-on-read MERGE (PP-02) hold their whole input or matched state outside the session's
pool, so a large enough statement exhausts the process instead of refusing. The definition
(#1044) asks the engine to spill where it can and refuse cleanly where it cannot; neither path
spills, so this unit measures the bound, writes it down and refuses above it.

Out of scope, by name: spilling either path; a streaming MERGE; PP-03 onwards; changing the commit
path, the fork or the error taxonomy beyond the one refusal; removing multi-match validation;
bounding Python allocations.

## 1. Rulings already made

- **Owner, 2026-10-13:** the closeout plan's v2 list, including "a stated resource bound …
  exceeded means refused" (U-11), and Iceberg first until the labels read PRODUCTION.
- **#1044, merged 2026-10-20** (`docs/production-grade.md`, "Resource discipline"): a declared
  bound; spill where it can, refuse cleanly where it cannot.
- **U-9 sketch §0 (#1067, merged 2026-10-20):** U-11 waits on the kill pin, so that a refusal
  above the bound and a crash have two different pinned outcomes.
- **Ruling 76** (carried by the 2026-10-20 plan): a hint names no data-deleting action.
- **Halts 55, 63, 70, 73; standing guidance 4, 5 and 8** (2026-10-20 plan §7).
- **Not ruled, so not decided here:** the refusal's error class and exact text; the session key
  that raises the bound and its default; whether PA-10 closes or is declared; the sizes multiple
  (the closeout plan says 1×, 4×, 16× the acceptance sizes; the replan names the audit grids).
  The measurement step proposes each; the owner rules before step 2.

## 2. Files

- `crates/repark-iceberg/src/write/distribution.rs` — `sort_batches_by_default_order` and the
  sorted fan-out arms (PP-01).
- `crates/repark-iceberg/src/write/merge/row_lineage.rs` — the lineage writer's drain (PP-01).
- `crates/repark-iceberg/src/write/merge/merge_on_read.rs` — `matched_work_mor` (PP-02).
- `crates/repark-core/src/session/spill.rs` — the pool and the detector (PA-10).
- `crates/repark-iceberg/src/catalog/changelog_view.rs` — the changelog view (PA-11).
- Tests: Rust pins beside U-9's `kill_mid_write.rs` in `crates/repark-spark/src/tests/` (one new
  module), detector unit tests in `repark-core`; the facade cell under
  `python/repark/tests/writes/`; each with its `map.md` row.
- Docs: the bound table as a new dated file under `docs/perf/` with its `map.md` row; PP-01 and
  PP-02 in `docs/perf/production-performance-audit-2026-10-11.md`; PA-10 and PA-11 in
  `docs/cutover/production-audit-2026-10-11.md`; the 1.5.6 release-notes draft; the card.
- Ceilings from `scripts/check_rust_file_size.py`; none rises.

## 3. Skeletons

Grade B: none. Step 1's sketch names the charge point per path, the refusal's signature and where
the bound is read from the session.

## 4. Steps

0. **Measure first** (docs and a probe only, one commit). The census row first (owner,
   2026-10-22): for each path this order bounds, the doors that reach it and the doors that do
   not, with Spark's behaviour at the bound on each; the hand-back ends with "doors I did not
   touch, and why". Then the sizes: PP-01 at 100k and 1M rows ×
   8 columns, ordered and unordered tables, unpartitioned and 32 identity partitions, writers 1
   and 4, pool 32 and 128 MiB; PP-02 at 200k and 1M rows over 32 data files, match fractions 1,
   10 and 100 %, a NOT MATCHED BY SOURCE leg, the same pools and writers; PA-11 a large changelog
   under a small pool with `LIMIT` and a narrow projection; then the closeout plan's 4× and 16×
   of the largest size that completes. The command: one release build, one probe script beside
   this order, each cell in its own process under `/usr/bin/time -v` (peak RSS) with the pool
   peak and spill bytes read from the session, the worker memory cap the lane brief names. The
   output: a table of input bytes, pool size, pool peak, process peak RSS, wall, files and
   per-file order, on format v2 and v3. **The number that becomes the bound:** per path, the
   largest input bytes per pool byte at which peak RSS stays under the process allowance with
   the headroom the table shows, proposed with its margin. Halt and hand the table to the owner;
   the owner rules the bound, the text and the key.
1. **Sketch** (one commit): the charge point on each path, the refusal and where it fires
   (before staging when the input size is known, else at the crossing), the detector rule for
   PA-10, the PA-11 declaration. One seam per path; a second halts.
2. **Red first** (tests only): the refusal cells above the bound on each path and each format
   version, and the controls below it, red on main.
3. **The refusal** (one commit per path).
4. **The detector** (one commit) or the PA-10 declaration, as ruled.
5. **Pins** on format v2 and v3: above the bound the statement refuses with the ruled text, the
   current snapshot and rows are unchanged, staged files are counted as pin K counts them; below
   it the write commits, values, types and per-file order equal main; the same statement under a
   kill (pin K) gives the kill's outcome, not the refusal's.
6. **Records** (one commit): the bound table under `docs/perf/` with the run ids, the four audit
   rows re-dated, the 1.5.6 line, the card, maps, a ledger under `task/ledgers/staging/`.

## 5. Gates and their expected output

| Command | Green means |
|---|---|
| `make verify` | exit 0 |
| `make preflight` | exit 0 |
| `make rust-panic-ban`, `make rust-clippy` | exit 0 |
| `python3 scripts/check_rust_file_size.py` | `files clean`, no ceiling raised |
| the new Rust module and the detector tests | every test passes, on v2 and v3 |
| U-9's `kill_mid_write.rs` | unchanged and green |
| write-cost medians on the write set below the bound | each within 1.05 of main (the bar ICE-STRUCT-PAIRING-1's order records from the owner's 2026-10-13 ruling; whether it binds here is the owner's call) |
| `make check-docs-links check-docs-compaction check-ledger-grammar`, `python3 scripts/sync_map_md.py --check` | exit 0 |

CI green before the verifier; one fresh, scoped Opus 5.5 verifier on the pins, the bound table
and the never-worse run; a FAIL parks and the owner hears first. The acceptance run and the P2
report land before the next lane opens.

## 6. Halt rules

Stop, commit what is green, and hand back on any of: the measurement table done (the owner rules
before step 2); a second seam on either path; a statement main answers below the bound that the
head refuses (ruling 70); a refusal that leaves a new snapshot or a changed row; a refusal text
that names a data-deleting action; a cost median above 1.05; a change needed in the commit path,
the fork or the error taxonomy; U-9's pin K not on main.

## 7. Hand-back

One JSON object: `status`; `head`; `commits`; `measurement` (the table's path, the proposed bound
per path with its margin); `pins` (`red_on_main`, `green_on_head`, per path and format version);
`kill_vs_refusal` (the two outcomes); `cost`; `gates` (command and exit code); `halts`;
`questions` (id, question, premise, lean); `out_of_scope_observed`.

Standing rules from [TEMPLATE.md](../TEMPLATE.md): no code comments from Anthropic models and
relocated code sheds its comments; `map.md` lockstep; the scratch clone, never the live worktree;
never push, never open a pull request; the TRO-Wolf identity and exactly one trailer,
`Authored-By: Claude (claude-opus-5-5) <noreply@anthropic.com>`.
