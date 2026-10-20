# ICE-RESOURCE-BOUND-1 (U-11) — a measured, written resource bound; exceeded means refused     grade: B   engine band: Claude Opus 5.5 measure and build, one scoped Opus 5.5 verifier   release: 1.5.6

**Status:** order, **awaits the owner's adopt of the 2026-10-20 plan**. Not dispatched; opens
only after U-9 (ICE-INTEGRITY-PINS-1) merges with pin K. Written by Claude (claude-opus-5-5),
clerk, on 2026-10-20, from card
[ICE-RESOURCE-BOUND-1](../../roadmap/mid-term/ice-resource-bound-1-card-2026-10-20.md), the
closeout plan's U-11 row, the 10-17 replan's U-11 text (unmerged branch, not adopted), audit
rows PP-01, PP-02, PA-10 and PA-11, and the U-9 sketch §0. Paths read on main `08b65ff1`; nothing
was run.

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

0. **Measure first** (docs and a probe only, one commit). The sizes: PP-01 at 100k and 1M rows ×
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
