# ICE-REPLACE-KILL-DOORS-1 — kill-mid-write pins for the replace doors     grade: B   engine band: Opus 5.5 builder; Muse MAX verifier   release: 1.5.6

Opened by the orchestrator (Claude Fable 5.1) on 2026-10-27 under the owner ruling of the same day ("adopt all three leans"): the replace doors get a card and a small pin unit before the format-v2 label (ICE-V2-LABEL-1, PR #1195) re-dates. Source: ICE-INTEGRITY-PINS-1 ledger C-024 (verifier S3, 2026-10-22): `publish_replace_table` is forwarded unheld by the hold catalog, and none of the seven pinned kill doors calls it, so RTAS and `CREATE OR REPLACE TABLE … AS SELECT` have no kill-mid-write pin. Card: `task/roadmap/mid-term/ice-replace-kill-doors-1-card-2026-10-27.md` (filed on the label branch, PR #1195).

## 0. The gap

`crates/repark-spark/src/tests/kill_mid_write.rs` pins K-1 and K-2 (INSERT), K-3 (INSERT OVERWRITE), K-4 and K-5 (MERGE), K-6 (DELETE), K-7 (CREATE TABLE AS SELECT) and the controls K-C0 and K-C1, on format v2 and v3, through `kill_mid_write_hold.rs`'s `HoldCatalog`, which holds `update_table` and `publish_create_table` under `HoldMode::Hold` and forwards `publish_replace_table`. A writer killed inside a replace has no pin that the table stays at its last committed snapshot and that its orphans are counted.

## 1. Rulings already made

- The pin shape is the existing one: the child reaches the hold, is killed, the parent asserts the state (rows, snapshot ids, newest metadata, orphan counts within the ruled ceiling) and the dry-run orphan count (ICE-INTEGRITY-PINS-1 rulings K-1 to K-4 stand).
- No product change is expected. If the replace path on a kill leaves a wrong state (a lost table, a served orphan, a stale pointer), that is a halt: the finding goes to the owner before any fix.
- Ruling 76(a): no refusal text, hint or record names a data-deleting action.

## 2. Steps

0. **Confirm the doors (short; in the hand-back, no separate round).** Which SQL spellings reach `publish_replace_table` on this head: `CREATE OR REPLACE TABLE … AS SELECT` over an existing table, and `REPLACE TABLE … AS SELECT` if the parser accepts it. A spelling the parser refuses is recorded as refused, not pinned.
1. **Build.** `HoldCatalog::publish_replace_table` holds under `HoldMode::Hold` as `publish_create_table` does (and forwards under the other modes). New cells K-8 (`CREATE OR REPLACE TABLE fault.sales.t … AS SELECT` over the seeded table) and K-9 (`REPLACE TABLE … AS SELECT`, if accepted), on v2 and v3, each asserting: after the kill the table reads the seeded rows, the snapshot id set is unchanged, the newest metadata file is the seeded one, orphan counts are within the ceiling and match the dry run. A mutant per cell: forward instead of hold reds the cell. No other file than `kill_mid_write.rs` and `kill_mid_write_hold.rs` changes (a fixture helper inside them is fine); file-size ratchets hold.
2. **Measure.** `cargo test -p repark-spark kill_mid_write` green on the head; the two mutants red; wall per cell recorded in the hand-back notes.
3. **Records** (clerk, Haiku 5.5 xhigh): ledger `task/ledgers/staging/ice-replace-kill-doors-1-ledger.md` (PROVEN rows with pins; one row citing ICE-INTEGRITY-PINS-1 C-024 as closed by this unit), the card retires, the integrity registry row's declared line (filed on the label branch) is superseded, maps, release-notes line (test-only, no stored value changes).

## 3. Gates

`cargo test -p repark-spark`, `make rust-panic-ban check-rust-file-size rust-fmt-check rust-clippy`, the comment gate (no comments in code), `bash scripts/check_map_md.sh --base origin/main` before the PR. CI green before the verifier; one Muse MAX verifier (reproduces each cell and each mutant); merge on the owner's word.

## 4. Halts

A wrong post-kill state on the replace path (owner first); a cell that cannot reach the hold within the harness's 60 s; any product file change.

## Step 0 outcome and ruling RKD-1 (orchestrator, 2026-10-27)

Both doors reach `publish_replace_table` through one path: `CREATE OR REPLACE TABLE … AS SELECT` over an existing table (`execute_ctas`, `CtasMode::Replace`, `StagedTableTransaction::begin_replace`, commit). `REPLACE TABLE … AS SELECT` is accepted by the parser (`rewrite_replace_table` prepends `CREATE OR`; a missing target is refused).

The builder halted on step 1 as section 4 requires: on the local catalog the replace writes its staged metadata file before it calls `publish_replace_table`, so a child killed at the hold leaves that file as the newest. Section 2's expected post-kill state (the seeded rows, the seeded snapshot set, the seeded metadata newest) was the orchestrator's error: by owner ruling 83 and orchestrator ruling K-1 (ICE-INTEGRITY-PINS-1) the local commit point is the newest metadata file, so the reopen reads the replaced table, consistent and writable, with no orphan, the same shape K-7 pins for `CREATE TABLE … AS SELECT`. Nothing is lost, no orphan is served, no pointer is stale.

**Ruling RKD-1.** Reading (a): K-8 and K-9 pin the K-1 reading on format v2 and v3 (the staged replace file newest; current is the attempted snapshot; the snapshot set is the seeded set plus the attempted one; the replace's rows and types; orphans 0/0/0/0/0 equal to the dry run; a follow-up INSERT reads back). The mutant discriminator is test-only: the hold writes a "holding" marker and the child writes a "returned" marker after its statement; the replace cells kill only once "holding" exists and assert "returned" is absent, so forward-instead-of-hold reds on state 20 of 20 (without it, 16 of 20 through the exit race). K-1 to K-7 and the controls are unchanged. A replace kill on Glue and S3 Tables is governed by the catalog pointer and has no cell (pin U's territory); the registry carries that as a dated line until a cell exists.

Build: `6585e4ac`, two test files, 28 kill-mid-write tests green, walls 339 to 357 ms per cell; reproduced by the orchestrator (4 of 4 green).

## Clerk hand-back (2026-10-27, claude-haiku-5-5)

Steps 1 to 5 are done on `fix/ice-replace-kill-doors-1`, one commit each, on top of the builder's
`6585e4ac`. No code file and no `.github/` file is touched. The branch is not pushed, no PR is open
and nothing is merged.

Commits (oldest first): `9e415fef` the map rows (the tests map, the `task/wo` index and this
order's map); `1292157d` the unit ledger `task/ledgers/staging/ice-replace-kill-doors-1-ledger.md`
and its link in the staging map; `9864fcbf` the dated line on ICE-LOCAL-COMMIT-POINT-1 in
`docs/spark-sql-iceberg-parity.md`; `92d84897` the one-line release-notes entry under "Behaviour
changes"; the last commit on the branch is this section.

Ledger: eight clauses. Six PROVEN, each cited by `pins:` from the tests map: C-001 the parser door,
C-002 the reading (a) assertions of the four replace functions, C-003 the hold and the SIGKILL, C-004
the mutant (20 of 20 red with the discriminator, 16 of 20 without), C-005 the K-1 to K-7 diff, C-006
the local-catalog closure of ICE-INTEGRITY-PINS-1 C-024. Two OPEN: C-007 the Glue and S3 Tables replace
kill (catalog pointer, no cell, pin U) and C-008 the verifier round (one Muse MAX verifier, and the
question of a cell for the options path and the native door).

Gates (clerk run, exit codes): `bash scripts/check_map_md.sh --base origin/main` 0;
`python3 scripts/sync_map_md.py --check` 0; `make check-ledger-grammar` 0 (357 live ledgers);
`make check-ledgers` 0; `make check-docs-links` 0; `make check-docs-compaction` 0; `make check-model-ban`
0; `make check-owner-ruling` 0. The commit hook ran on each commit and passed; `--no-verify` was not used.
The builder's Rust gates stand as recorded in the unit ledger (C-002): `cargo test -p repark-spark
kill_mid_write` 28 passed, and the fmt, clippy, panic-ban and file-size gates. The clerk ran no cargo
command, since no code changed.

Deviations from the order's text, recorded for the orchestrator: the order says K-1 to K-7 and the
controls are "byte-for-byte unchanged"; the build removes three lines from the shared helpers (C-005
states the exact change). The holding file is written by `hold()` for every held cell; only the replace
cells wait on it, and `returned` is written only by them. C-024 stays OPEN in its own staging ledger,
which is append-only; its closure is recorded here as C-006.
