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

Two clerk rounds, one commit each, on `fix/ice-replace-kill-doors-1` on top of the builder's
`6585e4ac` (K-8, K-9) and `34b5e921` (K-10). Round 1 was steps 1 to 5 of the order; round 2 is the
coordinator's follow-up for K-10 and the native door. No code file and no `.github/` file is
touched by the clerk. The branch is not pushed, no PR is open and nothing is merged.

Head: the commit of this section, the last on the branch.

Commits, oldest first:
- `9e415fef` round 1: map rows (the tests map, the `task/wo` index, this order's map).
- `1292157d` round 1: the unit ledger `task/ledgers/staging/ice-replace-kill-doors-1-ledger.md`, linked from the staging map.
- `9864fcbf` round 1: the dated replace-door line on ICE-LOCAL-COMMIT-POINT-1.
- `92d84897` round 1: the release-notes line (amended for bullet spacing).
- `eb6c5213` round 1: this section, first version.
- `34b5e921` builder, K-10 (`test(spark): pin the session-key replace door (K-10)`), not a clerk commit.
- `db0df752` round 2: the K-10 map paragraph; the kill-cell count reads 24.
- `0d121930` round 2: ledger rows C-009 (K-10 cells, PROVEN), C-010 (K-10 mutant, PROVEN), C-011 (supersedes C-005 for the head, PROVEN), C-012 (native-door gap, OPEN, citing the card); C-006 re-worded to the Spark door; C-008 re-worded to the six replace test functions.
- `37632bb0` round 2: card `task/roadmap/mid-term/native-door-replace-kill-1-card-2026-10-27.md` (grade C, open) and its row in the mid-term map.
- `a7eadcec` round 2: the registry line on ICE-LOCAL-COMMIT-POINT-1 names K-8, K-9, K-10 and 24 cells, and names the native-door card; the Pin bullet's 2026-10-22 count gains a dated parenthesis.
- `13817603` round 2: the release-notes line moved from Behaviour changes to the Integrity section (the draft has one), naming the Spark SQL door and stating that the native door is not yet pinned.
- the head: this section.

Ledger: eight clauses in round 1 and four more in round 2. PROVEN: C-001 (parser door), C-002 (reading (a), the four K-8 and K-9 functions), C-003 (hold and SIGKILL), C-004 (mutant 20 of 20 with the discriminator; 16 of 20 without), C-005 (K-1 to K-7 diff, for `6585e4ac`), C-006 (C-024, Spark door, local catalog), C-009 (K-10 cells), C-010 (K-10 mutant, 10 of 10), C-011 (K-1 to K-9 unchanged by the K-10 commit). OPEN: C-007 (Glue and S3 Tables replace kill, no cell, pin U), C-008 (verifier round over the six replace test functions and the mutants), C-012 (native-door replace, carded).

Gates (clerk, exit codes, on the head before this section): `bash scripts/check_map_md.sh --base origin/main`
0; `python3 scripts/sync_map_md.py --check` 0; `make check-ledger-grammar` 0; `make check-ledgers` 0;
`make check-docs-links` 0; `make check-docs-compaction` 0; `make check-model-ban` 0; `make check-owner-ruling` 0.
The commit hook ran on every commit and passed; `--no-verify` was not used. The clerk ran no cargo
command, since no code changed. The builder's Rust gates for `34b5e921` (30 kill-mid-write tests
passed; fmt, clippy, panic-ban, file-size, model-ban) and the orchestrator's 2-of-2 reproduction are
cited in the ledger (C-009), not re-run.

Deviations and points for the orchestrator:
- The order says K-1 to K-7 and the controls are "byte-for-byte unchanged"; the build removes three lines from the shared helpers (C-005 states the exact change, and C-011 carries it to the head).
- The holding file is written by `hold()` for every held cell; only the replace cells wait on it, and `returned` is written only by them.
- C-006 and C-008 were re-worded in place, as the coordinator directed; the ledger is unit-local and unmerged. C-024 itself stays OPEN in its own append-only ledger.
- The registry's Pin bullet (2026-10-22 text, untouched except the dated parenthesis) still lists the K-6 statement by its keyword. Ruling 76 (a) says no text names a data-deleting action, and the K-6 cell may be described by its id only. Left for the owner or the label unit to decide.
- The native door's replace has no cell (C-012, card NATIVE-DOOR-REPLACE-KILL-1); the Spark door's replace (K-8, K-9, K-10) is pinned.
