# GATE-RETIRE-1 — retire the R-011 gate arm the zone seam made unreachable, and its stale hint sentence

grade: B · engine band: self-directed, Claude Opus 5.5 (`claude-opus-5-5`) only · release: 1.5.6 · base: `origin/main` at `e91dd8fa` or later

## 0. Why, and what is out of scope

ZONE-SEAM-1 (PR #1139, merged 2026-10-23) converts a container cast of an instant to the session wall
before the router's R-011 gate reads the plan, so the gate arm `casts_a_zoned_value_to_naive`
(`crates/repark-spark/src/router/nested_ns.rs`, the seam sketch's arm A2) no longer meets the cast it was
written for, and the `MICROS_HINT` sentence "(a CAST of the whole struct, array or map is refused, …)" in
`crates/repark-iceberg/src/write/nested_ns_gate.rs` tells the user to avoid a shape that now stores
Spark's value. This unit removes the arm and the sentence, and proves with a measurement, not an
argument, that no cell refuses less than Spark does afterwards. Source of the unit: the 2026-10-20
pathway (owner-adopted) and `task/handoff-frontier-2026-10-23.md` §3 item 3; the arm is named in
`task/wo/zone-seam-1/sketch.md` §6 and in R-011's status line of 2026-10-22 in
`docs/spark-sql-iceberg-parity.md`.

Out of scope, by name: arm A1 (`holds_zoned_timestamp`, the planned-type read) and its New York nested
INSERT cost cell, which WRITE-SPINE-1 step 4 retires after the v2 label (card
`task/roadmap/mid-term/write-spine-1-card-2026-10-22.md`, step 4); arm A3 (`shifts_in_an_update`); arm
A4 (unplannable statement or `PREPARE`); the zone arm (`session_is_utc`); the ZS-9 refusal (step 4);
R-015 (`refuse_nested_ns_supply`); the DataFrame door's `expr()` cast refusal
(`test_the_dataframe_door_refuses_a_struct_or_array_cast_type_as_on_main`); the aggregate, grouping-key,
condition and sibling-field reads accepted as cost by owner ruling 2 of 2026-10-21 (they are reads of
A1's walk and stay until step 4); any re-recording of the step-0 census fixture; the U-8 `probe.py` matrix
(GR-6 below).

## 1. Rulings already made

- **GR-1** (owner pathway 2026-10-20; handoff item 3): remove only what the seam made unreachable; refuse
  only what Spark refuses. A removed arm must be shown unneeded by measurement.
- **GR-2** (card step 4; ruling ZS-4, 2026-10-22): A1 and ZS-9 stay. This unit touches no other arm.
- **GR-3** (orchestrator, 2026-10-25): the measurement is the WRITE-SPINE-1 step-0 census replayed on the
  head (`_record_write_spine_1.py --main`, 12,192 cells) and compared cell by cell with the committed
  main fixture `python/repark/tests/timestamps/write_spine_1_main_7a789fc5.json` (replay 0 of 12,192
  differing on main at `e91dd8fa`, measured by step 3b). The unit is green when every differing cell is
  `REFUSED` in the main fixture and equals the Spark fixture's value on the head (a listed move,
  refusal→EQUAL). Any cell that moves otherwise (an `EQUAL` that leaves, a refusal that becomes
  `DIFFERENT` or `ERROR`) is halt H2.
- **GR-4**: `casts_zoned_to_naive` in `nested_ns_gate.rs` is removed with its only caller. If a grep finds
  another caller, it stays and the hand-back says where.
- **GR-5**: the expected hint text in `crates/repark-spark/tests/ntz_nested_refuse.rs`
  (`the_refusal_names_the_table_the_column_and_the_leaf`) is updated to the new sentence; its banned-word
  loop stays and must pass (ruling 76(a): no hint names a data-deleting action). No other expected value
  in any test changes in this unit.
- **GR-6**: the census replay supersedes the U-8 `probe.py` matrix for this unit (its 28 shapes are the
  census's probe spellings); `probe.py` is not run.
- **GR-7** (owner, 2026-10-24, clerk tier): the builder commits code and tests only, with its own trailer.
  The ledger `task/ledgers/staging/gate-retire-1-ledger.md`, R-011's dated status line, the R-015
  "unchanged" line, the card lines, the release-notes line and every `map.md` row are the clerk's commit.
  The builder writes `handback-notes.md` (git-excluded) in the clone root instead.
- **GR-8**: if GR-3 lists moves, the main fixture is not re-recorded here. The head replay document stays
  out of `python/repark/tests/timestamps/` (so `test_head_replay_equals_main_fixture` skips) and the moves
  are tabled in the hand-back for the orchestrator's fixture ruling. With zero moves, the head replay is
  placed beside the fixtures (git-excluded) so that test runs and is green locally.

## 2. Files (exact; ceilings from `scripts/check_rust_file_size.py`, default 1000)

| file | change | lines now |
|---|---|---|
| `crates/repark-spark/src/router/nested_ns.rs` | remove `fn casts_a_zoned_value_to_naive`; remove its two uses (`lineage_has(&plan, &columns, casts_a_zoned_value_to_naive)` in the supply walk near line 513 and `!lineage_has(&plan, &picked, casts_a_zoned_value_to_naive)` in `naive_columns` near line 554); drop `casts_zoned_to_naive` from the import list | 910 |
| `crates/repark-iceberg/src/write/nested_ns_gate.rs` | `MICROS_HINT`: the parenthesis becomes "(in an UPDATE assign the field itself)"; remove `pub fn casts_zoned_to_naive` under GR-4 | 575 |
| `crates/repark-spark/tests/ntz_nested_refuse.rs` | the expected text under GR-5; one new pin (§3) | — |

No file is added except as §3 says; no ceiling is raised; no `map.md`, ledger or doc is edited by the builder.

## 3. The pin (one new Rust test in `ntz_nested_refuse.rs`)

`a_whole_container_cast_of_an_instant_stores_the_session_wall_and_the_hint_no_longer_names_it`: in a
`America/New_York` session, on a table with a `STRUCT<a: INT, b: TIMESTAMP_NTZ>` column, `INSERT INTO …
SELECT id, CAST(named_struct('a', 1, 'b', x) AS STRUCT<a: INT, b: TIMESTAMP_NTZ>), 0 FROM ice.ns.src`
(x an instant) stores the session wall, equal to the Spark fixture's value for the matching census cell
(`write_spine_1_spark_oracle.json`; name the cell id in `handback-notes.md`), one snapshot; and the
refusal raised by the existing unprovable shape (`named_struct('a', 1, 'b', x)`) does not contain the
words "whole struct". Before the arm is removed this test must be green on its first half only if the
seam already moved that cell (then the pin guards the move); either way its second half is red on main.

## 4. Steps (each commit its own step; trailer `Authored-By: Claude (claude-opus-5-5) <noreply@anthropic.com>`)

0. Read, in full: this order; `task/wo/zone-seam-1/sketch.md` §6; `nested_ns.rs` lines 480–570 and
   830–910; `nested_ns_gate.rs` lines 1–30 and 196–215; `ntz_nested_refuse.rs` lines 330–372;
   `_record_write_spine_1.py` argument block and `_write_spine_1_matrix.py::load_cells`. Nothing else
   unless a step needs it.
1. Commit this order file alone (the orchestrator wrote it; the commit is yours).
2. Build once: the native module as `COMMON.md` says, and `cargo build -p repark-spark --tests`.
3. **Measure first, on the unmodified head.** Apply the mutant `casts_a_zoned_value_to_naive → false`
   (body only). Run `cargo nextest run -p repark-spark -E 'test(nested_ns) | binary(ntz_nested_refuse) | test(nested_ns_routes)'`
   and `.venv/bin/python -m pytest python/repark/tests/timestamps/test_zone_seam_1.py -q`; record every red
   test by name (those are the arm's live pins; "none" is a finding too). In the background, under the
   memory scope, replay the census with the mutant build:
   `.venv/bin/python python/repark/tests/timestamps/_record_write_spine_1.py --main --base mutant-<sha> --out <scratch>/census-mutant.json`,
   then diff it against the main fixture with a five-line script (id, main status, head status, head
   value, Spark value). Revert the mutant. Decide by GR-3: proceed, or halt H2 with the table.
4. Edit the three files of §2 and write the §3 pin. Run the step-3 test selection and the §3 pin until
   green. Commit: code and tests only.
5. Gates (§5). Replay the census on the final head in the background while the gates run:
   `--main --base <head short sha> --out python/repark/tests/timestamps/write_spine_1_head_<sha>.json`
   (zero moves) or `--out <scratch>/census-head.json` (moves listed, GR-8). Diff as in step 3. Run
   `python/repark/tests/timestamps/test_write_spine_1_doors.py`, `test_write_spine_1_enumeration.py`,
   `test_write_spine_1_door_carrier.py` and `test_zone_oracle_1.py`.
6. `handback-notes.md` and `handback.json` (§7). No further commit.

## 5. Gates and the line that means green

| gate | green |
|---|---|
| comment grep (`COMMON.md`) | prints nothing |
| `make rust-fmt-check rust-clippy rust-panic-ban` | exit 0 |
| `python3 scripts/check_rust_file_size.py` and `scripts/check_crate_dag.sh` | clean, no ceiling line changed |
| `cargo nextest run -p repark-spark -E '…'` (step 3 selection plus the §3 pin) | all passed, counts in the hand-back |
| `cargo nextest run -p repark-iceberg` | all passed |
| pytest `test_zone_seam_1.py`, the three `test_write_spine_1_*.py`, `test_zone_oracle_1.py` | passed, 1 skipped per live tier at most |
| census diff on the head | `0 of 12192 differing`, or the GR-3 move table with every row refusal→EQUAL |
| `grep -rn "whole struct, array or map" crates/` | prints nothing |

## 6. Halt rules

- **H1** the runtime model id is not `claude-opus-5-5`.
- **H2** a census cell moves other than refusal→EQUAL (GR-3), or a step-3 red test asserts a refusal that
  Spark also makes for that shape (the arm is load-bearing there). Hand back the table; change nothing.
- **H3** a change is needed in a file §2 does not name.
- **H4** the replay cannot run (infrastructure); hand back partial with the gates that did run.
- **H5** the hour budget in the brief.

## 7. Hand-back

`handback.json` per `COMMON.md`, model id first, plus: the step-3 red-test list; the step-3 and step-5
census recounts (cells compared, differing, the move table if any); the §3 pin's census cell id;
`doors_not_touched` (the DataFrame `expr()` refusal, the native doors D14–D20, which the router gate does
not reach, and D21). `handback-notes.md`: one line per touched file (purpose, the claim it proves, the
test a `pins:` citation cites), the measurement sentences for ledger rows C-001 (arm retired, never
worse), C-002 (hint sentence, 76(a) word loop) and C-003 (the step-3 finding: which pins the arm still
had, or none), and the exact sentence for R-011's dated status line.

## Outcome (2026-10-25)

Measurement: the step-0 census replayed on the head differs from main in 0 of 12192 cells, but the census carries whole-container casts only in table-creating spellings, and beyond it an arity-mismatch CAST and a cross-position TRY_CAST of a whole struct store the UTC wall or NULL with the arm removed, where the arm refuses both in New York. Ruling: the Muse MAX verifier's round-1 FAIL on f7b78fed (S1-1 and S1-2, reproduced by the orchestrator on the head build and on Spark 4.1.2) made the arm load-bearing, and GR-9 retains it, so the removal in §0 and the removal steps are withdrawn. Shipped: the micros hint now says a whole-container CAST is refused unless its fields line up with the target's, with a renamed pin and four new pins holding the arm's refusals of INSERT, UPDATE and MERGE shapes (ledger C-002 and C-005), and ledger C-001 is REJECTED. Folds into WRITE-SPINE-1 step 4: retiring the arm, once such whole-container casts refuse as Spark refuses them (card CONTAINER-CAST-SHAPE-1).
