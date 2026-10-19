# ICE-TSNS-FLOOR-DOORS-1 (U-3, R-009): one floor on every door     grade: B   engine: Muse xhigh, guided   release: 1.5.6

**Status:** order of record for U-3 of the Iceberg closeout. Written by the orchestrator
(Claude, claude-opus-5-5) on 2026-10-13 from registry row ICE-TSNS-SQL-1-R-009, ledger question
Q2 of `task/ledgers/staging/ice-tsns-merge-wall-1-ledger.md` (claim C-014) and the closeout
plan's row U-3. Main at writing: `be011012`.

**Owner ruling, 2026-10-13 late:** U-1 (ICE-TSNS-TYPE-PLANNER-1) lands first. This unit's
measurement step 0 may run on Muse now; its build (steps 1 to 4) waits for the planner head and
re-measures the 72 control cells there.

## 0. Why, and what is out of scope

Into a microsecond `TIMESTAMP` column, INSERT floors a nanosecond source (`-1 ns` stores `-1 µs`)
and reads a naive wall in the session zone. `INSERT OVERWRITE`, the MERGE insert arm,
`overwritePartitions` and `insertInto(overwrite=True)` truncate toward zero (`-1 ns` stores
`0 µs`) and read the wall as UTC: 36 cells on each format version, UTC included. Into a
`TIMESTAMP_NTZ` column every door, INSERT included, truncates toward zero. One table can
therefore hold two values for one input, by door. Only values before the epoch with digits
below the microsecond differ by the truncation; the wall difference affects every non-UTC
session.

Out of scope, by name: R-017 (ICE-TSNS-TYPE-PLANNER-1); nested leaves (R-011, R-015,
ICE-TSNS-NESTED-1); the frame door (R-019, fixed); `applyInPandas` (R-020); the create door
(#1048); nanosecond targets (R-008, fixed); any plan walk, marker function or reader
accounting.

## 1. Rulings already made

- **Q-21c-8 (2026-09-18):** a nanosecond value narrowed to microseconds floors. It is the
  oracle; Spark 4.1.2 cannot read the source types, so there is no live Spark cell.
- **Reference door:** what INSERT stores into a microsecond `TIMESTAMP` column today is the
  target for every other door (the R-008 method: one door is the reference, the others equal it).
- **NTZ:** INSERT itself truncates into `TIMESTAMP_NTZ`; the target there is the floor by
  Q-21c-8, on every door including INSERT.
- **Cost gate:** write-cost medians within 1.05 of main, five interleaved sets, same build
  profile both sides (coercion rulings 2026-10-12).
- **Lessons of 2026-10-13 that bind:** no guard infers a unit from an integer's magnitude or
  equality; every provisional choice ships with a revert-alone-stays-green test; a claimed
  clean revert is committed on a throwaway branch with the test count pasted; a cell's expected
  value is derived by the harness from the rule (floor, session-zone wall), never typed in.
- **Proportionality:** counted against the finding class "a door narrows differently from
  INSERT". One fold after a FAIL if the verifier names a seam-consistent fix; a second FAIL
  parks and the owner hears first.

## 2. Files

Known on main:
- `crates/repark-functions/src/timestamp_ns_cast.rs` (`narrow_timestamp_ns_expr`, line 415;
  `timestamp_ns_cast_expr`, line 55; `conform_values_timestamp_columns`, line 510).
- Pins and recorder: `python/repark/tests/timestamps/test_ice_tsns_merge_wall_1.py`,
  `_ice_tsns_merge_wall_1_doors.py`, `_record_ice_tsns_merge_wall_1_main.py`,
  `ice_tsns_merge_wall_1_main.json` (the 72 recorded control cells).
- Method to copy: `python/repark/tests/timestamps/test_ice_tstzns_wall_1.py` (R-008's door matrix).

Not known until step 0: which function each of the four overwrite doors and the NTZ path
calls to narrow. The order names no product file to edit before the sketch does.

## 3. Sketch (owed before step 1)

One page from Opus lane B after step 0, naming the single seam where every door's
nanosecond-to-microsecond narrowing is decided, by target column type. If step 0 shows more
than one seam, the sketch says so and the unit halts for the owner (same test as T-4 applies to
R-016).

## 4. Steps (one commit each)

0. **Measure, no product code.** Re-record the 72 control cells on main's release module and
   confirm the count and the split (truncation; wall read as UTC) against the registry row. Add
   the NTZ doors and negative, zero, positive, sub-microsecond and range-edge values, both
   format versions, three session zones including one with a repeated and a skipped hour. For
   each door print the narrowing function reached (a debug trace or a debugger-free probe on a
   throwaway branch, never merged). Hand back the table and the seam list. **Halt here.**
1. **Fixture red-first.** The door matrix with expected values derived by the harness from
   the rule; commit it failing on main, with the red count.
2. **The seam.** The change the sketch names, at the one point, by type.
3. **Re-pin.** The 72 control cells move from "held as main" to the floor; the recorder file
   and the registry row R-009 (FIXED, dated) move in the same commit; no other test file edits.
4. **Cost.** The interleaved write set, ratios of five-set medians.

## 5. Gates

Release module build inside the build lock with `CARGO_BUILD_JOBS=6`, `CARGO_INCREMENTAL=0`;
the unit's pins plus `test_ice_tstzns_wall_1.py`, `test_ice_tsns_merge_wall_1.py` and the
frame-door pins green; `make rust-panic-ban`; `cargo fmt --all -- --check`; the comment gate
at 0; the docs, ledger and map gates. The whole facade suite, `make verify` and the parity
matrix are CI's. CI green before the verifier.

## 6. Halt rules

More than one seam; any cell where INSERT's own stored value changes for a microsecond
`TIMESTAMP` target; any change to a microsecond-source cell; any statement main answers that
the head refuses; cost above 1.05; a need to edit a file the sketch does not name.

## 7. Verifier and reporting

One fresh Opus 5.5 high verifier, scoped to the door matrix, the R-008 and frame-door pins as
neighbours, mutants on the seam, and cost. This unit changes a write conform, so under rule P2
its merge is reported with the cell count and the acceptance run id before the next lane opens.

## Questions for triage

1. **Order against U-1.** RULED 2026-10-13: U-1 lands first (see the top of this order).
2. **Data already stored.** Tables written by the overwrite doors before the fix hold the
   truncated value and the UTC wall. Lean: a release-note line, no migration.
3. **The NTZ change moves INSERT itself.** Lean: yes, by Q-21c-8; named in the release note.
