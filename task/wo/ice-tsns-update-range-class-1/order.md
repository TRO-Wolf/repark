# ICE-TSNS-UPDATE-RANGE-CLASS-1 (U-4, row R-012) — the unfiltered UPDATE reports an out-of-range nanosecond literal as out of range, like INSERT and the filtered UPDATE     grade: C   engine band: Muse MAX builds, Muse MAX verifies; an Opus 5.5 verifier only if a verifier finds a value moving   release: 1.5.6

**Status:** order, adopted by the owner 2026-10-26 with the v3 label plan (decisions D1, D2). Written by the
orchestrator (Claude Fable 5.1) on 2026-10-26 from registry row ICE-TSNS-SQL-1-R-012, ledger clause C-020 and
question Q5 of ICE-TSNS-MERGE-WALL-1, read on main `ab1b9bc8`. **Branches from main after WRITE-SPINE-1 step 2
(PR #1189) merges**, because the unfiltered UPDATE's conform path is the one that PR re-routes.

## 0. Why, and what is out of scope

Under strict mode (`spark.sql.ansi.enabled`), `UPDATE t SET v = TIMESTAMP '3000-01-01 00:00:00.000001+00:00'` with no
`WHERE` into a `timestamp_ns` column refuses as `[CAST_INVALID_INPUT] … because it is malformed`, where
`INSERT … VALUES` and `UPDATE … WHERE` of the same value refuse as `[CAST_OVERFLOW] The value 32503680000000001 of the
type "TIMESTAMP" …`. 16 measured cells (two far literals in three zones and each zone's edge instant, both row-level
modes; the record is ICE-TSNS-MERGE-WALL-1's). With strict mode off all three paths store NULL. No value is
mis-stored. The unit makes the unfiltered path report the same class and text as the other two. Cause, from the row:
the unfiltered UPDATE runs INSERT's conform, whose planner-cast peel reaches the typed literal's string before the
analyzer has typed it, so the nanosecond kernel parses text and reports a parse failure instead of a range failure.

Out of scope, by name: the stored value in any mode; the NULL under strict-off; the refusal class of any other
statement; the nested leaf refusal (R-015); the conform stage of WRITE-SPINE-1 (`conform_stage.rs` is not touched
by this unit; if the seam is there, halt).

## 1. Rulings already made

- Owner 2026-10-26: D1 runs now; D2 Muse builder and Muse verifier, Opus 5.5 only if a value moves.
- Oracle: documented, INSERT's class, as row R-007 ruled (Spark cannot write the type).
- Cost gate 1.05 on the write set (owner 2026-10-13, carried by every write unit since).
- Ruling 76(a): no refusal text names a data-deleting action.

## 2. Files

- The seam: the planner-cast peel on INSERT's conform path as taken by the unfiltered UPDATE (find it from the
  `crates/repark-iceberg/src/write/` maps and the row's rationale; name the file and function in the hand-back
  before the fix commit). One seam; a second halts.
- Tests: the 16 cells as facade pins in `python/repark/tests/timestamps/test_ice_tsns_merge_wall_1.py` beside
  `test_update_with_no_where_stores_a_literal` (class and text equal to the INSERT cell's, per zone and mode); the
  INSERT and filtered-UPDATE cells kept as controls (their class must not change); the strict-off NULL cells and the
  in-range literal as controls; one Rust pin beside the nearest nanosecond UPDATE test in `crates/repark-sql` or
  `crates/repark-spark` tests, red first.
- Records (clerk): row R-012 → FIXED 2026-10-xx with the pin; the 1.5.6 release-note line; ledger
  `task/ledgers/staging/ice-tsns-update-range-class-1-ledger.md`; `map.md` rows.

## 3. Steps

0. **Sketch line** (in the hand-back, one commit for the red tests): the file, the function, the one-line change.
1. **Red first**: the 16 cells red on main with the `CAST_INVALID_INPUT` text, green only after the fix; controls green
   before and after.
2. **The fix** (one commit): the typed literal keeps its type through the peel (or the peel skips typed cells as
   `INSERT … VALUES` does), so the kernel sees the integer and reports the range failure.
3. **Cost**: the write set medians (plain and nested INSERT, UPDATE, MERGE) within 1.05 of main, like for like.
4. **Hand-back notes** for the clerk round.

## 4. Gates

`make rust-clippy rust-panic-ban`, file sizes clean, comment grep empty, `make py-lint py-format-check`, the touched
facade files, `cargo test` for the touched crate; the clerk's map lockstep; CI green before the verifier; one Muse MAX
verifier round (mutants on the seam, the 16 cells, the controls, the cost table).

## 5. Halt rules

A second seam; any cell whose stored value changes in either mode; a change needed in `conform_stage.rs`, the error
taxonomy or the fork; a cost median above 1.05; a refusal text naming a data-deleting action.

## 6. Hand-back

One JSON: `model_id`, `status`, `head`, `commits`, `seam` (file, function), `pins` (red on main, green on head, per
zone and mode), `controls`, `cost`, `gates`, `halts`, `questions`, `out_of_scope_observed`, `hours_used`.

## 7. Outcome (2026-10-26): closed as already fixed, with pins

**Ruling (orchestrator, 2026-10-26):** U-4 closes as already-fixed with the committed pins. No
product change, no seam, no verifier round on a seam; a Muse verifier checks the pins only.

**Measured premise.** The red premise of §3 step 1 is false on this head. The builder (Muse MAX,
2026-10-26) found that on the unmodified head all 16 R-012 cells (the two far literals in three
zones plus each zone's edge instant, both row-level modes) already report INSERT's byte-identical
`[CAST_OVERFLOW]` text at the Rust door and at the facade door. The orchestrator reproduced it on
2026-10-26 on code equal to main `9fd60149`: INSERT, unfiltered UPDATE and filtered UPDATE of
`TIMESTAMP '3000-01-01 00:00:00.000001+00:00'` into a v3 `timestamp_ns` column under ANSI in
America/New_York all raise `[CAST_OVERFLOW] The value 32503680000000001 of the type "TIMESTAMP"
cannot be cast to "TIMESTAMP_NS" due to an overflow`, and the table keeps its one row.

**Cause.** The row's mechanism (C-020: the planner-cast peel strips the typed literal's cast and
hands the kernel text) no longer holds. ICE-TSNS-TYPE-PLANNER-1 (commit `c7121437`, merged
2026-10-16, PR #1082) plans `TIMESTAMP '…'` as `CAST(text AS Timestamp(us, UTC))`. The peel
(`peel_ns_casts`) strips only nanosecond-targeted casts, so it stops at that inner cast; the
microsecond analyzer rule folds it to a microsecond literal, and the kernel sees integer ticks and
reports the range failure. Row R-012 was closed as a side effect of that unit and was never
re-measured until this one. The seam named in §2 is therefore not on the path.

**Change (test-only).** No product line, so no cost table and no seam was taken.

- `46184319` — 133 facade lines in `python/repark/tests/timestamps/test_ice_tsns_merge_wall_1.py`:
  the 16 cells, `test_unfiltered_update_reports_inserts_overflow_class`, and their controls
  `test_filtered_update_reports_inserts_overflow_class` and
  `test_out_of_range_literal_stores_null_without_ansi`, plus `test_range_cases_are_the_sixteen_row_cells`.
- `d0d60540` — 48 Rust lines in `crates/repark-spark/tests/timestamp_ns_wall_doors.rs`:
  `an_out_of_range_literal_reports_inserts_overflow_on_unfiltered_update`, the 16 cells as one pin.
- Clerk commits on this branch: the removal of the builder's `handback-notes.md` (committed at the
  clone root by mistake in `9e7356a6`), this outcome block, registry row R-012 to FIXED, the ledger
  `task/ledgers/staging/ice-tsns-update-range-class-1-ledger.md`, the 1.5.6 release-note line and the
  map rows.

**Builder gates (on the unmodified tree).** Comment gate clean. `cargo test -p repark-spark --release
--test timestamp_ns_wall_doors` 16 of 16. `pytest` over `test_ice_tsns_merge_wall_1.py`,
`test_ice_tstzns_wall_1.py` and `test_write_spine_1_doors.py`: 546 passed, one pre-existing skip (the
replay test of `test_write_spine_1_doors.py`, which skips without a replay document). `make
rust-clippy rust-panic-ban` clean. File sizes clean. `make py-lint py-format-check` clean.

**Pins.** Red on main: none; the cells are green on the unmodified tree, so they pin a correct
behaviour that the planner already gives (the pins guard a future regression of the planner's
typing, not a fix). Facade pin names: `test_unfiltered_update_reports_inserts_overflow_class`,
`test_filtered_update_reports_inserts_overflow_class`, `test_out_of_range_literal_stores_null_without_ansi`,
`test_range_cases_are_the_sixteen_row_cells`. Rust pin name:
`an_out_of_range_literal_reports_inserts_overflow_on_unfiltered_update`.

**Out of scope, observed (dated notes in R-012, no new unit).**

- A bare far STRING past the range: the unfiltered UPDATE refuses `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]`
  at plan time where INSERT reports `[CAST_INVALID_INPUT]`. Pre-existing, unchanged.
- An in-range 9-digit `TIMESTAMP` literal stores the microsecond-truncated wall identically on INSERT
  and on the unfiltered UPDATE. This is R-024's territory.
- The R-008 zoned mirror (`timestamptz_ns`) keeps its own unit; its head behaviour is unmeasured here.

**Halt rules (§5).** None tripped: no second seam, no stored value changed in either mode, no change
in `conform_stage.rs`, the error taxonomy or the fork, no cost median, no refusal text naming a
data-deleting action.
