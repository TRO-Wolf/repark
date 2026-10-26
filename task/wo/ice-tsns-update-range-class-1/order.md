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
