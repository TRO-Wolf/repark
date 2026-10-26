# handback-notes — ICE-TSNS-UPDATE-RANGE-CLASS-1 (U-4, R-012)

HALT outcome: the brief's premise is false on this head, so there is no product
fix, no seam taken, no cost table, and no guards relaxed. The pins below hold
the row's already-correct behavior.

## Why there is no fix

Row R-012 (measured 2026-10-09) says the unfiltered UPDATE of an out-of-range
`TIMESTAMP` literal reports `[CAST_INVALID_INPUT] … malformed`. On this head
every one of the 16 cells reports INSERT's `[CAST_OVERFLOW]` text instead,
byte-identical to the INSERT cell of the same value. The ledger's mechanism
(C-020: the planner-cast peel strips the typed literal's own cast and hands
the kernel text) no longer holds: the Spark type planner
(`planned_type`, since ICE-TSNS-TYPE-PLANNER-1, commit `c7121437`) plans
`TIMESTAMP '…'` as `CAST(text AS Timestamp(us, UTC))`, the peel
(`peel_ns_casts`) strips only nanosecond-targeted casts so it stops at that
inner cast, the microsecond analyzer rule folds it to a microsecond literal,
and the kernel sees integer ticks and reports the range failure. The row was
closed as a side effect of that unit; it was never re-measured.

## Touched files, one line each

- `python/repark/tests/timestamps/test_ice_tsns_merge_wall_1.py` — the 16 R-012 cells plus filtered-UPDATE and strict-off controls.
- `crates/repark-spark/tests/timestamp_ns_wall_doors.rs` — the same 16 cells as one Rust pin.
- `handback-notes.md` — this file.

## Test names for the clerk's `pins:` citations

- `test_unfiltered_update_reports_inserts_overflow_class` — the 16 cells (mode × zone × literal, `RANGE_CASES`).
- `test_filtered_update_reports_inserts_overflow_class` — control, same 16 inputs through `UPDATE … WHERE`.
- `test_out_of_range_literal_stores_null_without_ansi` — control, same 16 inputs, ANSI off, NULL on all three paths.
- `test_range_cases_are_the_sixteen_row_cells` — the parametrization is exactly 16 cases.
- `an_out_of_range_literal_reports_inserts_overflow_on_unfiltered_update` — the Rust pin, same 16 cells.

## Evidence

- Facade gate on the unmodified tree: 546 passed, 1 pre-existing skip
  (`test_write_spine_1_doors.py` replay test, skips without a replay document).
- Rust pin against the pre-fix library: 16 of 16 green (full error-text equality with INSERT).
- Deleted probe: EXPLAIN shows the kernel over
  `TimestampMicrosecond(32503680000000001, UTC)` on both paths.

## Cost

Not measured: the tree carries test-only commits, so base and head are the same binary.

## Out of scope, observed

A bare far string (not a `TIMESTAMP` literal) past the range is refused
`[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]` on the unfiltered UPDATE at
plan time and `[CAST_INVALID_INPUT]` malformed on INSERT. Both pre-date this
unit and are unchanged.

## Recommendation

Close R-012 as already-fixed on this head with the pins above. No verifier
round on a seam (there is none). The R-008 zoned mirror keeps its own unit;
its head behavior is unmeasured here.
