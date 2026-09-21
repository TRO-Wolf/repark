# map — repark-core/src/time_travel/incremental

## Purpose

File-backed tests for the Iceberg incremental-read door (`../incremental.rs`): the reader-option
window parser, Java's `incrementalAppendScanBoundaries` refusals, and the time-travel-beside-a-window
refusal order.

## Contents

- `tests.rs` — window parsing (all four option names, case-insensitive; a non-numeric bound),
  `append_boundaries` (exclusive start / inclusive end, end-without-start, a timestamp bound on the
  append door) and `incremental_spec` (time-travel refusal, the legacy `snapshot-id` message after
  the boundary checks, and the resulting `TimeTravelSpec::Incremental`).
  pins: ice-changelog-1/C-001, C-004, C-005, C-006
