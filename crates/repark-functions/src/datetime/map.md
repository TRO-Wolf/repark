# map — repark-functions/src/datetime

## Purpose

Children of [`../datetime.rs`](../datetime.rs), split out when a change would take the parent
past its ratcheted size.

## Contents

- `session_wall.rs` — **ZONE-HORIZON-RENDER-1 (2026-10-08):** the one place this crate turns an
  instant into a session-zone wall clock, or a wall clock into an instant. Every function reads
  the zone through `repark_common::zone_horizon` (`wall_at_instant`, `offset_at_instant`,
  `offsets_at_wall`), so an instant after 2099 takes its offset from the proxy year, the same
  year the string → `TIMESTAMP` literal reads; inside 1200–2099 the helper is the zone itself.
  `datetime_from_micros`, `localize_wall_micros_in_zone`, `local_datetime_from_micros`,
  `offset_at_instant` and `micros_from_local_datetime` moved here from `datetime.rs` (which
  re-exports them, so `crate::datetime::…` paths are unchanged; `datetime.rs` 1699→1639).
  `micros_from_local_datetime` keeps Spark's rules: an overlap takes the earlier offset unless
  the caller's preferred offset is one of the two, and a gap takes the offset 26 hours earlier
  (no IANA transition is wider), which shifts the wall clock forward.
  `within_the_tables` and `instant_part` are new, for the calendar-field extractors (`year` …
  `second`). Those ran Arrow's `date_part` over a zone-labelled array, which reads chrono-tz
  directly and so rendered standard time after 2099. `within_the_tables` checks a batch once:
  one branch-free pass over the raw ticks against `zone_horizon::tabulated_utc_seconds()`
  (the value under a null slot may send a batch to the slow path, never the other way). A
  batch wholly inside 1200–2099 still runs Arrow's kernel, the code main ran, so data before
  2100 pays that one pass and nothing per value. Any other batch runs `instant_part`: decode
  the instant, attach the offset through `zoned_at_instant`, read the field with chrono, add
  the Spark index shift; one closure per field keeps each loop monomorphic, and a field with
  no reader is an internal error, not a silent fallback. A zone-free argument (`DATE`,
  `TIMESTAMP_NTZ`, `TIME`) goes to Arrow as before.
  Not routed here because they are not in this workspace: DataFusion's built-in `extract` /
  `date_part` (they never read the session zone) and `datafusion-spark`'s `from_utc_timestamp`
  / `to_utc_timestamp` (ledger R-1, R-2).
  Perf guard and mutations: the unit ledger's §Perf and §Mutations.
  pins: zone-horizon-render-1/C-011, C-013, C-014
