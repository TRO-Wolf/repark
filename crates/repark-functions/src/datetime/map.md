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
  `within_the_tables`, `instant_part`, `extraction_time_zone`, `is_instant`, `unpack_dictionary`,
  `resolve_instant_in_zone`, and `extract_calendar_part` serve every calendar-field extractor
  (`year` … `second`, `date_part`, `datepart`, and `extract`). A batch wholly inside 1200–2099
  runs Arrow's zone-labelled `date_part` kernel (after unpacking `Dictionary` arrays and
  re-annotating instant arrays in the session zone without copying ticks; a `Null` array returns
  an `Int32` null array); any other batch runs `instant_part` via `zoned_at_instant`. A zone-free
  argument (`DATE`, `TIMESTAMP_NTZ`, `TIME`) runs Arrow without zone conversion.
  Not routed here because they are not in this workspace: `datafusion-spark`'s `from_utc_timestamp`
  / `to_utc_timestamp` (ledger R-1, R-2).
- `spark_date_part.rs` — **DATE-PART-SESSION-ZONE-1 (2026-11-23):** owns Spark's `date_part` /
  `datepart` scalar UDF (`SparkDatePart`, `date_part_udf`) and the `SparkExtractDatePart` analyzer
  rule (`extract_date_part_rule`, installed in `registration::analyzer_rules` after
  `ltz_timestamp_cast_rule` and before `TypeCoercion`). `SparkDatePart` replaces `datafusion-spark`'s
  wrapper (which lowered to stock UTC `DatePartFunc` and its UTC year preimage): it parses every
  Spark calendar field and alias (`year`/`y`/`yr`/`yrs`/`years`, `yearofweek`/`year_iso`/`isoyear`,
  `quarter`/`qtr`, `month`/`mon`/`mons`/`months`, `week`/`w`/`weeks`/`isoweek`, `day`/`d`/`days`,
  `dayofweek`/`dow` shifted +1, `dayofweek_iso`/`dow_iso`/`isodow` shifted +1, `doy`/`dayofyear`,
  `hour`/`h`/`hr`/`hrs`/`hours`, `minute`/`m`/`min`/`mins`/`minutes`, `second`/`s`/`sec`/`secs`/
  `seconds`), routes calendar fields through `session_wall::extract_calendar_part`, and delegates
  sub-second extensions (session-zone resolved) and `epoch` to stock `date_part`. No `preimage`
  override is installed, so `WHERE date_part('year', ts) = …` never prunes in UTC.
  `SparkExtractDatePart` rewrites stock `date_part` emitted by DataFusion's `DatetimeFunctionPlanner`
  for `EXTRACT(<field> FROM x)` onto `SparkDatePart` only when `x` is `Timestamp`, `Date32`,
  `Date64`, `Null`, or a `Dictionary` over them, leaving `Utf8`, `Interval`, `Duration`, `Time`,
  and the native `repark.sql()` door untouched. `datetime.rs` shrunk 1655→1614.
  Perf guard and mutations: the unit ledger's §Perf and §Mutations.
