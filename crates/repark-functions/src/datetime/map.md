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
- `spark_date_part.rs` — **DATE-PART-SESSION-ZONE-1 (2026-11-23, delta 1):** owns Spark's
  `date_part` / `datepart` scalar UDF (`SparkDatePart`, `date_part_udf`) and the
  `SparkExtractDatePart` analyzer rule (`extract_date_part_rule`,
  `insert_extract_date_part_before_coercion`, seated before DataFusion's first `type_coercion` in
  `SparkExtension::configure_analyzer_rules` as well as in `registration::analyzer_rules` after
  `ltz_timestamp_cast_rule` so SQL `EXTRACT(<field> FROM <string>)` never hits stock `date_part`'s
  implicit `Timestamp(ns)` cast before rewrite).
  `SparkDatePart` accepts `Timestamp`, `Date`, `String`, and `Interval` sources (plus `Null` and
  `Dictionary` over them) and installs no `preimage` override so `WHERE date_part('year', ts) = …`
  never prunes in UTC. For temporal, `String`, and `Null` inputs it accepts only the exact 38
  Spark 4.1.2 ASCII case-insensitive field spellings (`year`/`y`/`yr`/`yrs`/`years`, `yearofweek`,
  `quarter`/`qtr`, `month`/`mon`/`mons`/`months`, `week`/`w`/`weeks`, `day`/`d`/`days`,
  `dayofweek`/`dow` shifted +1, `dayofweek_iso`/`dow_iso` shifted +1, `doy`,
  `hour`/`h`/`hr`/`hrs`/`hours`, `minute`/`m`/`min`/`mins`/`minutes`,
  `second`/`s`/`sec`/`secs`/`seconds`); every other spelling refuses with `[INVALID_EXTRACT_FIELD]`
  in `return_field_from_args` (catching `NULL` and 0-row tables) and `invoke_with_args`. `String`
  inputs follow Spark's split: date fields coerce via `csv::field::spark_string_to_date` (`DATE`),
  time fields (`hour`/`minute`/`second`) coerce via `spark_string_timestamp::cast_strings_to_ltz`
  (session-zone `TIMESTAMP`), both respecting `spark.sql.ansi.enabled` (`NULL` vs
  `[CAST_INVALID_INPUT]`) with nullable `Int32` output. `Interval` inputs validate Spark's interval
  fields (`year`/`month` on `YearMonth`, `day`/`hour`/`minute`/`second` on `DayTime`, all 6 on
  `MonthDayNano`, with Spark aliases) and delegate to stock DataFusion `date_part`.
  `SparkExtractDatePart` rejects any non-foldable arg-0 expression (column, subquery, volatile or
  stable function) on `SparkDatePart` and rewritten `EXTRACT` nodes with `[NON_FOLDABLE_ARGUMENT]`
  before optimizer or statistics folding, while allowing foldable expressions (`lower('HOUR')`,
  `concat(…)`); it rewrites stock `date_part` onto `SparkDatePart` for `Timestamp`, `Date32`,
  `Date64`, `Utf8`, `LargeUtf8`, `Utf8View`, `Interval`, `Null`, and `Dictionary` over them,
  leaving `Time`, `Duration`, and the native `repark.sql()` door untouched. `datetime.rs` shrunk
  1655→1614. Perf guard and mutations: the unit ledger's §Perf and §Mutations.
