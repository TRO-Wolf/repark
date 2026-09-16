# `temporal_ctor/` — FNP-11A temporal constructors, intervals and arithmetic

Fifteen Spark names over the engine's session-zone and ANSI carriers: the six
`make_timestamp` constructors, `make_ym_interval`, `try_make_interval`,
`months_between`, `convert_timezone`, `localtimestamp`, `timestampadd`,
`timestampdiff`, `dateadd` and `datediff`. String units only on the SQL door; bare-unit keywords need a
parser change (EX-FN-27, run 15c owns the parser).

- [`../temporal_ctor.rs`](../temporal_ctor.rs) — shared wall/instant helpers,
  zone parsing (IANA names plus `GMT`/`UTC`/`UT` and bare offsets with seconds,
  clamped to ±18:00), and the `functions()` registry root.
- [`make.rs`](make.rs) — `MakeTimestamp` zoned/ltz/ntz kernels (numeric and
  `(date, time[, zone])` forms, leap-second rollover, Java year range, nanos
  magnitude gate; scalar zones resolve once, numeric columns precast once).
  **FNP-11B step 6 (2026-09-15):** the time reader takes `HH:MM:SS[.ffffff]`
  strings (facade `lit(time)` arrives as text) beside TIME values; garbage
  raises, mirroring the date reader's malformed path. pins: fnp-11b/C-002.
  **FNP-11B remediation round 1 (2026-09-16):** the 2/3-arg arm precasts DATE
  to `Date32` and TIME to `Time64(µs)` once, with the row readers' casts and
  error texts; all-null and `Null` inputs still pass through.
  pins: fnp-11b/C-006, C-007.
- [`adddiff.rs`](adddiff.rs) — `timestampadd` (calendar months and days in the
  session zone, exact durations below the day; the result keeps the input
  family, so `timestamp_ntz` in answers `timestamp_ntz` with no zone shift)
  and `timestampdiff` (DAY/WEEK count whole wall-clock days in the session
  zone and sub-day units count wall clocks, so DST gaps never move a boundary;
  months, weeks and days break ties on the time of day). Batch work hoists: scalar units
  resolve once, columns precast once, overflow text renders lazily, sub-day
  adds shift instants directly.
- [`arith.rs`](arith.rs) — `months_between` (last-day and 31-day rules),
  `convert_timezone`, `localtimestamp` (session-zone wall, `timestamp_ntz`).
- [`date_alias.rs`](date_alias.rs) — `dateadd`/`datediff` arity routes (2 args
  keep the `datafusion-spark` `date_add`/`date_diff` kernels through their own
  coercion and simplify paths, 3 args answer `timestampadd`/`timestampdiff`).
  **SQL-LITERAL-TYPING-1 remediation round 1 (2026-09-16):** `DateOffset`
  shadows `date_add`/`date_sub` with `Signature::user_defined` + `coerce_types`
  accepting every integer width down to Int32 (the upstream `Exact` signatures
  refuse parser-Int64 literals at plan construction, before any analyzer rule
  runs), delegating kernels upstream; the 2-arg `dateadd` route now targets
  the shadow. Unit tests pin literal days, the alias, and the text refusal.
  pins: sql-literal-typing-1/L-002
- [`intervals.rs`](intervals.rs) — `make_ym_interval`, `try_make_interval`, the
  `CAST(interval AS STRING)` analyzer rule with Spark's spelled-out text (each
  unit carries its own sign from truncating division).

pins: fnp-11a/C-002, C-003, C-004, C-005, C-017, C-018, C-019
