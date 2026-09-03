# map — docs/examples/functions/

## Purpose

Worked examples for `F.*` names (`repark.functions` / `repark.spark.functions`).
Examples construct the session as `repark = ReparkSession.builder…`; see
[../map.md](../map.md). Each script keeps a one-line module docstring.

## Contents

- [abs.py](abs.py) — `F.abs`, `F.col`, `F.lit` on a three-row local frame.
- [roots.py](roots.py) — `F.sqrt`, `F.cbrt`, `F.hypot`: the two roots parting
  company on negative input (NaN versus a signed answer), then `hypot` against
  the long form `sqrt(a*a + b*b)`.
- [powers.py](powers.py) — `F.pow`, `F.power` (an alias pair, shown agreeing
  column for column) and `F.exp`, checked against `pow(e, x)`.
- [sign.py](sign.py) — `F.signum` and its alias `F.sign`, beside the unary
  `F.negative` / `F.positive` pair, on floats and on an integer column.
- [rint.py](rint.py) — `F.rint` and the half-to-even tie rule.
- [trig.py](trig.py) — the trigonometry family: ratios, inverses with their NaN and
  Infinity edges, `F.atan2` at the origin, and the degree round trip on `F.pi`.
- [hyperbolic.py](hyperbolic.py) — the hyperbolic six, with the inverse domains:
  `F.asinh` open, `F.acosh` from 1, `F.atanh` inside the unit interval.
- [logs.py](logs.py) — `F.ln` and one-argument `F.log`, the fixed-base spellings
  `F.log10` / `F.log2`, two-argument `F.log`, `F.log1p` / `F.expm1` at the tiny-arg
  edge, and `F.e`, whose ln is 1. pins: log1p-1-precise-kernels/C-003
- [rounding.py](rounding.py) — `F.ceil` / `F.ceiling` (an alias pair) and `F.floor`
  against the integers, and `F.round`, whose halfway cases go away from zero.
- [integer_math.py](integer_math.py) — `F.factorial`, `F.pmod` answering non-negative
  under a positive divisor, `F.greatest` / `F.least` skipping NULLs, `F.width_bucket`.
- [try_arithmetic.py](try_arithmetic.py) — the `F.try_*` quartet answering NULL on
  overflow and divide-by-zero, ordinary input unchanged.
- [epoch.py](epoch.py) — the epoch conversions: `F.unix_date`, `F.unix_seconds`,
  `F.unix_millis`, `F.unix_micros` count the distance from 1970; `F.date_from_unix_date`
  and `F.from_unixtime` build back.
- [timestamp_from_epoch.py](timestamp_from_epoch.py) — `F.timestamp_seconds`,
  `F.timestamp_millis`, `F.timestamp_micros` build instants from epoch counts, with the
  seconds round trip.
- [to_date_timestamp.py](to_date_timestamp.py) — `F.to_date` / `F.to_timestamp` parse
  calendar strings; `F.try_to_date` answers NULL on malformed input.
- [make_calendar.py](make_calendar.py) — `F.make_date` builds a date from year/month/day
  parts (column and literal forms); `F.make_dt_interval` a day-time duration.
- [utc_offsets.py](utc_offsets.py) — `F.from_utc_timestamp` / `F.to_utc_timestamp` render
  an instant between UTC and a named zone; `F.current_timezone` names the session zone.
- [partition_transforms.py](partition_transforms.py) — the partition transforms `F.years`,
  `F.months`, `F.days`, `F.bucket` through `writeTo(...).partitionedBy(...)`: rows read back
  from the created tables and the partition values asserted from the `.files` metadata
  (years 54/55, months 650/653, the two day dates, buckets 0/1/3).

## Pointers

- Up: [../map.md](../map.md)
