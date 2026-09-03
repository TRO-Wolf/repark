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
- [calendar_parts.py](calendar_parts.py) — the numeric calendar parts of a date
  (`F.year`, `F.quarter`, `F.month`, `F.weekofyear`, `F.day`, `F.dayofmonth`,
  `F.dayofyear`, `F.dayofweek`, `F.weekday`) and the clock parts of a timestamp
  (`F.hour`, `F.minute`, `F.second`).
- [current_datetime.py](current_datetime.py) — the six current date/timestamp
  spellings `F.curdate`, `F.current_date`, `F.currentDate`, `F.current_timestamp`,
  `F.currentTimestamp`, `F.now`, shown agreeing within each trio.
- [date_arithmetic.py](date_arithmetic.py) — moving a date by days with
  `F.date_add` / `F.dateadd` / `F.date_sub`, the month's `F.last_day`, and
  `F.next_day` to the next Monday and Sunday.
- [date_difference.py](date_difference.py) — `F.date_diff` / `F.datediff`,
  end minus start in days, negative when end precedes start.
- [date_format.py](date_format.py) — `F.date_format` rendering patterns beside
  the `F.dayname` / `F.monthname` name shorthands.
- [date_parts_sql.py](date_parts_sql.py) — the SQL field-extraction trio
  `F.date_part` / `F.datepart` / `F.extract`, shown agreeing.
- [date_truncation.py](date_truncation.py) — `F.date_trunc` on a timestamp and
  `F.trunc` on a date, at year, month, day and quarter granularity.

## Pointers

- Up: [../map.md](../map.md)
