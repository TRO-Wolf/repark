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
- [summarize.py](summarize.py) — `F.count` / `F.count("*")`, `F.sum`, `F.avg` /
  `F.mean`, `F.median` (an even-count group answers the interpolated middle, not a
  data value), `F.min` / `F.max`; NULLs skipped by every aggregate.
- [counting.py](counting.py) — `F.count_if` counts true rows only,
  `F.countDistinct` / `F.count_distinct` drop NULL from the count (and the tuple),
  `F.approx_count_distinct` exact on small input.
- [first_last.py](first_last.py) — `F.first` / `F.last` over an explicitly
  ordered window, and the `F.first_value` / `F.last_value` aliases answering
  identically.
- [booleans.py](booleans.py) — `F.bool_and` / `F.bool_or` with their `F.every` /
  `F.some` aliases; an all-NULL group answers NULL, not False.
- [collect.py](collect.py) — `F.collect_list` / `F.array_agg` and the
  de-duplicating `F.collect_set`; contents compared order-insensitively.
- [strings_agg.py](strings_agg.py) — `F.listagg` / `F.string_agg` joining a
  group's values into one delimited string.
- [grouping.py](grouping.py) — `F.grouping` inside a cube: 1 for the grand-total
  row, 0 for every member row.
- [try_aggregates.py](try_aggregates.py) — `F.try_sum` answers NULL when the
  group's sum overflows; `F.try_avg` averages in double and stays finite.

## Pointers

- Up: [../map.md](../map.md)
