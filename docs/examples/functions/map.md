# map — docs/examples/functions/

## Purpose

Worked examples for `F.*` names (`repark.functions` / `repark.spark.functions`).
Examples construct the session as `repark = ReparkSession.builder…`; see
[../map.md](../map.md). Each script keeps a one-line module docstring.

## Contents

- [abs.py](abs.py) — `F.abs`, `F.col`, `F.lit` on a three-row local frame.
- [arrays.py](arrays.py) — the array builders and counters: `F.array`, `F.array_repeat`,
  `F.sequence` (plain and stepped), and `F.size` / `F.cardinality` / `F.array_size` agreeing.
- [array_edit.py](array_edit.py) — `F.array_append`, `F.array_prepend`, `F.array_remove`,
  `F.array_compact`: grow, shrink, and clean an array, NULL elements and NULL arrays included.
- [array_elements.py](array_elements.py) — element access and membership: `F.element_at`,
  `F.try_element_at` (index spelled `F.lit`, like Spark), `F.get`, `F.slice`,
  `F.array_contains`.
- [array_order.py](array_order.py) — `F.sort_array` both directions, the extremes
  `F.array_max` / `F.array_min`, `F.array_join`, and `F.shuffle` shape-checked.
- [array_setops.py](array_setops.py) — the set algebra quartet: `F.array_distinct`,
  `F.array_union`, `F.array_intersect`, `F.array_except`.
- [explode.py](explode.py) — `F.explode` and `F.explode_outer`: one row per array element,
  the outer spelling keeping the empty and NULL rows.
- [higher_order.py](higher_order.py) — the lambda names: `F.exists`, `F.forall`, `F.filter`,
  `F.transform` (element and index forms), `F.aggregate` (with and without finish),
  `F.reduce`, `F.zip_with`; an `F.slice` empty array drives the empty-aggregate case.
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

## Pointers

- Up: [../map.md](../map.md)
