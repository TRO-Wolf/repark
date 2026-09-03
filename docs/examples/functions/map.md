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
- [nulls.py](nulls.py) — the NULL tests `F.isnull` / `F.isnotnull` / `F.equal_null` (two NULLs compare equal) and the substitutions `F.coalesce`, `F.ifnull`, `F.nvl`, `F.nvl2`, `F.nullif`, `F.nullifzero`, `F.nanvl` on rows carrying NULLs, with the NaN literal edges separate.
- [conditional.py](conditional.py) — `F.when` chains and the bare form, and `F.assert_true` passing, then raising with its message.
- [columns.py](columns.py) — `F.column`, the constructor spelling that agrees with `F.col`, NULL included.
- [sort_order.py](sort_order.py) — the six `F.asc*` / `F.desc*` orderings and where each places NULLs.
- [bitwise.py](bitwise.py) — `F.negate`, the `F.bitwiseNOT` / `F.bitwise_not` alias pair, `F.bit_count`, the bit readers `F.bit_get` / `F.getbit`, and the three shifts.
- [broadcast.py](broadcast.py) — `F.broadcast`, the join hint (single-node no-op in repark, python/repark/src/repark/spark/functions_session.py:49-56), checked to agree with the plain join.
- [session_context.py](session_context.py) — `F.current_catalog`, `F.current_database` and `F.current_schema` on a two-row frame.

## Pointers

- Up: [../map.md](../map.md)
