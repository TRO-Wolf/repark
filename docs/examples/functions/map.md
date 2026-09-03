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
  `F.log10` / `F.log2`, two-argument `F.log`, and `F.e`, whose ln is 1.
- [rounding.py](rounding.py) — `F.ceil` / `F.ceiling` (an alias pair) and `F.floor`
  against the integers, and `F.round`, whose halfway cases go away from zero.
- [integer_math.py](integer_math.py) — `F.factorial`, `F.pmod` answering non-negative
  under a positive divisor, `F.greatest` / `F.least` skipping NULLs, `F.width_bucket`.
- [try_arithmetic.py](try_arithmetic.py) — the `F.try_*` quartet answering NULL on
  overflow and divide-by-zero, ordinary input unchanged.
- [case.py](case.py) — `F.lcase`/`F.lower` and `F.ucase`/`F.upper` (alias pairs
  shown agreeing) and `F.initcap`, on mixed-case words, an empty string, NULL.
- [concat.py](concat.py) — `F.concat` propagating NULL beside `F.concat_ws`
  skipping it.
- [edges.py](edges.py) — `F.left` / `F.right` at a positive width, and the empty
  answer both give at a negative one.
- [format.py](format.py) — the printf-style `F.format_string` and its `F.printf`
  alias, including how a NULL argument renders.
- [length.py](length.py) — the three length spellings `F.length`,
  `F.char_length`, `F.character_length`, the first code point `F.ascii`, and the
  inverse pair `F.chr` / `F.char`.
- [matching.py](matching.py) — `F.contains` / `F.startswith` / `F.endswith`, the
  1-based positions `F.instr` and `F.locate`, and `F.levenshtein` edit distance.
- [padding.py](padding.py) — `F.lpad` / `F.rpad` with truncation, the space
  strips `F.ltrim` / `F.rtrim` / `F.trim`, and `F.btrim`, which names its
  characters.
- [unbase64.py](unbase64.py) — `F.unbase64` decoding a base64 string into bytes.

## Pointers

- Up: [../map.md](../map.md)
