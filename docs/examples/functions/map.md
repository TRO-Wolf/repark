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
- [hashing.py](hashing.py) — `F.md5`, `F.sha`/`F.sha1` (one digest, two
  spellings), `F.crc32`, and `F.xxhash64`, NULL included; `xxhash64(NULL)` is
  the seed value 42.
- [hex_binary.py](hex_binary.py) — `F.hex` and `F.bin` spelling integers,
  `F.hex` spelling a string's bytes, `F.unhex` back, and `F.try_to_binary`
  answering NULL on a bad charset.
- [random_values.py](random_values.py) — `F.uuid`, `F.rand`, `F.randn`,
  `F.random` asserted on shape and range only, never on a value.
- [url.py](url.py) — the URL codec round trip and `F.parse_url` part
  extraction, with `F.try_url_decode` / `F.try_parse_url` answering NULL where
  the strict spelling raises.
- [try_fallbacks.py](try_fallbacks.py) — `F.try_mod` by zero and
  `F.try_to_number` on a format mismatch answering NULL.

## Pointers

- Up: [../map.md](../map.md)
