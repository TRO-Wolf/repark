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
- [slice.py](slice.py) — `F.substr` / `F.substring` at positive and negative positions,
  and `F.overlay` replacing a slice in place, with and without the length.
- [split_part.py](split_part.py) — `F.split_part` by one-based and negative part number,
  and `F.substring_index` by left/right/beyond/zero delimiter count.
- [translate.py](translate.py) — `F.translate` per-character map, including
  deleting characters with an empty map; `F.replace` measured incompatible with
  the PySpark wrapper (repark takes a literal `search`, PySpark a column name)
  and stays on the backlog.
- [search.py](search.py) — `F.position` both ways round and `F.find_in_set` membership
  in a comma list, not-found zeros and NULLs included.
- [words.py](words.py) — `F.repeat` at 2, 0 and negative, `F.reverse`, `F.soundex`
  codes, and `F.quote` single-quote wrapping.
- [utf8.py](utf8.py) — `F.bit_length` / `F.octet_length` byte counts and the invalid
  UTF-8 trio: `F.is_valid_utf8` tests, `F.make_valid_utf8` repairs with U+FFFD,
  `F.try_validate_utf8` answers NULL.
- [regex.py](regex.py) — the `F.regexp` / `F.rlike` / `F.regexp_like` match predicates,
  `F.regexp_count`, `F.regexp_replace`, `F.regexp_substr`, `F.regexp_instr`, and
  `F.regexp_extract_all` by capture-group index.
- [like.py](like.py) — `F.like` wildcards (`%`, `_`) and the backslash escape, with
  `F.ilike` folding case on the same patterns.

## Pointers

- Up: [../map.md](../map.md)
