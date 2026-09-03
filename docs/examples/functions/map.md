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
- [nulls.py](nulls.py) — the NULL tests `F.isnull` / `F.isnotnull` / `F.equal_null` (two NULLs compare equal) and the substitutions `F.coalesce`, `F.ifnull`, `F.nvl`, `F.nvl2`, `F.nullif`, `F.nullifzero`, `F.nanvl` on rows carrying NULLs, with the NaN literal edges separate.
- [conditional.py](conditional.py) — `F.when` chains and the bare form, and `F.assert_true` passing, then raising with its message.
- [columns.py](columns.py) — `F.column`, the constructor spelling that agrees with `F.col`, NULL included.
- [sort_order.py](sort_order.py) — the six `F.asc*` / `F.desc*` orderings and where each places NULLs.
- [bitwise.py](bitwise.py) — `F.negate`, the `F.bitwiseNOT` / `F.bitwise_not` alias pair, `F.bit_count`, the bit readers `F.bit_get` / `F.getbit`, and the three shifts.
- [broadcast.py](broadcast.py) — `F.broadcast`, the join hint (single-node no-op in repark, python/repark/src/repark/spark/functions_session.py:49-56), checked to agree with the plain join.
- [session_context.py](session_context.py) — `F.current_catalog`, `F.current_database` and `F.current_schema` on a two-row frame.

- [map_parts.py](map_parts.py) — `F.map_keys`, `F.map_values`, `F.map_entries`,
- [map_shapes.py](map_shapes.py) — `F.map_from_arrays`, `F.map_from_entries`,
- [map_higher_order.py](map_higher_order.py) — `F.transform_keys`,
- [structs.py](structs.py) — `F.struct` and `F.named_struct`: fields by column and
- [hashing.py](hashing.py) — `F.md5`, `F.sha`/`F.sha1` (one digest, two
- [hex_binary.py](hex_binary.py) — `F.hex` and `F.bin` spelling integers,
- [random_values.py](random_values.py) — `F.uuid`, `F.rand`, `F.randn`,
- [url.py](url.py) — the URL codec round trip and `F.parse_url` part
- [try_fallbacks.py](try_fallbacks.py) — `F.try_mod` by zero and
- [epoch.py](epoch.py) — the epoch conversions: `F.unix_date`, `F.unix_seconds`,
- [timestamp_from_epoch.py](timestamp_from_epoch.py) — `F.timestamp_seconds`,
- [to_date_timestamp.py](to_date_timestamp.py) — `F.to_date` / `F.to_timestamp` parse
- [make_calendar.py](make_calendar.py) — `F.make_date` builds a date from year/month/day
- [utc_offsets.py](utc_offsets.py) — `F.from_utc_timestamp` / `F.to_utc_timestamp` render
- [partition_transforms.py](partition_transforms.py) — the partition transforms `F.years`,
- [window_ranking.py](window_ranking.py) — `F.row_number`, `F.rank`, `F.dense_rank`: ties counted three ways on one grouped ordered frame.
- [window_position.py](window_position.py) — `F.percent_rank`, `F.cume_dist`, `F.ntile`: where a row sits in its partition.
- [window_offset.py](window_offset.py) — `F.lag` and `F.lead` at two offsets, with and without the fill default.
- [window_nth_value.py](window_nth_value.py) — `F.nth_value`: the nth value seen so far in the ordered frame. The frame is spelled explicitly (`rowsBetween(unboundedPreceding, currentRow)`, Spark's default for an ordered window).
## Pointers

- Up: [../map.md](../map.md)
