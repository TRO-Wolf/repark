# map — docs/examples/functions/

## Purpose

Worked examples for `F.*` names (`repark.functions` / `repark.spark.functions`).
Examples construct the session as `repark = ReparkSession.builder…`; see
[../map.md](../map.md). Each script keeps a one-line module docstring and the
`main()` one-liner.

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
- [json_family.py](json_family.py) — the JSON names on one string column: `F.get_json_object`
  (a field, an indexed element, a nested object), `F.json_array_length`, `F.json_object_keys`,
  `F.from_json` against a DDL schema, `F.to_json`, and `F.schema_of_json`. A malformed document
  and a NULL both answer NULL rather than raising. The file is NOT named `json.py`: a script in
  this directory runs with the directory on `sys.path[0]`, so that name would shadow the
  standard library's `json` for every other example here.
- [map_build.py](map_build.py) — `F.create_map` per row, `F.map_concat` onto a constant map,
  and `F.array_insert` at the front, at the back through `-1`, and past the end where Spark
  pads with NULLs.

Both scripts carry the FNP-9/10 clause citations, which live here and not in the script bodies:
`json_family.py` pins: fnp-9-collections-json/C-002, C-003, C-004, C-005 ·
`map_build.py` pins: fnp-9-collections-json/C-006.
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
  `F.months`, `F.days`, `F.hours` and `F.bucket` through `writeTo(...).partitionedBy(...)`,
  each asserting rows and `.files` partition values (the hours slots 473698/473702 are
  Spark-grounded via `unix_seconds`). Scalar `F.hours` refuses on both engines.
  pins: ex-25-functions-a/C-007
- [summarize.py](summarize.py) — `F.count` / `F.count("*")`, `F.sum`, `F.avg` /
- [counting.py](counting.py) — `F.count_if` counts true rows only,
- [first_last.py](first_last.py) — `F.first` / `F.last` over an explicitly
- [booleans.py](booleans.py) — `F.bool_and` / `F.bool_or` with their `F.every` /
- [collect.py](collect.py) — `F.collect_list` / `F.array_agg` and the
- [strings_agg.py](strings_agg.py) — `F.listagg` / `F.string_agg` joining a
- [grouping.py](grouping.py) — `F.grouping` inside a cube: 1 for the grand-total
- [try_aggregates.py](try_aggregates.py) — `F.try_sum` answers NULL when the
- [window_ranking.py](window_ranking.py) — `F.row_number`, `F.rank`, `F.dense_rank`: ties counted three ways on one grouped ordered frame.
- [window_position.py](window_position.py) — `F.percent_rank`, `F.cume_dist`, `F.ntile`: where a row sits in its partition.
- [window_offset.py](window_offset.py) — `F.lag` and `F.lead` at two offsets, with and without the fill default.
- [window_nth_value.py](window_nth_value.py) — `F.nth_value`: the nth value seen so far in the ordered frame. The frame is spelled explicitly (`rowsBetween(unboundedPreceding, currentRow)`, Spark's default for an ordered window).
- [calendar_parts.py](calendar_parts.py) — the numeric calendar parts of a date
- [current_datetime.py](current_datetime.py) — the six current date/timestamp
- [date_arithmetic.py](date_arithmetic.py) — moving a date by days with
- [date_difference.py](date_difference.py) — `F.date_diff` / `F.datediff`,
- [date_format.py](date_format.py) — `F.date_format` rendering patterns beside
- [date_parts_sql.py](date_parts_sql.py) — the SQL field-extraction trio
- [date_truncation.py](date_truncation.py) — `F.date_trunc` on a timestamp and
- [dispersion.py](dispersion.py) — `F.std`/`F.stddev`/`F.stddev_samp`, `F.stddev_pop`, and the
- [covariance.py](covariance.py) — `F.corr`, `F.covar_pop`, `F.covar_samp` over (y, x) pairs,
- [regression.py](regression.py) — Spark's nine `F.regr_*` linear-regression aggregates over
- [bit_aggregates.py](bit_aggregates.py) — `F.bit_and`, `F.bit_or`, `F.bit_xor` folding each
- [case.py](case.py) — `F.lcase`/`F.lower` and `F.ucase`/`F.upper` (alias pairs)
- [concat.py](concat.py) — `F.concat` propagating NULL beside `F.concat_ws`
- [edges.py](edges.py) — `F.left` / `F.right` at a positive width, and the empty
- [format.py](format.py) — the printf-style `F.format_string` and its `F.printf`
- [length.py](length.py) — the three length spellings `F.length`,
- [matching.py](matching.py) — `F.contains` / `F.startswith` / `F.endswith`, the
- [padding.py](padding.py) — `F.lpad` / `F.rpad` with truncation, the space
- [unbase64.py](unbase64.py) — `F.unbase64` decoding a base64 string into bytes.
- [slice.py](slice.py) — `F.substr` / `F.substring` at positive and negative positions, and `F.overlay` replacing a slice in place, with and without the length.
- [split_part.py](split_part.py) — `F.split_part` by one-based and negative part number, and `F.substring_index` by left/right/beyond/zero delimiter count.
- [translate.py](translate.py) — `F.translate` per-character map, including deleting characters with an empty map; `F.replace` stays on the backlog (facade-spelling class).
- [search.py](search.py) — `F.position` both ways round and `F.find_in_set` membership in a comma list, not-found zeros and NULLs included.
- [words.py](words.py) — `F.repeat` at 2, 0 and negative, `F.reverse`, `F.soundex` codes, and `F.quote` single-quote wrapping.
- [utf8.py](utf8.py) — `F.bit_length` / `F.octet_length` byte counts and the invalid UTF-8
  trio: `F.is_valid_utf8` tests, `F.make_valid_utf8` repairs with U+FFFD,
  `F.try_validate_utf8` answers NULL, and `F.validate_utf8` passes valid bytes then
  raises `INVALID_UTF8_STRING` on a lone 0xFF (EX-28).
  pins: ex-28-scalar-remainder/C-002
- [regex.py](regex.py) — the `F.regexp` / `F.rlike` / `F.regexp_like` match predicates, `F.regexp_count`, `F.regexp_replace`, `F.regexp_substr`, `F.regexp_instr`, and `F.regexp_extract_all` by capture-group index.
- [like.py](like.py) — `F.like` wildcards (`%`, `_`) and the backslash escape, with `F.ilike` folding case on the same patterns.
- [array_more.py](array_more.py) — `F.array_position` found/missing/NULL, `F.array_sort`
  ascending with NULLs last, `F.arrays_overlap` with its NULL decisions, `F.flatten`
  over NULL sub-arrays, and `F.map_zip_with` merging two maps key by key.
  `F.arrays_zip` and the `posexplode` pair stay on the backlog (EX-FN-1, EX-FN-2).
  pins: ex-25-functions-a/C-002
- [strings_more.py](strings_more.py) — `F.chr` / `F.char` modulo-256 spellings with the
  negative-empty edge, `F.elt` in range with its `INVALID_ARRAY_INDEX` raise, `F.initcap`
  splitting words on spaces only, `F.regexp_extract` by group with empty no-match, and
  `F.sha2` at 224 and 256 bits. `F.base64` (BL-17), `F.encode` / `F.decode` (EX-FN-3),
  `F.replace` (EX-FN-15), `F.split` (EX-FN-18), `F.format_number` (EX-FN-5) and
  `F.sentences` (EX-FN-17) stay on the backlog.
  pins: ex-25-functions-a/C-003
- [dates_more.py](dates_more.py) — `F.add_months` from month ends both directions,
  `F.make_interval` shifting a date and a timestamp (the string-cast arm diverges,
  EX-FN-19), `F.unix_timestamp` / `F.to_unix_timestamp` on the default pattern
  (the format argument is EX-FN-21), and `F.try_to_time` matching Spark's
  `UNSUPPORTED_TIME_TYPE`. `F.months_between` (EX-FN-11), `F.make_timestamp`
  (EX-FN-10) and `F.try_to_timestamp` (EX-FN-20) stay on the backlog.
  pins: ex-25-functions-a/C-004
  pins: ex-28-scalar-remainder/C-003
- [stats.py](stats.py) — the `F.percentile_approx` / `F.approx_percentile` alias pair
  agreeing on the median and the extremes over 1..100 (the accuracy knob stays
  ignored, FN-APPROXPCT-ACC-1). `F.kurtosis` / `F.skewness` / `F.mode` stay on the
  backlog (EX-FN-9).
  pins: ex-25-functions-a/C-005
- [session_misc.py](session_misc.py) — `F.current_user` / `F.session_user` / `F.user`
  as non-empty strings, `F.version` as a stable non-empty string, seeded `F.uniform`
  matching Spark's XORShift draws on a single partition (Spark seeds each partition
  with `seed + partitionIndex`, so the stream is partition-dependent; the example
  runs `local[1]`), `F.randstr` lengths plain and seeded, and
  `F.isnan` with NULL answering false. `F.monotonically_increasing_id` /
  `F.spark_partition_id` (EX-FN-12), `F.input_file_name` (EX-FN-13),
  `F.raise_error` (EX-FN-14) and `F.expr` (EX-FN-4) stay on the backlog.
  pins: ex-25-functions-a/C-006
  pins: ex-28-scalar-remainder/C-004
## Pointers

- Up: [../map.md](../map.md)
