# map — repark-functions/src/cast_map

## Purpose

CAST-MAP-SPELL-1 support. The parent `cast_map.rs` holds the `__repark_cast_map__` /
`__repark_empty_map_cast__` UDFs; this directory holds the text half both SQL doors call and
the module's tests.

## Contents

- `leaf.rs` — **Round 3 (2026-09-19):** the Spark leaf casts and the key-legality predicate.
  `key_castable` is Spark 4.1.2's map-key rule as measured (8 source types x 9 key targets x
  ANSI / legacy / `try_cast`): ANSI needs ANSI castability; legacy and `try_cast` (either
  mode) need legacy castability and a key cast whose `forceNullable` is false.
  `leaf_cast` trims string leaves (chars <= space), parses integral text strictly (ANSI
  `CAST_INVALID_INPUT`, `try_cast` NULL) or legacy-style (a fractional part truncates),
  parses Spark's boolean words, narrows integral and fractional values with ANSI
  `CAST_OVERFLOW` (Spark's `Y` / `S` / `L` / `D` literal suffixes), legacy wrap or
  saturate-then-wrap, and `try_cast` NULL; any other pair keeps Arrow's cast with
  `safe = mode != ANSI`. RePark's scalar `CAST` has no Spark leaf kernel to reuse (it
  raises Arrow's error on both doors), so these live here.
  **Round 4 (2026-09-19):** `spark_trim` drops code points <= U+0020 and U+007F (DEL) only,
  as measured; U+0085 stays and fails the parse.
  **STRING-CAST-LENIENT-1 (2026-11-22):** the legacy parse answers 0 for `.`, `+.` and `-.` (Spark's
  `toInt` breaks at the separator with nothing read), and `cast_invalid_input` renders a NUL as `\u0000`
  (a NUL in an error crossing Arrow's FFI stream aborted the process).
- `rewrite.rs` — `rewrite_map_casts`: tokenizes the statement with the Spark dialect (strings, quoted identifiers and
  comments, `/*! … */` hints included, stay opaque), finds each `CAST(` / `TRY_CAST(` whose top-level `AS` target names
  `MAP <`, and splices the UDF call in by byte span so every other byte of the statement stays
  verbatim; operands are rewritten recursively (depth 64). `map_cast_target` parses the target
  with sqlparser's `SparkSqlDialect` data-type grammar (any case, spacing and nesting of
  `MAP<K, V>`, `ARRAY<T>`, `STRUCT<n: T>` / `STRUCT<n T>`; depth 32) into the Arrow type
  `type_table::arrow_type_from_spark` would build; a target that does not parse or names no map
  is left for the parser to refuse as before. `map_cast_token` is the DataFrame door's Spark
  token (`MAP<STRING, BIGINT>`), rendered from the parsed type, never from the input.
  **WO NTZ-1 slice 1 (2026-09-26):** it also answers `TIMESTAMP_NTZ` for
  `timestamp_ntz`. pins: cast-map-spell-1/C-004, C-005; ntz-1/C-003
- `string_integral.rs` — **STRING-CAST-LENIENT-1, INT slice (2026-11-22):** the Spark door's scalar `CAST` of a
  STRING (or a dictionary of strings) to TINYINT, SMALLINT, INT or BIGINT. `SparkStringIntegralCast`
  (`spark_string_integral_cast`, the last rule of `analyzer_rules()`) replaces each such `Expr::Cast` with
  `__repark_string_to_{tinyint,smallint,int,bigint}__(x)`, which runs `leaf.rs`'s `string_to_integral`: Spark's
  `UTF8String.toInt`/`toLong` trim (bytes <= U+0020 and U+007F), ANSI on strict (`CAST_INVALID_INPUT` for
  malformed, fraction and out-of-range text alike, as Spark's `toIntExact` does), ANSI off legacy (a fraction
  truncates, `.`/`+.`/`-.` answer 0, anything else NULL), the mode read from the session at invoke. The UDF keeps
  the child's schema name (DataFusion's `Cast` shows only its child) and answers nullable, as Spark's cast does.
  `TRY_CAST` is not touched (STRING-CAST-SILENT-1 owns its whitespace cells). Two guards keep other paths as they
  were: an `IN` list is left alone (Spark compares it as text with ANSI off), a cast planned inside a `Values` node is left alone (`INSERT ... VALUES` puts its store-assignment cast
  there, where Spark refuses the store), and a cast whose operand is `__repark_arrow_cast_operand__(x)` (an
  identity) keeps Arrow's parse; the JSON reader's user-schema casts and `smartCsv`'s integer rungs use it, so the
  readers stay as they were (a later slice). pins: `python/repark/tests/types/test_string_cast_lenient_int_1.py`
- `tests.rs` — the module's `#[cfg(test)]` suite: target parsing across case, spacing and
  nesting; malformed and map-free targets rejected; statements without a map cast untouched
  (string literals and comments included); the splice keeps surrounding text, nested and
  `try_cast` calls, and the bare `map()` operand; NULL, empty, widened and re-keyed maps; ANSI
  `CAST_INVALID_INPUT` versus legacy and `try_cast` NULL elements; the planning refusal; the
  planned nullability; the Spark projection name.
  Round 3: the comment hint, colliding keys kept in Spark's stored order, key legality
  by mode and `try_cast`, and leaf overflow, wrap, trimming and fractional text.
  Round 4: the trim set, the full `CAST_OVERFLOW` message for INT and BIGINT sources, and
  the loud refusal of a `try_cast` key that overflows.
  STRING-CAST-LENIENT-1: the scalar string-to-integer seat (trim and ANSI-off NULL per target, the
  name, the untouched `TRY_CAST`, the operand bypass and the `Values` guard).

## Pointers

- Up: [../map.md](../map.md)
