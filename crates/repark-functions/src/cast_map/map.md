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
  pins: cast-map-spell-1/C-011, C-013
  **Round 4 (2026-09-19):** `spark_trim` drops code points <= U+0020 and U+007F (DEL) only,
  as measured; U+0085 stays and fails the parse. pins: cast-map-spell-1/C-015
- `rewrite.rs` — `rewrite_map_casts`: tokenizes the statement with the Spark dialect (strings, quoted identifiers and
  comments, `/*! … */` hints included, stay opaque), finds each `CAST(` / `TRY_CAST(` whose top-level `AS` target names
  `MAP <`, and splices the UDF call in by byte span so every other byte of the statement stays
  verbatim; operands are rewritten recursively (depth 64). `map_cast_target` parses the target
  with sqlparser's `SparkSqlDialect` data-type grammar (any case, spacing and nesting of
  `MAP<K, V>`, `ARRAY<T>`, `STRUCT<n: T>` / `STRUCT<n T>`; depth 32) into the Arrow type
  `type_table::arrow_type_from_spark` would build; a target that does not parse or names no map
  is left for the parser to refuse as before. `map_cast_token` is the DataFrame door's Spark
  token (`MAP<STRING, BIGINT>`), rendered from the parsed type, never from the input.
  pins: cast-map-spell-1/C-004, C-005
- `tests.rs` — the module's `#[cfg(test)]` suite: target parsing across case, spacing and
  nesting; malformed and map-free targets rejected; statements without a map cast untouched
  (string literals and comments included); the splice keeps surrounding text, nested and
  `try_cast` calls, and the bare `map()` operand; NULL, empty, widened and re-keyed maps; ANSI
  `CAST_INVALID_INPUT` versus legacy and `try_cast` NULL elements; the planning refusal; the
  planned nullability; the Spark projection name. pins: cast-map-spell-1/C-005, C-006, C-007
  Round 3: the comment hint, colliding keys kept in Spark's stored order, key legality
  by mode and `try_cast`, and leaf overflow, wrap, trimming and fractional text.
  pins: cast-map-spell-1/C-011, C-012, C-013, C-014
  Round 4: the trim set, the full `CAST_OVERFLOW` message for INT and BIGINT sources, and
  the loud refusal of a `try_cast` key that overflows. pins: cast-map-spell-1/C-013, C-015, C-016

## Pointers

- Up: [../map.md](../map.md)
