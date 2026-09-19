# map — repark-functions/src/cast_map

## Purpose

CAST-MAP-SPELL-1 support. The parent `cast_map.rs` holds the `__repark_cast_map__` /
`__repark_empty_map_cast__` UDFs; this directory holds the text half both SQL doors call and
the module's tests.

## Contents

- `rewrite.rs` — `rewrite_map_casts`: tokenizes the statement (strings, quoted identifiers and
  comments stay opaque), finds each `CAST(` / `TRY_CAST(` whose top-level `AS` target names
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

## Pointers

- Up: [../map.md](../map.md)
