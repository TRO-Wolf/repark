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
- `string_integral.rs` — **STRING-CAST-LENIENT-1, INT slice (2026-11-22):** the Spark door's scalar `CAST` a user
  writes from a STRING (or a dictionary of strings) to TINYINT, SMALLINT, INT or BIGINT. Two rules.
  `SparkStringIntegralCast` (`spark_string_integral_cast`) runs just before the first `TypeCoercion`
  (`insert_string_integral_before_coercion`, called by the Spark extension and the `F.expr` context), so every such
  `Expr::Cast` it sees was written in SQL, `.cast`, `F.expr` or `selectExpr` (or built by the facade); it becomes
  `__repark_string_to_{tinyint,smallint,int,bigint}__(x)`, which runs `leaf.rs`'s `string_to_integral` with
  Spark's trim, ANSI on strict (`CAST_INVALID_INPUT`), ANSI off legacy (fraction truncates, NULL otherwise), the
  mode read at invoke. `SparkCoercionCastMark` (`spark_coercion_cast_mark`, last in `analyzer_rules()`) wraps the
  operand of every STRING-to-integer cast still present, the ones coercion inserted (`coalesce(s, 0)`, `s = 5`,
  `s + 1`, a join key), in the identity `__repark_arrow_cast_operand__`, so they keep Arrow's parse and a later
  analysis of the same plan (the `F.expr` context, `INSERT` over an analyzed frame) leaves them alone. Both skip a
  `Values` node and the projection directly under a DML node (the store-assignment casts the gates judge). In a
  projection, aggregate or window the UDF's schema name is the child's; elsewhere it is the call, so a join key
  stays distinct from its column. A projection's schema is widened in place (nullable, as Spark's cast is).
  `string_integral_cast_parts` lets the empty `INSERT OVERWRITE` guard judge the UDF as the cast. `TRY_CAST` is
  not touched. pins: `python/repark/tests/types/test_string_cast_lenient_int_1.py`
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
