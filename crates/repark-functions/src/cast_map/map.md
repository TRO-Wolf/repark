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
  **ANSI-OFF-NARROWING-OVERFLOW-1 (2026-11-23):** adds `narrow_numeric` (also used by `leaf_cast`): fast primitive
  `signed_to_signed` (`unary` wrap in legacy, `try_map_primitive` `CAST_OVERFLOW` in ANSI), `decimal_to_integral`
  (truncate toward zero + low-bit wrap in legacy, `CAST_OVERFLOW` with `BD` suffix in ANSI, `unscaled == 0` short-circuit
  before `checked_pow`), `decimal_to_decimal`
  (HALF_UP rescale, NULL on overflow in legacy, `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION` in ANSI), and exact
  Spark 4.1.2 `fractional_to_integral` (`java_float_text` without `F`, `java_double_text` with `D` for finite only,
  JVM saturate-to-i32-then-wrap for TINYINT/SMALLINT/INT and saturate-to-i64 for BIGINT, `2^63` accepted as `i64::MAX`
  in both ANSI modes, `NaN -> 0`). Dictionary inputs decode before narrowing; `Null` sources return typed null arrays.
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
- `string_integral.rs` — **STRING-CAST-LENIENT-1, INT slice (2026-11-22) + ANSI-OFF-NARROWING-OVERFLOW-1 (2026-11-23):**
  the Spark door's scalar `CAST` a user writes from STRING to TINYINT/SMALLINT/INT/BIGINT, or narrowing numeric
  (signed integer to narrower signed integer, overflowing DECIMAL to integer or DECIMAL, and FLOAT/DOUBLE to signed
  integer, including Dictionary encodings). Reuses the same two analyzer rules with no second pair:
  `SparkStringIntegralCast` (`spark_string_integral_cast`) runs just before the first `TypeCoercion`
  (`insert_string_integral_before_coercion`, called by the Spark extension and the `F.expr` context), rewriting user
  casts to `__repark_string_to_{tinyint,smallint,int,bigint}__`, `__repark_narrow_to_{tinyint,smallint,int,bigint}__`,
  or `__repark_narrow_to_decimal_<p>_<s>__`, reading `spark.sql.ansi.enabled` at invoke. In-range integer literals,
  synthetic `__repark_suffix_literal__` literal wrappers (`Literal` / `-Literal` only; non-synthetic wrappers around
  `CAST(...)` from unary-plus suffixed integer literals are peeled and rewritten so they keep `nullable = false`), non-overflowing DECIMAL casts (`DECIMAL(10,2)->INT`, `DECIMAL(10,2)->DECIMAL(10,1)`),
  `__repark_arrow_cast_operand__`, and `TRY_CAST` are left untouched so `FoldSparkNumericCasts`, `CastShieldRule` +
  `preimage` pushdown, and reader schema casts keep their paths. `SparkCoercionCastMark` (`spark_coercion_cast_mark`,
  last in `analyzer_rules()`) marks any remaining cast matching `cast_udf_for` (coercion-inserted casts) with
  `__repark_arrow_cast_operand__` so re-analysis leaves them alone. Both skip `Values` and an INSERT's store-assignment
  projection, and on an UPDATE's `Dml(WriteOp::Update)` projection skip only a top-level `Expr::Cast` (the store-assignment
  cast DataFusion's `update_to_plan` inserts) while still rewriting nested casts and user-written top-level casts wrapped
  in `__repark_arrow_cast_operand__` by `repark-spark/src/update_cast.rs`. Nullability is decided from the logical operand
  at rewrite time (`logical_operand_nullable`) and combined with physical field nullability (`force_nullable ||
  source.is_nullable()`) on both `SparkNarrowIntegral` and `ArrowCastOperand`, so integer->integer casts preserve logical
  child nullability and never narrow it at the physical layer (working around DataFusion 54.1's `TryCastExpr` /
  `NegativeExpr::return_field` bug); `SparkStringIntegral` and `SparkNarrowDecimal` unconditionally return nullable.
  `narrow_cast_parts` joins `string_integral_cast_parts` for the empty `INSERT OVERWRITE` guard.
  pins: `python/repark/tests/types/test_string_cast_lenient_int_1.py`,
  `python/repark/tests/types/test_ansi_off_narrowing_overflow_1.py`
- `narrow_tests.rs` — **ANSI-OFF-NARROWING-OVERFLOW-1 (2026-11-23):** Rust unit pins for the narrowing numeric kernel
  and analyzer seam (integer wrap + ANSI `CAST_OVERFLOW`, decimal-to-integer truncation/wrap/overflow + zero extreme
  negative scale, decimal-to-decimal HALF_UP/NULL/`NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`, float/double JVM
  saturation + `2^63` + `NaN` + `±Inf` + `-0.0`, nullability per source kind + logical nullable operands over `NOT NULL`
  columns, dictionary inputs, `TRY_CAST` untouched, `ARROW_CAST_OPERAND` bypass, synthetic vs cast `SUFFIX_LITERAL`,
  `Values` guard, non-overflowing decimal and in-range literal bypass, subquery outer `IS NULL`, join key distinct
  names, coercion idempotence).
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
