# map — repark-functions/src/csv

## Purpose

FNP-GEN-1 `from_csv` / `schema_of_csv` support. The scalar kernels parse one CSV
record per row; the `fold` analyzer rule validates the options literal and folds a
`schema_of_csv` schema argument into a DDL literal before `TypeCoercion` runs.

## Contents

- `from_csv.rs` — the `from_csv` scalar UDF plus its `#[cfg(test)]` pins (PERMISSIVE
  partial rows, FAILFAST, the corrupt-record column, the quoted separator, a sliced
  input, NULL documents). Argument checks live in `return_field_from_args` so both
  doors raise the same condition: arity `[WRONG_NUM_ARGS]`, non-string input
  `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]`, non-literal schema
  `[INVALID_SCHEMA.NON_STRING_LITERAL]`, bad DDL `[PARSE_SYNTAX_ERROR]`, complex
  field `[UNSUPPORTED_DATATYPE]`, non-string option `[INVALID_OPTIONS.NON_STRING_TYPE]`,
  non-map options `[INVALID_OPTIONS.NON_MAP_FUNCTION]`; the struct answers NULL for
  a NULL document and carries the input nullability.
  pins: fnp-gen-1/C-002, C-003, C-004
- `fold.rs` — the `CsvFold` analyzer rule, registered before `TypeCoercion` so a
  `map('sep', 1)`-style literal still carries its declared types. It refuses a
  `DROPMALFORMED` mode on a readable literal options map with
  `[PARSE_MODE_UNSUPPORTED]`; every other check is type-based in the kernels.
  pins: fnp-gen-1/C-003
- `fold.rs` — **Step 4b (run 18a):** the rule also folds a literal
  `schema_of_csv` call into its DDL string, aliased `schema_of_csv(<csv text>)`.
  It peels the `__repark_spark_nonnull__` shim `SparkNullability` wraps around a
  `map`/`make_array` options call, folds through the name-preserving outer alias
  that shim leaves behind, and reads the options map in its pre-coercion
  (`map(k, v, …)`) or post-coercion (`map(make_array, make_array)`) shape. A
  `from_csv` schema that is still not a literal after the fold raises
  `[INVALID_SCHEMA.NON_STRING_LITERAL]` here, so the kernel answers a `Null`
  placeholder meanwhile and the rebuilt `Projection` (also `Aggregate`/`Window`)
  carries the struct type. Non-foldable input raises
  `[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT]`, a NULL literal
  `[DATATYPE_MISMATCH.UNEXPECTED_NULL]`, a non-string literal
  `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]`, and an empty document the
  `[INTERNAL_ERROR]` defect.
  pins: fnp-gen-1/C-004
- `from_csv.rs` — **Step 7 (run 18a):** gate pass only — the token arms parse
  straight into their target width and the null-mark shares one helper; answers
  are unchanged.
  pins: fnp-gen-1/C-006
- `schema_of_csv.rs` — the `schema_of_csv` scalar UDF plus its `#[cfg(test)]`
  pins (the Spark `CSVInferSchema` ladder and renderer, the quoted separator, the
  `sep` option, the empty-document `INTERNAL_ERROR` defect, NULL, non-string
  input, non-map options). The kernel answers one DDL string per row and lets a
  `Null`-typed first argument through so the fold rule raises
  `[DATATYPE_MISMATCH.UNEXPECTED_NULL]`; the fold rule (step 4b) folds a literal
  call at analysis time.
  pins: fnp-gen-1/C-004

## Pointers

- Up: [../map.md](../map.md)
