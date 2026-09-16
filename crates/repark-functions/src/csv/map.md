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
- `schema_of_csv.rs` — the `schema_of_csv` scalar UDF plus its `#[cfg(test)]`
  pins (the Spark `CSVInferSchema` ladder and renderer, the quoted separator, the
  `sep` option, the empty-document `INTERNAL_ERROR` defect, NULL, non-string
  input, non-map options). The kernel answers one DDL string per row; the fold
  rule (step 4b) folds a literal call at analysis time.
  pins: fnp-gen-1/C-004

## Pointers

- Up: [../map.md](../map.md)
