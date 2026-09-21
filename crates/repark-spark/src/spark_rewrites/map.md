# map — repark-spark/src/spark_rewrites

## Purpose

Token-level rewrite planners for the Spark door's canonicalize layer
(`spark_literals::canonical_rewrite`). Each `plan_*_regions` function appends
[`LiteralRegion`](../spark_literals.rs) replacements applied to the caller's SQL
before any parse. Scoping decisions live here; the wiring order and the fast
path stay in `spark_literals.rs`.

## Contents

- `mod.rs` — the shared span helpers (`line_starts`, `byte_offset`,
  `skip_whitespace`, `matching_paren`) and the pre-existing rewrite families:
  numeric suffixes (**FNP-4B**), `0x` hex identifiers, FROM-less `DELETE`, DROP
  TEMPORARY, wildcard `EXCEPT`, the INSERT `PARTITION (…) (cols)` order swap,
  and call-base struct field access.
- `create_options.rs` — **D-5 (2026-09-21):** the `OPTIONS` → `TBLPROPERTIES`
  seam. Recognizes `CREATE [OR REPLACE] TABLE [IF NOT EXISTS] name [(cols)]
  USING iceberg` and rewrites its single well-formed `OPTIONS (k=v, …)` clause
  (before the `AS` boundary, for the CTAS door too) into `TBLPROPERTIES`
  carrying each key raw plus `option.`-prefixed, so both spellings reach the
  stored property map. Pair keys unquote through the Spark escape rules; values
  copy verbatim from the source with inner literal regions spliced, so values
  with commas or parens inside string literals survive. Non-iceberg providers,
  a missing `USING iceberg`, `WITH`/plain variants, malformed pairs, duplicate
  `OPTIONS` clauses, and DataFusion-style no-eq pairs stay untouched (they keep
  today's loud failure). `sql_may_have_create_options` guards the borrowed fast
  path in `spark_literals.rs`.
  Pins: the parse-level and near-miss pins in
  [tests/ice_ddl_clauses_1.rs](../tests/ice_ddl_clauses_1.rs), the end-to-end
  cells `D-CREATE-OPTIONS` / `D-CTAS-OPTIONS` in
  [tests/create_table.rs](../tests/create_table.rs) and
  [python/repark/tests/test_ice_ddl_clauses_1.py](../../../../python/repark/tests/test_ice_ddl_clauses_1.py).
