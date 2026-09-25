# map — repark-spark/src/merge/nested_assign

## Purpose

Helpers of [`../nested_assign.rs`](../map.md), the nested struct-field assignment fold for the
Spark door's UPDATE and MERGE (U8 WRITE-SQL PR2, 2026-09-25). This directory closes only if the
fold moves elsewhere.

## Contents

- `expand.rs` — fix round 3 (2026-09-25): a MERGE `UPDATE SET *` / `INSERT *` whose source
  struct differs from the target's field order or names is expanded here into explicit
  per-column values (`star_source`), so the struct goes through `value_sql_for`'s by-name
  rebuild. A star is left to repark-iceberg's expansion when no struct differs, when a target
  column has no unique source column, or under schema evolution (R-18: taking it there made
  `accept-any-schema` stars commit where Spark refuses). `fold_insert_values` sends a
  top-level struct value of `INSERT (…) VALUES (…)` through the same rebuild. Fix round 4
  (2026-09-25): under `spark.sql.caseSensitive=true` `unique_match` compares names exactly, and
  `refuse_unwritten_star_columns` reads a derived source's written select-list names (DataFusion
  folds an unquoted alias, so its schema cannot tell `ID` from `id`) and refuses Spark's
  `UNRESOLVED_COLUMN.WITH_SUGGESTION` with the suggestions sorted by name, then by edit
  distance. pins: u8-write-sql/C-030, C-033
- `render.rs` — the Spark text of every refusal the fold raises, and Spark's two renderings
  of an assignment. The pretty form (`toPrettySQL`) drops qualifiers and string quotes and
  fills the `Cannot resolve "…"` list. The `.sql` form (qualified column, backticked fields)
  fills the `Conflicting assignments` and nested-INSERT-key details. Also `scala_type`: Spark's
  `DataType.toString` for the `Updating nested fields is only supported for StructType`
  detail. Spark's `quoteIfNeeded` quotes a path part only when it is not a plain word.
- `tests.rs` — unit pins: key resolution and its refusals, the Scala type names, quoting,
  the pretty values, the struct-by-name leaf rules, the fold of several assignments into one
  rebuild, the combined refusal text, and a top-level struct value folded through the
  by-name check (missing, extra, deep missing, reordered), and fix round 4's exact-case refusal
  and the `NOT NULL` rebuild. pins: u8-write-sql/C-027, C-028, C-029, C-030, C-032, C-033

## Pointers

- Up: [../map.md](../map.md).
- Ledger: `../../../../../task/ledgers/staging/u8-write-sql-ledger.md` (PR2 section).
