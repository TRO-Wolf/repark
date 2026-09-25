# map — repark-spark/src/merge

## Purpose

MERGE INTO lowering helpers beside `../merge.rs` (IPI-19 + IPI-56, 2026-09-20).
`../merge.rs` sits exactly at the default size ceiling, so the evolution strip
and the star-sentinel rewrite live here instead of in it.

## Contents

- `schema_evolution.rs` — the `MERGE WITH SCHEMA EVOLUTION` pre-parse strip: a
  significant-token match on `MERGE` + `WITH` + `SCHEMA` + `EVOLUTION`
  (case-insensitive, comments and whitespace ignored), the four tokens removed
  so the rest parses as an ordinary MERGE, and a bool carried into the lowered
  plan. A plain `MERGE INTO` is never touched. File-backed tests in
  [schema_evolution/map.md](schema_evolution/map.md).
  pins: ipi-19-56-37-schema-evolution-write/C-005, C-007
- `stars.rs` — the `UPDATE SET *` / `INSERT *` sentinel rewrite, moved here from
  `../merge.rs` untouched: a `*` in star position becomes the `STAR_SENTINEL`
  form stock sqlparser parses, and only there.
  pins: ipi-19-56-37-schema-evolution-write/C-005

- `nested_assign.rs` + [nested_assign/](nested_assign/map.md) — **U8 WRITE-SQL PR2
  (2026-09-25):** nested struct-field assignment for the Spark door's UPDATE and MERGE. It
  lives here because `lib.rs` is at its manifest ceiling and `merge.rs` is near the file-size
  ceiling. `fold_nested_assignments` resolves each SET key as Spark's field extraction does:
  a qualifier, case-insensitive names, a map value or an array-of-struct field as extraction
  steps. It refuses `FIELD_NOT_FOUND`, `INVALID_EXTRACT_BASE_FIELD_TYPE` and
  `UNEXPECTED_INPUT_TYPE` at that step. Then it aligns the statement's assignments per
  top-level column in table order, as Spark's `AssignmentUtils` does. Repeated or conflicting
  keys and a non-struct parent are collected into one
  `INVALID_ROW_LEVEL_OPERATION_ASSIGNMENTS` refusal. The first failing leaf cast throws
  `INCOMPATIBLE_DATA_FOR_TABLE.*` at once. Each affected struct is rebuilt as `named_struct`
  over `get_field` reads, with the assigned leaves cast through
  `repark_iceberg::write::update_cast::store_assignment_cast_sql`. A leaf's source type comes
  from a `SELECT (<value>) FROM <probe_from> LIMIT 0` plan. If that probe cannot plan, the
  leaf is cast unchecked and the statement fails later if the value is wrong.
  A struct-valued leaf resolves by name (`CANNOT_FIND_DATA`, `EXTRA_STRUCT_FIELDS`). Arrow's
  struct cast fills a missing field with NULL, so this check must come before the cast.
  `fold_merge_clauses` loads the MERGE target and applies the fold to each UPDATE clause.
  The design did not reuse repark-iceberg's `resolve_nested_path`. Its path grammar is the
  ALTER one (`key` / `value` / `element` name map and list children, and its refusals are the
  ALTER texts). Spark reads the same spellings in a SET key as value extraction with other
  refusals, and the rebuild needs the Arrow types.
  pins: u8-write-sql/C-025, C-026, C-027, C-028, C-029

## Pointers

- Up: [../map.md](../map.md).
- Ledger: `../../../../task/ledgers/staging/ipi-19-56-37-schema-evolution-write-ledger.md`.
