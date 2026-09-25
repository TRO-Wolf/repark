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
  A top-level SET key that names a struct column goes through the same by-name check
  (fix round 2, 2026-09-25), so `SET st = named_struct('q', 1, 'b', 'z')` refuses
  `CANNOT_FIND_DATA` for `st`.`a` instead of writing a NULL field, an extra field refuses
  `EXTRA_STRUCT_FIELDS`, and a reordered or re-cased value resolves by name. A top-level key
  on an atomic column is folded only when it repeats (fix round 3), so a repeated key refuses
  Spark's `Multiple assignments` text; `repeats_a_column` lets the UPDATE route raise it before
  its cast refusal.
  `fold_merge_clauses` loads the MERGE target when any UPDATE clause has a SET key or any
  INSERT clause has a column list, and applies the fold to each UPDATE clause. A star whose
  source struct differs from the target by name or order is expanded first, and struct INSERT
  values are rebuilt by name ([nested_assign/expand.rs](nested_assign/map.md), fix round 3).
  Fix round 4 (2026-09-25): `AssignmentScope.case_sensitive` carries `spark.sql.caseSensitive`
  (the UPDATE route sets it too), and `value_sql_for` then matches struct fields exactly. The
  rebuild takes `named_struct` instead of the Arrow cast when the target struct has a
  `NOT NULL` field at any depth, since Arrow's by-name struct cast refuses a nullable source
  there. A repeated key in an INSERT column list joins the nested-INSERT-key refusal as
  `Multiple assignments for '<col>': …` (`repeated_insert_keys`).
  Fix round 5 (2026-09-25): under `case_sensitive` the key lookups (`unqualified`, `resolve_key`)
  match exactly; a key whose column matches only in another case refuses
  `UNRESOLVED_COLUMN.WITH_SUGGESTION` (`resolve_target`), so `INSERT (id, ID)` no longer groups
  as a repeat. pins: u8-write-sql/C-033
  The design did not reuse repark-iceberg's `resolve_nested_path`. Its path grammar is the
  ALTER one (`key` / `value` / `element` name map and list children, and its refusals are the
  ALTER texts). Spark reads the same spellings in a SET key as value extraction with other
  refusals, and the rebuild needs the Arrow types.
  pins: u8-write-sql/C-025, C-026, C-027, C-028, C-029, C-030, C-032, C-033

## Pointers

- Up: [../map.md](../map.md).
- Ledger: `../../../../task/ledgers/staging/ipi-19-56-37-schema-evolution-write-ledger.md`.
