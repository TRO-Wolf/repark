# map — repark-spark/src/insert_by_name

## Purpose

File-backed tests for `../insert_by_name.rs` (ICE-RTAS-BYNAME-1, 2026-09-17):
the token-level `BY NAME` strip and the name-resolution error rules. Each
recognized form has a row; the executor pins live in the Python suite
(`test_ice_rtas_byname_1.py`).

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../insert_by_name.rs`.
  Strip pins (plain / overwrite / `TABLE` keyword / case, `PARTITION` kept),
  absence pins (no `BY NAME`, `ORDER BY name` after the source, quoted `"BY"`),
  and the resolution-rule pins (count-first arity, case-insensitive match,
  duplicate source ambiguity, extra-column refusal). Round 2 (2026-09-17):
  case-sensitive exact match + `EXTRA_COLUMNS`, `STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST`
  and `CANNOT_FIND_DATA` texts, partition-literal rendering, verbatim
  syntactic names under the flag.
  pins: ice-rtas-byname-1/C-007, C-009, C-010

## Round 2 (2026-09-17)

`PARTITION (…)` routes through the positional arm: static overwrite delegates
to `execute_partition_overwrite` after name projection (the source must not
name a static column); dynamic overwrite is whole-table replace-all, which is
Spark's default-mode answer for a valueless spec (`partitionOverwriteMode`
is unread, same residue class as the positional always-dynamic path);
static append injects the clause literals into the projection; dynamic
append matches the full target list. Empty unpartitioned overwrite wipes
via `commit_overwrite_replace_all_to` after the empty-source type guard.
A missing required target refuses `CANNOT_FIND_DATA` before any write.
`spark.sql.caseSensitive=true` matches exact
(carrier `repark_functions::case_sensitive`). `plan_name_projection` owns
matching plus projection building; `execute_insert_by_name` owns routing
plus the two commits.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.
- Oracle: `../../../../python/repark/tests/ice_rtas_byname_1_spark_oracle.json`.
- Ledger: `../../../../task/ledgers/staging/ice-rtas-byname-1-ledger.md`.

## Debug

| Symptom | First check |
|---|---|
| A `BY NAME` after the source stopped stripping | `find_by_name_span` in `../insert_by_name.rs` — the span must precede the source start and survive the paren-depth walk |
| A quoted `"BY NAME"` strips | the tokenizer must mark quoted spans so the word walk skips them |

## IPI-19 + IPI-37 (2026-09-20) — the schema-evolving by-name append

- `evolution.rs` — `columns_to_add` applies the `write.spark.accept-any-schema`
  gate and the merge-schema flag to the source columns the table does not have,
  returning the names to add (empty means the existing refusal arms answer).
  `append_with_evolution` plans the projection, drops NULL-typed fill columns
  from the union input (a `NULL AS col` fill carries no type, and unioning it
  would refuse) while refusing outright if an *added* column is itself typeless,
  commits the union through `repark_iceberg::write::evolve_schema`, and writes
  the data files against the table that returns. `INSERT OVERWRITE … BY NAME`
  never evolves — no cell measures it and silently widening a schema on an
  overwrite is the wrong default.
  pins: ipi-19-56-37-schema-evolution-write/C-002, C-011
