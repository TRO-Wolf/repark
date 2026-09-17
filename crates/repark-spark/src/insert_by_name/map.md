# map — repark-spark/src/insert_by_name

## Purpose

File-backed tests for `../insert_by_name.rs` (ICE-RTAS-BYNAME-1, 2026-09-17):
the token-level `BY NAME` strip and the name-resolution error rules. Each
recognized form has a row; the executor pins live in the Python suite
(`test_ice_rtas_byname_1.py`).

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../insert_by_name.rs`.
  Strip pins (plain / overwrite / `TABLE` keyword / case), absence pins
  (no `BY NAME`, `ORDER BY name` after the source, quoted `"BY"`), and the
  resolution-rule pins (count-first arity, case-insensitive match, duplicate
  source ambiguity, extra-column refusal).

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.
- Oracle: `../../../../python/repark/tests/ice_rtas_byname_1_spark_oracle.json`.
- Ledger: `../../../../task/ledgers/staging/ice-rtas-byname-1-ledger.md`.

## Debug

| Symptom | First check |
|---|---|
| A `BY NAME` after the source stopped stripping | `find_by_name_span` in `../insert_by_name.rs` — the span must precede the source start and survive the paren-depth walk |
| A quoted `"BY NAME"` strips | the tokenizer must mark quoted spans so the word walk skips them |
