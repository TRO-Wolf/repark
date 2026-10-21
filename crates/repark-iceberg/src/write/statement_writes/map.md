# map — repark-iceberg/src/write/statement_writes

## Purpose

The unit tests of the parent `statement_writes.rs`: the walk `executed_writes` and the wrapper
refusal `refuse_wrapped_write` (EXPLAIN-DML-WRITE-1, 2026-10-21). The parent's prose stays in
[../map.md](../map.md).

## Contents

- `tests.rs` — **EXPLAIN-DML-WRITE-1 (2026-10-21, rulings 138, 140 to 142):** statements parsed
  with DataFusion's `DFParser`. `an_explain_analyze_of_a_write_refuses_with_its_text`: eight
  writes (`DELETE`, `UPDATE`, `INSERT`, `MERGE`, with plain and `IN (SELECT …)` predicates and a
  CTE-carried `DELETE`) under `EXPLAIN ANALYZE`, `EXPLAIN ANALYZE VERBOSE`, a doubled
  `EXPLAIN ANALYZE` and `EXPLAIN ANALYZE PREPARE` take the ruled `EXPLAIN ANALYZE` text.
  `a_prepare_of_a_write_refuses_with_its_text`: the same writes under `PREPARE` and under
  `PREPARE … AS EXPLAIN ANALYZE`, a parameterised `PREPARE`, and the prepared name in the text.
  `a_query_a_plain_explain_and_a_bare_statement_answer_nothing`: queries, `EXPLAIN ANALYZE` of a
  query, `PREPARE` of a query or a view, `EXECUTE`, `EXPLAIN ANALYZE COPY`, every bare write, and
  every plain `EXPLAIN` at any depth answer `Ok`. `neither_refusal_names_a_data_deleting_action`:
  ruling 76 (a). pins: explain-dml-write-1/C-005, C-007
  Five mutants of the refusal each die on these tests and on the ANSI door's pins, and each
  door's call removed alone reds that door's pins (ledger §5). pins: explain-dml-write-1/C-009
  The unit's never-worse census on four doors (§6), its records (§7) and its gates (§8) are in
  the ledger. pins: explain-dml-write-1/C-010, C-012, C-013
  **Fix round 1 (2026-10-21, verifier S2-3):** the `PREPARE` text names the statement as written,
  quoting included (`PREPARE "My P" …` → ``PREPARE `"My P"` …``); an unquoted name is unchanged.
  pins: explain-dml-write-1/C-015

Up: [../map.md](../map.md).
