# Unit ledger — SHOW-TABLE-EXTENDED-1

**Date:** 2026-09-23 · **Branch:** `xd/show-tblprops` · **Base:** `92c03ada` (`origin/main`)
**Model:** Codex (gpt-5.6-terra) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

## Plan

- Add red Rust pins for the parsed grammar, four-column result, exact information text, scope,
  error, filtering, property, and schema-tree cases.
- Add the token-level parser and route it before sqlparser.
- Build the metadata text from the shared Spark property view and the Arrow schema.
- Add the facade and dbt statement-surface pins, then correct the parity registry row.
- Run the requested Rust, Python, structural, and comment-ban gates before departure.

## Risk ledger

- Parser recognition must not claim `SHOW TABLES`, `SHOW TBLPROPERTIES`, or SHOW CREATE forms.
- Scope lookup must preserve the existing `SHOW TABLES` ambient and missing-namespace contracts.
- Metadata output must preserve Spark's line order, Unicode character splitting, and trailing newlines.
- Nested schemas must use Spark type names rather than DDL names.

## Proposition ledger

| Clause | Proposition | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The Spark dialect recognizes only `SHOW TABLE EXTENDED [IN\|FROM namespace] LIKE 'pattern' [PARTITION (...)]` before sqlparser. | Parser and near-miss pins. | **PROVEN** | The red-first `cargo test -p repark-spark show_table_extended` run failed seven pins on the base with sqlparser's `SHOW [VARIABLE]` refusal. The parser now recognizes case-insensitive keywords, quoted identifiers, required LIKE, optional PARTITION, and a trailing semicolon; `SHOW TABLES`, `SHOW TBLPROPERTIES`, and SHOW CREATE preserve their paths. pins: show-table-extended-1/C-001 |
| C-002 | The executor returns Spark's four columns and exact Iceberg metadata text, including sorted table names, property redaction, Unicode scalar splitting, snapshot changes, and the schema tree. | Memory-catalog end-to-end rows and exact-string pins. | **PROVEN** | `tests/show_table_extended.rs` pins the partitioned and plain rows, LOCATION management, v3 properties, an owner property, and struct, array, and map tree layouts. `spark_table_properties` remains the sole property source. pins: show-table-extended-1/C-002 |
| C-003 | Scope resolution and refusal behavior match the stated Spark surface without widening to views or session temporary views. | Ambient, missing namespace, partition, filtering, alternation, case, and view pins. | **PROVEN** | The test battery reuses `resolve_show_tables_scope`, asserts IN and FROM, ambient USE scope, sorted glob matching without views, case-insensitive alternation, no-LIKE syntax, partition management, missing-table partition, and missing namespace refusals. Session temporary views remain the named residue. pins: show-table-extended-1/C-003 |
| C-004 | The facade and dbt statement surfaces expose the four-column result, while `SHOW TBLPROPERTIES` remains refused and the registry states the measured boundary. | Targeted Python/dbt pins and requested repository gates. | **PROVEN** | `test_catalog_surface.py` asserts the facade columns and information prefix; `test_statement_surface.py` moves the extended form to served while retaining `R-SHOW-TBLPROPERTIES`. The registry records Spark 4.1.2's 2026-09-23 measurement and the temporary-view residue. pins: show-table-extended-1/C-004 |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: show-table-extended-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each statement form, row shape, information line, and residue in the work order has a named Rust or facade pin.
      artifacts: [crates/repark-spark/src/tests/show_table_extended.rs, python/repark/tests/test_catalog_surface.py]
    - id: AT-2
      status: ATTACKED
      evidence: The pins exercise no LIKE, optional scope, FROM, alternation, case folding, Unicode properties, v3, empty tables, snapshots, and nested types.
      artifacts: [crates/repark-spark/src/tests/show_table_extended.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The suite names the parser, missing namespace, missing table, and partition-management failures.
      artifacts: [crates/repark-spark/src/tests/show_table_extended.rs]
    - id: AT-4
      status: N/A
      justification: The statement reads catalog metadata and adds no concurrent state, retry, or cleanup path.
    - id: AT-5
      status: ATTACKED
      evidence: The diff makes no AWS, IAM, dependency, workflow, or outward-facing change; the comment-ban gate reports zero hits.
      artifacts: [crates/repark-spark/src/show_table_extended.rs, task/ledgers/staging/show-table-extended-1-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Existing SHOW statement paths remain pinned, and the new parser intercept appears only after the exact token grammar recognizes it.
      artifacts: [crates/repark-spark/src/tests/show_table_extended.rs]
    - id: AT-7
      status: N/A
      justification: The change has no migration, persisted format, or upgrade path.
    - id: AT-8
      status: ATTACKED
      evidence: The implementation reuses the existing scope resolver and shared Spark property view, with no new dependency.
      artifacts: [crates/repark-spark/src/use_ddl.rs, crates/repark-spark/src/table_props_view.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Returned information and refusal text remain directly observable through the Spark SQL and facade entry points.
      artifacts: [python/repark/tests/test_catalog_surface.py, python/dbt-repark/tests/test_statement_surface.py]
    - id: AT-10
      status: ATTACKED
      evidence: The end-to-end test module covers every new parser branch, and the facade/dbt pins verify the public entry points.
      artifacts: [crates/repark-spark/src/tests/show_table_extended.rs, python/repark/tests/test_catalog_surface.py, python/dbt-repark/tests/test_statement_surface.py]
```

## Completion record

Implementation and requested verification are complete on this branch. The unit moves to
`../completed/` in its last commit.
