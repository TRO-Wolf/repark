# Unit ledger — WO-C3 · SHOW CREATE TABLE critic remediation

**Date:** 2026-09-23 · **Branch:** `xd/show-create` · **Base:** `origin/main`
**Model:** gpt-5.6-terra · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the WO-C3 remediation commit lands.

**Why now.** The second critic for SHOW CREATE TABLE found five incomplete pins and one raw-text
comment scanner gap. The recorded Spark 4.1.2 probes `m2.json`, `m4.json`, `m8.json`, and
`m9.json` define this repair's answers and refusal contracts.

**Not in this unit:** STATUS, views, `describe_show.rs`, Cargo files, version changes, AWS commands,
pushes, or rebases.

## Plan

- [x] Replace the raw SHOW CREATE prefix split with a nested-comment-aware scanner and sweep matching raw keyword checks.
- [x] Pin full RePark refusal text and narrow the SHOW CREATE parity claim for IPI-51's caret-block residue.
- [x] Pin the complete multi-term sort-order CREATE text from the measured m2 answer.
- [x] Pin ParserError condition extraction for the measured INSERT BY NAME and multi-statement shapes.
- [x] Pin complete SHOW TABLES, SHOW COLUMNS, and SHOW TBLPROPERTIES rows and Arrow types.

## PROPOSITION LEDGER — WO-C3 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A SHOW CREATE TABLE lexical failure remains the typed invalid-statement refusal when line or nested bracket comments occur before or between its keywords. | Rust pre-check and parser pins plus facade labels from m8. | PROVEN | `comment_aware_show_create_*` and `test_show_create_comments_*`; the shared scanner also replaces router's raw default-SET precheck. |
| C-002 | SHOW CREATE parse refusals expose Spark's condition, SQLSTATE, and first line while retaining RePark's exact no-caret rendering. | Rust and facade exact-message pins; registry scope statement. | PROVEN | Exact Rust parser and rendered-error checks plus facade message equality; registry assigns the absent caret block to IPI-51. |
| C-003 | The multi-term sort-order fixture matches the complete measured m2 CREATE text after only catalog/name/location substitution. | Exact Rust CREATE-text pin. | PROVEN | `show_create_multi_term_sort_order_matches_spark` carries the `range` property and matches the full m2 text. |
| C-004 | ParserError-wrapped parse refusals report their bracketed condition without classifying malformed wrappers. | Facade and direct native-exception pins. | PROVEN | Facade pins cover INSERT BY NAME and multi-statement; direct parser-wrapper pins cover ordinary, lowercase, and no-prefix messages. |
| C-005 | SHOW TABLES, SHOW COLUMNS, and SHOW TBLPROPERTIES near misses preserve complete Spark row shapes and Arrow field types. | Exact Rust rows and schema pins. | PROVEN | SHOW TABLES and SHOW COLUMNS pin full rows and schemas; SHOW TBLPROPERTIES now serves the shared property rows and schema. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: wo-c3
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 through C-005 are PROVEN with Rust, facade, dbt, map, and registry evidence in the same branch.
      artifacts: [crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_show_create_table.py, task/ledgers/completed/wo-c3-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: The m8 comment labels, m2 multi-term sort terms, m4 SHOW rows, and m9 parser forms exercise the changed input boundaries.
      artifacts: [crates/repark-spark/src/show_create.rs, crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_e1_errorclass.py]
    - id: AT-3
      status: ATTACKED
      evidence: Typed parse refusals, malformed ParserError wrappers, unclosed comments, and unrelated SHOW forms retain explicit outcomes.
      artifacts: [crates/repark-spark/src/show_create.rs, python/repark/tests/test_show_create_table.py, python/repark/tests/test_ice_error_conditions_1.py]
    - id: AT-4
      status: N/A
      justification: The scanner and SHOW route use no shared mutable state, lock, task, or ordering-sensitive write.
    - id: AT-5
      status: N/A
      justification: The unit has no credential, network, AWS, IAM, secret, or destructive operation surface.
    - id: AT-6
      status: ATTACKED
      evidence: CREATE text, parse class, condition, SQLSTATE, error rendering, property rows, and Arrow schemas are all asserted at their entry points.
      artifacts: [crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_show_create_table.py, python/dbt-repark/tests/test_statement_surface.py]
    - id: AT-7
      status: N/A
      justification: The unit makes no performance claim and adds no data scan, background task, or unbounded allocation path.
    - id: AT-8
      status: ATTACKED
      evidence: The shared Spark-visible property list serves both SHOW CREATE and SHOW TBLPROPERTIES; m2, m4, m8, and m9 oracle shapes are pinned.
      artifacts: [crates/repark-spark/src/table_props_view.rs, crates/repark-spark/src/show_create.rs, docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: ATTACKED
      evidence: The visible parse and property-row surfaces use exact messages, full rows, field names, and Arrow types instead of fragments.
      artifacts: [crates/repark-spark/src/tests/show_create.rs, python/repark/tests/test_show_create_table.py]
    - id: AT-10
      status: ATTACKED
      evidence: The raw-keyword and partial-answer class sweeps cover the changed branch; rust-clippy, panic ban, size, map, Ruff, and comment-ban gates passed.
      artifacts: [crates/repark-spark/src/router.rs, crates/repark-spark/src/show_create.rs, crates/repark-spark/src/tests/show_create.rs]
```
