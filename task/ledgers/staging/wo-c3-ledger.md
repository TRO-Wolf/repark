# Unit ledger — WO-C3 · SHOW CREATE TABLE critic remediation

**Date:** 2026-09-23 · **Branch:** `xd/show-create` · **Base:** `origin/main`
**Model:** gpt-5.6-terra · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the WO-C5 remediation commit lands.

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
- [x] Pin complete SHOW TABLES and SHOW COLUMNS rows and Arrow types; keep the pinned SHOW TBLPROPERTIES refusal.

## PROPOSITION LEDGER — WO-C3 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A SHOW CREATE TABLE lexical failure remains the typed invalid-statement refusal when line or nested bracket comments occur before or between its keywords. | Rust pre-check and parser pins plus facade labels from m8. | PROVEN | `comment_aware_show_create_*` and `test_show_create_comments_*`; the shared scanner also replaces router's raw default-SET precheck. |
| C-002 | SHOW CREATE parse refusals expose Spark's condition, SQLSTATE, and first line while retaining RePark's exact no-caret rendering. | Rust and facade exact-message pins; registry scope statement. | PROVEN | Exact Rust parser and rendered-error checks plus facade message equality; registry assigns the absent caret block to IPI-51. |
| C-003 | The multi-term sort-order fixture matches the complete measured m2 CREATE text after only catalog/name/location substitution. | Exact Rust CREATE-text pin. | PROVEN | `show_create_multi_term_sort_order_matches_spark` carries the `range` property and matches the full m2 text. |
| C-004 | ParserError-wrapped parse refusals report their bracketed condition without classifying malformed wrappers. | Facade and direct native-exception pins. | PROVEN | Facade pins cover INSERT BY NAME and multi-statement; direct parser-wrapper pins cover ordinary, lowercase, and no-prefix messages. |
| C-005 | SHOW TABLES and SHOW COLUMNS preserve complete Spark rows and Arrow field types, and SHOW TBLPROPERTIES preserves its pinned analysis refusal. | Exact Rust rows, schemas, and refusal text. | PROVEN | `show_tables_matches_the_complete_spark_row_and_schema`, `show_columns_matches_the_complete_spark_row_and_schema`, and `show_tblproperties_keeps_its_current_analysis_refusal`. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: wo-c3
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The C-005 test pins both complete SHOW row schemas and the current TBLPROPERTIES refusal text.
      artifacts: [crates/repark-spark/src/tests/show_create.rs::show_tblproperties_keeps_its_current_analysis_refusal]
    - id: AT-2
      status: ATTACKED
      evidence: The tests exercise the served SHOW TABLES and SHOW COLUMNS forms and the TBLPROPERTIES refusal.
      artifacts: [crates/repark-spark/src/tests/show_create.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The TBLPROPERTIES near miss asserts the complete analysis failure message.
      artifacts: [crates/repark-spark/src/tests/show_create.rs::show_tblproperties_keeps_its_current_analysis_refusal]
    - id: AT-4
      status: N/A
      evidence: The change removes a parser and intercept and adds no shared state or ordering path.
      justification: This rollback has no state, ordering, or concurrency surface.
    - id: AT-5
      status: N/A
      evidence: The change removes a read-only parser and handler and adds no privileged operation.
      justification: This rollback has no authorization or security surface.
    - id: AT-6
      status: ATTACKED
      evidence: Existing full-row and schema pins remain for SHOW TABLES and SHOW COLUMNS, and the refusal pin covers TBLPROPERTIES.
      artifacts: [crates/repark-spark/src/tests/show_create.rs]
    - id: AT-7
      status: N/A
      evidence: The change removes table loading and batch construction for this intercepted statement.
      justification: This rollback adds no resource or performance path.
    - id: AT-8
      status: ATTACKED
      evidence: The test verifies the pinned Spark-shaped analysis error text for the intercepted statement.
      artifacts: [crates/repark-spark/src/tests/show_create.rs::show_tblproperties_keeps_its_current_analysis_refusal]
    - id: AT-9
      status: N/A
      evidence: The change removes statement handling and adds no operational signal.
      justification: This rollback has no logging, metrics, or operational behavior surface.
    - id: AT-10
      status: ATTACKED
      evidence: The named refusal test and full Rust and Python gates exercise the restored contract.
      artifacts: [crates/repark-spark/src/tests/show_create.rs::show_tblproperties_keeps_its_current_analysis_refusal]
  reattested: [AT-1, AT-2, AT-3, AT-6, AT-8, AT-10]
  complete: true
```

**WO-C4 (2026-09-23):** removed the branch-local SHOW TBLPROPERTIES parser, intercept, executor, and batch builder. The C-005 pin records the complete current refusal; SHOW TABLES, SHOW COLUMNS, `from_parts`, CREATE parsing, and the IPI-51 full-string error pin remain.

**WO-C5 (2026-09-23):** the Spark multi-statement parser refusal carries SQLSTATE `42601`; SHOW CREATE TABLE multi-statement SQL keeps Spark's `INVALID_STATEMENT_OR_CLAUSE`; unclosed bracket comments after the SHOW CREATE TABLE head map to `UNCLOSED_BRACKETED_COMMENT`, while an unclosed comment before TABLE falls through to the native tokenizer error. Refusal pins in the affected Rust and Python files assert their error variant or exception type and complete message contracts.

## PROPOSITION LEDGER — WO-C5 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-006 | Spark multi-statement refusals expose SQLSTATE `42601` and retain the SQL parser error variant. | Exact router message and Python exception contract. | PROVEN | `bug010_multi_statement_refuses_parse_class`; `test_parser_wrapper_refusals_report_the_native_error_condition`. |
| C-007 | An unclosed bracketed comment after the SHOW CREATE TABLE head returns Spark's `UNCLOSED_BRACKETED_COMMENT` parser message. | Pin the error variant, full Rust parser and rendered messages, Python type, condition, SQLSTATE, and full text. | PROVEN | `show_create_unclosed_bracketed_comments_keep_spark_parse_class`; `test_show_create_unclosed_bracketed_comment_has_spark_parse_contract`. |
| C-008 | An unclosed comment that hides TABLE falls through to the tokenizer error, and SHOW CREATE TABLE followed by another statement keeps Spark's invalid-statement answer. | Pin both exact outcomes and their parser error class. | PROVEN | `show_create_comment_before_table_keyword_keeps_tokenizer_fallthrough`; `show_create_multi_statement_keeps_spark_invalid_statement_class`; matching facade pins. |
| C-009 | Refusal tests touched by WO-C5 pin error variants or exception types and complete error messages. | Audit all five test files changed on the branch. | PROVEN | Updated Rust SHOW CREATE refusals and the multi-statement router pin; Python SHOW CREATE and multi-statement pins. Other checked refusals already pin the complete contract or are constructor/parser mechanism tests. |

pins: wo-c5/C-006, C-007, C-008, C-009
