# Unit ledger — WO-B6 · DESCRIBE tokenizer failures skip SQL comments

**Date:** 2026-09-23 · **Branch:** `xd/describe` · **Base:** `xd/show-create` (#810)
**Model:** gpt-6-luna · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the final WO-B6 commit lands.

**Why now.** The tokenizer-failure path used whitespace splitting for DESCRIBE detection and read
quote characters inside SQL comments. Spark's shared comment scanner already defines both comment
forms and nested block-comment behavior.

## Plan

- [x] Rebase on #810 and retain its shared table-property view and both router refusals.
- [x] Use the shared SQL comment scanner for DESCRIBE keyword checks and unclosed-quote scans.
- [x] Pin the eight measured parser messages, non-table fallthrough outcomes, and valid comment rows.
- [x] Run every WO-B6 gate and record its result: Rust DESCRIBE 168 passed; SHOW CREATE 34 passed;
  facade 138 passed and 3 skipped; clippy, panic ban, map, file-size, Ruff, and comment-ban clean.

## PROPOSITION LEDGER — WO-B6 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The DESCRIBE tokenizer-failure pre-check skips leading and between-keyword SQL comments with the shared scanner, and excludes known non-table heads. | Rust classifier tests and full-message session pins. | PROVEN | `d_blk_ns_head_unclosed_falls_through`, valid comment pins, and the session error tests. |
| C-002 | The unclosed-quote scan ignores quote characters inside line and nested block comments and preserves doubled quote handling. | Eight full-string parser pins plus comment-only quote controls. | PROVEN | Rust and Python pins cover all eight m11 quote cells; valid comment rows include a quote-only block comment. |
| C-003 | An unclosed block comment and an unclosed quote after `DESCRIBE NAMESPACE` stay on RePark's prior tokenizer path. | Full-string RePark outcome pins and classifier fallthrough assertions. | PROVEN | Rust and Python assert the exact `TokenizerError` text for both inputs. |
| C-004 | Valid leading, intervening, and quote-only comments preserve complete DESCRIBE table rows. | Exact Python Arrow-row assertions. | PROVEN | `test_describe_comments_keep_valid_table_answers` pins all four cases. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: wo-b6-describe-comments
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The table pre-check uses the shared scanner and leaves NAMESPACE outside its path.
      artifacts: [crates/repark-spark/src/tests/describe_column_errors.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Full Rust messages pin each quote delimiter and comment placement.
      artifacts: [crates/repark-spark/src/tests/describe_table.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The quote-only comment control proves a comment quote cannot close or open a token.
      artifacts: [crates/repark-spark/src/tests/describe_column_errors.rs]
    - id: AT-4
      status: N/A
      evidence: The scanner is local and adds no shared mutable state or ordering path.
      justification: This change only reads SQL text.
    - id: AT-5
      status: N/A
      evidence: The scanner does not access catalogs, files, credentials, or external services.
      justification: This change only reads SQL text.
    - id: AT-6
      status: ATTACKED
      evidence: Python pins cover valid comment rows and each parser refusal.
      artifacts: [python/repark/tests/test_describe_table.py]
    - id: AT-7
      status: ATTACKED
      evidence: The classifier uses a bounded forward scan over the statement text.
      artifacts: [crates/repark-spark/src/describe_show.rs]
    - id: AT-8
      status: ATTACKED
      evidence: The non-table and unclosed-comment tests pin the existing tokenizer messages.
      artifacts: [crates/repark-spark/src/tests/describe_table.rs]
    - id: AT-9
      status: N/A
      evidence: The change adds no logging, metrics, or operational signals.
      justification: The change is a parser pre-check only.
    - id: AT-10
      status: ATTACKED
      evidence: Rust and facade gates exercise exact parser outputs and normal rows.
      artifacts: [crates/repark-spark/src/tests/describe_table.rs, python/repark/tests/test_describe_table.py]
  reattested: [AT-1, AT-2, AT-3, AT-6, AT-7, AT-8, AT-10]
  complete: true
```
