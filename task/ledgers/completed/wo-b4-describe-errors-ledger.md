# Unit ledger — WO-B4 · DESCRIBE malformed-input error contract

## Round 1 (2026-09-23)

**Date:** 2026-09-23 · **Branch:** `xd/describe` · **Base:** `origin/main` ·
**Model:** Codex (gpt-5.6-terra) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Scope:** The Spark DESCRIBE intercept retains malformed table and column inputs after its head,
returns Spark 4.1.2's typed parse errors, preserves table-resolution errors, and leaves non-table
DESCRIBE forms on their existing parser paths.

## Plan

- [x] S0: add the scoped ledger and parser/error-contract test matrix.
- [x] S1: retain malformed table and column lexer/parser failures as ParseException-class errors.
- [x] S2: preserve four-part table and nested-column resolution contracts.
- [x] S3: add facade pins for error class, condition, SQLSTATE, text, and answer rows.
- [x] S4: update maps, sweep changed parser/error sites, run the required gates, and retire the ledger.

## PROPOSITION LEDGER — WO-B4 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every malformed column tail after a parsed DESCRIBE table has Spark's measured parse condition, SQLSTATE, and first-line text. | Parser and session pins cover every m6 malformed-tail row. | **PROVEN** | `cargo test -p repark-spark describe` exits 0 with exact parser-variant and session-message pins for every measured malformed tail. |
| C-002 | A malformed table name after DESCRIBE or DESC has Spark's measured parse error while namespace, database, schema, function, and query forms fall through. | Parser and session pins cover every m7 table-grammar row and near miss. | **PROVEN** | `describe_column_errors.rs` pins each m7 malformed name and the five non-table near misses; the targeted Rust suite exits 0. |
| C-003 | Four-part table names and measured column-resolution cases retain their Spark analysis contracts. | Session pins cover four-part, missing-table, missing-column, nested, and deep-nested rows. | **PROVEN** | Session pins cover four-part and missing-table text, missing-column suggestion text, and both nested-column outcomes; the two declared divergences are pinned separately. |
| C-004 | The Python facade exposes the measured ParseException class, condition, SQLSTATE, text, and answer rows. | Facade pins cover the required m6 and m7 rows. | **PROVEN** | `make develop` and the required facade pytest pair exit 0; each new error pin checks exception class, condition, SQLSTATE, and full text. |
| C-005 | The scoped parser/error sweep finds no swallowed tokenize or parse result and no weakened new error pin. | Diff audit and required gates are recorded before ledger retirement. | **PROVEN** | The class sweep finds no added parser/tokenizer `.ok()` fall-through or weak error assertion. `make verify` and the required gate set exit 0. |

## Self Logic Review — SLR-001

The intercept returns typed parser errors before literal normalization. An unmatched quote therefore cannot take the
generic lexer path. Four-part names use the catalog's existing all-parts not-found builder, except that a last part
naming a metadata table with no column tail leaves the name to the #806 metadata-table path (WO-B10). Five-part
names remain an explicit current-behaviour pin; DESCRIBE-on-view column tails match Spark 4.1.2 (measured by WO-B10).

## Validation (2026-09-23)

`cargo test -p repark-spark describe`, `make rust-clippy`, `make rust-panic-ban`,
`python3 scripts/check_rust_file_size.py`, `bash scripts/check_map_md.sh`,
`python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xd-show origin/main HEAD`, and
`.venv/bin/python -m pytest python/repark/tests/test_describe_table.py
python/repark/tests/test_catalog_surface_1.py -q` exit 0. `make develop` exits 0 before the
facade test. `make verify` exits 0 after the formatter-only Python baseline ratchet.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: wo-b4-describe-errors
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Exact parser and session pins cover every measured m6 and m7 malformed statement.
      artifacts: [crates/repark-spark/src/tests/describe_column_errors.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Pins distinguish token kinds, end of input, unmatched quotes, partition syntax, and multipart names.
      artifacts: [crates/repark-spark/src/tests/describe_column_errors.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The router pin proves parser-class errors are returned before literal canonicalization.
      artifacts: [crates/repark-spark/src/router.rs, crates/repark-spark/src/tests/describe_column_errors.rs]
    - id: AT-4
      status: N/A
      justification: The unit changes statement parsing and error construction only. It adds no shared state, lock, or async work.
    - id: AT-5
      status: N/A
      justification: The unit adds no credential, authorization, network, filesystem, or destructive operation.
    - id: AT-6
      status: ATTACKED
      evidence: Near-miss pins keep namespace, database, schema, function, query, SELECT literal, and DESCRIBED paths unchanged.
      artifacts: [crates/repark-spark/src/tests/describe_column_errors.rs]
    - id: AT-7
      status: N/A
      justification: The parser reads one statement and builds bounded error text. It adds no input-scaled execution path.
    - id: AT-8
      status: ATTACKED
      evidence: Rust and Python pins compare exact typed errors, condition, SQLSTATE, messages, and answer rows.
      artifacts: [crates/repark-spark/src/tests/describe_column_errors.rs, python/repark/tests/test_describe_table.py]
    - id: AT-9
      status: ATTACKED
      evidence: The unmatched-quote and malformed-tail pins prove the intercept preserves parse altitude before table resolution.
      artifacts: [crates/repark-spark/src/describe_show.rs, crates/repark-spark/src/describe_column.rs]
    - id: AT-10
      status: ATTACKED
      evidence: The class sweep rejects new swallowed parser results and weak error assertions in the touched DESCRIBE tests.
      artifacts: [crates/repark-spark/src/tests/describe_column_errors.rs, python/repark/tests/test_describe_table.py]
  complete: true
```
