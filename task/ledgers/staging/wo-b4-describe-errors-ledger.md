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

- [ ] S0: add the scoped ledger and parser/error-contract test matrix.
- [ ] S1: retain malformed table and column lexer/parser failures as ParseException-class errors.
- [ ] S2: preserve four-part table and nested-column resolution contracts.
- [ ] S3: add facade pins for error class, condition, SQLSTATE, text, and answer rows.
- [ ] S4: update maps, sweep changed parser/error sites, run the required gates, and retire the ledger.

## PROPOSITION LEDGER — WO-B4 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every malformed column tail after a parsed DESCRIBE table has Spark's measured parse condition, SQLSTATE, and first-line text. | Parser and session pins cover every m6 malformed-tail row. | **OPEN** | Implementation pending. |
| C-002 | A malformed table name after DESCRIBE or DESC has Spark's measured parse error while namespace, database, schema, function, and query forms fall through. | Parser and session pins cover every m7 table-grammar row and near miss. | **OPEN** | Implementation pending. |
| C-003 | Four-part table names and measured column-resolution cases retain their Spark analysis contracts. | Session pins cover four-part, missing-table, missing-column, nested, and deep-nested rows. | **OPEN** | Implementation pending. |
| C-004 | The Python facade exposes the measured ParseException class, condition, SQLSTATE, text, and answer rows. | Facade pins cover the required m6 and m7 rows. | **OPEN** | Implementation pending. |
| C-005 | The scoped parser/error sweep finds no swallowed tokenize or parse result and no weakened new error pin. | Diff audit and required gates are recorded before ledger retirement. | **OPEN** | Implementation pending. |
