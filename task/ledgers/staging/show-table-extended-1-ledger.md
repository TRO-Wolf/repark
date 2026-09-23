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

## Completion record

Pending implementation and gates.
