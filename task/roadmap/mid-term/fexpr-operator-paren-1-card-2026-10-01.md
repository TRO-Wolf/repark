# FEXPR-OPERATOR-PAREN-1 — F.expr fragments without spaced operators lose their parentheses

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 78;
#890 re-verify 3, finding **VE4-1**). **Severity: HIGH** (wrong values).
**Status:** pre-existing on base. **Target:** mid-term.

## The divergence it records

An `F.expr` fragment without spaces around a binary operator is composed without
parentheses, so `cube(F.expr('a-b')*2)` and `F.expr('(a) - (b)')` give wrong
cube/rollup grouping keys: head and base give -1/-16/3, Spark gives
-14/0/10/4.

## Repro, verbatim from the source line

```python
cube(F.expr('a-b')*2)
F.expr('(a) - (b)')
```

Measured on head and base versus Spark 4.1.2: RePark answers -1/-16/3, Spark
answers -14/0/10/4.

## Suspected cause and suggested fix direction (a suggestion, not a decision)

Cause, verbatim: `python/repark/src/repark/spark/functions.py:326-329,338`
adds parentheses only for space-padded operators. (Verified present at base
`02c2f1be` at the same lines.) Suggested fix, verbatim: always paren-wrap the
stored `sql_expr`, except when it is already a single paren group, an alias or
`*`. This direction is suggested by the verifier, not a decision.

## Clauses (draft)

- **C-001** `cube(F.expr('a-b')*2)` and the `F.expr('(a) - (b)')` grouping key answer Spark's values.
- **C-002** Already-parenthesized fragments, aliases and `*` keep their current rendering (no double wrap).
- **C-003** The neighbouring `F.expr` display cells (`1 + 1` → `(1 + 1)`) still answer Spark.

## Pointers

- Up: [map.md](map.md)
