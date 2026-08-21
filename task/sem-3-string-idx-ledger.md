# Unit ledger — SEM-3 · `regexp_extract_all` accepts a string `idx`

**Date:** 2026-08-21 · **Branch:** `fix/spark-semantics` · **Base:** `a07c4ec` (SEM-1) ·
**Charter:** [sem-0-charter-ledger.md](sem-0-charter-ledger.md)

A regression, not a gap. `F.regexp_extract_all(s, pattern, "1")` raised where Spark, repark's own
SQL door, and repark's own sibling `F.regexp_instr` all accept the string — so this repository
disagreed with itself on plain input.

## 1. Reproduced first

On `a07c4ec`:

```
F.regexp_extract_all("s", F.lit("([a-z])([0-9])"), "1")
  → AnalysisException: Schema error: No field named "1".
```

The string is read as a column name. Meanwhile, on the same tree:

| Same intent, other doors | Result |
|---|---|
| `spark.sql("SELECT regexp_extract_all('a1b2','([a-z])([0-9])','1')")` | `['a','b']` |
| `F.regexp_instr(s, pattern, "0")` | `1` |

## 2. Provenance — the correct half and the incorrect half were removed together

[fnp-6a-regexp-ledger.md](fnp-6a-regexp-ledger.md) records the wrapper as having carried
`lit_indices={1, 2}`. Position **1** (`regexp`) genuinely had to go: Spark reads a bare string there
as a **column name**, which the oracle confirms —

```
df has columns s and p, p holding the pattern
F.regexp_extract_all("s", "p")  →  ['a', 'b']      (Spark 4.1.2)
```

— and forcing it to a literal was the defect F-FNP6A-1 was raised on. But the fix dropped the
**whole set** rather than narrowing it to `{2}`, taking the correct half with it. SEM-3 narrows.

This is worth stating plainly because the round-1 → round-2 pattern repeats here: a remediation
that over-corrects is how three of the parity campaign's round-2 S1s were introduced, and this is a
fourth instance of the same shape, found later.

## 3. The oracle

Live PySpark 4.1.2. A string `idx` is a literal group index in Spark, not a column reference:

| Call | Spark |
|---|---|
| `regexp_extract_all('a1b2','([a-z])([0-9])','0')` | `['a1','b2']` |
| `regexp_extract_all('a1b2','([a-z])([0-9])','1')` | `['a','b']` |
| `regexp_extract_all('a1b2','([a-z])([0-9])','2')` | `['1','2']` |
| `regexp_extract_all(s, p)` with `p` a **column** | `['a','b']` |

## 4. The change

`python/repark/src/repark/spark/functions_expr.py`, the three-argument arm only:

```python
lit_indices=None if isinstance(idx, Column) else frozenset({2})
```

Position 1 stays free, so a bare `regexp` remains a column name. A `Column` `idx` stays free, so a
genuine column reference is not stringified into a literal — the half that is easy to get wrong in
the other direction, and which has its own assertion.

Deliberately the same shape as `regexp_instr`'s existing line, so the two siblings read alike.

## 5. Pin, red before the fix

`python/repark/tests/test_sem3_string_idx.py` — 8 assertions, **3 failed / 5 passed** immediately
before the edit; **8 passed** after. The 5 that passed are the controls: the integer form, the
`Column` form, and the bare-string-`regexp` case.

**A sequencing note.** This pin was written first, before SEM-4 and SEM-1, and its
bare-string-`regexp` assertion was red for a reason that had nothing to do with this unit — it
expects Spark's `['a','b']`, which needed `RE-1` closed. Rather than weaken the assertion to
today's value, the unit order changed: SEM-4 → SEM-1 → SEM-3, so every pin lands green in one step
against Spark's real answer. The pin was parked, not edited.

## 6. Gates

Each captured alone with its own `$?`:

| Gate | Result |
|---|---|
| facade, regexp-adjacent (`-k "regexp or lrs6 or fnp6 or critic or sem"`) | 270 passed, 6 skipped, 0 failed |
| `make ci` | exit 0 |

No Rust changed in this unit.

## 7. Found in passing

Nothing new. The `regexp_substr` NULL finding surfaced in SEM-1 and is registered by SEM-5.
