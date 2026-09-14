# Card REPLACE-NOVALUE-1 — `DataFrame.replace(to_replace)` with no `value`

**Date:** 2026-09-14 · **Filed by:** run 13b under owner ruling Q-13b-2 (card only, no implementation) · **Source:**
REPLACE-LINEAR-1 step 1's disclosed residue
([replace-linear-1-ledger.md](../../ledgers/staging/replace-linear-1-ledger.md)).

## Why

PySpark 4.1.2's `DataFrame.replace(to_replace, value=_NoValue, subset=None)` refuses a non-dict `to_replace` without
`value`, raising `PySparkTypeError [ARGUMENT_REQUIRED]`. repark's signature defaults `value` to `None`, so `replace(1)`
is read as "replace 1 with NULL" and silently nulls matching cells. The owner ruled on 2026-09-14 that REPLACE-LINEAR-1
keeps today's behaviour and that the fix gets its own card, because it changes the public signature's default.

## Decisions (proposed)

| id | decision |
|---|---|
| D-1 | A module-private sentinel becomes `value`'s default. A non-dict `to_replace` with the sentinel raises `PySparkTypeError` errorClass `ARGUMENT_REQUIRED` with PySpark's message parameters. An explicit `value=None` keeps replacing with NULL. |
| D-2 | Measure first: the live PySpark 4.1.2 answers for `replace(1)`, `replace(1, None)`, `replace([1, 2])`, `replace({1: 2})` and `replace(1, subset=["x"])`, pinned before the change. |
| D-3 | The signature snapshot and the API-surface census that record `replace`'s defaults move in the same PR as declared deltas. |

## Steps

| step | worker | what |
|---|---|---|
| 0 | Devin | D-2 oracle cells (live Spark, box alone) and red-first pins. |
| 1 | Devin | D-1 sentinel, D-3 snapshot deltas, pins green. |

**Home:** `python/repark/src/repark/spark/dataframe/replace_expr.py`, `dataframe/core.py` (`replace` signature only),
`python/repark/tests/test_replace_linear_1.py` or a sibling pin file. **Gates:** `make verify`, `make preflight`, the
parity suite; Grok critic-logic before the PR.

## Pointers

- Up: [map.md](map.md)
