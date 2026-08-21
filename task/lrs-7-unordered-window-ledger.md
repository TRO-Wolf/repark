# Unit ledger — LRS-7 · the default frame for an unordered window

**Date:** 2026-08-20 · **Branch:** `fix/low-risk-sweep` · **Charter:**
[lrs-0-charter-ledger.md](lrs-0-charter-ledger.md) · **Found by:**
[lrs-1-higher-order-refusals-ledger.md](lrs-1-higher-order-refusals-ledger.md)

## Where it came from

Not from any finding. LRS-1 added a refusal to the shared `partitionBy` / `orderBy` normalizer, and
the pin written to prove the refusal did not over-fire — "an ordinary column still passes this
path" — failed. `count(v).over(Window.partitionBy("k"))` was broken for **every** column:

```
PySparkException: datafusion engine error: type_coercion
  caused by Internal error: ORDER BY column cannot be empty.
  This issue was likely caused by a bug in DataFusion's code.
```

That is ordinary PySpark, and Spark answers `[2, 2, 1]`. The pin that found it exists only to bound
a different fix.

## Why it happened

Spark documents two frame defaults: an **ordered** window frames `RANGE BETWEEN UNBOUNDED PRECEDING
AND CURRENT ROW`, an **unordered** one frames `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING`. DataFusion supplies the first and has no answer for the second — its RANGE default needs
an ordering column, does not find one, and fails at physical planning.

## What changed, and the part that nearly went wrong

`PyColumn::over` now supplies Spark's unordered default when there is no ordering and no explicit
frame. The first version of that fix was **wrong**, and the oracle caught it: with the frame applied
unconditionally, `lag`, `lead`, `nth_value`, `percent_rank` and `cume_dist` started returning
answers over an unordered window — and **Spark refuses all five**. The fix would have traded an
internal error for five silent divergences.

Measured, both sides, eleven functions:

| | Spark | repark before | repark after |
|---|---|---|---|
| `count` / `sum` / `max` | `[2,2,1]` / `[30,30,30]` / `[20,20,30]` | internal error | same as Spark |
| `first` / `last` | `[10,10,30]` / `[20,20,30]` | internal error | same as Spark |
| `row_number` `rank` `dense_rank` `ntile` | "requires window to be ordered" | internal error | same message |
| `percent_rank` `cume_dist` `lag` `lead` `nth_value` | "requires window to be ordered" | internal error | same message |

**The split is read off the function's kind, not a name list.** Spark's ordering-requiring set is
exactly the window UDFs; `first` / `last`, which Spark allows, arrive through the aggregate path.
So `unordered_window_frame` returns the frame for an aggregate and the error for a window UDF, and
a function added later lands on the correct side without anyone maintaining a list. The message is
the one DataFusion and Spark both use, so the four DataFusion already caught keep their wording.

## Evidence

- 18 pins in `python/repark/tests/test_lrs7_unordered_window.py`. Against the base with the two
  source files stashed: **12 failed, 6 passed**. Every expected value is Spark's answer for the same
  query, taken from the oracle — not read back from repark.
- One pin guards the interaction with the round-2 unsigned-count fix: `approx_count_distinct` over
  an unordered window must still come back `int64`, because `over` peels and re-applies a CAST and
  this unit changed the code between those two steps.
- `cargo test --workspace` — 45 binaries, **1,990 passed, 0 failed**.
- facade — **3,582 passed, 70 skipped, 0 failed** (3,564 before, +18). `make ci` exit 0.

## Disposition

**DELIVERED.** Charter C-001 held: every shape this unit changed was failing with an internal error
before it, including the five that now refuse.
