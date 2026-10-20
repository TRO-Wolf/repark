# Column `OR` build cost — COLUMN-OR-BUILD-1 baseline (2026-10-19)

What an n-term `OR` costs to *build* with `|` over `Column` objects on the Python door, against
what the engine then spends refusing it. Measurement only; no product change. Found by
DEEP-BATTERY-SPLIT-1 (#1115): once every deep-battery shape ran in its own interpreter, the
facade suite's floor was one test,
`python/repark/tests/sql/test_deep_subquery_expression_1.py::test_twenty_thousand_term_or_refuses_through_filter`,
and this page says which phase owns its wall.

This file closes when a COLUMN-OR-BUILD-1 unit merges, or when the owner closes the candidate
row in [STATUS.md](../../STATUS.md).

## Machine and profile

| key | value |
|---|---|
| date | 2026-10-19, load average 2.95 at the end of the run, `taskset -c 0-25` |
| cpu | AMD Ryzen Threadripper 3970X 32-Core (64 threads) |
| ram | 125 GiB (`MemTotal` 131760120 kB) |
| kernel | Linux 6.8.0-146-generic |
| python | 3.12.3 |
| native | `python/repark/src/repark/_native.abi3.so`, 361,497,528 B, `maturin develop --profile ci` |
| release proof | `repark._native.__debug_assertions__ is True` (the `ci` profile inherits `dev`; disclosed) |
| tree | `210ee778` (`main` after #1115) |
| fixture | `session.createDataFrame([(i, i * 2) for i in range(50)], "a INT, b INT")` |
| expression | `reduce(lambda x, y: x \| y, [F.col("a") == i for i in range(n)])`, the battery's own spelling |
| timer | `time.perf_counter()` around the `reduce` (build) and around `base.filter(expr).count()` (run); one process, the four sizes in order |

## Cells

| n terms | build s | `filter().count()` s | verdict | build ratio to the previous row |
|---|---|---|---|---|
| 2,500 | 2.277 | 0.011 | `AnalysisException` | — |
| 5,000 | 11.062 | 0.029 | `AnalysisException` | 4.86 |
| 10,000 | 61.189 | 0.068 | `AnalysisException` | 5.53 |
| 20,000 | 302.496 | 0.137 | `AnalysisException` | 4.94 |

Doubling n multiplies the build by about five, so the fitted exponent over the four points is
2.35 (log-log, 2,500 to 20,000). The refusal is linear and under 0.2 s at every size; the
depth guard fires at 2,500 terms already, so the 20,000-term pin exercises nothing in the engine
that the 2,500-term shape does not. The suite spends 302 s of its 355 s floor in Python string
work before the engine sees the plan.

## Suspected owner

`Column._binary` in `python/repark/src/repark/spark/column.py` hands
`_native.PyColumnParts.binary` the left side's three rendered parts (`spark_wrap_display_part`,
`sql_expr_part`, `join_sql_part`) and receives three new ones. Each part is the rendered text of
the whole subtree, so at level k the call copies text of length proportional to k, and a
left-deep chain of n levels copies about n²/2 characters per part. That is the quadratic.

## Acceptance experiment

1. `py-spy record` (or `cProfile` with the native frames collapsed) on the 10,000-term build:
   the hypothesis holds if the time is in `PyColumnParts.binary` and the string composition
   behind it, not in `_reject_nested_generator` or `Column.__init__`.
2. Measure the three parts' lengths at n = 2,500 and 5,000: a doubling of n that doubles the
   lengths confirms the rendered parts carry the whole subtree.
3. A fix that renders the parts lazily (on first read, from the native expression) or that
   caches the left side's rendering by identity must leave every cell of the deep batteries
   (`python/repark/tests/sql/test_deep_*.py`) at the same value and error class, and bring the
   20,000-term build under the 2,500-term build's current 2.3 s times eight (linear budget).
   Spark's own `Column.__or__` is a constant-time wrapper construction; the comparison is
   PySpark 4.1.2 on this box.

## Pointers

- Up: [map.md](map.md)
- The finding's origin: [../../task/wo/ci-2-slow-test-inputs.md](../../task/wo/ci-2-slow-test-inputs.md) (superseded 2026-10-19) and the rundown [../artifacts/test-suite-rundown-2026-10-19.html](../artifacts/test-suite-rundown-2026-10-19.html)
- The candidate row: [../../STATUS.md](../../STATUS.md) "Performance campaign"
