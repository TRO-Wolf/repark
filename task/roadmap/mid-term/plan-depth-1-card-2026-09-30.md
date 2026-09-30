# Card PLAN-DEPTH-1 — `count()` on a chain of 200 `filter` calls crashes the process

**Date:** 2026-09-30 · **Filed by:** a Claude session (claude-fable-5-1) on the owner's ruling of the same day (ATTR-ID-1 OD-3: file the #881 verifier's pre-existing findings as their own cards) · **Source:** the fourth re-verify of PR #881, finding RC5-5 (S3, `new_since_base: false`).

## What was measured

- `base = createDataFrame([(i, i*2) for i in range(50)], "a INT, b INT")`, then `f = f.filter(f"a >= {i % 7 - 10}")` 200 times, then `f.count()`: the Python process dies with SIGSEGV (`rc=139`) on main and on the #881 head. 150 filters pass; 200, 300 and 1000 all crash. String and Column filters, both `caseSensitive` settings.
- Spark 4.1.2 answers 50.
- The verifier's cause: recursion depth in the optimizer or physical planner over a deep `Filter` chain, not anything in #881.

## Why it is a card and not a fold

The crash is present on released main and is independent of the CASESENS work. It is a process kill, not an error class, so no test can pin it green today without a fix.

## Decisions (proposed)

| id | decision |
|---|---|
| D-1 | Measure the depth at which the crash starts on main, for `filter`, `select`, `withColumn` and `union` chains, and record it. |
| D-2 | Fix in Rust: either the recursive plan walks become iterative (DataFusion's `TreeNode` API already offers non-recursive rewrites for most passes), or planning and execution run on a thread with a larger stack. The thread option is the smaller change; the iterative option is the correct one where RePark's own passes recurse. |
| D-3 | Pin: a 1,000-deep filter chain answers Spark's count under both settings; red on main by process exit code, not by exception. |

## Done condition

A 1,000-deep chain of each of the four shapes in D-1 answers Spark on main; the measured depth limit, if any remains, is recorded in `docs/spark-sql-iceberg-parity.md`.
