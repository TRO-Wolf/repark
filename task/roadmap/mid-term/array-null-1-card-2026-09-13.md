# Card ARRAY-NULL-1 — null-preserving `array_append` / `array_prepend` with a linear plan

**Date:** 2026-09-13 · **Authored by:** run 12b under G-5 · **Source:** ABS-EXPR-1's C-004 audit residue
([abs-expr-1-ledger.md](../../ledgers/staging/abs-expr-1-ledger.md), row "`F.array_append` / `F.array_prepend`").

## Why

`F.array_append` and `F.array_prepend` go through `_glue_element`
(`python/repark/src/repark/spark/functions_collections.py`), which builds
`when(isnull(arr), NULL).otherwise(flatten(array(arr, array(x))))`. Each level embeds the array column twice, so a
chain of N appends references it 2^N times. ABS-EXPR-1 measured ~×2.2 memory per level: 19 MB at depth 12, 66 MB at
14. The facade composes the CASE because DataFusion's `array_append` / `array_prepend` drop the input's null buffer
(NULL array → `[x]`), while Spark answers NULL.

## Decisions

| id | decision |
|---|---|
| D-1 | A null-preserving native arm, one call per level. Step 0 measures the smallest correct route first: (a) a `ScalarUDF` in the Rust function dispatch that calls DataFusion's `array_append`/`array_prepend` kernel and then applies the input array's null buffer to the result; (b) a Rust-built CASE that references the child once through a plan-level alias. Take the first route that is correct on every D-2 cell and linear at depth 40. If neither is linear, HALT with the measurement. |
| D-2 | Spark 4.1.2 oracle cells, pinned before the change: NULL array, NULL element (append NULL to a non-null array), empty array, nested arrays (append an array to an array of arrays), element type coercion (int element into a bigint array, int into a double array, string into an int array: refusal or cast as Spark does), a NULL element into an array whose element type is not nullable, and both functions on a literal array. |
| D-3 | The SQL door: if `array_append`/`array_prepend` resolve on the SQL door to the same unpatched kernel, record the NULL-array divergence in `EXPECTED_DIVERGENCES` with its measured reason or route the door through the same arm. Choose by step 0's measurement; the door-parity ratchet must stay green. |
| D-4 | No new public name; `functions_collections.py` and the Rust dispatch files keep or lower their size ceilings. |

## Steps

| step | worker | what |
|---|---|---|
| 0 | Devin | Measure: D-2 oracle cells (live Spark, box alone, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`), today's facade answers, the route (a)/(b) spike numbers at depth 12/40, and the red-first depth-40 memory pin in a subprocess under `RLIMIT_AS` (S2-8 idiom), in the style of `python/repark/tests/test_abs_expr_1.py`. |
| 1 | Devin | The chosen D-1 arm, answer pins, memory pin green, door-parity row, COVERAGE_ATTESTATION. |

**Home:** `python/repark/src/repark/spark/functions_collections.py`, the Rust function dispatch under
`crates/repark-python/src/column/` (or `crates/repark-functions/` for a UDF), `door_parity_tests.rs`,
`python/repark/tests/test_array_null_1.py`. **Gates:** new and existing `array_append`/`array_prepend` pins,
`make verify`, `make preflight`, the parity suite; S2-21 Rust and Python reviewers and Grok critic-logic before the PR.

## Pointers

- Up: [map.md](map.md)
