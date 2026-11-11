# Cost: main `68f70b0f` against this head

**Gate:** ≤ 1.05x median of three on each case. **Result:** all three cases pass.

| Case | What | Main median (s) | Head median (s) | Head / main |
|---|---|---|---|---|
| (a) | the family table's `sql`-door statements on the facade, in one process: 1039 queries (both case settings, `PREPARE`/`EXECUTE` included) and 192 DML cells (setup, statement and read-back); the sum of each cell's time | 19.623 | 18.716 | **0.9538** |
| (b) | #1292's planning-only probe, loaded from its `cost_probe.py`: 200 `EXPLAIN`s, after 10 warm-ups, of a 300-column struct-free projection with 50 comparisons | 23.872 | 24.142 | **1.0113** |
| (c) | join-heavy planning: 200 `EXPLAIN`s, after 10 warm-ups, of a 20-way `USING` join over struct-free `INT` keys (`uj{i}` joins `USING (a{i})`) | 11.301 | 11.318 | **1.0014** |

The runs, in seconds, in the order they ran:

| Case | Main run 1, 2, 3 | Head run 1, 2, 3 |
|---|---|---|
| (a) | 20.172, 19.623, 19.553 | 18.716, 19.028, 18.613 |
| (b) | 23.915, 23.857, 23.872 | 24.117, 24.142, 24.267 |
| (c) | 11.341, 11.269, 11.301 | 11.318, 11.268, 11.364 |

## How it was measured

- **Builds.** Both are release builds made by `maturin develop --profile release`: main in a scratch worktree of
  `68f70b0f`, head (`9d058674b`, product code of item 2's `e998bbb90`) in the lane clone. `_native.abi3.so` md5:
  main `439ba958…`, head `8937538f…`.
- **Runs.** One `repark.slice` unit, holding the build lock so no build ran, each process pinned to cores 32–47.
  The order interleaves: for each run, cases (a), (b) and (c), each main then head, each in a fresh process with a
  fresh warehouse. Both interpreters are CPython 3.12.3 with `-I`. `cost-raw.jsonl` holds the 18 records.
- **Case (c)'s shape.** The order names a 20-way `USING` join over struct-free keys. A chain sharing one key list
  (`USING (k, w)` at every step) refuses at four tables on main and on head with `AMBIGUOUS_REFERENCE`, where Spark
  answers (proposed card `USING-CHAIN-AMBIGUOUS-1`). So each step joins on its own key.
