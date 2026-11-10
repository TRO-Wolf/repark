# Cost: plan and run, main `45018a2c` against head `adaa27494`

**Gate:** ≤ 1.05x median on each case. **Result:** both cases pass.

| case | what | main median (s) | head median (s) | head / main |
|---|---|---|---|---|
| (a) | the 2796 step-0 `sql`-door statements on the facade, in one process: the sum of each statement's execute time | 24.197 | 23.050 | **0.9526** |
| (b) | planning only: 200 `EXPLAIN`s, after 10 warm-up runs, of a 300-column projection over a struct-free 300-column view; 50 of the projected columns are comparisons (`c{i} = c{i+1}`) | 23.983 | 24.315 | **1.0139** |

The runs, in seconds, in the order they ran:

| case | main run 1, 2, 3 | head run 1, 2, 3 |
|---|---|---|
| (a) | 24.197, 24.077, 24.520 | 23.309, 23.050, 22.936 |
| (b) | 24.041, 23.983, 23.970 | 24.357, 24.315, 23.981 |

## How it was measured

- **Builds.** Both are release builds made by `maturin develop --release` with the workspace's release profile
  (thin LTO, one codegen unit). Main was built in a scratch worktree of `45018a2c`, whose target directory was seeded
  from the head build so that only the workspace crates rebuilt. Each build was installed into its own venv.
  `_native.abi3.so` sha256: main `b54f086fbaceb96224c063576649ad3496ccf6309e1faa84d839c524f39a41a3`, head
  `65adf5915890e623eb657ea9217d490f59316499d6c722e0affe4ede8276530e`.
- **Runs.** One `repark.slice` unit ran all twelve processes back to back, with no build running on the box. The order
  was interleaved: run i = (a) main, (a) head, (b) main, (b) head, for i = 1, 2, 3. Each process used a fresh
  warehouse. Command: `<venv>/bin/python -I cost_probe.py --case <a|b> --label <leg>-run<i> --warehouse <warehouse>
  --out cost-raw.jsonl`.
- **Records.** `cost-raw.jsonl` holds one record per process, with the per-kind outcome counts for case (a).

## Reading

- **(b) is the rule's planning cost** on a wide struct-free plan: +1.4%. The rule walks every expression, finds no
  struct operand, and leaves the plan unchanged.
- **(a) is the end-to-end cost** on the clause's own statements. Head is faster, but not because it plans faster:
  - the outcome mix differs. Head refuses 835 statements (main 702) and answers 1961 (main 2094);
  - a refusal stops at planning, so the 553 statements that 1.5.6 and main accepted against Spark's refusal now cost
    less.

  The ratio therefore bounds the cost on these statements, and does not isolate the rule.
- **Case (a) times only the statement.** Each DML cell's fresh table and its seed are made before its timer starts.
