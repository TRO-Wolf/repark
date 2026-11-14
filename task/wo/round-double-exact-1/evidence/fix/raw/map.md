# task/wo/round-double-exact-1/evidence/fix/raw/ — the build's committed output

- `moves-head.json` — written by [../moves.py](../moves.py). It counts every step 0 cell by its verdict on
  main and on the head (EQUAL or DIFF against Spark), lists every cell that was EQUAL and moved (none), and
  gives what still differs, by context, door, source and function.
- `facade-red-summary.txt`, `facade-green-summary.txt` — `test_round_double_exact_1.py` on main's package
  (262 failed, 30 passed, with the failing ids) and on the head's (292 passed).
- `rust-red-summary.txt` — `spark_math/round_tests.rs` against main's kernels: 5 failed, 1 passed (the pin
  of the unchanged INT and DECIMAL arms).
- `cost-samples.jsonl`, `cost-round.jsonl` — the alternating cost samples: wall time per case and build,
  and the 10M-row probe's per-query medians.
- `cost-inline-samples.jsonl`, `cost-inline-round.jsonl` — the same round rows on a build with `#[inline]` and
  `#[cold]` hints (not committed: no gain beyond noise).

Up: [../map.md](../map.md).
