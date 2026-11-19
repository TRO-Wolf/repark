# task/wo/sum-dec-i128wrap-1/evidence/built/delta1/compare/ — delta 1's comparisons

[../../compare_bar.py](../../compare_bar.py) over two verdict sets: `compare.md` (transitions, verdicts by family, the
cells left by named class, the EQUAL or BOTH_RAISE cells that left) and `moved.jsonl` per pair.

- `head-vs-main/` — the head against main `pkg-view1-c54f5fc7`, with `left.jsonl` (every non-EQUAL cell on the head
  and its class; the same for every pair).
- `head-vs-base/` — against the build's base `pkg-avg-27d6149f` (the step 0's raw).
- `head-vs-build/` — against the build's head `pkg-sum-a8016e75`.
- `main-vs-base/` — main against the build's base: the 9 cells where the two bases differ.

Up: [../map.md](../map.md).
