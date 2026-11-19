# task/wo/sum-dec-i128wrap-1/evidence/built/delta3/det/ — the verifier's determinism cells on delta 3's head

The delta 2 verifier's runner, unchanged (`S/verify-sum/det/det.py` and its `table.py`, not copied here): four-file
Parquet tables at `(10,2)`, `(19,0)`, `(20,0)`, `(28,4)`, `(38,0)` and `(38,10)`, twelve scenarios per column, 240
queries per mode, 20 runs each, the `SET` before `read.parquet`; five configurations (work stealing on at
`target_partitions` 2, 4 and 8, off at 4, and on at 4 with `batch_size` 8), both modes, on cores 48–57.

- `head.jsonl` — every query's answers on `pkg-sum-845766fd` (2,400 lines; the scratch directory written as
  `<scratchpad>`).
- `table.md` — the verifier's `table.py` over it: 0 queries with more than one answer and 0 inexact in every
  configuration and mode; the 13 per row set apart are the verifier's own exclusions (12 `GROUP BY ()`, which both
  builds refuse, and its `c38_10 * 1` model error).

Up: [../map.md](../map.md).
