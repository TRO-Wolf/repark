# task/wo/sum-dec-i128wrap-1/evidence/built/delta2/compare/ — the head against delta 1, main now and Spark local[4]

Each pairing is the sizing's `compare.py`: every cell whose verdict or answer moved, with its family and kind.

- `head-vs-delta1.jsonl`, [head-vs-delta1.txt](head-vs-delta1.txt) — the head against delta 1's head
  `pkg-sum-6d730107` (its verdicts are in `../../delta1/raw/head/`): 388 cells move, the same 388 as the sizing's
  prototype (i).
- `head-vs-main.jsonl`, [head-vs-main.txt](head-vs-main.txt) — the head against main now: 1,230 cells move.
- `main-vs-main.jsonl` — main now against main before #1361 (`pkg-view1-c54f5fc7`, `../../delta1/raw/main/`): 6 cells.
- `spread-moved.jsonl`, [spread.txt](spread.txt) — the head's 172 pass-back queries per mode against Spark 4.1.2's
  recorded `local[4]` runs (both split packings), beside delta 1's.
- [check-exact.txt](check-exact.txt) — the sizing's `check_exact.py` over every moved value: each is the exact total.

Up: [../map.md](../map.md).
