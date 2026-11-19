# task/wo/sum-dec-i128wrap-1/evidence/built/compare/ — main against head

Written by `../compare_bar.py ../../raw <analysis dir> .`.

- `compare.md` — transitions (run, ANSI, main → head), the head's verdicts by family, the cells left by named class,
  and the 46 EQUAL→other cells.
- `moved.jsonl` — every cell whose verdict moved (1,329); `left.jsonl` — every non-EQUAL cell on the head with its
  class and both answers; `target.md` — per family and mode, silent and good cells on main and head.
- `pins-on-main.txt` — `test_sum_dec_i128wrap_1.py` run on main's package: 138 failed, 18 passed (the controls).

Up: [../map.md](../map.md).
