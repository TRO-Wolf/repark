# task/wo/signed-zero-const-compare-1/evidence/after/ — the table on the head build

- [table.md](table.md) — the step-0 table re-measured on the release build of the fix: the cell-by-cell moves from main (4,096 WRONG to EQUAL, no EQUAL cell moves) and the totals.
- [cost.md](cost.md) — base against head over five alternating runs: the C-012 set and a 200-predicate constant plan, every row at or below 1.05x.
- [const200.py](const200.py) — the 200-predicate cost probe (comment-free Python): `const200.py <build> <run> <out jsonl>`.
- [raw/](raw/map.md) — the head build's raw cells (gzip) and the joined verdicts.

Up: [../map.md](../map.md).
