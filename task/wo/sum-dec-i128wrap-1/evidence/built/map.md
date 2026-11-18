# task/wo/sum-dec-i128wrap-1/evidence/built/ — the build's bar, cost and records (2026-11-17)

The SUM-DEC-I128WRAP-1 build (product commit `a8016e75`, package `pkg-sum-a8016e75`): the bar against main and
Spark, the cost, the card's "Built" append, and the scripts that made them. The step 0's own evidence is one level up
and unchanged.

- [bar.md](bar.md) — the bar: the step 0's whole domain table re-run on the head, counts per verdict, every class of
  cell left, the 46 EQUAL→other layout cells the build halted on, the unchanged sets, the pin files.
- [cost.md](cost.md) — K1 to K5 and C-012, base against head with the control band, and the marker's K1 numbers.
- [card-append.md](card-append.md) — the "Built" append for the card (the card is not on this branch yet).
- [compare_bar.py](compare_bar.py) — pairs main's verdicts (`../raw/`) with the head's (`raw/`): transitions,
  moved cells, cells left by named class.
- [cost_table.py](cost_table.py) — prints the cost table from the runner's two files.
- [record_oracle.py](record_oracle.py), [merge_oracle.py](merge_oracle.py) — record the pin file's Spark oracle,
  one ANSI mode per run, and merge the two runs.
- [raw/](raw/map.md) — the head's compacted RePark cells, Spark's cross-reads of the head's tables, the head's
  verdicts.
- [compare/](compare/map.md) — the comparison's outputs and the pin file's result on main.
- [cost/](cost/map.md) — the cost runner's samples and per-case timings.

Up: [../map.md](../map.md) is the step 0's evidence map; this folder is linked from [../../map.md](../../map.md).
