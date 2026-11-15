# task/wo/decimal-double-union-coercion-1/evidence/step0b/cells/ — the joined cells and the counts

- [cells.jsonl](cells.jsonl) has 6,855 rows. Each row carries:
  - Spark's answer;
  - main `dc6b4c94`'s answer (`main`);
  - the recorded `ff34baf5` answer (`ff34`);
  - step 0's `6f9f2aaa` answer (`main_6f9f`);
  - each build's verdict;
  - the cell's part (`part 1`, `part 2 (seat V)`, `native (Q2)`, a card, or a control root).
- [summary.json](summary.json) has the counts per section and mode, and the wrong-on-main counts by part.
- [moved-since-6f9f.jsonl](moved-since-6f9f.jsonl) lists the cells whose answer on main moved since step 0's main.
- [head-summary.json](head-summary.json) has:
  - the emulated-head counts for the fuzz, the structured set and the DML cells;
  - the kernel checks;
  - Spark's answers for the widened forms.
- [head-wrong.jsonl](head-wrong.jsonl) lists the cells wrong on the `k1k2` head, the full part-1 emulation. It leaves
  out the ANSI-off `DECIMAL(5,0)` overflow block, which is wrong on every build and counted in the summary. It also
  lists every DML cell wrong on the `seat_e` and `k1k2` heads.

Up: [../map.md](../map.md).
