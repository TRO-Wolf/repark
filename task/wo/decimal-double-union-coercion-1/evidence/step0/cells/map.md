# task/wo/decimal-double-union-coercion-1/evidence/step0/cells/ — the joined step-0 cells

Written by [../probes/analyze_step0.py](../probes/analyze_step0.py) and
[../probes/analyze_widened.py](../probes/analyze_widened.py); one JSON object per line.

- `cells.jsonl` — every cell of the re-measured domain table, the DataFrame and `F.expr` cells and the DML
  cells: Spark's answer, main's and `ff34baf5`'s, each verdict, and the transition between the builds.
- `coerce-moves.jsonl` — the coercion-fuzz cells whose verdict differs between main and `ff34baf5`.
- `summary.json` — the counts behind [../table.md](../table.md).
- `widened-summary.json` — the emulated head (the widened forms on `ff34baf5`), its faithfulness check on Spark,
  and the DECIMAL→DOUBLE kernel counts.
- `replay-888-moves.jsonl` is not written when no #888 replay cell moves between the builds (none does).
- `widened-held-wrong.jsonl` — every cell the emulated head answers wrong although main or `ff34baf5` answers it
  as Spark, and every value whose DECIMAL→DOUBLE cast differs from Spark's.

Up: [../map.md](../map.md).
