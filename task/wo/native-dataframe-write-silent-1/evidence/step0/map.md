# map — task/wo/native-dataframe-write-silent-1/evidence/step0

## Purpose

Step 0 of NATIVE-DATAFRAME-WRITE-SILENT-1 on main `1d9d2808`. It covers every DataFrame writer
verb on every target kind the native door reaches, with four frame shapes, and the same verbs on
RePark's Spark door. A diagnostic run collects the frame the writer drops. The table also holds
the partitioned-table reach probe and the measured workarounds.

## Contents

- [cells.py](cells.py) — the measuring script (`native` or `spark-door`;
  `CELLS_COLLECT_RETURNED=1` for the diagnostic).
- [render.py](render.py) — renders jsonl files into one table.
- [native-door.jsonl](native-door.jsonl), [spark-door.jsonl](spark-door.jsonl) — the raw cells.
- [native-door-collect-returned.jsonl](native-door-collect-returned.jsonl),
  [spark-door-collect-returned.jsonl](spark-door-collect-returned.jsonl) — the diagnostic cells.
- [table.md](table.md) — the rendered step 0 table.
- [table-collect-returned.md](table-collect-returned.md) — the rendered diagnostic table.
- [provenance.md](provenance.md) — the build, doors, data, commands and the diagnostic.

## Pointers

- Up: [evidence map](../map.md)
