# task/wo/struct-setop-reorder-refusal-1/evidence/ — the set-operation domain table

- [probe_setop.py](probe_setop.py) — the measurement script. One engine leg per run writes a leg header and one JSON line per cell; `--render` writes the domain table and the label counts. Measurement only.
- [raw/spark.jsonl](raw/spark.jsonl) — the Spark 4.1.2 oracle leg (200 cells; the 80 native-door cells are ORACLE-MISSING).
- [raw/repark-main-fdc61f26.jsonl](raw/repark-main-fdc61f26.jsonl) — the release wheel built from `fdc61f26` (200 cells).
- [setop-table.md](setop-table.md) — the domain table: one row per shape, operation, door and form, with each engine's pairing and the labels for the RePark cell.
- [provenance.md](provenance.md) — the wheel and jar hashes, the versions, the commands run, the shapes and their spellings, and the deviations from the brief.

Up: [../map.md](../map.md).
