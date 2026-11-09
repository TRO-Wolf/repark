# task/wo/struct-setop-reorder-refusal-1/evidence/ — the set-operation domain table

- [after-table.md](after-table.md) — the domain table re-rendered against the delta-2 head leg (`raw/repark-head-76d82c28.jsonl`), body-identical to the Item 1 render: EQUAL=12, BOTH-REFUSED=48, REFUSED-WHERE-SPARK-ACCEPTED=60, ORACLE-MISSING=80. Measurement only.
- [probe_setop.py](probe_setop.py) — the measurement script. One engine leg per run writes a leg header and one JSON line per cell; `--render` writes the domain table and the label counts. Measurement only.
- [raw/repark-head-56aeac59.jsonl](raw/repark-head-56aeac59.jsonl) — the release wheel built from Item 1 (`56aeac59`), the same 200 cells (20 answered, 176 refused, 4 setup-refused).
- [raw/repark-head-76d82c28.jsonl](raw/repark-head-76d82c28.jsonl) — the release wheel built from the delta-2 product head (`76d82c28`), the same 200 cells with the same outcomes.
- [raw/spark.jsonl](raw/spark.jsonl) — the Spark 4.1.2 oracle leg (200 cells; the 80 native-door cells are ORACLE-MISSING).
- [raw/repark-main-fdc61f26.jsonl](raw/repark-main-fdc61f26.jsonl) — the release wheel built from `fdc61f26` (200 cells).
- [setop-table.md](setop-table.md) — the domain table: one row per shape, operation, door and form, with each engine's pairing and the labels for the RePark cell.
- [provenance.md](provenance.md) — the wheel and jar hashes, the versions, the commands run, the shapes and their spellings, and the deviations from the brief.
- [delta2/delta2-table.md](delta2/delta2-table.md) — the clerk's measured table for the delta-2 rows (memo §6 rows 1–13 on Spark, v1.5.6 and the old head), with `raw/` and the probe beside it. Absolute paths scrubbed to placeholders.
- [delta2/probe_delta2.py](delta2/probe_delta2.py) — the measurement script for the delta-2 rows.
- [delta2/raw/spark.jsonl](delta2/raw/spark.jsonl), [delta2/raw/head.jsonl](delta2/raw/head.jsonl), [delta2/raw/base.jsonl](delta2/raw/base.jsonl) — the three measured legs.

Up: [../map.md](../map.md).
