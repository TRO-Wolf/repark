# task/wo/empty-dml-shared-guard-1/evidence/ — the Step 0.3 probe and its tables

- [probe.py](probe.py) — the measurement script. One engine leg per run writes a leg header and one JSON line per cell; `--render` writes the two tables, the label counts and the detail lines. Measurement only.
- [raw/spark.jsonl](raw/spark.jsonl) — the Spark 4.1.2 oracle leg (87 cells).
- [raw/repark-main-fdc61f26.jsonl](raw/repark-main-fdc61f26.jsonl) — the release wheel built from `fdc61f26` (150 cells).
- [raw/repark-pypi-1-5-5.jsonl](raw/repark-pypi-1-5-5.jsonl) — the PyPI `repark==1.5.5` wheel (150 cells).
- [empty-dml-table.md](empty-dml-table.md) — matrix 1, the empty-DML cells, one row per cell and door.
- [struct-table.md](struct-table.md) — matrix 2, the struct comparison and UNION pairing cells.
- [provenance.md](provenance.md) — the wheel and jar hashes, the versions, the commands run, and the deviations from the brief.

Up: [../map.md](../map.md).
