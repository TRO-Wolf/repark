# task/wo/followup-dml-conjunct-and-signed-zero/evidence/ — the follow-up probe and its tables

- [probe_followup.py](probe_followup.py) — the measurement script. `--table conjunct` or `--table signed-zero` selects the axis; one run is one leg; `--render` writes that axis's table from the leg files. Measurement only.
- [raw/](raw/) — one JSONL file per engine leg and axis: `spark-*.jsonl` (the Spark 4.1.2 oracle) and `repark-main-*.jsonl` (the release wheel built from `fdc61f26`). The first line of each file is the leg header.
- [conjunct-table.md](conjunct-table.md) — Table A: the DML conjunct drop, one row per RePark cell, with label totals and one line per finding cell.
- [signed-zero-table.md](signed-zero-table.md) — Table B: signed zero, the Spark result against the RePark result per operation, the DML writes and one line per finding cell.
- [provenance.md](provenance.md) — wheel and jar hashes, versions, session settings, the commands run, the label rules and the deviations from the brief.

Up: [../map.md](../map.md).
