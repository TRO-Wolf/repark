# task/wo/write-path-envelope-matrix-1/evidence/ — the envelope matrix probe and its tables

- [probe_envelope.py](probe_envelope.py) — the measurement script. One run is one leg shard: it appends a leg header line and one JSON line per cell; `--render` reads the shards of both engines and writes the three markdown files below. Measurement only.
- [raw/](raw/) — one JSONL file per engine leg and shard: `spark-*.jsonl` (the Spark 4.1.2 oracle) and `repark-main-*.jsonl` (the release wheel built from `fdc61f26`). The first line of each file is the leg header.
- [envelope-table.md](envelope-table.md) — one row per RePark cell: predicate, door, seat, table kind, both outcomes, the door's own `SELECT` ids, snapshots added, labels.
- [summary.md](summary.md) — label counts per door and seat, and the non-`EQUAL` labels per predicate.
- [findings.md](findings.md) — one line per cell that carries a finding label.
- [provenance.md](provenance.md) — wheel and jar hashes, versions, session settings, the commands run, the label rules and the deviations from the brief.

Up: [../map.md](../map.md).
