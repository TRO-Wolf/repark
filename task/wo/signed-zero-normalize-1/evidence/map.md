# task/wo/signed-zero-normalize-1/evidence/ — the signed-zero domain table

Measurement only. The probe measures sketch section 9 cells D1 to D13 and D15 to D17 on the Spark 4.1.2 oracle and on
the release wheel built from `fdc61f26` (repark 1.5.5). Each raw file is one leg; the tables and the summary are
rendered from the raw files by the probe itself.

- [probe_signed_zero.py](probe_signed_zero.py) — the measurement script: `--part reads|dml|timing|spill|spill-facade|spill-native` selects a leg; `--render --raw-dir raw --out-dir .` writes `domain-table.md` and `summary.md`.
- [raw/](raw/) — one JSONL file per leg: `spark-signed-zero-*.jsonl` (the Spark oracle) and `repark-main-signed-zero-*.jsonl` (the release wheel). The first line of each file is the leg header.
- [domain-table.md](domain-table.md) — one row per RePark cell on the test layout, with the truth, Spark, RePark-main and label columns; generated.
- [summary.md](summary.md) — counts by label, row group and route; the findings list; the timing and spill sections; the legs; generated.
- [provenance.md](provenance.md) — wheel and jar hashes, versions, session settings, the commands run, the label rules and the deviations from the brief.

Up: [../map.md](../map.md).
