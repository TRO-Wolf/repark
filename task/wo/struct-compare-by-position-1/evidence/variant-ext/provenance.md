# Provenance

- Model: claude-haiku-5-5 (confirmed from the environment before the first action).
- Brief: `<scratch>/briefs/haiku-cmp-variant-ext.md`.
- Read: the Unit C park memo (Q2, Q4) in the owner's AgentLessons folder (`<agentlessons>/Unit C park memo 2026-11-09.md`); the variant table's `notes.md`, `provenance.md`, `variant_probe.py` and `table.md` (`<scratch>/variant-table/`); the step-0 comparison table's `notes.md`, `gen_cmp.py`, `drive_cmp.py` and `table.md` (`<scratch>/cmp-step0-table/`).
- Probe lineage: `<scratch>/variant-table/variant_probe.py` copied to `<scratch>/cmp-variant-ext/` and rewritten there as `probe_cmp_variant.py` (`--leg spark|base`, `--render`). The original was not edited.
- Spark leg: `<c26-oracle>/bin/python` (Python 3.12, pyspark 4.1.2, pyarrow installed in that venv; nothing installed into it). JVM `<zulu-17>/bin/java` via `JAVA_HOME`. Iceberg runtime `org.apache.iceberg_iceberg-spark-runtime-4.1_2.13-1.11.0.jar` from `<ivy2>/jars/`. Spark session `local[1]`, ANSI on, hadoop catalog `ice` in `<scratch>/cmp-variant-ext/work/spark/wh`.
- Base leg: `<pypi-156-venv>/bin/python`, RePark 1.5.6 (the PyPI wheel), polars and pandas already installed there. Warehouse and memory catalogs under `<scratch>/cmp-variant-ext/work/base`.
- Runs: each leg was a `systemd-run --user --wait --slice=repark.slice` unit. The Spark leg ran under `flock <spark-jvm-lock>`. Exit codes were 0 for both legs. Logs: `logs/base.log`, `logs/spark.log`, exit files `logs/base.exit` and `logs/spark.exit`. `PYTHONDONTWRITEBYTECODE=1` was set for the base leg. The first full run (with whole-map depth cells) was superseded by the final run and its raw files were overwritten.
- Live repository: not touched. No clone was edited, built or committed to. Nothing was committed.
