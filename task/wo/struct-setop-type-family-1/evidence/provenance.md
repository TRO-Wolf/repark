# Provenance

- Model: claude-haiku-5-5 (confirmed from the environment before the first action).
- Brief: scratchpad/briefs/haiku-variant-table.md, including the orchestrator ruling on the prune (2026-11-09).
- Memo read: Unit C park memo 2026-11-09, sections 0, 1.1, 1.2, Q2, Q4, Q5, Q6 (in the owner's AgentLessons folder, path redacted).
- Probe lineage: the memo's variant_probe.py (copied by the first clerk), extended here with a full cell plan, `--leg spark|base|head` and `--render`. `plan_count.py` is the first clerk's plan, unchanged.
- Spark leg: `<c26-oracle>/bin/python`, Python 3.12.3, pyspark 4.1.2, pyarrow 25.0.1 (no pandas, no polars; nothing installed into it). JVM `<zulu-17>/bin/java`, Zulu 17 (`JAVA_HOME` set in the systemd unit). Iceberg runtime `org.apache.iceberg_iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, sha256 d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242, 47,959,591 bytes. Spark session `local[1]`, ANSI on, hadoop catalog `ice` in the run's `work/spark/wh`.
- Base leg: `<pypi-156-venv>/bin/python`, Python 3.12.3, repark 1.5.6 (the PyPI wheel), pyarrow 26.0.1, polars 1.43.2 and pandas 3.0.5 (installed with uv, pinned to head's versions; 2.1.0 was installed first and replaced).
- Head leg: `<verify-c1-muse>/.venv/bin/python`, Python 3.12.3, repark from the clone's `python/repark/src` at git HEAD b8ad7a9c (the version string 1.5.6 is not evidence of the commit), polars 1.43.2, pandas 3.0.5, pyarrow 25.0.1. The clone's untracked `dml_err.txt` is pre-existing and was not touched. `PYTHONDONTWRITEBYTECODE=1` was set in every leg's unit so the clone was not written to.
- Tools: systemd-run (units `variant-table-spark2`, `variant-table-base2`, `variant-table-head2`, under `repark.slice`), flock (`<spark-jvm-lock>`, held by the Spark unit for its whole run), uv.
- Runs: each leg writes `raw/<leg>.jsonl` and an exit file under `logs/`. The Spark warehouse and the repark work directories were cleared before the final runs. The final exit codes were 0 for all three legs.
