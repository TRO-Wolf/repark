# Provenance

Placeholders: `<OUT>` = `<SCRATCH>/listview-step0-table`, `<SCRATCH>` = the session scratchpad, `<FAM_MAIN_WT>` = `<SCRATCH>/fam-main-wt` (main 68f70b0f, release, debug assertions off), `<SPARK_PY>` = the pinned PySpark oracle interpreter (Python 3.12.3, pyspark 4.1.2), `<ICEBERG_JAR>` = the Iceberg Spark runtime jar for Spark 4.1 (Iceberg 1.11.0), `<JAVA_HOME>` = the Zulu 17 JDK, `<LOCK>` = the shared Spark-JVM flock file, `<SYSTEM_PY>` = the system Python 3, `<LANE>` = `<SCRATCH>/lane-empty-dml-guard-1` (read only, via `git show`).

## Source material (read only)

- `git -C <LANE> show origin/main:task/wo/struct-compare-seat-family-1/evidence/after/listview.txt`
- `git -C <LANE> show origin/main:task/wo/struct-compare-seat-family-1/evidence/after/probe_listview.py`

The probe was extended, not copied. Nothing was checked out, edited, built or committed in any clone.

## Model

- `claude-haiku-5-5` (clerk), verified from the environment before the first action.

## Interpreters and sessions

- Spark leg: `<SPARK_PY> -I <OUT>/probe_listview_table.py --leg spark`, run under `flock <LOCK>` with `JAVA_HOME=<JAVA_HOME>` and `PYSPARK_PYTHON=<SPARK_PY>`, as a `repark.slice` unit. Spark config: `spark.jars=<ICEBERG_JAR>`, Iceberg extensions, catalog `ice` (hadoop, warehouse `<OUT>/warehouse-spark`), `local[4]`, 8 shuffle partitions.
- Main leg: `<FAM_MAIN_WT>/.venv/bin/python -I <OUT>/probe_listview_table.py --leg main`, as a `repark.slice` unit. No JVM. Catalog `ice` config keys set (hadoop, warehouse `<OUT>/warehouse-main`).
- Recorded meta (raw line 1): main `repark_file` = `<FAM_MAIN_WT>/python/repark/src/repark/__init__.py`, `debug_assertions` = false. Spark pyspark 4.1.2, spark_version 4.1.2.

## Render

- `<SYSTEM_PY> -I <OUT>/probe_listview_table.py --render` writes `table.md` and `explain_failing.md`.

## Files in `<OUT>`

- `probe_listview_table.py`: the probe (both legs, render).
- `raw/spark.jsonl` (300 records plus meta), `raw/main.jsonl` (326 records plus meta): one JSON line per cell.
- `table.md`: counts by label × key type, join form and join type; label by key type × form × join type; wrong list; cross-product list; labelled cell table for main; oracle cell table for Spark.
- `explain_failing.md`: `explain(True)` text for the captured `df_str` and `sql_on` inner base cells on main. Only the failing ones are listed for labels other than EQUAL.
- `notes.md`: judgement calls, oracle observations, run history, gaps.
- `logs/spark.log`, `logs/main.log`: run logs for the final legs.
- `spark.exit`, `main.exit`: exit codes of the final legs (both 0).
- `warehouse-spark/`, `warehouse-main/`: Iceberg warehouse scratch from the DML cells.
- Smoke leftovers, kept: `typecheck_main.py`, `smoke_main.log`, `smoke_spark.log`.

## Commands (final legs)

- `systemd-run --user --collect --slice=repark.slice --unit=lv-step0-spark-2 bash -c "... flock <LOCK> <SPARK_PY> -I <OUT>/probe_listview_table.py --leg spark > <OUT>/logs/spark.log; echo $? > <OUT>/spark.exit"`
- `systemd-run --user --collect --slice=repark.slice --unit=lv-step0-main-3 bash -c "<FAM_MAIN_WT>/.venv/bin/python -I <OUT>/probe_listview_table.py --leg main > <OUT>/logs/main.log; echo $? > <OUT>/main.exit"` (after `rm -rf <OUT>/warehouse-main`).
- `<SYSTEM_PY> -I <OUT>/probe_listview_table.py --render`.

## Nothing committed

No commit, push, branch or edit in any repo. The live repository was not touched.

## The committed copy (build item 1, 2026-11-11)

Build item 1 of LISTVIEW-INNER-JOIN-KEY-1 (`claude-opus-5-5`) committed this directory from `<OUT>`. What changed against the scratch run:

- **Paths.** Every absolute path is a placeholder above. `table.md` and `raw/main.jsonl` carried one each (the main leg's `repark_file`), now `<FAM_MAIN_WT>/...`.
- **The probe.** `probe_listview_table.py` takes the output directory as `--out` and the jar from the `ICEBERG_JAR` environment variable instead of two path literals. It also gains type annotations, the `fn` name for the functions module, and ruff formatting, so it passes `ruff check` and `ruff format --check` on its own. No cell, value or label rule changed. The committed table was rendered by the scratch copy, not re-rendered.
- **Not committed:** `logs/`, `main.exit`, `spark.exit`, both warehouses, and the smoke leftovers (`typecheck_main.py`, `smoke_main.log`, `smoke_spark.log`). Both exit files read `0`.
