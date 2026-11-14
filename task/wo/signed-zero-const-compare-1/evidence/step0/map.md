# task/wo/signed-zero-const-compare-1/evidence/step0/ — the step 0 domain table

Measured 2026-11-14 on the release build of main `d843d37e` (the lane venv, `maturin develop --profile release`)
and on PySpark 4.1.2 (the c26 oracle environment, Zulu JDK 17, `local[1]`, UTC session zone, a private hadoop
catalog per run). Every probe ran under the shared measure lock on cores 48–57, each Spark JVM under the Spark slot.

- [probe_const.py](probe_const.py) — the cell definitions and runner (comment-free Python): `--engine spark|repark --ansi true|false --part select|nan|context|dml --warehouse <dir> --out <jsonl> [--jar <iceberg jar>]`.
- [compare.py](compare.py) — joins each RePark cell with Spark's cell on the same door (the native and `filter-str` doors against Spark SQL, ANSI on) and prints the tables: `compare.py raw repark-main raw/cells.jsonl.gz`. An optional fourth argument names the Spark raw directory (default: the first argument); the head re-measure in `../after/` passes it.
- [plans.py](plans.py) — `EXPLAIN VERBOSE` of the headline cells on the Spark and native doors, and the DataFrame headline answer; [plans.txt](plans.txt) is its output on `d843d37e`.
- [table.md](table.md) — the generated table: totals per part, door and ANSI mode, wrong cells per operator, and every non-EQUAL DML cell.
- [raw/](raw/map.md) — the raw cells (gzip) and the joined verdicts.

Up: [../map.md](../map.md).
