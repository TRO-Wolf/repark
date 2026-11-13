# Provenance — SQL-SETOP-ALL-REFUSE-1 step 0

- **RePark:** main `a3f8c7d92eabfa84a543e11fb602fdcfbdb1b464` (2026-11-12), built with
  `maturin develop --profile release` (maturin 1.14.1), Python 3.12.3.
- **Spark:** PySpark 4.1.2, `iceberg-spark-runtime-4.1_2.13` 1.11.0, Java 17.0.15 (Zulu), `local[2]`,
  a Hadoop catalog `ice`; the persistent-view cells use `spark_catalog.default`, because a Hadoop
  catalog has no views.
- **RePark doors:** `spark-door` is `ReparkSession.sql` over a memory catalog `ice`; `native` is
  `repark.sql()` over a `PyReparkSession.native()` session with a memory catalog `ice`.
- **Data:** `l = {1, 1, 1, 2, 2, 4}`, `r = {1, 2, 2, 3}` as temporary views (native: views), and the
  same rows in Iceberg tables `ice.ns.pl` / `ice.ns.pr`. This is Fable's data with a `4` added to `l`,
  so `EXCEPT` has a non-empty answer. Spark answers `INTERSECT ALL` with `1, 2, 2` and `EXCEPT ALL` /
  `MINUS ALL` with `1, 1, 4`.
- **Commands:** `cells.py spark <warehouse> spark.jsonl`, `cells.py spark-door <warehouse>
  spark-door.jsonl`, `cells.py native <warehouse> native-door.jsonl`, then `render.py spark.jsonl
  spark-door.jsonl native-door.jsonl "Step 0 on main" > table.md`. The committed `cells.py` and
  `render.py` are the measured scripts after `ruff format`; `render.py` also cuts a long error text at a word boundary (the typos gate reads the tables). The measuring logic is unchanged.
