# Provenance — NATIVE-CTAS-ICEBERG-REFUSE-1 step 0

- **RePark:** main `a3f8c7d92eabfa84a543e11fb602fdcfbdb1b464` (2026-11-12), built with
  `maturin develop --profile release` (maturin 1.14.1), Python 3.12.3.
- **Spark:** PySpark 4.1.2, `iceberg-spark-runtime-4.1_2.13` 1.11.0, Java 17.0.15 (Zulu),
  `local[2]`, a Hadoop catalog `hd`.
- **RePark doors:** `native` is `repark.sql()` over `PyReparkSession.native(config=…)` with a
  `type=hadoop` catalog `hd` from the build config and a memory catalog `ice` from
  `register_memory_catalog`; the default-catalog cells use a second native session. `spark-door`
  is `ReparkSession.sql` over the same `type=hadoop` block.
- **Data:** `src = {1, 1, 2, 3}` and `src2 = {2, 3, 4}` (`id INT`) in each Iceberg catalog; the
  in-memory targets read `ice.ns.src`. An existing table holds `{7, 8}`. The external-table
  cells point at one Parquet file `{5, 6}` under the `ice` warehouse.
- **Commands:** `cells.py native <warehouse> native-door.jsonl`, `cells.py spark-door <warehouse>
  spark-door.jsonl`, `cells.py spark <warehouse> spark.jsonl` (with `SPARK_ICEBERG_JAR` set),
  then `render.py native-door.jsonl spark-door.jsonl spark.jsonl "<title>" > table.md`. The
  warehouse path in the committed jsonl is replaced by `<warehouse>`. The committed `cells.py` is
  the measured script after `ruff format` and two string splits; the measuring logic is unchanged.
- **Run note:** the first native run's `INSERT … SELECT … UNION SELECT 9` control failed with
  DataFusion's `cannot convert text Int64 into byte Int32` (an `INT` column against a `BIGINT`
  literal in a `UNION`), unrelated to CTAS; the control was changed to a union of the two `INT`
  sources and the native run repeated. The committed jsonl is the second run.
