# Provenance — NATIVE-DATAFRAME-WRITE-SILENT-1 step 0

- **RePark:** main `1d9d2808`, a release build (`maturin develop --profile release`, maturin
  1.14.1) reused from an earlier unit's lane; Python 3.12.3. Between `1d9d2808` and the branch
  base `2707924e` only test and Markdown files changed (`git diff --stat 1d9d2808 2707924e --
  crates python` lists test files and maps only), so the product is the same.
- **No Spark oracle run.** The `expected` column is the script's model of Spark's semantics
  (`expected_rows` in [cells.py](cells.py)). RePark's own Spark door is measured beside it. The
  guard keys on what the native door does (returns, then stores nothing), not on the model.
- **Doors.** `native`: `repark.sql()` over `PyReparkSession.native(config=…)`, with a
  `type=hadoop` catalog `hd` from the config, a memory catalog `ice` from
  `register_memory_catalog`, and `CREATE SCHEMA ice.ms` (DataFusion's in-memory schema under the
  Iceberg catalog name). `spark-door`: `ReparkSession` over the same `type=hadoop` block, in the
  same warehouse. In `native` mode the script also opens the Spark door, only to create the
  partitioned pool for the reach probe.
- **Data.** Source `src = {(9, 1), (10, 3)}` and `src2 = {(11, 1)}` (`id INT, p INT`). Every
  existing target starts as `{(7, 1), (8, 2)}`. The frames are the source itself (`sql`),
  `filter("id >= 9")`, `select("id", "p")` and `union(src2)`. `writeTo(...).overwrite` uses
  `functions.expr("p = 1")`. An existing path target is seeded with one file of the target rows
  in the verb's format. The text verbs write `CAST(id AS STRING) AS value`.
- **Each cell** builds a fresh target. It records the call's outcome, the rows before and after,
  the latest metadata's snapshot count and the data and metadata file counts (for Iceberg), or
  the files and rows under the path. A temp-view or view target also records the rows of
  `spark_catalog.default.<name>`, the table a `saveAsTable` of that name writes.
- **Commands:**
  `cells.py native <warehouse> native-door.jsonl`, `cells.py spark-door <warehouse>
  spark-door.jsonl`, then the same two with `CELLS_COLLECT_RETURNED=1` into the
  `*-collect-returned.jsonl` files, then `render.py "<title>" <jsonl> <jsonl> > table.md` (and
  `table-collect-returned.md`). The warehouse path in the committed jsonl is replaced by
  `<warehouse>`. Each run took about seven seconds.
- **The diagnostic mode.** `CELLS_COLLECT_RETURNED=1` wraps
  `repark._native.session_sql_with_write_options` so the wrapper collects (`count()`) the frame it
  returns before the writer drops it. It shows that the dropped frame is the pending write: with
  it collected, every native SILENT and S1 cell stores the model's rows, except the two
  `insertInto(overwrite=True)` cells on in-memory tables, which then refuse loudly
  (`Insert Overwrite not implemented for MemoryTable yet`). The Spark door changes in no cell, so
  its router never returns a pending write.
- **After the runs**, `ruff format` reflowed `cells.py` and the `functions as F` import was
  renamed for `N812`. The measuring logic is unchanged.
