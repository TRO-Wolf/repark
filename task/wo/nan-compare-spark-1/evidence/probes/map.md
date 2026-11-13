# task/wo/nan-compare-spark-1/evidence/probes/ — the runners

Comment-free Python; the commands and the environment are in [../map.md](../map.md).

- [common.py](common.py) — the rows (two NaN rows per column, `±Infinity`, `±0.0`, NULL, `1.5`, `-2.5`), the
  four NaN bit patterns per width (`D_BITS`, `F_BITS`), the two file layouts (`l1` one file, `l5` five files),
  the partition rows, value tokens (a NaN is `"NaN"` whatever its bits; its bits go to `nan_bits`), the jsonl
  writer.
- [write_sources.py](write_sources.py) — writes the stored sources: the pyarrow Parquet files first (the only
  writer that takes the payload bits as given), then, from them, each engine's Iceberg tables (Parquet, ORC,
  Avro; `l1`, `l5`; one partitioned by `pk`) and its plain Parquet files.
- [stats.py](stats.py) — reads every table's `files` and `manifests` metadata tables on Spark (bounds, NaN
  counts, partition summaries) and every Parquet footer with pyarrow.
- [ops.py](ops.py) — the operations: 53 SQL templates (aggregate, window, `greatest`/`least`, sort, TopK,
  comparisons with literals and columns, `CASE`, arrays, keys), the five mixed-shape cells, the DataFrame API
  and `F.expr` builders, and the ten NaN producers.
- [cells.py](cells.py) — runs every source × column × operation on one engine and ANSI mode: the Spark door,
  the DataFrame API, `F.expr` and (RePark) the native door, with `EXPLAIN ANALYZE` scan lines for the pruning
  operations on the many-file and partitioned sources.
- [analyze.py](analyze.py) — pairs every RePark cell with Spark's cell on the same door and source ("exact") and
  with Spark's in-memory cell ("semantic"), and writes the verdicts.
- [render.py](render.py) — the tables in [../../domain-table.md](../../domain-table.md).
- [critic_cells.py](critic_cells.py) — after the critic: `write` (pyarrow files `pa_grp`, `pa_const`,
  `pa_const48`, `pa_dyn`, `pa_part`; the `*_grp_*` tables from both engines; base's `rb_meta_*` tables) and
  `cells` (grouped `max`/`min`, the constant-column and round-trip reads, dynamic filters on and off,
  quantiles, function equality, struct leaves, the metadata DELETE) on one engine.
- [critic_table.py](critic_table.py) — the domain table's §9 rows.
- [ingest_probe.py](ingest_probe.py) — `spark.read.parquet(pa_const).writeTo(<Iceberg table>)` on RePark,
  read back by both engines.

Up: [../map.md](../map.md).
