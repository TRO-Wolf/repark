"""PERF-ICE-WRITEPATH-1 — the CTAS write node: file layout, determinism, and wall."""

import os
import statistics
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live write-path oracle is skipped (CI is JVM-free)"

CATALOG = "writepath_perf"
THREADS = "8"
SEED_ROWS = 120_000
SEED_FILES = 4
WALL_ROWS = 1_000_000
WALL_ITERATIONS = 3
CTAS_OVER_PARQUET_SINK_MAX = 2.2
PARTITIONED_OVER_PARQUET_SINK_MAX = 3.4


def _rows(start: int, stop: int) -> pa.Table:
    ids = pa.array(range(start, stop), type=pa.int64())
    parts = pa.array([index % 8 for index in range(start, stop)], type=pa.int32())
    labels = pa.array([f"r{index:07d}" for index in range(start, stop)], type=pa.string())
    return pa.table({"id": ids, "part": parts, "label": labels})


def _seed(path: Path, rows: int) -> Path:
    """One file, written once, so the layout the writers see is fixed."""
    import pyarrow.parquet as pq

    pq.write_table(_rows(0, rows), path, row_group_size=10_000)
    return path


def _seed_files(directory: Path, rows: int, files: int) -> Path:
    """A fixed `files`-way layout, written once: the plan then has `files` partitions."""
    import pyarrow.parquet as pq

    directory.mkdir(parents=True, exist_ok=True)
    per_file = rows // files
    for index in range(files):
        start = index * per_file
        stop = rows if index == files - 1 else start + per_file
        pq.write_table(_rows(start, stop), directory / f"part-{index}.parquet")
    return directory


def _session(name: str, warehouse: Path, cap: str | None = None) -> ReparkSession:
    builder = ReparkSession.builder.appName(name).config("spark.sql.shuffle.partitions", THREADS)
    if cap is not None:
        builder = builder.config("repark.write.max-concurrent-files", cap)
    engine = builder.getOrCreate()
    engine.register_memory_catalog(CATALOG, warehouse)
    engine.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
    return engine


def _file_counts(engine: ReparkSession, table: str) -> list[int]:
    files = engine.sql(f"SELECT record_count FROM {table}.files ORDER BY file_path").to_arrow()
    return [int(value.as_py()) for value in files.column("record_count")]


def _manifest_order(engine: ReparkSession, table: str) -> list[Any]:
    files = engine.sql(f"SELECT file_path, record_count FROM {table}.files").to_arrow()
    return [
        (Path(str(path.as_py())).name, int(count.as_py()))
        for path, count in zip(files.column("file_path"), files.column("record_count"), strict=True)
    ]


def test_ctas_writes_one_data_file_per_plan_partition(tmp_path: Path) -> None:
    """C-004: the CTAS write node writes one data file per DataFusion partition."""
    source = _seed_files(tmp_path / "seed", SEED_ROWS, SEED_FILES)
    engine = _session("writepath-file-count", tmp_path / "wh")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        engine.sql(f"CREATE TABLE {CATALOG}.w.many USING iceberg AS SELECT * FROM src").collect()
        counts = _file_counts(engine, f"{CATALOG}.w.many")
        assert len(counts) == SEED_FILES, counts
        assert sum(counts) == SEED_ROWS
        rows = engine.sql(f"SELECT sum(id) AS s, count(*) AS n FROM {CATALOG}.w.many").to_arrow()
        assert rows.column("n")[0].as_py() == SEED_ROWS
        assert rows.column("s")[0].as_py() == SEED_ROWS * (SEED_ROWS - 1) // 2
    finally:
        engine.stop()


def test_one_concurrent_file_still_writes_exactly_one(tmp_path: Path) -> None:
    """C-004: `repark.write.max-concurrent-files = 1` still writes a single data file."""
    source = _seed_files(tmp_path / "seed", SEED_ROWS, SEED_FILES)
    engine = _session("writepath-one-file", tmp_path / "wh", cap="1")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        engine.sql(f"CREATE TABLE {CATALOG}.w.one USING iceberg AS SELECT * FROM src").collect()
        counts = _file_counts(engine, f"{CATALOG}.w.one")
        assert len(counts) == 1, counts
        assert counts[0] == SEED_ROWS
        rows = engine.sql(f"SELECT sum(id) AS s FROM {CATALOG}.w.one").to_arrow()
        assert rows.column("s")[0].as_py() == SEED_ROWS * (SEED_ROWS - 1) // 2
    finally:
        engine.stop()


def test_repeated_ctas_writes_the_same_files_in_the_same_order(tmp_path: Path) -> None:
    """C-005: the parallel section is followed by a deterministic file order."""
    source = _seed_files(tmp_path / "seed", SEED_ROWS, SEED_FILES)
    engine = _session("writepath-determinism", tmp_path / "wh")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        orders = []
        for run in range(3):
            table = f"{CATALOG}.w.run{run}"
            engine.sql(f"CREATE TABLE {table} USING iceberg AS SELECT * FROM src").collect()
            orders.append([count for _, count in _manifest_order(engine, table)])
        assert orders[0] == orders[1] == orders[2], orders
        assert orders[0] == [SEED_ROWS // SEED_FILES] * SEED_FILES, orders[0]
    finally:
        engine.stop()


def test_partitioned_ctas_files_ascend_by_partition_value(tmp_path: Path) -> None:
    """C-006: V3-11 — one commit's data files reach the manifest in ascending partition order."""
    source = _seed_files(tmp_path / "seed", SEED_ROWS, SEED_FILES)
    engine = _session("writepath-partition-order", tmp_path / "wh")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        engine.sql(
            f"CREATE TABLE {CATALOG}.w.part USING iceberg PARTITIONED BY (part) "
            "AS SELECT * FROM src"
        ).collect()
        files = engine.sql(
            f"SELECT partition.part AS p, record_count FROM {CATALOG}.w.part.files"
        ).to_arrow()
        values = [int(value.as_py()) for value in files.column("p")]
        assert values == sorted(values), values
        assert set(values) == set(range(8)), values
        counts = engine.sql(f"SELECT count(*) AS n FROM {CATALOG}.w.part").to_arrow()
        assert counts.column("n")[0].as_py() == SEED_ROWS
    finally:
        engine.stop()


def _median_ms(fn: Any, iterations: int) -> float:
    fn(-1)
    samples = []
    for index in range(iterations):
        started = time.perf_counter()
        fn(index)
        samples.append((time.perf_counter() - started) * 1000.0)
    return statistics.median(samples)


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_ctas_wall_is_within_the_parquet_sink_ratio(tmp_path: Path) -> None:
    """C-007: the CTAS wall is read against the DataFusion parquet sink measured in the same run."""
    import shutil

    source = _seed(tmp_path / "wall.parquet", WALL_ROWS)
    engine = _session("writepath-wall", tmp_path / "wh")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")

        def ctas(index: int) -> None:
            engine.sql(f"DROP TABLE IF EXISTS {CATALOG}.w.wall{index}")
            engine.sql(
                f"CREATE TABLE {CATALOG}.w.wall{index} USING iceberg AS SELECT * FROM src"
            ).collect()

        def ctas_part(index: int) -> None:
            engine.sql(f"DROP TABLE IF EXISTS {CATALOG}.w.wallp{index}")
            engine.sql(
                f"CREATE TABLE {CATALOG}.w.wallp{index} USING iceberg PARTITIONED BY (part) "
                "AS SELECT * FROM src"
            ).collect()

        def sink(index: int) -> None:
            out = tmp_path / f"sink{index}"
            shutil.rmtree(out, ignore_errors=True)
            engine.sql("SELECT * FROM src").write.parquet(
                str(out), mode="overwrite", compression="zstd"
            )

        control = _median_ms(sink, WALL_ITERATIONS)
        plain = _median_ms(ctas, WALL_ITERATIONS)
        partitioned = _median_ms(ctas_part, WALL_ITERATIONS)

        assert plain < control * CTAS_OVER_PARQUET_SINK_MAX, (
            f"CTAS {plain:.0f} ms vs the parquet sink {control:.0f} ms in the same run"
        )
        assert partitioned < control * PARTITIONED_OVER_PARQUET_SINK_MAX, (
            f"partitioned CTAS {partitioned:.0f} ms vs the parquet sink {control:.0f} ms"
        )
    finally:
        engine.stop()


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_written_table_row_set_matches_spark(tmp_path: Path) -> None:
    """C-008: Spark's own CTAS of the same seed produces the same row set as the write node."""
    import _live_parity as live_parity
    from pyspark.sql import SparkSession

    source = _seed_files(tmp_path / "seed", 20_000, SEED_FILES)
    engine = _session("writepath-rowset", tmp_path / "wh")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        engine.sql(
            f"CREATE TABLE {CATALOG}.w.rows USING iceberg PARTITIONED BY (part) "
            "AS SELECT * FROM src"
        ).collect()
        engine_rows = engine.sql(
            f"SELECT id, part, label FROM {CATALOG}.w.rows ORDER BY id"
        ).to_arrow()
    finally:
        engine.stop()

    owned = SparkSession.getActiveSession() is None
    warehouse = tmp_path / "spark-wh"
    oracle = live_parity.build_spark_iceberg_engine(warehouse)
    catalog = live_parity.LIFECYCLE_SPARK_CATALOG
    session = oracle.session
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.w")
        session.read.parquet(str(source)).createOrReplaceTempView("spark_src")
        session.sql(
            f"CREATE TABLE {catalog}.w.rows USING iceberg PARTITIONED BY (part) "
            "AS SELECT * FROM spark_src"
        )
        spark_rows = session.sql(
            f"SELECT id, part, label FROM {catalog}.w.rows ORDER BY id"
        ).toPandas()
    finally:
        if owned:
            session.stop()

    assert engine_rows.num_rows == len(spark_rows)
    assert [value.as_py() for value in engine_rows.column("id")] == list(spark_rows["id"])
    assert [value.as_py() for value in engine_rows.column("part")] == list(spark_rows["part"])
    assert [value.as_py() for value in engine_rows.column("label")] == list(spark_rows["label"])
