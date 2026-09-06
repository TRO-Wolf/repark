"""WRITE-DISTRIBUTION-1 — the hash distribution rule before a partitioned Iceberg write."""

import os
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live write-path oracle is skipped (CI is JVM-free)"

CATALOG = "writedist"
SHUFFLE_PARTITIONS = "8"
SEED_ROWS = 120_000
SEED_FILES = 4
PARTITION_VALUES = 8
BUCKETS = 4
NULL_EVERY = 5
V3 = "'format-version' = '3'"


def _rows(start: int, stop: int) -> pa.Table:
    ids = pa.array(range(start, stop), type=pa.int64())
    parts = pa.array([index % PARTITION_VALUES for index in range(start, stop)], type=pa.int32())
    labels = pa.array(
        [None if index % NULL_EVERY == 0 else f"l{index % 3}" for index in range(start, stop)],
        type=pa.string(),
    )
    return pa.table({"id": ids, "part": parts, "label": labels})


def _seed_files(directory: Path, rows: int, files: int) -> Path:
    import pyarrow.parquet as pq

    directory.mkdir(parents=True, exist_ok=True)
    per_file = rows // files
    for index in range(files):
        start = index * per_file
        stop = rows if index == files - 1 else start + per_file
        pq.write_table(_rows(start, stop), directory / f"part-{index}.parquet")
    return directory


def _session(name: str, warehouse: Path) -> ReparkSession:
    engine = (
        ReparkSession.builder.appName(name)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    engine.register_memory_catalog(CATALOG, warehouse)
    engine.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
    return engine


def _files(engine: ReparkSession, table: str, key: str) -> list[tuple[object, int, object]]:
    files = engine.sql(
        f"SELECT {key} AS k, record_count, first_row_id FROM {table}.files"
    ).to_arrow()
    return [
        (key.as_py(), int(count.as_py()), first.as_py())
        for key, count, first in zip(
            files.column("k"),
            files.column("record_count"),
            files.column("first_row_id"),
            strict=True,
        )
    ]


def test_partitioned_ctas_writes_one_data_file_per_partition_value(tmp_path: Path) -> None:
    """C-001/C-007: eight values over a four-partition plan commit exactly eight data files."""
    source = _seed_files(tmp_path / "seed", SEED_ROWS, SEED_FILES)
    engine = _session("writedist-identity", tmp_path / "wh")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        engine.sql(
            f"CREATE TABLE {CATALOG}.w.part USING iceberg PARTITIONED BY (part) "
            f"TBLPROPERTIES ({V3}) AS SELECT * FROM src"
        ).collect()
        files = _files(engine, f"{CATALOG}.w.part", "partition.part")
        values = [value for value, _, _ in files]
        assert values == list(range(PARTITION_VALUES)), files
        counts = [count for _, count, _ in files]
        assert counts == [SEED_ROWS // PARTITION_VALUES] * PARTITION_VALUES, files
        firsts = [first for _, _, first in files]
        assert firsts == [sum(counts[:index]) for index in range(len(counts))], files
        rows = engine.sql(f"SELECT sum(id) AS s, count(*) AS n FROM {CATALOG}.w.part").to_arrow()
        assert rows.column("n")[0].as_py() == SEED_ROWS
        assert rows.column("s")[0].as_py() == SEED_ROWS * (SEED_ROWS - 1) // 2
    finally:
        engine.stop()


def test_bucket_partitioned_ctas_writes_one_data_file_per_bucket(tmp_path: Path) -> None:
    """C-005: the key is the transform value — bucket(4, id) commits four data files."""
    source = _seed_files(tmp_path / "seed", SEED_ROWS, SEED_FILES)
    engine = _session("writedist-bucket", tmp_path / "wh")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        engine.sql(
            f"CREATE TABLE {CATALOG}.w.bucketed USING iceberg "
            f"PARTITIONED BY (bucket({BUCKETS}, id)) AS SELECT * FROM src"
        ).collect()
        files = _files(engine, f"{CATALOG}.w.bucketed", "partition.id_bucket")
        buckets = [value for value, _, _ in files]
        assert buckets == list(range(BUCKETS)), files
        assert sum(count for _, count, _ in files) == SEED_ROWS
    finally:
        engine.stop()


def test_null_partition_value_lands_in_one_data_file(tmp_path: Path) -> None:
    """C-004: NULL is one partition value, so its rows from every plan partition share a file."""
    source = _seed_files(tmp_path / "seed", SEED_ROWS, SEED_FILES)
    engine = _session("writedist-null", tmp_path / "wh")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        engine.sql(
            f"CREATE TABLE {CATALOG}.w.labelled USING iceberg PARTITIONED BY (label) "
            "AS SELECT * FROM src"
        ).collect()
        files = _files(engine, f"{CATALOG}.w.labelled", "partition.label")
        assert [value for value, _, _ in files] == [None, "l0", "l1", "l2"], files
        null_rows = [count for value, count, _ in files if value is None]
        assert null_rows == [SEED_ROWS // NULL_EVERY], files
    finally:
        engine.stop()


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_partitioned_ctas_file_count_matches_spark(tmp_path: Path) -> None:
    """C-007: Spark's own CTAS of the same seed at eight shuffle partitions writes as many files."""
    import _live_parity as live_parity
    from pyspark.sql import SparkSession

    source = _seed_files(tmp_path / "seed", SEED_ROWS, SEED_FILES)
    engine = _session("writedist-spark", tmp_path / "wh")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        engine.sql(
            f"CREATE TABLE {CATALOG}.w.part USING iceberg PARTITIONED BY (part) "
            "AS SELECT * FROM src"
        ).collect()
        engine_files = sorted(_files(engine, f"{CATALOG}.w.part", "partition.part"))
    finally:
        engine.stop()

    owned = SparkSession.getActiveSession() is None
    oracle = live_parity.build_spark_iceberg_engine(
        tmp_path / "spark-wh", (("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS),)
    )
    catalog = live_parity.LIFECYCLE_SPARK_CATALOG
    session = oracle.session
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.w")
        session.read.parquet(str(source)).createOrReplaceTempView("spark_src")
        session.sql(
            f"CREATE TABLE {catalog}.w.part USING iceberg PARTITIONED BY (part) "
            "AS SELECT * FROM spark_src"
        )
        spark_files = session.sql(
            f"SELECT partition.part AS k, record_count FROM {catalog}.w.part.files"
        ).toPandas()
    finally:
        if owned:
            session.stop()

    spark_layout = sorted(
        zip(spark_files["k"].tolist(), spark_files["record_count"].tolist(), strict=True)
    )
    assert [(value, count) for value, count, _ in engine_files] == [
        (int(value), int(count)) for value, count in spark_layout
    ]
    assert len(engine_files) == PARTITION_VALUES
