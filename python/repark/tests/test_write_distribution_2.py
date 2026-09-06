"""WRITE-DISTRIBUTION-2 — the hash distribution rule on the stream write paths."""

import os
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live write-path oracle is skipped (CI is JVM-free)"

CATALOG = "writedist2"
SHUFFLE_PARTITIONS = "8"
SEED_ROWS = 120_000
SEED_FILES = 4
PARTITION_VALUES = 8
TRUNCATE_WIDTH = 3
COLUMNS = "(id BIGINT, part INT, s STRING)"
STATEMENTS = ("insert_overwrite", "merge_insert")
MERGE_SEED_ROWS = 400_000


def _seed_rows(statement: str) -> int:
    return MERGE_SEED_ROWS if statement == "merge_insert" else SEED_ROWS


def _rows(start: int, stop: int) -> pa.Table:
    ids = pa.array(range(start, stop), type=pa.int64())
    parts = pa.array([index % PARTITION_VALUES for index in range(start, stop)], type=pa.int32())
    labels = pa.array(
        [f"s{index % 5:02d}x{index}" for index in range(start, stop)], type=pa.string()
    )
    return pa.table({"id": ids, "part": parts, "s": labels})


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
        .getOrCreate()
    )
    engine.register_memory_catalog(CATALOG, warehouse)
    engine.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
    return engine


def _write(engine: ReparkSession, statement: str, table: str) -> None:
    if statement == "insert_overwrite":
        engine.sql(f"INSERT OVERWRITE {table} SELECT * FROM src").collect()
    else:
        engine.sql(
            f"MERGE INTO {table} t USING src s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"
        ).collect()


def _layout(engine: ReparkSession, table: str, key: str) -> list[tuple[object, int]]:
    files = engine.sql(f"SELECT {key} AS k, record_count FROM {table}.files").to_arrow()
    return sorted(
        (key.as_py(), int(count.as_py()))
        for key, count in zip(files.column("k"), files.column("record_count"), strict=True)
    )


def _write_partitioned(engine: ReparkSession, statement: str) -> list[tuple[object, int]]:
    table = f"{CATALOG}.w.{statement}"
    engine.sql(f"CREATE TABLE {table} {COLUMNS} USING iceberg PARTITIONED BY (part)")
    _write(engine, statement, table)
    rows = engine.sql(f"SELECT count(*) AS n, sum(id) AS s FROM {table}").to_arrow()
    total = _seed_rows(statement)
    assert rows.column("n")[0].as_py() == total
    assert rows.column("s")[0].as_py() == total * (total - 1) // 2
    return _layout(engine, table, "partition.part")


@pytest.mark.parametrize("statement", STATEMENTS)
def test_stream_write_commits_one_data_file_per_partition_value(
    tmp_path: Path, statement: str
) -> None:
    """C-001: INSERT OVERWRITE and MERGE write Spark's eight files, one per value."""
    source = _seed_files(tmp_path / "seed", _seed_rows(statement), SEED_FILES)
    engine = _session(f"writedist2-{statement}", tmp_path / "wh")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        layout = _write_partitioned(engine, statement)
        per_value = _seed_rows(statement) // PARTITION_VALUES
        expected = [(value, per_value) for value in range(PARTITION_VALUES)]
        assert layout == expected, layout
    finally:
        engine.stop()


def test_truncate_partitioned_ctas_keys_on_the_cast_string(tmp_path: Path) -> None:
    """C-002: `truncate(3, s)` over a parquet string commits one file per truncated prefix."""
    source = _seed_files(tmp_path / "seed", SEED_ROWS, SEED_FILES)
    engine = _session("writedist2-truncate", tmp_path / "wh")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        table = f"{CATALOG}.w.truncated"
        engine.sql(
            f"CREATE TABLE {table} USING iceberg PARTITIONED BY (truncate({TRUNCATE_WIDTH}, s)) "
            "AS SELECT * FROM src"
        ).collect()
        layout = _layout(engine, table, "partition.s_trunc")
        assert [prefix for prefix, _ in layout] == [f"s{index:02d}" for index in range(5)], layout
        assert sum(count for _, count in layout) == SEED_ROWS
    finally:
        engine.stop()


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
@pytest.mark.parametrize("statement", STATEMENTS)
def test_stream_write_layout_matches_spark(tmp_path: Path, statement: str) -> None:
    """C-003: Spark 4.1.2 commits the same (partition value, record count) layout for the seed."""
    import _live_parity as live_parity
    from pyspark.sql import SparkSession

    source = _seed_files(tmp_path / "seed", _seed_rows(statement), SEED_FILES)
    engine = _session(f"writedist2-live-{statement}", tmp_path / "wh")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        engine_layout = _write_partitioned(engine, statement)
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
        session.read.parquet(str(source)).createOrReplaceTempView("src")
        table = f"{catalog}.w.{statement}"
        session.sql(f"CREATE TABLE {table} {COLUMNS} USING iceberg PARTITIONED BY (part)")
        if statement == "insert_overwrite":
            session.sql(f"INSERT OVERWRITE {table} SELECT * FROM src")
        else:
            session.sql(
                f"MERGE INTO {table} t USING src s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"
            )
        spark_files = session.sql(
            f"SELECT partition.part AS k, record_count FROM {table}.files"
        ).toPandas()
    finally:
        if owned:
            session.stop()

    spark_layout = sorted(
        (int(value), int(count))
        for value, count in zip(
            spark_files["k"].tolist(), spark_files["record_count"].tolist(), strict=True
        )
    )
    assert engine_layout == spark_layout
    assert len(engine_layout) == PARTITION_VALUES
