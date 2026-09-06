"""WRITE-ORDER-DIST-1 — ALTER TABLE WRITE ORDERED/DISTRIBUTED BY and the writes they shape."""

import glob
import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live write-order oracle is skipped (CI is JVM-free)"

CATALOG = "writeorder"
SHUFFLE_PARTITIONS = "8"
SEED_ROWS = 120_000
SEED_FILES = 4
PARTITION_VALUES = 8


def _seed_table(start: int, stop: int) -> pa.Table:
    ids = pa.array(range(start, stop), type=pa.int64())
    parts = pa.array([index % PARTITION_VALUES for index in range(start, stop)], type=pa.int32())
    names = pa.array([f"n{index:06d}" for index in range(start, stop)], type=pa.string())
    return pa.table({"id": ids, "name": names, "part": parts})


def _seed_files(directory: Path, rows: int, files: int) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    per_file = rows // files
    for index in range(files):
        start = index * per_file
        stop = rows if index == files - 1 else start + per_file
        pq.write_table(_seed_table(start, stop), directory / f"part-{index}.parquet")
    return directory


def _session(name: str, warehouse: Path, max_files: str | None = None) -> ReparkSession:
    builder = (
        ReparkSession.builder.appName(name)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("repark.sql.allowCreateFormatVersion3", "true")
    )
    if max_files is not None:
        builder = builder.config("repark.write.max-concurrent-files", max_files)
    engine = builder.getOrCreate()
    engine.register_memory_catalog(CATALOG, warehouse)
    engine.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
    return engine


def _metadata(warehouse: Path, table: str) -> dict:
    directory = warehouse / "repark_ctas" / CATALOG / "w" / table / "metadata"
    hint = directory / "version-hint.text"
    if hint.exists():
        with open(hint) as handle:
            version = handle.read().strip()
        candidates = sorted(glob.glob(str(directory / f"{version}*.metadata.json")))
        if candidates:
            with open(candidates[-1]) as handle:
                return json.load(handle)
    metas = sorted(glob.glob(str(directory / "*.metadata.json")))
    with open(metas[-1]) as handle:
        return json.load(handle)


def _metadata_count(warehouse: Path, table: str) -> int:
    return len(
        glob.glob(str(warehouse / "repark_ctas" / CATALOG / "w" / table / "metadata" / "*.metadata.json"))
    )


def _write_state(warehouse: Path, table: str) -> tuple[list[dict], int, str | None]:
    meta = _metadata(warehouse, table)
    orders = sorted(meta.get("sort-orders", []), key=lambda order: order["order-id"])
    return (
        orders,
        meta.get("default-sort-order-id", -1),
        meta.get("properties", {}).get("write.distribution-mode"),
    )


def _data_files(engine: ReparkSession, table: str) -> list:
    return engine.sql(f"SELECT file_path, record_count FROM {table}.files").to_arrow().to_pylist()


def _is_monotone(path: str, column: str) -> tuple[int, bool]:
    values = pq.read_table(path, columns=[column]).column(column).to_pylist()
    return len(values), all(a <= b for a, b in zip(values, values[1:]))


def _ctas(engine: ReparkSession, table: str, version: str, extra: str = "") -> None:
    engine.sql(
        f"CREATE TABLE {table} USING iceberg PARTITIONED BY (part) "
        f"TBLPROPERTIES ('format-version' = '{version}'{extra}) AS SELECT * FROM src"
    ).collect()


def test_write_ordered_by_sets_sort_order_and_range(tmp_path: Path) -> None:
    """C-001: WRITE ORDERED BY appends the sort order, makes it default, sets range."""
    for version in ("2", "3"):
        warehouse = tmp_path / f"wh-{version}"
        source = _seed_files(tmp_path / f"seed-{version}", 8_000, 2)
        engine = _session(f"wo-ordered-{version}", warehouse)
        try:
            engine.read.parquet(str(source)).createOrReplaceTempView("src")
            _ctas(engine, f"{CATALOG}.w.t", version)
            engine.sql(
                f"ALTER TABLE {CATALOG}.w.t WRITE ORDERED BY (id, name DESC NULLS LAST)"
            ).collect()
            orders, default, dist = _write_state(warehouse, "t")
            assert dist == "range", (orders, default, dist)
            assert default == 1, (orders, default, dist)
            assert len(orders) == 2, (orders, default, dist)
            fields = orders[1]["fields"]
            assert fields == [
                {"transform": "identity", "source-id": 1, "direction": "asc", "null-order": "nulls-first"},
                {"transform": "identity", "source-id": 2, "direction": "desc", "null-order": "nulls-last"},
            ], fields
        finally:
            engine.stop()


def test_write_locally_ordered_by_leaves_distribution_untouched(tmp_path: Path) -> None:
    """C-002: WRITE LOCALLY ORDERED BY sets the order and leaves the property alone."""
    warehouse = tmp_path / "wh"
    source = _seed_files(tmp_path / "seed", 8_000, 2)
    engine = _session("wo-local", warehouse)
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        _ctas(engine, f"{CATALOG}.w.t", "2")
        assert _write_state(warehouse, "t")[2] is None
        engine.sql(f"ALTER TABLE {CATALOG}.w.t WRITE LOCALLY ORDERED BY (id DESC)").collect()
        orders, default, dist = _write_state(warehouse, "t")
        assert dist is None, (orders, default, dist)
        assert default == 1, (orders, default, dist)
        assert orders[1]["fields"] == [
            {"transform": "identity", "source-id": 1, "direction": "desc", "null-order": "nulls-last"}
        ], orders
    finally:
        engine.stop()


def test_write_distributed_by_partition_sets_hash_and_resets_order(tmp_path: Path) -> None:
    """C-003: WRITE DISTRIBUTED BY PARTITION sets hash and resets the default order."""
    warehouse = tmp_path / "wh"
    source = _seed_files(tmp_path / "seed", 8_000, 2)
    engine = _session("wo-dist", warehouse)
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        _ctas(engine, f"{CATALOG}.w.t", "2")
        engine.sql(f"ALTER TABLE {CATALOG}.w.t WRITE ORDERED BY (id)").collect()
        assert _write_state(warehouse, "t")[1] == 1
        engine.sql(f"ALTER TABLE {CATALOG}.w.t WRITE DISTRIBUTED BY PARTITION").collect()
        orders, default, dist = _write_state(warehouse, "t")
        assert dist == "hash", (orders, default, dist)
        assert default == 0, (orders, default, dist)
        assert len(orders) == 2, (orders, default, dist)
    finally:
        engine.stop()


def test_write_distributed_by_partition_locally_ordered_sets_both(tmp_path: Path) -> None:
    """C-004: DISTRIBUTED BY PARTITION LOCALLY ORDERED BY sets the order and hash."""
    warehouse = tmp_path / "wh"
    source = _seed_files(tmp_path / "seed", 8_000, 2)
    engine = _session("wo-dist-local", warehouse)
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        _ctas(engine, f"{CATALOG}.w.t", "2")
        engine.sql(
            f"ALTER TABLE {CATALOG}.w.t WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY (id)"
        ).collect()
        orders, default, dist = _write_state(warehouse, "t")
        assert dist == "hash", (orders, default, dist)
        assert default == 1, (orders, default, dist)
        assert len(orders) == 2, (orders, default, dist)
    finally:
        engine.stop()


def test_write_unordered_resets_order_and_sets_none(tmp_path: Path) -> None:
    """C-005: WRITE UNORDERED resets to the unsorted order and sets none."""
    warehouse = tmp_path / "wh"
    source = _seed_files(tmp_path / "seed", 8_000, 2)
    engine = _session("wo-unordered", warehouse)
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        _ctas(engine, f"{CATALOG}.w.t", "2")
        engine.sql(f"ALTER TABLE {CATALOG}.w.t WRITE ORDERED BY (id)").collect()
        assert _write_state(warehouse, "t")[1] == 1
        engine.sql(f"ALTER TABLE {CATALOG}.w.t WRITE UNORDERED").collect()
        orders, default, dist = _write_state(warehouse, "t")
        assert dist == "none", (orders, default, dist)
        assert default == 0, (orders, default, dist)
        assert len(orders) == 2, (orders, default, dist)
    finally:
        engine.stop()


def test_write_order_bad_column_refuses_without_committing(tmp_path: Path) -> None:
    """C-006: an unknown sort column refuses loud and commits no metadata version."""
    warehouse = tmp_path / "wh"
    source = _seed_files(tmp_path / "seed", 8_000, 2)
    engine = _session("wo-badcol", warehouse)
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        _ctas(engine, f"{CATALOG}.w.t", "2")
        before = _metadata_count(warehouse, "t")
        with pytest.raises(Exception, match="nope"):
            engine.sql(f"ALTER TABLE {CATALOG}.w.t WRITE ORDERED BY (nope)").collect()
        with pytest.raises(Exception, match="expecting 'PARTITION'"):
            engine.sql(f"ALTER TABLE {CATALOG}.w.t WRITE DISTRIBUTED BY (id)").collect()
        assert _metadata_count(warehouse, "t") == before
    finally:
        engine.stop()


def test_write_distribution_none_ctas_skips_the_hash_rule(tmp_path: Path) -> None:
    """C-007: dist=none CTAS writes writers x values; hash keeps one file per value."""
    for mode, expected in (("none", SEED_FILES * PARTITION_VALUES), ("hash", PARTITION_VALUES)):
        warehouse = tmp_path / f"wh-{mode}"
        source = _seed_files(tmp_path / f"seed-{mode}", SEED_ROWS, SEED_FILES)
        engine = _session(f"wo-gate-{mode}", warehouse)
        try:
            engine.read.parquet(str(source)).createOrReplaceTempView("src")
            _ctas(engine, f"{CATALOG}.w.t", "2", f", 'write.distribution-mode' = '{mode}'")
            files = _data_files(engine, f"{CATALOG}.w.t")
            assert len(files) == expected, [(f["file_path"], f["record_count"]) for f in files]
            assert sum(f["record_count"] for f in files) == SEED_ROWS
        finally:
            engine.stop()


def test_write_ordered_overwrite_writes_sorted_files(tmp_path: Path) -> None:
    """C-008: after WRITE ORDERED BY, an INSERT OVERWRITE commits monotone files."""
    warehouse = tmp_path / "wh"
    source = _seed_files(tmp_path / "seed", SEED_ROWS, SEED_FILES)
    engine = _session("wo-sorted-overwrite", warehouse, "1")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        _ctas(engine, f"{CATALOG}.w.t", "2")
        engine.sql(f"ALTER TABLE {CATALOG}.w.t WRITE ORDERED BY (id)").collect()
        engine.sql(f"INSERT OVERWRITE {CATALOG}.w.t SELECT * FROM src ORDER BY id DESC").collect()
        files = _data_files(engine, f"{CATALOG}.w.t")
        assert len(files) == PARTITION_VALUES, [f["record_count"] for f in files]
        for row in files:
            count, monotone = _is_monotone(row["file_path"], "id")
            assert monotone, (row["file_path"], count)
        assert sum(f["record_count"] for f in files) == SEED_ROWS
    finally:
        engine.stop()


def test_write_ordered_merge_writes_sorted_files(tmp_path: Path) -> None:
    """C-008: after WRITE ORDERED BY, a MERGE commits monotone files and Spark's rows."""
    warehouse = tmp_path / "wh"
    source = _seed_files(tmp_path / "seed", SEED_ROWS, SEED_FILES)
    engine = _session("wo-sorted-merge", warehouse, "1")
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        _ctas(engine, f"{CATALOG}.w.t", "2")
        engine.sql(f"ALTER TABLE {CATALOG}.w.t WRITE ORDERED BY (id)").collect()
        engine.sql(
            f"MERGE INTO {CATALOG}.w.t t USING (SELECT * FROM src) s "
            f"ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name"
        ).collect()
        files = _data_files(engine, f"{CATALOG}.w.t")
        assert len(files) == PARTITION_VALUES, [f["record_count"] for f in files]
        for row in files:
            count, monotone = _is_monotone(row["file_path"], "id")
            assert monotone, (row["file_path"], count)
        rows = engine.sql(f"SELECT count(*) AS n FROM {CATALOG}.w.t").to_arrow()
        assert rows.column("n")[0].as_py() == SEED_ROWS
    finally:
        engine.stop()


def test_write_ordered_ctas_replace_writes_sorted_files(tmp_path: Path) -> None:
    """C-010: range keeps the CTAS at one sorted file per partition value."""
    warehouse = tmp_path / "wh"
    source = _seed_files(tmp_path / "seed", SEED_ROWS, SEED_FILES)
    engine = _session("wo-sorted-ctas", warehouse)
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        _ctas(engine, f"{CATALOG}.w.t", "2")
        engine.sql(f"ALTER TABLE {CATALOG}.w.t WRITE ORDERED BY (id)").collect()
        engine.sql(
            f"CREATE OR REPLACE TABLE {CATALOG}.w.t USING iceberg PARTITIONED BY (part) "
            f"AS SELECT * FROM src"
        ).collect()
        files = _data_files(engine, f"{CATALOG}.w.t")
        assert len(files) == PARTITION_VALUES, [f["record_count"] for f in files]
        for row in files:
            count, monotone = _is_monotone(row["file_path"], "id")
            assert monotone, (row["file_path"], count)
    finally:
        engine.stop()


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_write_order_metadata_matches_spark_after_same_statements(tmp_path: Path) -> None:
    """C-009: metadata.json sort order + property equal Spark's after the same DDL."""
    import _live_parity as live_parity
    from pyspark.sql import SparkSession

    forms = [
        "WRITE ORDERED BY (id, name DESC NULLS LAST)",
        "WRITE LOCALLY ORDERED BY (id)",
        "WRITE DISTRIBUTED BY PARTITION",
        "WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY (id)",
        "WRITE UNORDERED",
    ]
    warehouse = tmp_path / "wh"
    source = _seed_files(tmp_path / "seed", 8_000, 2)
    engine = _session("wo-live", warehouse)
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        for index in range(len(forms)):
            _ctas(engine, f"{CATALOG}.w.t{index}", "2")
        for index, form in enumerate(forms):
            engine.sql(f"ALTER TABLE {CATALOG}.w.t{index} {form}").collect()
        states = [_write_state(warehouse, f"t{index}") for index in range(len(forms))]
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
        for index in range(len(forms)):
            session.sql(
                f"CREATE TABLE {catalog}.w.t{index} USING iceberg PARTITIONED BY (part) "
                f"TBLPROPERTIES ('format-version' = '2') AS SELECT * FROM spark_src"
            )
        for index, form in enumerate(forms):
            session.sql(f"ALTER TABLE {catalog}.w.t{index} {form}")
        spark_states = []
        for index in range(len(forms)):
            metas = sorted(
                glob.glob(str(tmp_path / "spark-wh" / "w" / f"t{index}" / "metadata" / "*.metadata.json"))
            )
            with open(metas[-1]) as handle:
                meta = json.load(handle)
            spark_states.append((
                sorted(meta.get("sort-orders", []), key=lambda order: order["order-id"]),
                meta.get("default-sort-order-id", -1),
                meta.get("properties", {}).get("write.distribution-mode"),
            ))
    finally:
        if owned:
            session.stop()

    for index, form in enumerate(forms):
        assert states[index][1] == spark_states[index][1], form
        assert states[index][2] == spark_states[index][2], form
        assert states[index][0] == spark_states[index][0], form
