"""PERF-ICE-WRITEPATH-1 — the CTAS write node: file layout, determinism, and wall."""

import os
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
UNEQUAL_SIZES = (5000, 10000, 20000, 40000, 7000, 3000, 60000, 1000)
DETERMINISM_RUNS = 5
V3 = "'format-version' = '3'"
WALL_ROWS = 1_000_000
WALL_ITERATIONS = 5


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


def _wide_rows(start: int, stop: int) -> pa.Table:
    """The analysis' seven-column bed: the write is encode-bound, not commit-bound."""
    return pa.table(
        {
            "id": pa.array(range(start, stop), type=pa.int64()),
            "ts": pa.array(
                [1_600_000_000 + index for index in range(start, stop)], type=pa.int64()
            ),
            "v": pa.array([float(index) for index in range(start, stop)], type=pa.float64()),
            "vi": pa.array([index % 1000 for index in range(start, stop)], type=pa.int32()),
            "s": pa.array([f"s{index:015d}" for index in range(start, stop)], type=pa.string()),
            "cat": pa.array(
                [f"c{index % 100:02d}" for index in range(start, stop)], type=pa.string()
            ),
            "part": pa.array([index % 8 for index in range(start, stop)], type=pa.int32()),
        }
    )


def _wide_seed(directory: Path, rows: int, files: int) -> Path:
    """A fixed `files`-way layout of the wide bed, so the plan's partition count is the layout."""
    import pyarrow.parquet as pq

    directory.mkdir(parents=True, exist_ok=True)
    per_file = rows // files
    for index in range(files):
        start = index * per_file
        stop = rows if index == files - 1 else start + per_file
        pq.write_table(
            _wide_rows(start, stop),
            directory / f"part-{index}.parquet",
            compression="zstd",
            row_group_size=100_000,
        )
    return directory


def _unequal_seed(directory: Path) -> Path:
    """Unequal file sizes, written once: equal sizes hide a file-order defect."""
    import pyarrow.parquet as pq

    directory.mkdir(parents=True, exist_ok=True)
    start = 0
    for index, size in enumerate(UNEQUAL_SIZES):
        pq.write_table(_rows(start, start + size), directory / f"part-{index}.parquet")
        start += size
    return directory


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


def _ctas_commit(engine: ReparkSession, table: str) -> dict[str, Any]:
    engine.sql(
        f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ({V3}) AS SELECT * FROM src"
    ).collect()
    files = engine.sql(
        f"SELECT record_count, first_row_id, readable_metrics FROM {table}.files"
    ).to_arrow()
    counts = [int(value.as_py()) for value in files.column("record_count")]
    firsts = [int(value.as_py()) for value in files.column("first_row_id")]
    lows = [value.as_py()["id"]["lower_bound"] for value in files.column("readable_metrics")]
    totals = engine.sql(f"SELECT count(*) AS n, sum(id) AS s FROM {table}").to_arrow()
    return {
        "counts": counts,
        "firsts": firsts,
        "lows": lows,
        "rows": totals.column("n")[0].as_py(),
        "sum_id": totals.column("s")[0].as_py(),
    }


@pytest.mark.parametrize("partitions", ["4", "16"])
def test_ctas_commit_is_ordered_and_contiguous_at_any_partition_count(
    tmp_path: Path, partitions: str
) -> None:
    """C-005: the manifest ascends by content and `_row_id` tiles it, at every partition count."""
    total = sum(UNEQUAL_SIZES)
    source = _unequal_seed(tmp_path / "seed")
    engine = (
        ReparkSession.builder.appName(f"writepath-order-{partitions}")
        .config("spark.sql.shuffle.partitions", partitions)
        .config("repark.write.max-concurrent-files", "4")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    try:
        engine.register_memory_catalog(CATALOG, tmp_path / "wh")
        engine.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        seen: dict[str, list[int]] = {}
        for run in range(DETERMINISM_RUNS):
            commit = _ctas_commit(engine, f"{CATALOG}.w.o{partitions}_{run}")
            assert commit["lows"] == sorted(commit["lows"]), (
                f"run {run} at {partitions} partitions committed a manifest that does not ascend "
                f"by content: {commit['lows']}"
            )
            expected = [sum(commit["counts"][:index]) for index in range(len(commit["counts"]))]
            assert commit["firsts"] == expected, (
                f"run {run} at {partitions} partitions did not tile `_row_id` over the manifest: "
                f"{commit['firsts']} against {expected}"
            )
            assert sum(commit["counts"]) == total
            assert commit["rows"] == total
            assert commit["sum_id"] == total * (total - 1) // 2
            key = str(commit["counts"])
            if key in seen:
                assert seen[key] == commit["firsts"], (
                    f"two runs at {partitions} partitions produced the same file grouping "
                    f"{key} but different `_row_id` ranges: {seen[key]} against {commit['firsts']}"
                )
            seen[key] = commit["firsts"]
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


def _ctas_at_scale(source: Path, warehouse: Path, name: str, cap: str) -> dict[str, Any]:
    engine = _session(name, warehouse, cap=cap)
    try:
        engine.read.parquet(str(source)).createOrReplaceTempView("src")
        walls = []
        for index in range(WALL_ITERATIONS + 1):
            started = time.perf_counter()
            engine.sql(
                f"CREATE TABLE {CATALOG}.w.t{index} USING iceberg AS SELECT * FROM src"
            ).collect()
            if index:
                walls.append((time.perf_counter() - started) * 1000.0)
        answer = engine.sql(
            f"SELECT count(*) AS n, sum(id) AS s, sum(vi) AS v FROM {CATALOG}.w.t{WALL_ITERATIONS}"
        ).to_arrow()
        return {
            "files": len(_file_counts(engine, f"{CATALOG}.w.t{WALL_ITERATIONS}")),
            "rows": answer.column("n")[0].as_py(),
            "sum_id": answer.column("s")[0].as_py(),
            "sum_vi": answer.column("v")[0].as_py(),
            "best_ms": round(min(walls)),
        }
    finally:
        engine.stop()


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_partition_writers_answer_the_single_writer_at_scale(tmp_path: Path) -> None:
    """C-007: at 1e6 rows the two writer shapes agree on every answer, and on their layout."""
    source = _wide_seed(tmp_path / "wall", WALL_ROWS, int(THREADS))
    one = _ctas_at_scale(source, tmp_path / "wh_one", "writepath-scale-one", cap="1")
    many = _ctas_at_scale(source, tmp_path / "wh_many", "writepath-scale-many", cap="4")
    assert one["files"] == 1, one
    assert many["files"] == int(THREADS), many
    for key in ("rows", "sum_id", "sum_vi"):
        assert one[key] == many[key], (key, one, many)
    assert one["rows"] == WALL_ROWS
    assert one["sum_id"] == WALL_ROWS * (WALL_ROWS - 1) // 2


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
