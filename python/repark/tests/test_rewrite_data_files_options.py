"""rewrite_data_files where / strategy / sort_order facade pins.

pins: maint-rewrite-data-files-options/C-003, C-004, C-005, C-006, C-007
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession
from repark.errors import (
    IllegalArgumentException,
    PySparkException,
    UnsupportedOperationException,
)

COW = """
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
"""


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """Memory catalog session for rewrite option pins."""
    session = ReparkSession.builder.appName("pytest-rewrite-options").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _file_paths(spark: ReparkSession, table: str) -> list[str]:
    """Return live data-file paths in table order."""
    files = spark.sql(f"SELECT file_path FROM {table}.files").to_arrow()
    return [str(value) for value in files.column("file_path").to_pylist()]


def _read_bytes(path: str) -> bytes:
    """Read a data file, stripping a file: URI prefix if present."""
    local = path.removeprefix("file://").removeprefix("file:")
    return Path(local).read_bytes()


def _paths_for_part(spark: ReparkSession, table: str, part_value: int) -> set[str]:
    """Return live data-file paths whose identity partition equals ``part_value``."""
    files = spark.sql(f"SELECT file_path, partition FROM {table}.files").to_arrow()
    kept: set[str] = set()
    for index in range(files.num_rows):
        path = str(files.column("file_path")[index].as_py())
        partition = files.column("partition")[index].as_py()
        if isinstance(partition, dict):
            value = partition.get("part")
        else:
            value = getattr(partition, "part", None)
        assert value is not None, f"files.partition must expose part on {path}: {partition!r}"
        if int(value) == part_value:
            kept.add(path)
    return kept


def test_rewrite_where_keeps_out_of_scope_files_byte_identical(spark: ReparkSession) -> None:
    """Filtered rewrite leaves the excluded partition's files byte-identical."""
    table = "mem.ns.filt"
    spark.sql(
        f"CREATE TABLE {table} (id INT, part INT) USING iceberg "
        f"PARTITIONED BY (part) TBLPROPERTIES ({COW})"
    )
    for index in range(1, 6):
        spark.sql(f"INSERT INTO {table} VALUES ({index}, 0)")
    for index in range(101, 106):
        spark.sql(f"INSERT INTO {table} VALUES ({index}, 1)")
    before_paths = _file_paths(spark, table)
    assert len(before_paths) == 10
    before_bytes = {path: _read_bytes(path) for path in before_paths}
    part1_before = _paths_for_part(spark, table, 1)
    part0_before = _paths_for_part(spark, table, 0)
    assert len(part1_before) == 5
    assert len(part0_before) == 5
    result = spark.sql(
        "CALL mem.system.rewrite_data_files(table => 'ns.filt', where => 'part = 0')"
    ).to_arrow()
    assert result.schema.field("rewritten_data_files_count").type == pa.int32()
    assert result.schema.field("rewritten_bytes_count").type == pa.int64()
    assert not result.schema.field("rewritten_data_files_count").nullable
    assert result.column("rewritten_data_files_count")[0].as_py() == 5
    assert result.column("added_data_files_count")[0].as_py() == 1
    after_paths = set(_file_paths(spark, table))
    assert part1_before <= after_paths
    assert part0_before.isdisjoint(after_paths)
    for path in part1_before:
        assert _read_bytes(path) == before_bytes[path]


def test_rewrite_unknown_strategy_matches_spark_message(spark: ReparkSession) -> None:
    """Unknown strategy text is Spark's `unsupported strategy` sentence."""
    spark.sql(f"CREATE TABLE mem.ns.events USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id")
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"unsupported strategy: nope\. Only binpack or sort is supported",
    ):
        spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.events', strategy => 'nope')")


def test_rewrite_sort_order_refuses_loud(spark: ReparkSession) -> None:
    """A bare sort_order sorts; the pre-unit refusal is retired (ICE-RDF-SORT-PARSE-1).

    Spark 4.1.2 leaves the single small file alone (no rewrite-all): the CALL
    succeeds with an all-zero row, the one file keeps id 1, and no snapshot is
    added. Recorded 2026-09-20; the engine answers the same row and file.
    """
    spark.sql(f"CREATE TABLE mem.ns.events USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id")
    before = _file_paths(spark, "mem.ns.events")
    result = spark.sql(
        "CALL mem.system.rewrite_data_files(table => 'ns.events', sort_order => 'id ASC')"
    ).to_arrow()
    assert [result.column(name)[0].as_py() for name in result.column_names] == [0, 0, 0, 0, 0]
    after = _file_paths(spark, "mem.ns.events")
    assert after == before
    assert len(after) == 1
    local = after[0].removeprefix("file://").removeprefix("file:")
    assert pq.read_table(local, columns=["id"]).column("id").to_pylist() == [1]
    snapshots = spark.sql("SELECT snapshot_id FROM mem.ns.events.snapshots").to_arrow()
    assert snapshots.num_rows == 1


def test_rewrite_sort_strategy_refuses_loud(spark: ReparkSession) -> None:
    """Strategy sort on an unsorted table is the fork's refusal, not the old text."""
    spark.sql(f"CREATE TABLE mem.ns.events USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id")
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.events', strategy => 'sort')")
    assert str(caught.value) == (
        "Cannot sort data without a valid sort order, "
        "table 'ns.events' is unsorted and no sort order is provided"
    )


def test_rewrite_bad_where_matches_spark_message(spark: ReparkSession) -> None:
    """Unparsable where text is Spark's `Cannot parse predicates in where option` wrapper."""
    spark.sql(f"CREATE TABLE mem.ns.events USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id")
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"Cannot parse predicates in where option: id === 1",
    ):
        spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.events', where => 'id === 1')")
