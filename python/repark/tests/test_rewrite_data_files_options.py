"""rewrite_data_files where / strategy / sort_order facade pins.

pins: maint-rewrite-data-files-options/C-003, C-004, C-005, C-006, C-007
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkException, UnsupportedOperationException

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
    result = spark.sql(
        "CALL mem.system.rewrite_data_files(table => 'ns.filt', where => 'part = 0')"
    ).to_arrow()
    assert result.schema.field("rewritten_data_files_count").type == pa.int32()
    assert result.schema.field("rewritten_bytes_count").type == pa.int64()
    assert not result.schema.field("rewritten_data_files_count").nullable
    assert result.column("rewritten_data_files_count")[0].as_py() == 5
    assert result.column("added_data_files_count")[0].as_py() == 1
    after_paths = set(_file_paths(spark, table))
    identical = 0
    for path, payload in before_bytes.items():
        if path in after_paths:
            assert _read_bytes(path) == payload
            identical += 1
    assert identical == 5


def test_rewrite_unknown_strategy_matches_spark_message(spark: ReparkSession) -> None:
    """Unknown strategy text is Spark's `unsupported strategy` sentence."""
    spark.sql(f"CREATE TABLE mem.ns.events USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id")
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"unsupported strategy: nope\. Only binpack or sort is supported",
    ):
        spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.events', strategy => 'nope')")


def test_rewrite_sort_order_refuses_loud(spark: ReparkSession) -> None:
    """Named sort_order refuses; it is never a silent binpack."""
    spark.sql(f"CREATE TABLE mem.ns.events USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id")
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"sort_order.*not supported",
    ):
        spark.sql(
            "CALL mem.system.rewrite_data_files(table => 'ns.events', sort_order => 'id ASC')"
        )


def test_rewrite_bad_where_matches_spark_message(spark: ReparkSession) -> None:
    """Unparsable where text is Spark's `Cannot parse predicates in where option` wrapper."""
    spark.sql(f"CREATE TABLE mem.ns.events USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id")
    with pytest.raises(
        (UnsupportedOperationException, PySparkException),
        match=r"Cannot parse predicates in where option: id === 1",
    ):
        spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.events', where => 'id === 1')")
