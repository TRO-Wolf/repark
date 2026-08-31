"""DML-C: facade and ANSI-callable TRUNCATE TABLE.

pins: dml-c-truncate/C-004, C-006, C-007
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark.errors import AnalysisException, UnsupportedOperationException


def test_facade_truncate_table_wipes_rows_stamps_delete_and_time_travel(tmp_path: Path) -> None:
    """Spark-door facade `.sql()` TRUNCATE is Spark-equal on rows, files, and operation."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("dml-c-truncate").getOrCreate()
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        spark.sql(
            "CREATE TABLE ice.sales.t (id INT, name STRING) USING iceberg "
            "TBLPROPERTIES ('format-version' = '2')"
        )
        spark.sql("INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        pre = spark.sql("SELECT snapshot_id FROM ice.sales.t.snapshots").to_arrow()
        assert pre.num_rows == 1
        pre_id = pre.column("snapshot_id").to_pylist()[0]
        spark.sql("TRUNCATE TABLE ice.sales.t")
        live = spark.sql("SELECT * FROM ice.sales.t").to_arrow()
        assert live.num_rows == 0
        files = spark.sql("SELECT * FROM ice.sales.t.files").to_arrow()
        assert files.num_rows == 0
        snaps = spark.sql(
            "SELECT snapshot_id, operation FROM ice.sales.t.snapshots ORDER BY committed_at"
        ).to_arrow()
        assert snaps.num_rows == 2
        ops = snaps.column("operation").to_pylist()
        assert ops[-1] == "delete"
        travelled = spark.sql(
            f"SELECT * FROM ice.sales.t VERSION AS OF {pre_id} ORDER BY id"
        ).to_arrow()
        assert travelled.schema.field("id").type == live.schema.field("id").type
        assert travelled.column("id").to_pylist() == [1, 2, 3]
    finally:
        spark.stop()


def test_facade_truncate_missing_table_is_table_or_view_not_found(tmp_path: Path) -> None:
    """Missing-table TRUNCATE raises Spark's TABLE_OR_VIEW_NOT_FOUND class."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("dml-c-truncate-missing").getOrCreate()
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        with pytest.raises(AnalysisException) as raised:
            spark.sql("TRUNCATE TABLE ice.sales.does_not_exist")
        assert "TABLE_OR_VIEW_NOT_FOUND" in str(raised.value)
    finally:
        spark.stop()


def test_facade_truncate_view_is_expect_table_not_view(tmp_path: Path) -> None:
    """TRUNCATE of a view raises Spark's EXPECT_TABLE_NOT_VIEW class and leaves the table."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("dml-c-truncate-view").getOrCreate()
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        spark.sql(
            "CREATE TABLE ice.sales.t (id INT) USING iceberg TBLPROPERTIES ('format-version' = '2')"
        )
        spark.sql("INSERT INTO ice.sales.t VALUES (1)")
        spark.sql("CREATE VIEW v_trunc AS SELECT * FROM ice.sales.t")
        with pytest.raises(AnalysisException) as raised:
            spark.sql("TRUNCATE TABLE v_trunc")
        assert "EXPECT_TABLE_NOT_VIEW" in str(raised.value)
        live = spark.sql("SELECT * FROM ice.sales.t").to_arrow()
        assert live.num_rows == 1
    finally:
        spark.stop()


def test_ansi_callable_truncate_no_longer_uses_the_refuse_substitute() -> None:
    """Native `repark.sql()` routes TRUNCATE; it does not steer at empty overwrite."""
    import repark

    with pytest.raises((AnalysisException, UnsupportedOperationException)) as raised:
        repark.sql("TRUNCATE TABLE ice.sales.does_not_exist")
    message = str(raised.value)
    assert "no truncate primitive" not in message
    assert "INSERT OVERWRITE" not in message or "TABLE_OR_VIEW_NOT_FOUND" in message
