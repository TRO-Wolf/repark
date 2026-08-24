"""V3-2: CREATE/CTAS format-version=3 behind ``repark.sql.allowCreateFormatVersion3``.

pins: v3-2-create-v3-opt-in/C-010, C-011
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import UnsupportedOperationException

_ALLOW_CREATE_V3_KEY = "repark.sql.allowCreateFormatVersion3"


def test_format_version_three_refuses_without_opt_in(tmp_path: Path) -> None:
    """Default session: TBLPROPERTIES format-version=3 refuses naming the conf."""
    spark = ReparkSession.builder.appName("v3-2-default").getOrCreate()
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        with pytest.raises(UnsupportedOperationException, match=_ALLOW_CREATE_V3_KEY):
            spark.sql(
                "CREATE TABLE ice.sales.v3 (id BIGINT) USING iceberg "
                "TBLPROPERTIES ('format-version' = '3')"
            ).collect()
        with pytest.raises(UnsupportedOperationException, match=_ALLOW_CREATE_V3_KEY):
            spark.sql(
                "CREATE TABLE ice.sales.v3ctas USING iceberg "
                "TBLPROPERTIES ('format-version' = '3') AS SELECT 1 AS id"
            ).collect()
        assert not spark.catalog.tableExists("ice.sales.v3")
        assert not spark.catalog.tableExists("ice.sales.v3ctas")
    finally:
        spark.stop()


def test_format_version_three_create_with_opt_in_is_v3_and_rewrite_refuses(
    tmp_path: Path,
) -> None:
    """Opt-in CREATE lands format v3; rewrite_data_files still names row lineage."""
    spark = (
        ReparkSession.builder.appName("v3-2-opt-in")
        .config(_ALLOW_CREATE_V3_KEY, "true")
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", tmp_path)
        spark.sql("CREATE NAMESPACE ice.sales")
        spark.sql(
            "CREATE TABLE ice.sales.v3 (id BIGINT) USING iceberg "
            "TBLPROPERTIES ('format-version' = '3')"
        ).collect()
        spark.sql(
            "CREATE TABLE ice.sales.v3ctas USING iceberg "
            "TBLPROPERTIES ('format-version' = '3') AS SELECT 1 AS id"
        ).collect()
        spark.sql("INSERT INTO ice.sales.v3 SELECT 1 AS id").collect()
        rows = spark.sql("SELECT id FROM ice.sales.v3").to_arrow()
        assert rows.column("id").to_pylist() == [1]
        assert spark.sql("SELECT id FROM ice.sales.v3ctas").to_arrow().column("id").to_pylist() == [
            1
        ]
        with pytest.raises(UnsupportedOperationException, match="row lineage"):
            spark.sql("CALL ice.system.rewrite_data_files(table => 'sales.v3')").collect()
    finally:
        spark.stop()
