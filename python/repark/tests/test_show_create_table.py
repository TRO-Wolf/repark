from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession

CATALOG = "mem"
NAMESPACE = "scns1"
TABLE = "sc1"
QUALIFIED = f"{CATALOG}.{NAMESPACE}.{TABLE}"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-show-create-table").getOrCreate()
    session.register_memory_catalog(CATALOG, tmp_path)
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    session.sql(
        f"CREATE TABLE {QUALIFIED} (id BIGINT NOT NULL COMMENT 'c', data STRING) USING iceberg "
        "PARTITIONED BY (bucket(4, id)) TBLPROPERTIES ('k'='v')"
    )
    return session


def _location(spark: ReparkSession) -> str:
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {QUALIFIED}").collect()
    return next(row[1] for row in rows if row[0] == "Location")


def test_show_create_table_answers_spark_text(spark: ReparkSession) -> None:
    frame = spark.sql(f"SHOW CREATE TABLE {QUALIFIED}")
    assert frame.columns == ["createtab_stmt"]
    rows = frame.collect()
    assert len(rows) == 1
    assert rows[0][0] == (
        f"CREATE TABLE {QUALIFIED} (\n"
        "  id BIGINT NOT NULL COMMENT 'c',\n"
        "  data STRING)\n"
        "USING iceberg\n"
        "PARTITIONED BY (bucket(4, id))\n"
        f"LOCATION '{_location(spark)}'\n"
        "TBLPROPERTIES (\n"
        "  'current-snapshot-id' = 'none',\n"
        "  'format' = 'iceberg/parquet',\n"
        "  'format-version' = '2',\n"
        "  'k' = 'v',\n"
        "  'write.parquet.compression-codec' = 'zstd')\n"
    )
