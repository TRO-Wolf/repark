"""R-ICEBERG-HYGIENE oracles — column-def CREATE + ref DDL time-travel pins.

Oracle min: schema equality (name + Arrow type) vs CTAS twin; branch/tag created via
SQL DDL (not only ``_testing_create_ref``). Local memory catalog only — no AWS, no JVM.

Fork cite (pin ``b009ac15``): ``manage_snapshots.rs:90-145`` create/remove branch|tag.
CTAS+explicit column list rejection remains pinned in Rust and below.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException

TABLE = "mem.ns.events"
COW = """
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
"""


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-iceberg-hygiene").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _schema_names_types(table: pa.Table) -> list[tuple[str, str]]:
    return [(field.name, str(field.type)) for field in table.schema]


def _arrow_ids(table: pa.Table) -> list[int]:
    ids = table.column("id").to_pylist()
    return sorted(int(value) for value in ids if value is not None)


def test_column_def_create_schema_equals_ctas_twin(spark: ReparkSession) -> None:
    """Column-def CREATE lands empty with schema equal to a zero-row CTAS twin."""
    spark.sql(
        "CREATE TABLE mem.ns.col_def (id BIGINT NOT NULL, name STRING, active BOOLEAN) "
        "USING iceberg"
    )
    spark.sql(
        "CREATE TABLE mem.ns.ctas_twin USING iceberg AS "
        "SELECT CAST(NULL AS BIGINT) AS id, CAST(NULL AS STRING) AS name, "
        "CAST(NULL AS BOOLEAN) AS active WHERE false"
    )

    col_arrow = spark.sql("SELECT * FROM mem.ns.col_def").to_arrow()
    twin_arrow = spark.sql("SELECT * FROM mem.ns.ctas_twin").to_arrow()
    assert col_arrow.num_rows == 0
    assert twin_arrow.num_rows == 0
    assert _schema_names_types(col_arrow) == _schema_names_types(twin_arrow)
    assert _schema_names_types(col_arrow) == [
        ("id", "int64"),
        ("name", "string"),
        ("active", "bool"),
    ]
    # DEFAULT must refuse loud (not silent ignore).
    with pytest.raises((UnsupportedOperationException, AnalysisException)) as caught:
        spark.sql("CREATE TABLE mem.ns.with_def (id BIGINT DEFAULT 0) USING iceberg")
    assert "not supported" in str(caught.value).lower() or "DEFAULT" in str(caught.value)


def test_column_def_create_partitioned_and_props(spark: ReparkSession) -> None:
    spark.sql(
        "CREATE TABLE mem.ns.parted (id BIGINT, category STRING) "
        "USING iceberg PARTITIONED BY (category) "
        "TBLPROPERTIES ('write.format.default' = 'parquet')"
    )
    arrow = spark.sql("SELECT * FROM mem.ns.parted").to_arrow()
    assert arrow.num_rows == 0
    assert _schema_names_types(arrow) == [("id", "int64"), ("category", "string")]


def test_ctas_explicit_column_list_still_rejected(spark: ReparkSession) -> None:
    """CTAS + explicit column list stays OUT — pin so it cannot silently drift."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql(
            "CREATE TABLE mem.ns.cl (a INT, b STRING) USING iceberg AS SELECT 1 AS a, 'x' AS b"
        )
    message = str(caught.value)
    assert "Schema may not be specified" in message
    # No orphan table.
    with pytest.raises((AnalysisException, Exception)):
        spark.sql("SELECT * FROM mem.ns.cl").to_arrow()


def test_ref_ddl_create_branch_tag_time_travel(spark: ReparkSession) -> None:
    """CREATE BRANCH/TAG via SQL DDL, then VERSION AS OF the DDL-created refs."""
    spark.sql(
        f"CREATE TABLE {TABLE} USING iceberg TBLPROPERTIES ({COW}) AS "
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)"
    )
    snaps = spark._testing_list_snapshots(TABLE)
    s1 = snaps[-1][0]

    spark.sql(f"INSERT INTO {TABLE} SELECT 4 AS id, 'd' AS name")
    snaps = spark._testing_list_snapshots(TABLE)
    s2 = snaps[-1][0]
    assert s2 != s1

    # Product SQL surface — not only _testing_create_ref.
    spark.sql(f"ALTER TABLE {TABLE} CREATE TAG tag_s1 AS OF VERSION {s1}")
    spark.sql(f"CREATE BRANCH branch_s2 IN {TABLE} AS OF VERSION {s2}")
    # Default AS OF = current.
    spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH cur_default")

    tag_arrow = spark.sql(f"SELECT id FROM {TABLE} VERSION AS OF 'tag_s1' ORDER BY id").to_arrow()
    assert _arrow_ids(tag_arrow) == [1, 2, 3]
    assert _schema_names_types(tag_arrow) == [("id", "int32")]

    branch_arrow = spark.sql(
        f"SELECT id FROM {TABLE} VERSION AS OF 'branch_s2' ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(branch_arrow) == [1, 2, 3, 4]
    cur_arrow = spark.sql(
        f"SELECT id FROM {TABLE} VERSION AS OF 'cur_default' ORDER BY id"
    ).to_arrow()
    assert _arrow_ids(cur_arrow) == [1, 2, 3, 4]

    # DROP main / kind mismatch refuse (wrong-target).
    with pytest.raises((AnalysisException, Exception)) as drop_main:
        spark.sql(f"ALTER TABLE {TABLE} DROP BRANCH main")
    assert "main" in str(drop_main.value).lower()
    with pytest.raises((AnalysisException, Exception)) as kind_err:
        spark.sql(f"ALTER TABLE {TABLE} DROP BRANCH tag_s1")
    assert "tag" in str(kind_err.value).lower() or "branch" in str(kind_err.value).lower()

    spark.sql(f"ALTER TABLE {TABLE} DROP TAG tag_s1")
    with pytest.raises((AnalysisException, Exception)):
        spark.sql(f"SELECT id FROM {TABLE} VERSION AS OF 'tag_s1'").to_arrow()

    spark.sql(f"DROP BRANCH branch_s2 IN {TABLE}")
    current = spark.sql(f"SELECT id FROM {TABLE} ORDER BY id").to_arrow()
    assert _arrow_ids(current) == [1, 2, 3, 4]


def test_ref_ddl_replace_and_retain(spark: ReparkSession) -> None:
    """CREATE OR REPLACE lands; misspelled RETENTION (not RETAIN) still refuses loud."""
    spark.sql(
        f"CREATE TABLE {TABLE} USING iceberg TBLPROPERTIES ({COW}) AS SELECT 1 AS id, 'a' AS name"
    )
    spark.sql(f"ALTER TABLE {TABLE} CREATE OR REPLACE BRANCH audit")
    ids = (
        spark.sql(f"SELECT id FROM {TABLE} VERSION AS OF 'audit' ORDER BY id")
        .to_arrow()
        .column("id")
        .to_pylist()
    )
    assert ids == [1]
    # Misspelled RETENTION (Spark uses RETAIN) must refuse, not silently create.
    with pytest.raises((UnsupportedOperationException, AnalysisException)) as trail:
        spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH other AS OF VERSION 1 RETENTION 7 DAYS")
    assert "not supported" in str(trail.value).lower() or "trailing" in str(trail.value).lower()
