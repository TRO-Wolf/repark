"""R-MERGEINTO — ``DataFrame.mergeInto`` builder lowers to existing SQL MERGE.

Oracle surface: live PySpark 4.1.2 ``MergeIntoWriter`` (shapes, no-clause error tag,
return type of ``merge()``). End-to-end pins run against the in-memory Iceberg catalog
(local only — no AWS).
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    PySparkException,
    PySparkTypeError,
    PySparkValueError,
)
from repark.spark.functions import col, lit, when
from repark.spark.merge import MergeIntoWriter

FQ = "mem.ns.entity"
COW_PROPS = """
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
"""
MOR_PROPS = """
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
"""


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mergeinto").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _seed(spark: ReparkSession, *, props: str = COW_PROPS, table: str = FQ) -> None:
    spark.sql("SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'").createOrReplaceTempView("seed")
    spark.sql(f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ({props}) AS SELECT * FROM seed")


def _rows(spark: ReparkSession, table: str = FQ) -> list[dict[str, object]]:
    return spark.sql(f"SELECT id, name FROM {table} ORDER BY id").to_arrow().to_pylist()


def test_merge_into_upsert_equals_sql_merge(spark: ReparkSession) -> None:
    """Builder upsert equals the publish job's SQL-MERGE row set (Arrow path)."""
    _seed(spark)
    updates = spark.sql("SELECT 2 AS id, 'bee' AS name UNION ALL SELECT 3, 'c'")

    # SQL path baseline (same session, same seed pattern re-seeded on sibling table).
    spark.sql("SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'").createOrReplaceTempView(
        "seed_sql"
    )
    spark.sql(
        f"CREATE TABLE mem.ns.entity_sql USING iceberg TBLPROPERTIES ({COW_PROPS}) "
        "AS SELECT * FROM seed_sql"
    )
    updates.createOrReplaceTempView("upd_sql")
    spark.sql(
        "MERGE INTO mem.ns.entity_sql AS target USING upd_sql AS source "
        "ON target.id = source.id "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    sql_rows = _rows(spark, "mem.ns.entity_sql")

    result = (
        updates.mergeInto(FQ, "id").whenMatched().updateAll().whenNotMatched().insertAll().merge()
    )
    assert result is None  # oracle: MergeIntoWriter.merge() -> None
    builder_rows = _rows(spark)

    assert (
        builder_rows
        == sql_rows
        == [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "bee"},
            {"id": 3, "name": "c"},
        ]
    )
    table = spark.sql(f"SELECT id, name FROM {FQ} ORDER BY id").to_arrow()
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("name").type == pa.string()


def test_merge_into_mor_upsert(spark: ReparkSession) -> None:
    """MoR table: builder upsert commits (row set on Arrow path)."""
    table = "mem.ns.mor_entity"
    _seed(spark, props=MOR_PROPS, table=table)
    (
        spark.sql("SELECT 2 AS id, 'bee' AS name UNION ALL SELECT 4, 'd'")
        .mergeInto(table, "id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge()
    )
    assert _rows(spark, table) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "bee"},
        {"id": 4, "name": "d"},
    ]


def test_merge_into_delete_matched(spark: ReparkSession) -> None:
    _seed(spark)
    spark.sql("SELECT 1 AS id, 'x' AS name").mergeInto(FQ, "id").whenMatched().delete().merge()
    assert _rows(spark) == [{"id": 2, "name": "b"}]


def test_merge_into_partial_update_and_insert_dict(spark: ReparkSession) -> None:
    _seed(spark)
    (
        spark.sql("SELECT 2 AS id, 'ZZ' AS name UNION ALL SELECT 9, 'nine'")
        .mergeInto(FQ, "id")
        .whenMatched()
        .update({"name": col("source.name")})
        .whenNotMatched()
        .insert({"id": col("source.id"), "name": lit("N")})
        .merge()
    )
    assert _rows(spark) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "ZZ"},
        {"id": 9, "name": "N"},
    ]


def test_merge_into_column_condition(spark: ReparkSession) -> None:
    _seed(spark)
    condition = col("target.id") == col("source.id")
    (
        spark.sql("SELECT 2 AS id, 'bee' AS name")
        .mergeInto(FQ, condition)
        .whenMatched()
        .updateAll()
        .merge()
    )
    assert _rows(spark) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "bee"},
    ]


def test_merge_into_temp_view_cleaned_up(spark: ReparkSession) -> None:
    """Generated ``__repark_merge_src_*`` view is dropped after success and after failure."""
    _seed(spark)
    spark.sql("SELECT 3 AS id, 'c' AS name").mergeInto(
        FQ, "id"
    ).whenNotMatched().insertAll().merge()
    # Failure path must also clean up (finally).
    with pytest.raises(PySparkException):
        (
            spark.sql("SELECT 1 AS id, 'z' AS name")
            .mergeInto("mem.ns.__does_not_exist__", "id")
            .whenMatched()
            .updateAll()
            .merge()
        )
    spark._ensure_information_schema()
    views = (
        spark.sql("SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'")
        .to_arrow()
        .to_pylist()
    )
    leaked = [
        row["table_name"]
        for row in views
        if str(row["table_name"]).startswith("__repark_merge_src_")
    ]
    assert leaked == []


def test_merge_into_no_clauses_raises(spark: ReparkSession) -> None:
    """Oracle: live 4.1.2 raises SparkRuntimeException [NO_MERGE_ACTION_SPECIFIED].

    repark maps the same tag onto AnalysisException (no SparkRuntimeException class).
    """
    _seed(spark)
    writer = spark.sql("SELECT 1 AS id, 'a' AS name").mergeInto(FQ, "id")
    assert isinstance(writer, MergeIntoWriter)
    with pytest.raises(AnalysisException, match="NO_MERGE_ACTION_SPECIFIED"):
        writer.merge()


def test_merge_into_not_matched_by_source_engine_rejects(spark: ReparkSession) -> None:
    """Surface accepts whenNotMatchedBySource; engine rejects (not_matched_by_source_rejected)."""
    _seed(spark)
    with pytest.raises(PySparkException, match=r"NOT MATCHED BY SOURCE|not supported"):
        (
            spark.sql("SELECT 1 AS id, 'a' AS name")
            .mergeInto(FQ, "id")
            .whenNotMatchedBySource()
            .delete()
            .merge()
        )


def test_merge_into_type_errors(spark: ReparkSession) -> None:
    df = spark.sql("SELECT 1 AS id, 'a' AS name")
    with pytest.raises(PySparkTypeError, match="table"):
        df.mergeInto("", "id")  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match="condition"):
        df.mergeInto(FQ, 123)  # type: ignore[arg-type]
    with pytest.raises(PySparkTypeError, match="assignments"):
        df.mergeInto(FQ, "id").whenMatched().update("nope")  # type: ignore[arg-type]
    with pytest.raises(PySparkValueError, match="empty"):
        df.mergeInto(FQ, "id").whenMatched().update({})


def test_merge_into_with_schema_evolution_fails_loud(spark: ReparkSession) -> None:
    """Schema evolution is unsupported — refuse rather than silent no-op."""
    from repark.errors import UnsupportedOperationException

    df = spark.sql("SELECT 1 AS id, 'a' AS name")
    writer = df.mergeInto(FQ, "id")
    with pytest.raises(UnsupportedOperationException, match="withSchemaEvolution"):
        writer.withSchemaEvolution()


def test_merge_into_render_sql_shape() -> None:
    """Unit-level SQL render (no engine) — clause order and star forms."""
    # Lightweight: exercise private render via a stub-free construction path is hard;
    # pin _on_sql sugar and assignment quoting instead.
    from repark.spark.functions import col, lit
    from repark.spark.merge import _column_sql, _on_sql, _quote_assign_target

    assert _on_sql("id") == 'target."id" = source."id"'
    assert _on_sql("  name  ") == 'target."name" = source."name"'
    assert _quote_assign_target("name") == "name"
    assert _quote_assign_target("weird-name") == '"weird-name"'
    # != / CASE / coalesce must quote string literals for MERGE embed.
    assert "'bee'" in _column_sql(col("source.name") != lit("bee"))
    assert "'x'" in _column_sql(when(col("id") > 0, lit("x")).otherwise(lit("y")))
    assert "'2020-01-01'" in _column_sql(lit("2020-01-01").cast("date"))
    from repark.spark.functions import concat

    concat_sql = _column_sql(concat(lit("a"), lit("b")))
    assert "'a'" in concat_sql
    assert "IS NULL" in concat_sql  # null-propagation guard for MERGE
