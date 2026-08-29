"""MERGE ``WHEN NOT MATCHED`` source-only resolution scope.

Oracle (Spark 4): NOT MATCHED conditions and ``VALUES`` expressions resolve against the SOURCE
plan only; a target-column reference is an ``UNRESOLVED_COLUMN`` analysis error. Evaluating
``t.col`` over the join instead silently disables the insert or inserts NULL. These pins are
red without the source-only insert scope (merge/mod.rs ``insert_sql`` sentinel subquery).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

FQ = "mem.ns.scope_t"
SRC = "mem.ns.scope_s"
COW_PROPS = """
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
"""


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-merge-insert-scope").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _seed(spark: ReparkSession) -> None:
    columns = "id BIGINT, score BIGINT"
    spark.sql(f"CREATE TABLE {FQ} ({columns}) USING iceberg TBLPROPERTIES ({COW_PROPS})")
    spark.sql(f"INSERT INTO {FQ} VALUES (1, 10)")
    spark.sql(f"CREATE TABLE {SRC} ({columns}) USING iceberg TBLPROPERTIES ({COW_PROPS})")
    spark.sql(f"INSERT INTO {SRC} VALUES (7, 3)")


def _rows(spark: ReparkSession) -> list[dict[str, object]]:
    return spark.sql(f"SELECT id, score FROM {FQ} ORDER BY id").to_arrow().to_pylist()


def test_not_matched_condition_rejects_target_column(spark: ReparkSession) -> None:
    """``WHEN NOT MATCHED AND t.score > 0``: loud analysis error, target untouched."""
    _seed(spark)
    with pytest.raises(AnalysisException, match=r"No field named t\.score"):
        spark.sql(
            f"MERGE INTO {FQ} AS t USING {SRC} AS s ON t.id = s.id "
            "WHEN NOT MATCHED AND t.score > 0 THEN INSERT (id, score) VALUES (s.id, s.score)"
        )
    assert _rows(spark) == [{"id": 1, "score": 10}]


def test_not_matched_values_rejects_target_column(spark: ReparkSession) -> None:
    """``VALUES (s.id, t.score)``: loud analysis error, nothing inserted."""
    _seed(spark)
    with pytest.raises(AnalysisException, match=r"No field named t\.score"):
        spark.sql(
            f"MERGE INTO {FQ} AS t USING {SRC} AS s ON t.id = s.id "
            "WHEN NOT MATCHED THEN INSERT (id, score) VALUES (s.id, t.score)"
        )
    assert _rows(spark) == [{"id": 1, "score": 10}]


def test_not_matched_source_references_still_work(spark: ReparkSession) -> None:
    """Positive control: source-qualified condition + VALUES insert the source row."""
    _seed(spark)
    spark.sql(
        f"MERGE INTO {FQ} AS t USING {SRC} AS s ON t.id = s.id "
        "WHEN NOT MATCHED AND s.score > 0 THEN INSERT (id, score) VALUES (s.id, s.score)"
    )
    assert _rows(spark) == [{"id": 1, "score": 10}, {"id": 7, "score": 3}]


def test_not_matched_bare_column_resolves_to_source(spark: ReparkSession) -> None:
    """A bare column name shared by both sides resolves to the SOURCE (Spark source-only
    resolution)."""
    _seed(spark)
    spark.sql(
        f"MERGE INTO {FQ} AS t USING {SRC} AS s ON t.id = s.id "
        "WHEN NOT MATCHED AND score > 0 THEN INSERT (id, score) VALUES (s.id, score)"
    )
    assert _rows(spark) == [{"id": 1, "score": 10}, {"id": 7, "score": 3}]
