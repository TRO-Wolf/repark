from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException


@pytest.fixture
def spark() -> ReparkSession:
    """A session for the SQL-temp-view pins."""
    return ReparkSession.builder.appName("pytest-attr-view-semantics-1").getOrCreate()


def _seeded(spark: ReparkSession) -> Any:
    """Register ``tv`` and the SQL temp view ``sv`` over it; return the source frame."""
    frame = spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
    frame.createOrReplaceTempView("tv")
    spark.sql("CREATE OR REPLACE TEMP VIEW sv AS SELECT * FROM tv")
    return frame


def test_sql_view_write_parquet_answers(spark: ReparkSession, tmp_path: Path) -> None:
    """V-3a: a parquet write over ``table(sv)`` reads back the source rows."""
    _source = _seeded(spark)
    path = str(tmp_path / "sv_parquet")
    spark.table("sv").write.mode("overwrite").parquet(path)
    got = sorted(tuple(row) for row in spark.read.parquet(path).collect())
    assert got == [(1, 10), (2, 20)]


def test_sql_view_groupby_count_answers(spark: ReparkSession) -> None:
    """V-3b: ``table(sv).groupBy("id").count()`` answers Spark."""
    _source = _seeded(spark)
    got = sorted(tuple(row) for row in spark.table("sv").groupBy("id").count().collect())
    assert got == [(1, 1), (2, 1)]


def test_sql_view_no_internal_error_escapes(spark: ReparkSession, tmp_path: Path) -> None:
    """V-3a, V-3b, and every in-scope sibling answer without an internal error."""
    _source = _seeded(spark)
    spark.table("sv").write.mode("overwrite").parquet(str(tmp_path / "svq"))
    spark.table("sv").groupBy("id").count().collect()
    spark.sql("CREATE OR REPLACE TEMP VIEW sv_again AS SELECT * FROM tv")
    spark.table("sv_again").write.mode("overwrite").parquet(str(tmp_path / "sv_again"))
    spark.table("sv_again").groupBy("id").count().collect()
    spark.sql("CREATE OR REPLACE TEMP VIEW sv_v AS SELECT v FROM tv")
    spark.table("sv_v").write.mode("overwrite").parquet(str(tmp_path / "sv_v"))
    spark.table("sv_v").groupBy("v").count().collect()
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW svj AS "
        "SELECT a.id, a.v, b.v AS w FROM tv a JOIN tv b ON a.id = b.id"
    )
    spark.table("svj").write.mode("overwrite").parquet(str(tmp_path / "svj"))
    spark.table("svj").groupBy("id").count().collect()
    spark.sql("SELECT * FROM sv").groupBy("id").count().collect()
    spark.table("sv").write.mode("overwrite").saveAsTable("default.saved_sv")
    spark.table("sv").write.mode("overwrite").csv(str(tmp_path / "sv_csv"))
    spark.table("sv").write.mode("overwrite").json(str(tmp_path / "sv_json"))


def test_sql_view_v9_unchanged(spark: ReparkSession) -> None:
    """V9 still answers the pre-existing ``[(10,), (20,)]``."""
    source = _seeded(spark)
    got = sorted(tuple(row) for row in spark.table("sv").select(source["v"]).collect())
    assert got == [(10,), (20,)]


def test_sql_view_over_stamped_frame_describe_keeps_comments(spark: ReparkSession) -> None:
    """DESCRIBE of a commented SQL view over stamped ``tv`` keeps names, types and comments."""
    _source = _seeded(spark)
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW sv (id COMMENT 'the id', v COMMENT 'the v') "
        "AS SELECT * FROM tv"
    )
    frame = spark.sql("DESCRIBE sv")
    assert [tuple(row) for row in frame.collect()] == [
        ("id", "bigint", "the id"),
        ("v", "bigint", "the v"),
    ]
    assert frame.dtypes == [
        ("col_name", "string"),
        ("data_type", "string"),
        ("comment", "string"),
    ]


def test_sql_view_over_stamped_frame_cycle_refused(spark: ReparkSession) -> None:
    """A REPLACE closing a cycle through a stamped SQL view is RECURSIVE_VIEW naming the path."""
    _source = _seeded(spark)
    spark.sql("CREATE TEMP VIEW a AS SELECT * FROM tv")
    spark.sql("CREATE TEMP VIEW b AS SELECT * FROM a")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("CREATE OR REPLACE TEMP VIEW a AS SELECT * FROM b")
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == (
        "Error during planning: [RECURSIVE_VIEW] Recursive view `a` detected "
        "(cycle: `a` -> `b` -> `a`). SQLSTATE: 42K0H"
    )
    assert caught.value.getCondition() == "RECURSIVE_VIEW"
    assert caught.value.getSqlState() == "42K0H"
    got = sorted(tuple(row) for row in spark.sql("SELECT * FROM a").collect())
    assert got == [(1, 10), (2, 20)]
