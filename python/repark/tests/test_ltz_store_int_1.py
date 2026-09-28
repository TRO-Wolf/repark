"""WO LTZ-STORE-INT-1: an INT stored into a TIMESTAMP column refuses like Spark.

Spark 4.1.2 refuses ``INSERT INTO sc.ns.l VALUES (0, 1)`` with
``INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`` (recorded
``ntz4-spark.json`` key ``ins_l_int``); RePark silently wrote
``1970-01-01 00:00:00.000001``. DataFusion conforms VALUES literals inside the
``Values`` node before any analyzer rule runs, so the shared store-assignment
matrix never saw the INT. The Spark door now judges VALUES cells against
TIMESTAMP columns through that same matrix before planning.

pins: ltz-store-int-1/C-001
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

FQ = "sc.ns.l"
RECORDED_ERROR = "AnalysisException"
RECORDED_CONDITION = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST"
RECORDED_SQLSTATE = "KD000"
RECORDED_MSG = (
    "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the "
    'table `sc`.`ns`.`l`: Cannot safely cast `c` "INT" to "TIMESTAMP". SQLSTATE: KD000'
)


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-ltz-store-int-1").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    session.sql(f"CREATE TABLE {FQ} (id INT, c TIMESTAMP) USING iceberg")
    return session


def _rows(spark: ReparkSession) -> list[dict[str, object]]:
    return spark.sql(f"SELECT id, c FROM {FQ} ORDER BY id").to_arrow().to_pylist()


def test_values_int_into_timestamp_matches_recorded_spark_refusal(
    spark: ReparkSession,
) -> None:
    """VALUES INT into TIMESTAMP refuses with Spark's recorded class and text."""
    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"INSERT INTO {FQ} VALUES (0, 1)")
    error = caught.value
    assert type(error).__name__ == RECORDED_ERROR
    assert error.getCondition() == RECORDED_CONDITION
    assert error.getSqlState() == RECORDED_SQLSTATE
    assert str(error).splitlines()[0] == f"Error during planning: {RECORDED_MSG}"
    assert _rows(spark) == []


def test_dataframe_append_int_into_timestamp_refuses(
    spark: ReparkSession,
) -> None:
    """A DataFrame append of INT into TIMESTAMP refuses and writes nothing."""
    frame = spark.createDataFrame([(0, 1)], ["id", "c"])
    with pytest.raises(
        AnalysisException, match=r"INCOMPATIBLE_DATA_FOR_TABLE\.CANNOT_SAFELY_CAST"
    ):
        frame.writeTo(FQ).append()
    assert _rows(spark) == []
