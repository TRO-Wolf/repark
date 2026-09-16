"""LIT-DECIMAL-1 pins: F.lit(Decimal) typing and like/ilike escapeChar, both doors.

pins: lit-decimal-1/C-001, C-002, C-003, C-004, C-005, C-006
"""

from __future__ import annotations

import inspect
import json
import re
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException
from repark.spark import functions as F  # noqa: N812
from repark.spark.dataframe.core import DataFrame

_ORACLE_PATH: Path = Path(__file__).with_name("lit_decimal_1_spark_oracle.json")
_LIKE_ROWS: list[tuple[str | None]] = [("a_b",), ("a%b",), ("axb",), (None,)]
_TYPEOF_CALL: re.Pattern[str] = re.compile(r"^SELECT typeof\((?P<inner>.+)\)$")


def _cells(*kinds: str) -> list[dict[str, Any]]:
    """Oracle cells of the given kinds, in fixture order."""
    payload: dict[str, Any] = json.loads(_ORACLE_PATH.read_text())
    return [cell for cell in payload["cells"] if cell["kind"] in kinds]


def _cell_id(cell: dict[str, Any]) -> str:
    """Pytest id for one oracle cell."""
    return cell["id"]


def _facade_expression(cell: dict[str, Any]) -> Any:
    """Replay the recorded facade expression text."""
    return eval(cell["expr"], {"F": F, "Decimal": Decimal})


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """One session per test, stopped on teardown."""
    session = ReparkSession.builder.appName("pytest-lit-decimal-1").getOrCreate()
    yield session
    session.stop()


def _like_frame(spark: ReparkSession) -> DataFrame:
    """The oracle's four-row string frame."""
    return spark.createDataFrame(_LIKE_ROWS, "s string")


def _assert_single_decimal(frame: DataFrame, cell: dict[str, Any]) -> None:
    """Assert the dtype and the numerically-equal collected value of ``v``."""
    assert frame.schema["v"].dataType.simpleString() == cell["dtype"]
    rows = frame.collect()
    assert len(rows) == 1
    assert Decimal(str(rows[0]["v"])) == Decimal(cell["value"])


def _number_format_message(cell: dict[str, Any]) -> str:
    """The ``java.lang.NumberFormatException`` line of the recorded py4j error."""
    match = re.search(r"java\.lang\.NumberFormatException: (.+)", cell["error"])
    assert match is not None
    return match.group(1)


def _sql_cell_frame(spark: ReparkSession, cell: dict[str, Any]) -> DataFrame:
    """Evaluate one SQL cell; a ``typeof`` call translates to a schema assert."""
    match = _TYPEOF_CALL.match(cell["expr"])
    if match is not None:
        return spark.sql(f"SELECT {match.group('inner')}")
    return spark.sql(cell["expr"])


def _sql_cell_dtype(cell: dict[str, Any]) -> str:
    """Expected field type: ``typeof`` cells carry it in ``value``, others in ``schema``."""
    if _TYPEOF_CALL.match(cell["expr"]) is not None:
        return cell["value"]
    return cell["schema"].rsplit(":", 1)[1].rstrip(">")


@pytest.mark.parametrize(
    "cell",
    [
        cell
        for cell in _cells("lit_decimal", "lit_decimal_tuple", "lit_decimal_int")
        if cell["outcome"] == "ok"
    ],
    ids=_cell_id,
)
def test_lit_decimal_cells(spark: ReparkSession, cell: dict[str, Any]) -> None:
    """In-range Decimal literals type and value as recorded. pins: lit-decimal-1/C-001"""
    frame = spark.range(1).select(_facade_expression(cell).alias("v"))
    _assert_single_decimal(frame, cell)
    assert frame.schema["v"].nullable == cell["nullable"]


@pytest.mark.parametrize(
    "cell",
    [
        cell
        for cell in _cells("lit_decimal", "lit_decimal_from_float")
        if cell["outcome"] == "error"
    ],
    ids=_cell_id,
)
def test_lit_decimal_over_precision_raises(spark: ReparkSession, cell: dict[str, Any]) -> None:
    """Past-38-digit Decimals raise Spark's precision condition. pins: lit-decimal-1/C-002"""
    with pytest.raises(PySparkException, match=re.escape(cell["error"])):
        spark.range(1).select(_facade_expression(cell).alias("v")).collect()


@pytest.mark.parametrize("cell", _cells("lit_decimal_special"), ids=_cell_id)
def test_lit_decimal_non_finite_raises(spark: ReparkSession, cell: dict[str, Any]) -> None:
    """Non-finite Decimals raise Spark's NumberFormatException shape. pins: lit-decimal-1/C-003"""
    with pytest.raises(PySparkException, match=re.escape(_number_format_message(cell))):
        spark.range(1).select(_facade_expression(cell).alias("v")).collect()


@pytest.mark.parametrize("cell", _cells("lit_decimal_arith"), ids=_cell_id)
def test_lit_decimal_arith_cells(spark: ReparkSession, cell: dict[str, Any]) -> None:
    """A decimal literal feeds Spark's arithmetic result-type rules. pins: lit-decimal-1/C-004"""
    frame = spark.range(1).select(_facade_expression(cell).alias("v"))
    _assert_single_decimal(frame, cell)


@pytest.mark.parametrize(
    "cell",
    [cell for cell in _cells("sql_decimal_literal") if cell["outcome"] == "ok"],
    ids=_cell_id,
)
def test_sql_decimal_literal_cells(spark: ReparkSession, cell: dict[str, Any]) -> None:
    """SQL-door decimal literals answer the recorded type and value. pins: lit-decimal-1/C-004"""
    frame = _sql_cell_frame(spark, cell)
    assert frame.schema.fields[0].dataType.simpleString() == _sql_cell_dtype(cell)
    if _TYPEOF_CALL.match(cell["expr"]) is not None:
        return
    rows = frame.collect()
    assert len(rows) == 1
    assert Decimal(str(rows[0][0])) == Decimal(cell["value"])


@pytest.mark.parametrize(
    "cell",
    [cell for cell in _cells("sql_decimal_literal") if cell["outcome"] == "error"],
    ids=_cell_id,
)
def test_sql_decimal_literal_over_precision_raises(
    spark: ReparkSession, cell: dict[str, Any]
) -> None:
    """A >38-digit SQL literal raises Spark's precision condition. pins: lit-decimal-1/C-002"""
    with pytest.raises(PySparkException, match=re.escape(cell["error"])):
        spark.sql(cell["expr"]).collect()


def test_like_ilike_signatures_carry_escape_char() -> None:
    """like/ilike take PySpark 4.1.2's optional escapeChar parameter. pins: lit-decimal-1/C-005"""
    for function in (F.like, F.ilike):
        parameter = inspect.signature(function).parameters.get("escapeChar")
        assert parameter is not None and parameter.default is None


@pytest.mark.parametrize(
    "cell",
    [cell for cell in _cells("like_escape") if cell["outcome"] == "ok"],
    ids=_cell_id,
)
def test_like_escape_cells(spark: ReparkSession, cell: dict[str, Any]) -> None:
    """The escape argument answers the recorded boolean column. pins: lit-decimal-1/C-005"""
    frame = _like_frame(spark).select(_facade_expression(cell).alias("v"))
    assert frame.schema["v"].dataType.simpleString() == cell["dtype"]
    assert [str(row["v"]) for row in frame.collect()] == cell["values"]


@pytest.mark.parametrize(
    "cell",
    [cell for cell in _cells("like_escape") if cell["outcome"] == "error"],
    ids=_cell_id,
)
def test_like_escape_bad_escape_raises(spark: ReparkSession, cell: dict[str, Any]) -> None:
    """A non-single-char escape raises INVALID_ESCAPE_CHAR. pins: lit-decimal-1/C-006"""
    with pytest.raises(AnalysisException, match=re.escape(cell["error"])):
        _like_frame(spark).select(_facade_expression(cell).alias("v")).collect()


@pytest.mark.parametrize("cell", _cells("sql_like_escape"), ids=_cell_id)
def test_sql_like_escape_cells(spark: ReparkSession, cell: dict[str, Any]) -> None:
    """The SQL door answers LIKE … ESCAPE and three-arg like(). pins: lit-decimal-1/C-005"""
    frame = spark.sql(cell["expr"])
    assert frame.schema.fields[0].dataType.simpleString() == "boolean"
    assert [str(row[0]) for row in frame.collect()] == cell["values"]
