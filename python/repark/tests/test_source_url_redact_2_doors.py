from __future__ import annotations

import traceback
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from repark.errors import PySparkException, PySparkTypeError
from repark.spark import SparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.functions import arrow_udtf, udtf
from repark.spark.window import Window

pa = pytest.importorskip("pyarrow")
pytest.importorskip("pandas")

USERINFO = "u" + ":pw@"
SECRET_URL = "http://" + USERINFO + "127.0.0.1:9/x"
_RAISED: list[BaseException] = []


def _boom(*args: object) -> Any:
    error = ValueError("fetch failed for " + SECRET_URL)
    _RAISED.append(error)
    raise error


def _boom_batches(batches: Iterator[Any]) -> Iterator[Any]:
    for batch in batches:
        _boom()
        yield batch


def _boom_record_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    for batch in batches:
        _boom()
        yield batch


@udtf(returnType="x: int")
class _EvalBoom:
    def eval(self, value: object) -> Iterator[tuple[int]]:
        _boom()
        yield (1,)


@udtf(returnType="x: int")
class _StartBoom:
    def start(self) -> None:
        _boom()

    def eval(self, value: object) -> Iterator[tuple[int]]:
        yield (1,)


@udtf(returnType="x: int")
class _TerminateBoom:
    def eval(self, value: object) -> Iterator[tuple[int]]:
        yield (1,)

    def terminate(self) -> None:
        _boom()


@udtf(returnType="x: int")
class _TableEvalBoom:
    def eval(self, row: object) -> Iterator[tuple[int]]:
        _boom()
        yield (1,)


@arrow_udtf(returnType="x: int")
class _ArrowEvalBoom:
    def eval(self, values: object) -> Iterator[Any]:
        _boom()
        yield pa.table({"x": pa.array([1], pa.int32())})


class _BoomDataType:
    def simpleString(self) -> str:  # noqa: N802 — PySpark camelCase
        return _boom()


def _frame(session: SparkSession) -> Any:
    return session.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0)], ["k", "v"])


def _grouped_agg(session: SparkSession) -> None:
    agg = F.pandas_udf(_boom, "double", "grouped_agg")
    _frame(session).groupBy("k").agg(agg("v")).collect()


def _grouped_agg_unbounded_window(session: SparkSession) -> None:
    agg = F.pandas_udf(_boom, "double", "grouped_agg")
    _frame(session).withColumn("a", agg("v").over(Window.partitionBy("k"))).collect()


def _apply_in_pandas(session: SparkSession) -> None:
    _frame(session).groupBy("k").applyInPandas(_boom, "k long, v double").collect()


def _cogroup_apply_in_pandas(session: SparkSession) -> None:
    grouped = _frame(session).groupBy("k")
    grouped.cogroup(_frame(session).groupBy("k")).applyInPandas(_boom, "k long").collect()


def _apply_in_arrow(session: SparkSession) -> None:
    _frame(session).groupBy("k").applyInArrow(_boom, "k long, v double").collect()


def _apply_in_arrow_iterator(session: SparkSession) -> None:
    grouped = _frame(session).groupBy("k")
    grouped.applyInArrow(_boom_record_batches, "k long, v double").collect()


def _cogroup_apply_in_arrow(session: SparkSession) -> None:
    grouped = _frame(session).groupBy("k")
    grouped.cogroup(_frame(session).groupBy("k")).applyInArrow(_boom, "k long").collect()


def _map_in_pandas(session: SparkSession) -> None:
    _frame(session).mapInPandas(_boom_batches, "k long, v double").collect()


def _map_in_arrow(session: SparkSession) -> None:
    _frame(session).mapInArrow(_boom_batches, "k long, v double").collect()


def _map_in_arrow_call(session: SparkSession) -> None:
    _frame(session).mapInArrow(_boom, "k long, v double").collect()


def _udtf_eval(session: SparkSession) -> None:
    _EvalBoom(F.lit(1)).collect()


def _udtf_start(session: SparkSession) -> None:
    _StartBoom(F.lit(1)).collect()


def _udtf_terminate(session: SparkSession) -> None:
    _TerminateBoom(F.lit(1)).collect()


def _udtf_table_eval(session: SparkSession) -> None:
    session.udtf.register("redact_table_boom", _TableEvalBoom)
    _frame(session).createOrReplaceTempView("redact_doors_t")
    session.sql("SELECT * FROM redact_table_boom(TABLE(redact_doors_t) PARTITION BY k)").collect()


def _arrow_udtf_eval(session: SparkSession) -> None:
    _ArrowEvalBoom(F.lit(1)).collect()


def _udf_return_type_simple_string(session: SparkSession) -> None:
    F.udf(len, _BoomDataType())


DOORS: dict[str, Callable[[SparkSession], None]] = {
    "grouped_agg": _grouped_agg,
    "grouped_agg_unbounded_window": _grouped_agg_unbounded_window,
    "applyInPandas": _apply_in_pandas,
    "cogroup_applyInPandas": _cogroup_apply_in_pandas,
    "applyInArrow": _apply_in_arrow,
    "applyInArrow_iterator": _apply_in_arrow_iterator,
    "cogroup_applyInArrow": _cogroup_apply_in_arrow,
    "mapInPandas": _map_in_pandas,
    "mapInArrow": _map_in_arrow,
    "mapInArrow_call": _map_in_arrow_call,
    "udtf_eval": _udtf_eval,
    "udtf_start": _udtf_start,
    "udtf_terminate": _udtf_terminate,
    "udtf_table_eval": _udtf_table_eval,
    "arrow_udtf_eval": _arrow_udtf_eval,
    "udf_return_type_simple_string": _udf_return_type_simple_string,
}


@pytest.mark.parametrize("door", sorted(DOORS))
def test_user_callback_door_formatted_traceback_is_masked(door: str) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        _RAISED.clear()
        with pytest.raises((PySparkException, PySparkTypeError)) as caught:
            DOORS[door](session)
        error = caught.value
        assert _RAISED
        assert USERINFO in str(_RAISED[-1])
        assert "http://u:***@" in str(error)
        assert USERINFO not in str(error)
        assert USERINFO not in repr(error)
        assert USERINFO not in "".join(traceback.format_exception(error))
        assert error.__context__ is None
        assert error.__cause__ is not None
        assert error.__cause__ is not _RAISED[-1]
    finally:
        session.stop()
