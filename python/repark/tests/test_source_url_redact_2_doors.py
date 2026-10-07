from __future__ import annotations

import functools
import traceback
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from repark.errors import PySparkException, PySparkTypeError, PySparkValueError
from repark.spark import SparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.dataframe import udf_bridge
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


def _raise_pyspark_url(*args: object) -> Any:
    error = PySparkValueError("bad " + SECRET_URL)
    _RAISED.append(error)
    raise error


def _raise_pyspark_plain(*args: object) -> Any:
    error = PySparkValueError(errorClass="CANNOT_BE_NONE", messageParameters={"arg_name": "x"})
    _RAISED.append(error)
    raise error


def _raise_pyspark_batches(batches: Iterator[Any], *, raiser: Callable[..., Any]) -> Iterator[Any]:
    for batch in batches:
        raiser()
        yield batch


def _pyspark_dataframe_udf(session: SparkSession, raiser: Callable[..., Any]) -> None:
    session.range(2).select(F.udf(raiser, "string")("id")).collect()


def _pyspark_pandas_udf(session: SparkSession, raiser: Callable[..., Any]) -> None:
    session.range(2).select(F.pandas_udf(raiser, "string")("id")).collect()


def _pyspark_apply_in_pandas(session: SparkSession, raiser: Callable[..., Any]) -> None:
    _frame(session).groupBy("k").applyInPandas(raiser, "k long, v double").collect()


def _pyspark_map_in_arrow(session: SparkSession, raiser: Callable[..., Any]) -> None:
    mapper = functools.partial(_raise_pyspark_batches, raiser=raiser)
    _frame(session).mapInArrow(mapper, "k long, v double").collect()


PYSPARK_DOORS: dict[str, Callable[[SparkSession, Callable[..., Any]], None]] = {
    "dataframe_udf": _pyspark_dataframe_udf,
    "pandas_udf": _pyspark_pandas_udf,
    "applyInPandas": _pyspark_apply_in_pandas,
    "mapInArrow": _pyspark_map_in_arrow,
}


@pytest.mark.parametrize("door", sorted(PYSPARK_DOORS))
def test_user_raised_pyspark_exception_is_scrubbed(door: str) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        _RAISED.clear()
        with pytest.raises(PySparkValueError) as caught:
            PYSPARK_DOORS[door](session, _raise_pyspark_url)
        error = caught.value
        assert USERINFO in str(_RAISED[-1])
        assert error is not _RAISED[-1]
        assert type(error) is PySparkValueError
        assert "http://u:***@" in str(error)
        assert USERINFO not in str(error)
        assert USERINFO not in repr(error)
        assert USERINFO not in "".join(traceback.format_exception(error))
        assert error.__context__ is None
    finally:
        session.stop()


@pytest.mark.parametrize("door", sorted(PYSPARK_DOORS))
def test_user_raised_pyspark_exception_without_a_secret_keeps_identity(door: str) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        _RAISED.clear()
        with pytest.raises(PySparkValueError) as caught:
            PYSPARK_DOORS[door](session, _raise_pyspark_plain)
        assert caught.value is _RAISED[-1]
        assert caught.value.getErrorClass() == "CANNOT_BE_NONE"
    finally:
        session.stop()


def _call_python_udf_door(raiser: Callable[..., Any]) -> None:
    batch = pa.RecordBatch.from_pydict({"a": [1, 2]})
    slot = {"input_inter_names": ["a"], "user_func": raiser, "function_name": "f"}
    udf_bridge._run_python_udf_on_batch(batch, slot)


@pytest.mark.parametrize(
    ("raiser", "keeps_identity"),
    [(_raise_pyspark_url, False), (_raise_pyspark_plain, True)],
    ids=["secret", "plain"],
)
def test_dataframe_udf_door_scrubs_a_user_raised_pyspark_exception(
    raiser: Callable[..., Any], keeps_identity: bool
) -> None:
    _RAISED.clear()
    with pytest.raises(PySparkValueError) as caught:
        _call_python_udf_door(raiser)
    assert (caught.value is _RAISED[-1]) is keeps_identity
    assert USERINFO not in "".join(traceback.format_exception(caught.value))
    assert caught.value.__context__ is None


def _boom_series(series: Iterator[Any]) -> Iterator[Any]:
    for values in series:
        _boom()
        yield values


def test_scalar_iter_consume_door_formatted_traceback_is_masked() -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        _RAISED.clear()
        frame = session.createDataFrame([(1.0,), (2.0,)], ["v"])
        boom = F.pandas_udf(_boom_series, "double", "scalar_iter")
        with pytest.raises(PySparkException) as caught:
            frame.select(boom("v")).collect()
        error = caught.value
        assert "while consuming SCALAR_ITER output" in str(error)
        assert "http://u:***@" in str(error)
        assert USERINFO not in repr(error)
        assert USERINFO not in "".join(traceback.format_exception(error))
        assert error.__context__ is None
    finally:
        session.stop()


def test_scalar_iter_consume_site_masks_its_own_detail() -> None:
    _RAISED.clear()
    batch = pa.RecordBatch.from_pydict({"a": [1.0, 2.0]})
    slot = {"input_inter_names": ["a"], "user_func": _boom_series, "function_name": "f"}
    with pytest.raises(PySparkException) as caught:
        udf_bridge._run_pandas_udf_scalar_iter([batch], slot)
    error = caught.value
    assert "while consuming SCALAR_ITER output" in str(error)
    assert "Traceback" in str(error)
    assert USERINFO not in str(error)
    assert USERINFO not in "".join(traceback.format_exception(error))
    assert error.__context__ is None
    assert error.__cause__ is not _RAISED[-1]
