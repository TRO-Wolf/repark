from __future__ import annotations

import functools
import time
import traceback
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from repark.errors import PySparkException, PySparkTypeError, PySparkValueError
from repark.spark import SparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark._secrets import (
    mask_credentials,
    mask_url_userinfo,
    mask_user_visible,
    scrub_exception,
    scrub_user_failure,
)
from repark.spark.dataframe import udf_bridge
from repark.spark.functions import arrow_udtf, udtf
from repark.spark.window import Window

pa = pytest.importorskip("pyarrow")
pytest.importorskip("pandas")

USERINFO = "u" + ":pw@"
SECRET_URL = "http://" + USERINFO + "127.0.0.1:9/x"
_RAISED: list[BaseException] = []
_TAIL: dict[str, str] = {"text": ""}
SURROGATE_TAIL = " at /data/\udcff"


def _boom(*args: object) -> Any:
    error = ValueError("fetch failed for " + SECRET_URL + _TAIL["text"])
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


class _TaggedGroup(ExceptionGroup):
    pass


def _raised(factory: Callable[[], BaseException]) -> BaseException:
    try:
        raise factory()
    except BaseException as error:
        return error


def _noted(note: str) -> BaseException:
    error = ValueError("plain")
    error.add_note(note)
    return error


def _formatted(error: BaseException) -> str:
    return "".join(traceback.format_exception(error))


@pytest.mark.parametrize(
    "factory",
    [
        lambda: ExceptionGroup("several", [ValueError("fetch " + SECRET_URL), KeyError("k")]),
        lambda: ExceptionGroup("several " + SECRET_URL, [ValueError("plain")]),
        lambda: _TaggedGroup("several " + SECRET_URL, [ValueError("plain")]),
        lambda: BaseExceptionGroup("stop " + SECRET_URL, [KeyboardInterrupt()]),
        lambda: ExceptionGroup("outer", [ExceptionGroup("inner", [ValueError(SECRET_URL)])]),
        lambda: _noted("while fetching " + SECRET_URL),
        lambda: ExceptionGroup("several", [_noted("while fetching " + SECRET_URL)]),
    ],
    ids=["sub", "message", "subclass", "base_group", "nested", "note", "sub_note"],
)
def test_scrub_exception_masks_groups_and_notes(factory: Callable[[], BaseException]) -> None:
    original = _raised(factory)
    before = _formatted(original)
    scrubbed = scrub_exception(original)
    assert USERINFO in before
    assert scrubbed is not original
    assert type(scrubbed) is type(original)
    assert USERINFO not in str(scrubbed)
    assert USERINFO not in _formatted(scrubbed)
    assert "http://u:***@" in _formatted(scrubbed)
    assert scrubbed.__traceback__ is original.__traceback__
    assert _formatted(original) == before
    if isinstance(original, BaseExceptionGroup):
        assert isinstance(scrubbed, BaseExceptionGroup)
        assert len(scrubbed.exceptions) == len(original.exceptions)


def test_scrub_exception_keeps_a_clean_group_and_note() -> None:
    group = _raised(lambda: ExceptionGroup("several", [ValueError("plain"), _noted("a note")]))
    assert scrub_exception(group) is group
    noted = _raised(lambda: _noted("a note"))
    assert scrub_exception(noted) is noted


def _udf_group(value: object) -> str:
    raise ExceptionGroup("several", [ValueError("fetch " + SECRET_URL)])


def _udf_note(value: object) -> str:
    raise _noted("while fetching " + SECRET_URL)


@pytest.mark.parametrize("raiser", [_udf_group, _udf_note], ids=["group", "note"])
def test_dataframe_udf_door_masks_groups_and_notes(raiser: Callable[..., Any]) -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        with pytest.raises(PySparkException) as caught:
            session.range(2).select(F.udf(raiser, "string")("id")).collect()
        assert USERINFO not in _formatted(caught.value)
        assert caught.value.__context__ is None
    finally:
        session.stop()


def test_scrub_exception_walks_a_deep_chain_in_linear_time() -> None:
    root = ValueError("fetch " + SECRET_URL)
    top: BaseException = root
    for index in range(5000):
        link = RuntimeError(str(index))
        link.__cause__ = top
        top = link
    started = time.perf_counter()
    scrubbed = scrub_exception(top)
    assert time.perf_counter() - started < 2.0
    assert scrubbed is not top
    depth = 0
    cursor: BaseException | None = scrubbed
    while cursor is not None and cursor.__cause__ is not None:
        cursor = cursor.__cause__
        depth += 1
    assert depth == 5000
    assert cursor is not root
    assert USERINFO not in str(cursor)
    assert USERINFO in str(root)


class _InterruptWithNew(KeyboardInterrupt):
    def __new__(cls, text: str, code: int) -> _InterruptWithNew:
        return super().__new__(cls, text, code)

    def __init__(self, text: str, code: int) -> None:
        super().__init__(text)


class _NewOverrideError(Exception):
    def __new__(cls, text: str, code: int) -> _NewOverrideError:
        return super().__new__(cls, text, code)

    def __init__(self, text: str, code: int) -> None:
        super().__init__(text)


class _TaggedNewGroup(BaseExceptionGroup):
    def __new__(cls, tag: str, message: str, subs: list[BaseException]) -> _TaggedNewGroup:
        return super().__new__(cls, message, subs)

    def __init__(self, tag: str, message: str, subs: list[BaseException]) -> None:
        super().__init__(message, subs)


class _TaggedNewExceptionGroup(ExceptionGroup):
    def __new__(cls, tag: str, message: str, subs: list[BaseException]) -> _TaggedNewExceptionGroup:
        return super().__new__(cls, message, subs)

    def __init__(self, tag: str, message: str, subs: list[BaseException]) -> None:
        super().__init__(message, subs)


class _SlottedError(Exception):
    __slots__ = ("__private", "url")

    def __init__(self, url: str, code: int) -> None:
        super().__init__(f"connect {url} failed")
        self.url = url
        self.__private = url

    def private(self) -> str:
        return self.__private


class _KeywordCodeError(Exception):
    def __init__(self, message: str, *, code: int, url: str) -> None:
        super().__init__(message)
        self.code = code
        self.url = url


def test_scrub_exception_base_only_stand_in_is_never_an_exception() -> None:
    original = _raised(lambda: _InterruptWithNew("stop " + SECRET_URL, 1))
    scrubbed = scrub_exception(original)
    assert not isinstance(scrubbed, Exception)
    assert isinstance(scrubbed, KeyboardInterrupt)
    assert USERINFO not in _formatted(scrubbed)
    assert "http://u:***@" in str(scrubbed)
    assert scrubbed.__traceback__ is original.__traceback__


def test_scrub_exception_exception_stand_in_is_a_pyspark_exception() -> None:
    original = _raised(lambda: _NewOverrideError("bad " + SECRET_URL, 1))
    scrubbed = scrub_exception(original)
    assert type(scrubbed) is PySparkException
    assert USERINFO not in _formatted(scrubbed)
    assert "http://u:***@" in str(scrubbed)


@pytest.mark.parametrize(
    ("group_class", "subs"),
    [
        ("exception_group", [ValueError("plain")]),
        ("base_group", [ValueError("plain")]),
        ("base_group", [KeyboardInterrupt("stop")]),
    ],
    ids=["exception_group", "base_group_exception_subs", "base_group_base_subs"],
)
def test_scrub_exception_unbuildable_group_keeps_the_exception_split(
    group_class: str, subs: list[BaseException]
) -> None:
    cls = _TaggedNewExceptionGroup if group_class == "exception_group" else _TaggedNewGroup
    original = _raised(lambda: cls("tag", "several " + SECRET_URL, subs))
    scrubbed = scrub_exception(original)
    assert isinstance(scrubbed, Exception) is isinstance(original, Exception)
    assert USERINFO not in _formatted(scrubbed)
    assert "http://u:***@" in _formatted(scrubbed)


def test_scrub_exception_new_copy_carries_slots_masked() -> None:
    original = _raised(lambda: _SlottedError(SECRET_URL, 7))
    scrubbed = scrub_exception(original)
    assert type(scrubbed) is _SlottedError
    assert isinstance(scrubbed, _SlottedError)
    assert scrubbed.url == "http://u:***@127.0.0.1:9/x"
    assert scrubbed.private() == "http://u:***@127.0.0.1:9/x"
    assert USERINFO not in str(scrubbed)
    assert USERINFO in original.url


def test_scrub_exception_new_copy_carries_keyword_only_attributes_masked() -> None:
    original = _raised(lambda: _KeywordCodeError("bad " + SECRET_URL, code=3, url=SECRET_URL))
    scrubbed = scrub_exception(original)
    assert type(scrubbed) is _KeywordCodeError
    assert isinstance(scrubbed, _KeywordCodeError)
    assert scrubbed.code == 3
    assert scrubbed.url == "http://u:***@127.0.0.1:9/x"
    assert USERINFO not in str(scrubbed)
    assert original.url == SECRET_URL


def _dataframe_udf(session: SparkSession) -> None:
    session.range(2).select(F.udf(_boom, "string")("id")).collect()


def _sql_udf(session: SparkSession) -> None:
    session.udf.register("redact_surrogate_boom", _boom, "string")
    session.sql("SELECT redact_surrogate_boom(id) FROM range(2)").collect()


def _raise_surrogate_os_error(*args: object) -> Any:
    error = FileNotFoundError(2, "No such file", "/data/\udcff" + SECRET_URL)
    _RAISED.append(error)
    raise error


def _dataframe_udf_os_error(session: SparkSession) -> None:
    session.range(2).select(F.udf(_raise_surrogate_os_error, "string")("id")).collect()


SURROGATE_DOORS: dict[str, Callable[[SparkSession], None]] = {
    **DOORS,
    "dataframe_udf": _dataframe_udf,
    "sql_udf": _sql_udf,
    "dataframe_udf_os_error": _dataframe_udf_os_error,
}


@pytest.mark.parametrize("door", sorted(SURROGATE_DOORS))
def test_user_callback_door_masks_a_lone_surrogate(
    door: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(_TAIL, "text", SURROGATE_TAIL)
    session = SparkSession.builder.getOrCreate()
    try:
        _RAISED.clear()
        with pytest.raises((PySparkException, PySparkTypeError)) as caught:
            SURROGATE_DOORS[door](session)
        error = caught.value
        assert "\udcff" in str(getattr(_RAISED[-1], "filename", None) or _RAISED[-1])
        assert "http://u:***@" in str(error)
        assert USERINFO not in str(error)
        assert USERINFO not in repr(error)
        assert USERINFO not in _formatted(error)
        assert error.__context__ is None
        assert error.__cause__ is not _RAISED[-1]
    finally:
        session.stop()


def test_scrub_exception_masks_a_lone_surrogate_message() -> None:
    original = _raised(lambda: ValueError("bad \udcff " + SECRET_URL))
    scrubbed = scrub_exception(original)
    assert type(scrubbed) is ValueError
    assert scrubbed is not original
    assert str(scrubbed).startswith("bad ")
    assert "http://u:***@" in str(scrubbed)
    assert USERINFO not in _formatted(scrubbed)
    assert str(original) == "bad \udcff " + SECRET_URL


def test_scrub_exception_masks_an_os_error_with_a_surrogate_filename() -> None:
    original = _raised(lambda: FileNotFoundError(2, "No such file", "/data/\udcff" + SECRET_URL))
    scrubbed = scrub_exception(original)
    assert type(scrubbed) is FileNotFoundError
    assert isinstance(scrubbed, FileNotFoundError)
    assert scrubbed.errno == 2
    assert "http://u:***@" in scrubbed.filename
    assert USERINFO not in _formatted(scrubbed)


def test_scrub_user_failure_masks_a_lone_surrogate() -> None:
    try:
        raise ValueError("fetch \udcff " + SECRET_URL)
    except ValueError as error:
        detail, failure = scrub_user_failure(error)
    assert "http://u:***@" in detail
    assert USERINFO not in detail
    assert USERINFO not in _formatted(failure)


def test_scrub_exception_keeps_a_clean_surrogate_message_identity() -> None:
    original = _raised(lambda: ValueError("plain \udcff text"))
    assert scrub_exception(original) is original


@pytest.mark.parametrize("mask", [mask_credentials, mask_url_userinfo, mask_user_visible])
def test_mask_entry_points_are_total_on_a_lone_surrogate(mask: Callable[[str], object]) -> None:
    assert mask("plain \udcff text") == "plain \udcff text"
    masked = mask("at \udcff " + SECRET_URL)
    assert isinstance(masked, str)
    assert masked.startswith("at ")
    assert USERINFO not in masked
    assert "http://u:***@" in masked


class _ArgsRefusingError(ValueError):
    @property
    def args(self) -> tuple[object, ...]:
        raise RuntimeError("args refuses")


def test_scrub_exception_returns_a_masked_stand_in_when_the_walk_raises() -> None:
    original = _raised(lambda: _ArgsRefusingError("fetch " + SECRET_URL))
    scrubbed = scrub_exception(original)
    assert isinstance(scrubbed, PySparkException)
    assert "http://u:***@" in str(scrubbed)
    assert USERINFO not in _formatted(scrubbed)
    assert scrubbed.__traceback__ is original.__traceback__


class _UrlNote:
    def __str__(self) -> str:
        return "object note " + SECRET_URL


class _RefusingNote:
    def __str__(self) -> str:
        raise RuntimeError("str refuses")


def _with_notes(notes: object) -> BaseException:
    error = ValueError("plain")
    error.__notes__ = notes
    return error


@pytest.mark.parametrize(
    ("notes", "expected"),
    [
        ([_UrlNote()], ["object note http://u:***@127.0.0.1:9/x"]),
        ([b"bytes note " + SECRET_URL.encode()], ["b'bytes note http://u:***@127.0.0.1:9/x'"]),
        ("string notes " + SECRET_URL, "string notes http://u:***@127.0.0.1:9/x"),
        ([_RefusingNote(), "at " + SECRET_URL], ["_RefusingNote", "at http://u:***@127.0.0.1:9/x"]),
    ],
    ids=["object", "bytes", "str_notes", "refusing_str"],
)
def test_scrub_exception_masks_non_str_note_carriers(notes: object, expected: object) -> None:
    original = _raised(lambda: _with_notes(notes))
    assert USERINFO in _formatted(original)
    scrubbed = scrub_exception(original)
    assert scrubbed is not original
    assert type(scrubbed) is ValueError
    assert scrubbed.__notes__ == expected
    assert USERINFO not in _formatted(scrubbed)
    assert original.__notes__ is notes


def test_scrub_exception_masks_tuple_notes() -> None:
    notes = ("tuple note " + SECRET_URL, "plain")
    original = _raised(lambda: _with_notes(notes))
    assert USERINFO in _formatted(original)
    scrubbed = scrub_exception(original)
    assert scrubbed is not original
    assert scrubbed.__notes__ == ["tuple note http://u:***@127.0.0.1:9/x", "plain"]
    assert USERINFO not in _formatted(scrubbed)
    assert original.__notes__ is notes


_INIT_CALLS: list[object] = []


class _RecordingInterrupt(KeyboardInterrupt):
    def __init__(self, message: str, code: int) -> None:
        _INIT_CALLS.append(message)
        super().__init__(message, code)


class _RecordingError(ValueError):
    def __init__(self, message: str) -> None:
        _INIT_CALLS.append(message)
        super().__init__(message)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: _RecordingInterrupt("stop " + SECRET_URL, 1),
        lambda: _RecordingError("v " + SECRET_URL),
    ],
    ids=["base_only", "exception"],
)
def test_scrub_exception_never_reruns_init_with_raw_args(
    factory: Callable[[], BaseException],
) -> None:
    original = _raised(factory)
    _INIT_CALLS.clear()
    scrubbed = scrub_exception(original)
    assert type(scrubbed) is type(original)
    assert USERINFO not in _formatted(scrubbed)
    assert len(_INIT_CALLS) == 1
    assert all(USERINFO not in str(call) for call in _INIT_CALLS)
    assert "http://u:***@" in str(_INIT_CALLS[0])
    assert USERINFO in str(original)
