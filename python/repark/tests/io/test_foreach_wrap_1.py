from __future__ import annotations

import json
import traceback
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from repark.spark import SparkSession

USERINFO = "u" + ":pw@"
SECRET_URL = "http://" + USERINFO + "127.0.0.1:9/x"
PLAIN_TEXT = "fetch failed for row 7 at /data/x"
SURROGATE_TAIL = " at /data/\udcff"
_RAISED: list[BaseException] = []
_TEXT: dict[str, str] = {"message": ""}
_ORACLE: dict[str, Any] = {
    cell["cell"]: cell["error"]
    for cell in json.loads(
        (
            Path(__file__).resolve().parents[2]
            / "repark-parity"
            / "tests"
            / "live_spark"
            / "fw1_callback_oracle.json"
        ).read_text(encoding="utf-8")
    )["cells"]
}


def _boom(*args: object) -> Any:
    error = ValueError(_TEXT["message"])
    _RAISED.append(error)
    raise error


def _boom_partition(rows: Iterator[Any]) -> None:
    for _row in rows:
        _boom()


def _foreach(session: SparkSession) -> None:
    _frame(session).foreach(_boom)


def _foreach_partition(session: SparkSession) -> None:
    _frame(session).foreachPartition(_boom_partition)


def _frame(session: SparkSession) -> Any:
    return session.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0)], ["k", "v"])


DOORS: dict[str, Callable[[SparkSession], None]] = {
    "foreach": _foreach,
    "foreachPartition": _foreach_partition,
}


def _raise_through(door: str, message: str) -> BaseException:
    session = SparkSession.builder.getOrCreate()
    try:
        _RAISED.clear()
        _TEXT["message"] = message
        with pytest.raises(ValueError) as caught:
            DOORS[door](session)
        assert _RAISED
        return caught.value
    finally:
        session.stop()


@pytest.mark.parametrize("tail", ["", SURROGATE_TAIL], ids=["plain", "surrogate"])
@pytest.mark.parametrize("door", sorted(DOORS))
def test_foreach_door_raises_a_masked_copy_of_the_user_class(door: str, tail: str) -> None:
    error = _raise_through(door, "fetch failed for " + SECRET_URL + tail)
    assert USERINFO in str(_RAISED[-1])
    assert type(error) is ValueError
    assert error is not _RAISED[-1]
    assert not hasattr(error, "getErrorClass")
    assert "http://u:***@" in str(error)
    assert USERINFO not in str(error)
    assert USERINFO not in repr(error)
    assert USERINFO not in "".join(traceback.format_exception(error))
    assert error.__context__ is None
    assert error.__cause__ is None
    assert _ORACLE[f"FW1-{door}"]["secret_in_str"] is True
    assert _ORACLE[f"FW1-{door}"]["has_get_error_class"] is False


@pytest.mark.parametrize("door", sorted(DOORS))
def test_foreach_door_keeps_a_credential_free_error(door: str) -> None:
    error = _raise_through(door, PLAIN_TEXT)
    assert error is _RAISED[-1]
    assert str(error) == PLAIN_TEXT
    assert error.args == (PLAIN_TEXT,)
    assert error.__context__ is None


def test_transform_passes_the_user_exception_through() -> None:
    session = SparkSession.builder.getOrCreate()
    try:
        _RAISED.clear()
        _TEXT["message"] = "fetch failed for " + SECRET_URL
        with pytest.raises(ValueError) as caught:
            _frame(session).transform(_boom)
        assert caught.value is _RAISED[-1]
        assert USERINFO in str(caught.value)
        cell = _ORACLE["FW1-transform"]
        assert cell["is_original"] is True
        assert cell["class"] == "builtins.ValueError"
        assert cell["secret_in_str"] is True
    finally:
        session.stop()
