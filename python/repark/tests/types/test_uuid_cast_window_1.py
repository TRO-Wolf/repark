"""WO UUID-CAST-WINDOW-1: the UUID refusal's SQL window counts as Spark counts.

The oracle is ``uuid_cast_window_1_spark_oracle.json`` (Spark 4.1.2, measured
2026-09-27): the four ``CAST('a' AS UUID)`` refusals — the ASCII control, forty
``é`` before the token, the same text on a previous line, and thirty-four
emoji. Every case compares the full refusal message byte for byte: Spark counts
the ``position`` in Unicode scalar values and cuts the window at UTF-16 code
unit offsets, thirty-two units of left context and thirty-six past the token
start, with ``...`` on each cut side.

pins: uuid-cast-window-1/C-001
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import ParseException

ORACLE_PATH: Path = Path(__file__).with_name("uuid_cast_window_1_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))

_CASES: tuple[str, ...] = ("case1_ascii", "case2_nonascii", "case3_newline", "case4_emoji")


def _open() -> ReparkSession:
    """Open a bare session; the refusal needs no catalog."""
    return (
        ReparkSession.builder.appName("uuid-cast-window-1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


@pytest.mark.parametrize("key", _CASES)
def test_uuid_cast_window_matches_spark(key: str) -> None:
    """One oracle refusal replays Spark's class, condition, state and message."""
    case: dict[str, Any] = _ORACLE["cases"][key]
    session = _open()
    try:
        with pytest.raises(ParseException) as caught:
            session.sql(case["sql"]).collect()
    finally:
        session.stop()
    assert type(caught.value) is ParseException
    assert caught.value.getCondition() == case["condition"]
    assert caught.value.getSqlState() == case["sqlstate"]
    assert str(caught.value) == case["message"]
