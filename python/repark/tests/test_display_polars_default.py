"""DISPLAY-POLARS-1 step 2 pins: the styled polars show pays one fetch for small frames.

pins: display-polars-1/C-003
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from contextlib import redirect_stdout
from typing import Any, cast

import pytest

from repark import ReparkSession
from repark.spark.session import _DISPLAY_STYLE_KEY

_ORDERED_7_SQL = """\
SELECT id FROM (VALUES (1), (2), (3), (4), (5), (6), (7)) AS t(id) ORDER BY id
"""

_ORDERED_12_SQL = """\
SELECT id FROM (VALUES
 (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12)
) AS t(id) ORDER BY id
"""


@pytest.fixture
def polars_session() -> Iterator[ReparkSession]:
    """Fresh session pinned to the polars display style via builder config."""
    session = ReparkSession.builder.config(_DISPLAY_STYLE_KEY, "polars").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def _capture_show(frame: object, *args: object, **kwargs: object) -> str:
    """Run ``frame.show(...)`` and return stdout without the trailing newline."""
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        cast(Any, frame).show(*args, **kwargs)
    return buffer.getvalue().removesuffix("\n")


def _install_count_spy(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Patch ``DataFrame.count`` to append one entry per call and return the call log."""
    from repark import dataframe as dataframe_module

    calls: list[int] = []
    original = dataframe_module.DataFrame.count

    def counting_count(frame_arg: Any) -> int:
        calls.append(1)
        return cast(int, original(frame_arg))

    monkeypatch.setattr(dataframe_module.DataFrame, "count", counting_count)
    return calls


def test_small_frame_renders_without_count(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 7-row frame renders whole through the probe fetch with no count and no tail fetch."""
    calls = _install_count_spy(monkeypatch)
    frame = polars_session.sql(_ORDERED_7_SQL)
    out = _capture_show(frame, truncate=False)
    assert calls == []
    assert out.startswith("shape: (7, 1)")
    assert "│ 1   │" in out
    assert "│ 7   │" in out
    assert "│ …   │" not in out


def test_large_frame_counts_once(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 12-row frame pays exactly one count, one tail fetch, and keeps the ellipsis shape."""
    calls = _install_count_spy(monkeypatch)
    frame = polars_session.sql(_ORDERED_12_SQL)
    out = _capture_show(frame, truncate=False)
    assert calls == [1]
    assert out.startswith("shape: (12, 1)")
    assert "│ 1   │" in out
    assert "│ 12  │" in out
    assert "│ …   │" in out
    assert "│ 6   │" not in out
    assert "│ 7   │" not in out
