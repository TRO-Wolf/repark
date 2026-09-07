"""DFCORE-6 eager-preview pins: footers without count(), bounded bridge peek."""

from __future__ import annotations

import io
from collections.abc import Iterator
from contextlib import redirect_stdout
from unittest.mock import patch

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkException
from repark.spark.dataframe import DataFrame


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """Fresh session per test with eager eval off unless the test enables it."""
    session = ReparkSession.builder.appName("dfcore6-preview").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def _sized_frame(session: ReparkSession, size: int) -> DataFrame:
    """Build a single-column frame with exactly ``size`` rows."""
    return session.createDataFrame([(index,) for index in range(size)], "x INT")


def _enable_eager(session: ReparkSession, max_rows: int) -> None:
    """Turn eager eval on with the given preview cap."""
    session.conf.set("spark.sql.repl.eagerEval.enabled", "true")
    session.conf.set("spark.sql.repl.eagerEval.maxNumRows", str(max_rows))


def _disable_eager(session: ReparkSession) -> None:
    """Turn eager eval back off."""
    session.conf.set("spark.sql.repl.eagerEval.enabled", "false")


def _double_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    """Double the ``x`` column of every batch (mapInArrow test bridge)."""
    for batch in batches:
        values = [None if item is None else int(item) * 2 for item in batch.column(0).to_pylist()]
        yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])


def _raising_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    """Raise on the first row of the first non-empty batch (failing bridge)."""
    for batch in batches:
        for _ in range(batch.num_rows):
            raise ValueError("preview boom")
        yield batch


def _footer_text(max_rows: int) -> str:
    """Render the only-showing footer for the given preview cap."""
    unit = "row" if max_rows == 1 else "rows"
    return f"only showing top {max_rows} {unit}"


def _capture_show_text(frame: DataFrame, n: int, vertical: bool) -> str:
    """Run ``frame.show(n, vertical=...)`` and return stdout sans trailing newline."""
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        frame.show(n, vertical=vertical)
    return buffer.getvalue().removesuffix("\n")


class _RowCountingBridge:
    """Doubling bridge that yields one single-row batch per input row."""

    def __init__(self) -> None:
        """Start with zero yielded rows."""
        self.yielded: list[int | None] = []

    def __call__(self, batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Double each input row and record every yielded output row."""
        for batch in batches:
            column = batch.column(0)
            for index in range(batch.num_rows):
                value = column[index].as_py()
                doubled = None if value is None else int(value) * 2
                self.yielded.append(doubled)
                yield pa.record_batch([pa.array([doubled], type=pa.int32())], names=["x"])


def test_preview_doors_never_count(spark: ReparkSession) -> None:
    """Full plain previews show the footer without calling count()."""
    _enable_eager(spark, 20)
    try:
        frame = _sized_frame(spark, 25)
        cached = _sized_frame(spark, 25).cache()
        real_count = DataFrame.count
        with patch.object(DataFrame, "count", autospec=True) as count_mock:
            count_mock.side_effect = real_count
            assert repr(frame).endswith(_footer_text(20))
            assert count_mock.call_count == 0
            html = frame._repr_html_()
            assert html is not None and html.endswith(_footer_text(20))
            assert count_mock.call_count == 0
            assert _capture_show_text(frame, 20, True).endswith(_footer_text(20))
            assert count_mock.call_count == 0
            assert "only showing top" not in _capture_show_text(frame, 20, False)
            assert count_mock.call_count == 0
            assert repr(cached).endswith(_footer_text(20))
            assert count_mock.call_count == 0
            assert repr(cached) == repr(frame)
    finally:
        _disable_eager(spark)


def test_styled_show_keeps_its_count(spark: ReparkSession) -> None:
    """Polars and duckdb shows still count once for head/tail totals."""
    frame = _sized_frame(spark, 12)
    real_count = DataFrame.count
    try:
        with patch.object(DataFrame, "count", autospec=True) as count_mock:
            count_mock.side_effect = real_count
            for style in ("polars", "duckdb"):
                spark.display_style = style
                before = count_mock.call_count
                _capture_show_text(frame, 20, False)
                assert count_mock.call_count == before + 1
    finally:
        spark.display_style = "spark"


def test_repr_footer_presence_matches_row_count(spark: ReparkSession) -> None:
    """Repr footers appear exactly when rows exceed the cap, plain and bridged."""
    try:
        for max_rows in (1, 20):
            _enable_eager(spark, max_rows)
            for size in (0, max_rows, max_rows + 1):
                plain = _sized_frame(spark, size)
                bridged = plain.mapInArrow(_double_batches, "x INT")
                for frame in (plain, bridged):
                    rendered = repr(frame)
                    if size > max_rows:
                        assert rendered.endswith(_footer_text(max_rows))
                    else:
                        assert "only showing top" not in rendered
    finally:
        _disable_eager(spark)


def test_html_footer_presence_matches_row_count(spark: ReparkSession) -> None:
    """HTML footers appear exactly when rows exceed the cap, plain and bridged."""
    try:
        for max_rows in (1, 20):
            _enable_eager(spark, max_rows)
            for size in (0, max_rows, max_rows + 1):
                plain = _sized_frame(spark, size)
                bridged = plain.mapInArrow(_double_batches, "x INT")
                for frame in (plain, bridged):
                    rendered = frame._repr_html_()
                    assert rendered is not None
                    if size > max_rows:
                        assert rendered.endswith(_footer_text(max_rows))
                    else:
                        assert "only showing top" not in rendered
    finally:
        _disable_eager(spark)


def test_max_num_rows_edge_shapes_preserved(spark: ReparkSession) -> None:
    """Zero, negative, and non-int caps plus truncate behave exactly as today."""
    try:
        session = spark
        session.conf.set("spark.sql.repl.eagerEval.enabled", "true")
        session.conf.set("spark.sql.repl.eagerEval.maxNumRows", "0")
        assert repr(_sized_frame(session, 3)).endswith(_footer_text(0))
        assert "only showing top" not in repr(_sized_frame(session, 0))
        session.conf.set("spark.sql.repl.eagerEval.maxNumRows", "-3")
        assert repr(_sized_frame(session, 3)).endswith(_footer_text(0))
        session.conf.set("spark.sql.repl.eagerEval.maxNumRows", "abc")
        assert repr(_sized_frame(session, 21)).endswith(_footer_text(20))
        assert "only showing top" not in repr(_sized_frame(session, 20))
        session.conf.set("spark.sql.repl.eagerEval.maxNumRows", "1")
        session.conf.set("spark.sql.repl.eagerEval.truncate", "3")
        rendered = repr(_sized_frame(session, 2))
        assert rendered.endswith(_footer_text(1))
        html = _sized_frame(session, 2)._repr_html_()
        assert html is not None and html.endswith(_footer_text(1))
    finally:
        spark.conf.set("spark.sql.repl.eagerEval.truncate", "20")
        _disable_eager(spark)


def test_mapinarrow_preview_bounds_udf_rows(spark: ReparkSession) -> None:
    """Repr and HTML yield at most maxNumRows + 1 rows from the bridge."""
    _enable_eager(spark, 20)
    try:
        for door in ("repr", "html"):
            bridge = _RowCountingBridge()
            frame = _sized_frame(spark, 100).mapInArrow(bridge, "x INT")
            if door == "repr":
                rendered: str | None = repr(frame)
            else:
                rendered = frame._repr_html_()
            assert rendered is not None and rendered.endswith(_footer_text(20))
            assert len(bridge.yielded) <= 21
    finally:
        _disable_eager(spark)


def test_raising_action_surfaces_identically(spark: ReparkSession) -> None:
    """A raising bridge fails every preview door with the same error."""
    _enable_eager(spark, 20)
    try:
        heads: list[str] = []
        for door in ("repr", "html", "show", "vshow"):
            frame = _sized_frame(spark, 5).mapInArrow(_raising_batches, "x INT")
            with pytest.raises(PySparkException, match="preview boom") as caught:
                if door == "repr":
                    repr(frame)
                elif door == "html":
                    frame._repr_html_()
                elif door == "show":
                    _capture_show_text(frame, 20, False)
                else:
                    _capture_show_text(frame, 20, True)
            heads.append(str(caught.value).splitlines()[0])
        assert heads[0] == heads[1] == heads[2] == heads[3]
    finally:
        _disable_eager(spark)


def test_vertical_footer_without_golden(spark: ReparkSession) -> None:
    """Vertical show footers appear exactly when rows exceed n (no golden)."""
    frame = _sized_frame(spark, 5)
    for n, expected in ((0, False), (1, True), (2, True), (4, True), (5, False), (6, False)):
        rendered = _capture_show_text(frame, n, True)
        if expected:
            assert rendered.endswith(_footer_text(n))
        else:
            assert "only showing top" not in rendered
    empty = _sized_frame(spark, 0)
    assert "only showing top" not in _capture_show_text(empty, 2, True)
