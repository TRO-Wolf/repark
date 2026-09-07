"""DFCORE-4b eager golden pins: byte-identical repr and HTML across the display move.

Every expected string below was recorded on the pre-slice tree; the move-only
relocation of ``__repr__``, ``_repr_html_``, and the eager-eval conf reads into
``display.py`` must reproduce each one byte-identically, with the same
``count()`` tallies (the DFCORE-6 baseline: the preview still counts to decide
the footer).
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark.dataframe import DataFrame

_REPR_M1_R0 = """\
+-+
|x|
+-+
+-+"""

_REPR_M1_R1 = """\
+-+
|x|
+-+
|0|
+-+"""

_REPR_M1_R2 = """\
+-+
|x|
+-+
|0|
+-+
only showing top 1 row"""

_REPR_M2_R0 = """\
+-+
|x|
+-+
+-+"""

_REPR_M2_R2 = """\
+-+
|x|
+-+
|0|
|1|
+-+"""

_REPR_M2_R3 = """\
+-+
|x|
+-+
|0|
|1|
+-+
only showing top 2 rows"""

_REPR_M20_R0 = """\
+-+
|x|
+-+
+-+"""

_REPR_M20_R20 = """\
+--+
| x|
+--+
| 0|
| 1|
| 2|
| 3|
| 4|
| 5|
| 6|
| 7|
| 8|
| 9|
|10|
|11|
|12|
|13|
|14|
|15|
|16|
|17|
|18|
|19|
+--+"""

_REPR_M20_R21 = """\
+--+
| x|
+--+
| 0|
| 1|
| 2|
| 3|
| 4|
| 5|
| 6|
| 7|
| 8|
| 9|
|10|
|11|
|12|
|13|
|14|
|15|
|16|
|17|
|18|
|19|
+--+
only showing top 20 rows"""

_HTML_M1_R0 = """\
<table border='1'>
<tr><th>x</th></tr>
</table>"""

_HTML_M1_R1 = """\
<table border='1'>
<tr><th>x</th></tr>
<tr><td>0</td></tr>
</table>"""

_HTML_M1_R2 = """\
<table border='1'>
<tr><th>x</th></tr>
<tr><td>0</td></tr>
</table>
only showing top 1 row"""

_HTML_M2_R0 = """\
<table border='1'>
<tr><th>x</th></tr>
</table>"""

_HTML_M2_R2 = """\
<table border='1'>
<tr><th>x</th></tr>
<tr><td>0</td></tr>
<tr><td>1</td></tr>
</table>"""

_HTML_M2_R3 = """\
<table border='1'>
<tr><th>x</th></tr>
<tr><td>0</td></tr>
<tr><td>1</td></tr>
</table>
only showing top 2 rows"""

_HTML_M20_R0 = """\
<table border='1'>
<tr><th>x</th></tr>
</table>"""

_HTML_M20_R21 = """\
<table border='1'>
<tr><th>x</th></tr>
<tr><td>0</td></tr>
<tr><td>1</td></tr>
<tr><td>2</td></tr>
<tr><td>3</td></tr>
<tr><td>4</td></tr>
<tr><td>5</td></tr>
<tr><td>6</td></tr>
<tr><td>7</td></tr>
<tr><td>8</td></tr>
<tr><td>9</td></tr>
<tr><td>10</td></tr>
<tr><td>11</td></tr>
<tr><td>12</td></tr>
<tr><td>13</td></tr>
<tr><td>14</td></tr>
<tr><td>15</td></tr>
<tr><td>16</td></tr>
<tr><td>17</td></tr>
<tr><td>18</td></tr>
<tr><td>19</td></tr>
</table>
only showing top 20 rows"""

_MIA_REPR = """\
+-+
|x|
+-+
|2|
|4|
|6|
+-+"""

_MIA_HTML = """\
<table border='1'>
<tr><th>x</th></tr>
<tr><td>2</td></tr>
<tr><td>4</td></tr>
<tr><td>6</td></tr>
</table>"""

_MIA_SHOW = """\
+---+
| x |
+---+
| 2 |
| 4 |
| 6 |
+---+"""

_MIA_SHOW_VERTICAL = """\
-RECORD 0-
 x | 2
-RECORD 1-
 x | 4
-RECORD 2-
 x | 6"""

_REPR_OFF = "DataFrame[key: bigint, value: string]"

_HTML_OFF = None


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """Fresh session per test (eager eval off unless the test enables it)."""
    session = ReparkSession.builder.appName("dfcore4b-eager").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def _sized_frame(session: ReparkSession, size: int) -> DataFrame:
    """Build a single-column frame with exactly ``size`` rows."""
    return session.createDataFrame([(index,) for index in range(size)], "x INT")


def _double_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    """Double the ``x`` column of every batch (mapInArrow test bridge)."""
    for batch in batches:
        values = [None if item is None else int(item) * 2 for item in batch.column(0).to_pylist()]
        yield pa.record_batch([pa.array(values, type=pa.int32())], names=["x"])


def _enable_eager(session: ReparkSession, max_rows: int) -> None:
    """Turn eager eval on with the given preview cap."""
    session.conf.set("spark.sql.repl.eagerEval.enabled", "true")
    session.conf.set("spark.sql.repl.eagerEval.maxNumRows", str(max_rows))


def test_eager_disabled_repr_and_html(spark: ReparkSession) -> None:
    """Eager off answers the schema form and no HTML (pre-slice output)."""
    frame = spark.createDataFrame([(1, "1"), (22222, "22222")], ("key", "value"))
    assert repr(frame) == _REPR_OFF
    assert frame._repr_html_() is None
    assert frame._repr_html_() == _HTML_OFF


def test_repr_max1_footer_shapes(spark: ReparkSession) -> None:
    """maxNumRows=1: empty, exact, and over-cap previews with the singular footer."""
    _enable_eager(spark, 1)
    try:
        assert repr(_sized_frame(spark, 0)) == _REPR_M1_R0
        assert repr(_sized_frame(spark, 1)) == _REPR_M1_R1
        assert repr(_sized_frame(spark, 2)) == _REPR_M1_R2
    finally:
        spark.conf.set("spark.sql.repl.eagerEval.enabled", "false")


def test_repr_max2_footer_shapes(spark: ReparkSession) -> None:
    """maxNumRows=2: empty, exact, and over-cap previews with the plural footer."""
    _enable_eager(spark, 2)
    try:
        assert repr(_sized_frame(spark, 0)) == _REPR_M2_R0
        assert repr(_sized_frame(spark, 2)) == _REPR_M2_R2
        assert repr(_sized_frame(spark, 3)) == _REPR_M2_R3
    finally:
        spark.conf.set("spark.sql.repl.eagerEval.enabled", "false")


def test_repr_default_max_rows_shapes(spark: ReparkSession) -> None:
    """Default cap (20): empty, exact-20, and 21-row previews."""
    spark.conf.set("spark.sql.repl.eagerEval.enabled", "true")
    try:
        assert repr(_sized_frame(spark, 0)) == _REPR_M20_R0
        assert repr(_sized_frame(spark, 20)) == _REPR_M20_R20
        assert repr(_sized_frame(spark, 21)) == _REPR_M20_R21
    finally:
        spark.conf.set("spark.sql.repl.eagerEval.enabled", "false")


def test_html_footer_shapes(spark: ReparkSession) -> None:
    """HTML previews match at caps 1, 2, and 20, including footers."""
    try:
        _enable_eager(spark, 1)
        assert _sized_frame(spark, 0)._repr_html_() == _HTML_M1_R0
        assert _sized_frame(spark, 1)._repr_html_() == _HTML_M1_R1
        assert _sized_frame(spark, 2)._repr_html_() == _HTML_M1_R2
        _enable_eager(spark, 2)
        assert _sized_frame(spark, 0)._repr_html_() == _HTML_M2_R0
        assert _sized_frame(spark, 2)._repr_html_() == _HTML_M2_R2
        assert _sized_frame(spark, 3)._repr_html_() == _HTML_M2_R3
        _enable_eager(spark, 20)
        assert _sized_frame(spark, 0)._repr_html_() == _HTML_M20_R0
        assert _sized_frame(spark, 21)._repr_html_() == _HTML_M20_R21
    finally:
        spark.conf.set("spark.sql.repl.eagerEval.enabled", "false")


def test_mapinarrow_backed_display_shapes(spark: ReparkSession) -> None:
    """A mapInArrow-backed frame renders repr, HTML, and both show doors."""
    import io
    from contextlib import redirect_stdout

    spark.conf.set("spark.sql.repl.eagerEval.enabled", "true")
    try:
        base = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
        frame = base.mapInArrow(_double_batches, "x INT")
        assert repr(frame) == _MIA_REPR
        assert frame._repr_html_() == _MIA_HTML
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            frame.show()
        assert buffer.getvalue().removesuffix("\n") == _MIA_SHOW
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            frame.show(vertical=True)
        assert buffer.getvalue().removesuffix("\n") == _MIA_SHOW_VERTICAL
    finally:
        spark.conf.set("spark.sql.repl.eagerEval.enabled", "false")


def test_eager_count_tallies(
    spark: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Previews count exactly as on the pre-slice tree (DFCORE-6 baseline)."""
    _enable_eager(spark, 2)
    base = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    frame = base.mapInArrow(_double_batches, "x INT")
    calls: list[str] = []
    original = DataFrame.count

    def counting(self: DataFrame) -> int:
        calls.append("count")
        return original(self)

    monkeypatch.setattr(DataFrame, "count", counting)
    try:
        repr(_sized_frame(spark, 3))
        assert len(calls) == 1
        calls.clear()
        repr(_sized_frame(spark, 2))
        assert len(calls) == 1
        calls.clear()
        _sized_frame(spark, 3)._repr_html_()
        assert len(calls) == 1
        calls.clear()
        repr(frame)
        assert len(calls) == 1
    finally:
        spark.conf.set("spark.sql.repl.eagerEval.enabled", "false")
