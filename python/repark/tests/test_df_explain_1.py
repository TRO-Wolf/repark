"""DF-EXPLAIN-1 step 1: red-first pins for explain printing plan text, not Row reprs."""

from __future__ import annotations

import contextlib
import io
from collections.abc import Iterator

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812
from repark.errors import PySparkValueError
from repark.spark.dataframe.core import DataFrame

_LOGICAL_HEADER: str = "== Optimized Logical Plan =="
_PHYSICAL_HEADER: str = "== Physical Plan =="
_MODE_NAMES: tuple[str, ...] = ("simple", "extended", "formatted", "cost", "codegen")


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-df-explain-1").getOrCreate()
    yield session
    session.stop()


def _build_plan_frame(spark: ReparkSession) -> DataFrame:
    """Build the select + filter + withColumn frame the plan pins render."""
    return (
        spark.sql("SELECT id, id * 2 AS doubled FROM (VALUES (1), (2), (3)) t(id)")
        .filter(F.col("doubled") > 2)
        .withColumn("tripled", F.col("doubled") * 3)
    )


def _refuse_collect(_frame: DataFrame) -> None:
    raise AssertionError("DataFrame.collect must not run during explain")


def test_explain_prints_plan_text_without_row_repr(spark: ReparkSession) -> None:
    """Smoke: explain() prints plan text and never a Row repr."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        _build_plan_frame(spark).explain()
    output = buffer.getvalue()
    assert output.strip() != ""
    assert "Row(" not in output


def test_explain_text_carries_the_physical_plan_header(spark: ReparkSession) -> None:
    """The rendered string carries Spark's physical-plan header."""
    assert _PHYSICAL_HEADER in _build_plan_frame(spark)._explain_text()


def test_explain_text_spans_at_least_three_plan_lines(spark: ReparkSession) -> None:
    """The select + filter + withColumn plan renders at least three lines."""
    text = _build_plan_frame(spark)._explain_text()
    assert len(text.splitlines()) >= 3


def test_explain_extended_lists_logical_plan_before_physical(spark: ReparkSession) -> None:
    """extended renders the optimized logical header before the physical one."""
    text = _build_plan_frame(spark)._explain_text(True)
    assert _LOGICAL_HEADER in text
    assert _PHYSICAL_HEADER in text
    assert text.index(_LOGICAL_HEADER) < text.index(_PHYSICAL_HEADER)


def test_explain_formatted_carries_datafusion_tree_glyphs(spark: ReparkSession) -> None:
    """formatted renders DataFusion's box-drawing tree glyphs."""
    text = _build_plan_frame(spark)._explain_text(mode="formatted")
    assert "┌" in text
    assert "└" in text


def test_explain_unknown_mode_raises_naming_the_five_modes(spark: ReparkSession) -> None:
    """An unknown mode string raises PySparkValueError naming the five modes."""
    with pytest.raises(PySparkValueError) as caught:
        _build_plan_frame(spark).explain(mode="bogus")
    message = str(caught.value)
    for mode_name in _MODE_NAMES:
        assert mode_name in message


def test_explain_does_not_invoke_collect(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """explain() renders the plan without invoking DataFrame.collect."""
    frame = _build_plan_frame(spark)
    monkeypatch.setattr(DataFrame, "collect", _refuse_collect)
    frame.explain()
