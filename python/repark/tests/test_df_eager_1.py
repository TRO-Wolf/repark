"""DF-EAGER-1 step 1: red-first pins for .eager(), .compute() and .lazy().

pins: display-lazy-1/C-002
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException
from repark.spark._temp_views import local_view_name
from repark.spark.dataframe.core import DataFrame
from repark.spark.session import _DISPLAY_STYLE_KEY

_ORDERED_12_SQL = (
    "SELECT id FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12)) "
    "AS t(id) ORDER BY id"
)


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-df-eager-1").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def polars_session() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.config(_DISPLAY_STYLE_KEY, "polars").getOrCreate()
    yield session
    session.stop()


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


def _install_action_spy(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Patch ``DataFrame._action_inner`` to append one entry per call and return the call log."""
    from repark import dataframe as dataframe_module

    calls: list[int] = []
    original = dataframe_module.DataFrame._action_inner

    def counting_action(frame_arg: Any) -> Any:
        calls.append(1)
        return original(frame_arg)

    monkeypatch.setattr(dataframe_module.DataFrame, "_action_inner", counting_action)
    return calls


def test_eager_returns_new_frame_with_eager_shape_rows_and_columns(
    spark: ReparkSession,
) -> None:
    """eager() answers a new frame whose _eager_shape is (rows, column count)."""
    frame = spark.sql("SELECT 1 AS id, 'x' AS label UNION ALL SELECT 2, 'y'")
    rows = frame.count()
    eager = frame.eager()
    assert eager is not frame
    assert eager._eager_shape == (rows, len(frame.columns))


def test_eager_leaves_source_frame_unchanged(spark: ReparkSession) -> None:
    """eager() answers a new frame and leaves the source plan and cache mark untouched."""
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2")
    assert sorted(row.id for row in frame.collect()) == [1, 2]
    eager = frame.eager()
    assert eager is not frame
    assert frame.is_cached is False
    assert sorted(row.id for row in frame.collect()) == [1, 2]


def test_compute_is_eager() -> None:
    """compute() is the same function object as eager() on the DataFrame class."""
    assert DataFrame.compute is DataFrame.eager


def test_lazy_identities_per_d3(spark: ReparkSession) -> None:
    """lazy() answers self on a lazy frame and a shape-less copy on an eager frame."""
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2")
    assert frame.lazy() is frame
    eager = frame.eager()
    rows = eager.count()
    lazy_back = eager.lazy()
    assert lazy_back is not eager
    assert getattr(lazy_back, "_eager_shape", None) is None
    assert lazy_back.count() == rows
    assert eager.count() == rows


def test_repr_of_eager_frame_skips_count_and_matches_lazy_table(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lazy repr is the schema header; eager repr prints the data table with zero counts."""
    calls = _install_count_spy(monkeypatch)
    frame = polars_session.sql(_ORDERED_12_SQL)
    lazy_lines = repr(frame).splitlines()
    assert calls == []
    assert lazy_lines[0].startswith("lazy: ")
    eager = frame.eager()
    calls.clear()
    eager_lines = repr(eager).splitlines()
    assert calls == []
    assert eager_lines[0] == "shape: (12, 1)"
    assert eager_lines[1:5] == lazy_lines[1:5]
    assert eager_lines[-1] == lazy_lines[-1]


def test_eager_frame_survives_csv_source_deletion(
    spark: ReparkSession,
    tmp_path: Path,
) -> None:
    """An eager()ed CSV read answers from the materialised view after the file is deleted."""
    csv_path = tmp_path / "eager_source.csv"
    csv_path.write_text("id\n1\n2\n", encoding="utf-8")
    frame = spark.read.csv(str(csv_path), header=True, inferSchema=True)
    eager = frame.eager()
    csv_path.unlink()
    assert eager.count() == 2
    assert sorted(row.id for row in eager.collect()) == [1, 2]


def test_eager_over_max_bytes_refuses_naming_eager(spark: ReparkSession) -> None:
    """The max_bytes guard refuses eager() naming .eager() and repark.cache.max_bytes."""
    spark.conf.set("repark.cache.max_bytes", "1")
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2")
    with pytest.raises(IllegalArgumentException) as excinfo:
        frame.eager()
    message = str(excinfo.value)
    assert ".eager()" in message
    assert "repark.cache.max_bytes" in message


def test_eager_count_returns_shape_without_action(
    spark: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """count() on an eager frame answers the shape with no engine action."""
    calls = _install_action_spy(monkeypatch)
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2")
    eager = frame.eager()
    calls.clear()
    assert eager.count() == eager._eager_shape[0] == 2
    assert calls == []
    assert frame.count() == 2
    assert calls == [1]


def test_polars_frame_eager_wraps_spark_eager(spark: ReparkSession) -> None:
    """PolarsFrame.eager() wraps the Spark eager frame; collect stays polars."""
    polars = pytest.importorskip("polars")
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2")
    eager = frame.pl.eager()
    assert eager.spark._eager_shape == (2, 1)
    out = eager.collect()
    assert isinstance(out, polars.DataFrame)
    assert sorted(out["id"].to_list()) == [1, 2]


def test_guard_spark_collect_still_returns_rows(spark: ReparkSession) -> None:
    """Keep-green guard: Spark collect() answers Row objects, untouched by DF-EAGER-1."""
    rows = spark.sql("SELECT 1 AS id UNION ALL SELECT 2").collect()
    assert isinstance(rows, list)
    assert sorted(row.id for row in rows) == [1, 2]


def test_guard_pl_collect_still_returns_polars_dataframe(spark: ReparkSession) -> None:
    """Keep-green guard: df.pl.collect() answers a real polars DataFrame, untouched."""
    polars = pytest.importorskip("polars")
    out = spark.sql("SELECT 1 AS id UNION ALL SELECT 2").pl.collect()
    assert isinstance(out, polars.DataFrame)
    assert sorted(out["id"].to_list()) == [1, 2]


def test_checkpointed_eager_lazy_collects_own_rows(spark: ReparkSession) -> None:
    """lazy() on a checkpointed eager frame answers a shape-less copy with the same rows."""
    eager = spark.sql("SELECT 1 AS id UNION ALL SELECT 2").eager().localCheckpoint()
    lazy_back = eager.lazy()
    assert lazy_back is not eager
    assert lazy_back._eager_shape is None
    assert sorted(row.id for row in lazy_back.collect()) == [1, 2]
    assert lazy_back.count() == 2


def test_checkpointed_eager_lazy_ignores_none_view(spark: ReparkSession) -> None:
    """lazy() on a checkpointed eager frame never reads a temp view named none."""
    eager = spark.sql("SELECT 1 AS id UNION ALL SELECT 2").eager()
    spark.sql("SELECT 99 AS id").createOrReplaceTempView("none")
    lazy_back = eager.localCheckpoint().lazy()
    assert sorted(row.id for row in lazy_back.collect()) == [1, 2]


def test_lazy_checkpoint_count_discharges_without_action(
    spark: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """count() after localCheckpoint(eager=False) discharges the checkpoint with no action."""
    calls = _install_action_spy(monkeypatch)
    eager = spark.sql("SELECT 1 AS id UNION ALL SELECT 2").eager()
    old_view = eager._cache_view
    assert old_view is not None
    eager.localCheckpoint(eager=False)
    assert eager._checkpoint_lazy is True
    calls.clear()
    assert eager.count() == 2
    assert calls == []
    assert eager._checkpoint_lazy is False
    assert eager._cache_view is None
    assert local_view_name(old_view) not in spark.list_temp_view_names()


def test_lazy_checkpoint_styled_repr_discharges_without_count_query(
    polars_session: ReparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """repr after localCheckpoint(eager=False) discharges the checkpoint with no count call."""
    calls = _install_count_spy(monkeypatch)
    eager = polars_session.sql(_ORDERED_12_SQL).eager()
    eager.localCheckpoint(eager=False)
    assert eager._checkpoint_lazy is True
    calls.clear()
    rendered = repr(eager)
    assert calls == []
    assert eager._checkpoint_lazy is False
    assert eager._cache_view is None
    assert eager.count() == 12
    assert "12" in rendered
    assert sorted(row.id for row in eager.collect()) == list(range(1, 13))
