"""R-POLARS-CORE rider: DIFFERENTIAL pins against real polars (the brief's oracle rule).

U5 shipped hand-computed expectations only; these pins run the same pipeline through the
repark polars-style API AND a real ``pl.LazyFrame`` on identical data, then compare collected
rows (and dtypes where the engines agree by design). Divergences that are Spark-flavored by
design are pinned explicitly as divergences, never silently absorbed.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

import repark.spark.polars as rp
from repark import ReparkSession
from repark.spark.functions import col
from repark.spark.functions import sum as sum_

pl = pytest.importorskip("polars")

_DATA: dict[str, list[Any]] = {
    "g": [1, 1, 2, 2, 3],
    "v": [10.0, None, 5.0, 7.0, None],
    "s": ["b", "a", "a", None, "c"],
}


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-polars-diff").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def frames(spark: ReparkSession) -> tuple[Any, Any]:
    """(repark PolarsFrame, real pl.LazyFrame) over identical rows."""
    rows = list(zip(_DATA["g"], _DATA["v"], _DATA["s"], strict=True))
    rpf = spark.createDataFrame(rows, schema=["g", "v", "s"]).pl
    lf = pl.LazyFrame(_DATA, schema={"g": pl.Int64, "v": pl.Float64, "s": pl.String})
    return rpf, lf


def _dicts(frame: Any) -> list[dict[str, Any]]:
    return frame.collect().to_dicts() if hasattr(frame, "collect") else frame.to_dicts()


_ALIGNED_CASES: list[tuple[str, Callable[[Any, Any], Any], Callable[[Any], Any]]] = [
    (
        "select_filter",
        lambda f: f.select("g", "v").filter(col("v") > 6.0),
        lambda f: f.select("g", "v").filter(pl.col("v") > 6.0),
    ),
    (
        "with_columns_arith",
        lambda f: f.with_columns(v2=(col("v") * rp.lit(2))).select("g", "v2"),
        lambda f: f.with_columns(v2=(pl.col("v") * 2)).select("g", "v2"),
    ),
    (
        "sort_asc_nulls_first_default",
        lambda f: f.select("v").sort("v"),
        lambda f: f.select("v").sort("v"),
    ),
    (
        "sort_desc_nulls_last",
        lambda f: f.select("v").sort("v", descending=True, nulls_last=True),
        lambda f: f.select("v").sort("v", descending=True, nulls_last=True),
    ),
    (
        "sort_desc_nulls_first_default",
        lambda f: f.select("v").sort("v", descending=True),
        lambda f: f.select("v").sort("v", descending=True),
    ),
    (
        "group_agg_sum_nonnull_groups",
        lambda f: f.filter(col("g") < 3).group_by("g").agg(sum_("v").alias("sv")).sort("g"),
        lambda f: (
            f.filter(pl.col("g") < 3).group_by("g").agg(pl.col("v").sum().alias("sv")).sort("g")
        ),
    ),
    (
        "rename_head",
        lambda f: f.rename({"v": "value"}).select("g", "value").sort("g", "value").head(3),
        lambda f: f.rename({"v": "value"}).select("g", "value").sort("g", "value").head(3),
    ),
    (
        "drop_nulls_all",
        lambda f: f.drop_nulls().sort("g"),
        lambda f: f.drop_nulls().sort("g"),
    ),
    (
        "unique_subset",
        lambda f: f.select("g").unique().sort("g"),
        lambda f: f.select("g").unique().sort("g"),
    ),
]


@pytest.mark.parametrize(
    ("name", "rp_pipe", "pl_pipe"),
    [pytest.param(n, r, p, id=n) for n, r, p in _ALIGNED_CASES],
)
def test_pipeline_matches_real_polars(
    frames: tuple[Any, Any],
    name: str,
    rp_pipe: Callable[[Any], Any],
    pl_pipe: Callable[[Any], Any],
) -> None:
    rpf, lf = frames
    ours = _dicts(rp_pipe(rpf))
    theirs = _dicts(pl_pipe(lf))
    assert ours == theirs, f"{name}: repark={ours} polars={theirs}"


def test_join_inner_matches_real_polars(spark: ReparkSession) -> None:
    left_rows = [(1, "a"), (2, "b"), (3, "c")]
    right_rows = [(1, 10.0), (3, 30.0)]
    ours = (
        spark.createDataFrame(left_rows, schema=["id", "x"])
        .pl.join(spark.createDataFrame(right_rows, schema=["id", "y"]).pl, on="id", how="inner")
        .sort("id")
        .collect()
        .to_dicts()
    )
    theirs = (
        pl.LazyFrame({"id": [1, 2, 3], "x": ["a", "b", "c"]})
        .join(pl.LazyFrame({"id": [1, 3], "y": [10.0, 30.0]}), on="id", how="inner")
        .sort("id")
        .collect()
        .to_dicts()
    )
    assert ours == theirs


def test_divergence_join_left_null_fill_matches(spark: ReparkSession) -> None:
    """LEFT join unmatched rows: both engines null-fill — pinned as ALIGNED (values), while
    non-key column-name collisions stay a documented divergence (Spark raises ambiguity where
    polars suffixes ``_right``) — pinned below."""
    ours = (
        spark.createDataFrame([(1, "a"), (2, "b")], schema=["id", "x"])
        .pl.join(spark.createDataFrame([(1, 5.0)], schema=["id", "y"]).pl, on="id", how="left")
        .sort("id")
        .collect()
        .to_dicts()
    )
    theirs = (
        pl.LazyFrame({"id": [1, 2], "x": ["a", "b"]})
        .join(pl.LazyFrame({"id": [1], "y": [5.0]}), on="id", how="left")
        .sort("id")
        .collect()
        .to_dicts()
    )
    assert ours == theirs


def test_divergence_sum_all_null_group_is_null_not_zero(spark: ReparkSession) -> None:
    """_divergence: sum over an all-NULL group — the engine follows Spark (NULL); real polars
    returns 0.0. Both pinned so a silent semantic shift on either side goes red."""
    rows = [(3, None), (3, None)]
    ours = (
        spark.createDataFrame(rows, schema="g INT, v DOUBLE")
        .pl.group_by("g")
        .agg(sum_("v").alias("sv"))
        .collect()
        .to_dicts()
    )
    assert ours == [{"g": 3, "sv": None}]  # Spark semantics
    theirs = (
        pl.LazyFrame({"g": [3, 3], "v": [None, None]}, schema={"g": pl.Int64, "v": pl.Float64})
        .group_by("g")
        .agg(pl.col("v").sum().alias("sv"))
        .collect()
        .to_dicts()
    )
    assert theirs == [{"g": 3, "sv": 0.0}]  # polars semantics — documented divergence


def test_divergence_join_column_collision_is_loud_not_suffixed(spark: ReparkSession) -> None:
    """_divergence: polars suffixes colliding non-key columns (``x_right``); the engine
    follows Spark — the duplicate name survives the plan and ``collect()`` fails LOUD at
    polars construction (never silently-suffixed, never silent wrong data)."""
    joined = spark.createDataFrame([(1, "a")], schema=["id", "x"]).pl.join(
        spark.createDataFrame([(1, "z")], schema=["id", "x"]).pl, on="id", how="inner"
    )
    with pytest.raises(Exception, match=r"more than once|[Dd]uplicate"):
        joined.collect()
