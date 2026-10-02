from __future__ import annotations

from unittest.mock import patch

import pytest

from repark import ReparkSession, _native
from repark.errors import AnalysisException
from repark.spark import column_fields as _column_fields
from repark.spark import functions as F  # noqa: N812
from repark.spark.column import Column
from repark.spark.dataframe import DataFrame
from repark.spark.dataframe.join_attr_tokens import _ATTR_TOKEN_RE
from repark.spark.dataframe.replace_expr import _bind_qualified_column
from repark.spark.session.session_configuration import (
    SPARK_SQL_FAIL_AMBIGUOUS_SELF_JOIN_KEY,
    SPARK_SQL_SELF_JOIN_AUTO_RESOLVE_KEY,
)

_FAIL_KEY = SPARK_SQL_FAIL_AMBIGUOUS_SELF_JOIN_KEY
_AUTO_KEY = SPARK_SQL_SELF_JOIN_AUTO_RESOLVE_KEY


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-attr-id-1-sj2").getOrCreate()


def _frame(spark: ReparkSession) -> DataFrame:
    return spark.createDataFrame([(1, 2), (2, 1), (3, 3)], ["x", "y"])


def _token_frame(token: str) -> int:
    match = _ATTR_TOKEN_RE.search(token)
    assert match is not None
    return int(match.group(2))


def test_sj2_tokens_carry_the_birth_node_id(spark: ReparkSession) -> None:
    """Every token-bearing bind site renders ``F<id>`` of its birth node."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = _frame(spark)
    node_id = frame._frame_node.id
    assert _token_frame(frame["x"].join_sql_part()) == node_id
    assert _token_frame(frame.x.join_sql_part()) == node_id
    joined = frame.join(frame.alias("r"), frame.x == frame.alias("r").x)
    first = joined._iter_bound_columns()[0]
    assert _token_frame(first.join_sql_part()) == joined._frame_node.id
    twins = frame.select("x", "x")
    twin_first = twins._iter_bound_columns()[0]
    assert _token_frame(twin_first.join_sql_part()) == twins._frame_node.id
    sort_bound = _column_fields._resolve_sort_name(frame, "x")
    assert _token_frame(sort_bound.join_sql_part()) == node_id
    child = frame.filter("x > 0")
    rebound = _column_fields._bind_stable_id_column(child, frame["x"])
    assert rebound is not None
    assert _token_frame(rebound.join_sql_part()) == child._frame_node.id
    assert _token_frame(frame["x"].alias("renamed").join_sql_part()) == node_id
    assert _token_frame(frame["x"].desc().join_sql_part()) == node_id


def test_sj2_frameless_columns_render_f0(spark: ReparkSession) -> None:
    """A token with no birth frame renders ``F0``; free names carry no id."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = _frame(spark)
    assert _column_fields._column_frame_id(F.col("x")) is None
    assert _column_fields._column_frame_id(F.lit(1)) is None
    assert "__REPARK_ATTR_" not in F.col("x").join_sql_part()
    qualified = _bind_qualified_column(frame, "x", "l", 0)
    assert qualified._attr_id is not None
    assert _token_frame(qualified.join_sql_part()) == 0
    bare = Column(_native.PyColumn.column("x"), attr_id="abc123")
    assert bare.join_sql_part() == "__REPARK_ATTR_abc123__F0____"


def test_sj2_frame_node_ids_derive_and_checkpoint_reroots(spark: ReparkSession) -> None:
    """Children mint fresh node ids; checkpoint returns a fresh root."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = _frame(spark)
    root_id = frame._frame_node.id
    assert root_id >= 1
    child = frame.filter("x > 0")
    assert child._frame_node.id != root_id
    unioned = frame.union(frame)
    assert unioned._frame_node.id != root_id
    assert unioned._frame_node.id != child._frame_node.id
    marked = frame.checkpoint(False)
    assert marked._frame_node.id != root_id
    assert [tuple(row) for row in marked.collect()] == [(1, 2), (2, 1), (3, 3)]


def test_sj2_self_join_confs_reach_native_config(spark: ReparkSession) -> None:
    """Both self-join keys forward through the native runtime setter."""
    with patch.object(_native, "set_runtime_config", wraps=_native.set_runtime_config) as recorded:
        spark.conf.set(_FAIL_KEY, "false")
        spark.conf.set(_AUTO_KEY, "false")
    forwarded = [(call.args[1], call.args[2]) for call in recorded.call_args_list]
    assert (_FAIL_KEY, "false") in forwarded
    assert (_AUTO_KEY, "false") in forwarded
    assert spark.conf.get(_FAIL_KEY) == "false"
    assert spark.conf.get(_AUTO_KEY) == "false"


def test_sj2_self_join_confs_default_true_and_refuse_loud(spark: ReparkSession) -> None:
    """Unset keys read ``true``; invalid values refuse before storing."""
    assert spark.conf.get(_FAIL_KEY) == "true"
    assert spark.conf.get(_AUTO_KEY) == "true"
    with pytest.raises(Exception, match="INVALID_CONF_VALUE"):
        spark.conf.set(_FAIL_KEY, "maybe")
    with pytest.raises(Exception, match="INVALID_CONF_VALUE"):
        spark.conf.set(_AUTO_KEY, "maybe")
    assert spark.conf.get(_FAIL_KEY) == "true"
    assert spark.conf.get(_AUTO_KEY) == "true"
    spark.conf.set(_FAIL_KEY, "false")
    spark.conf.unset(_FAIL_KEY)
    assert spark.conf.get(_FAIL_KEY) == "true"


def test_sj2_describe_and_summary_answer(spark: ReparkSession) -> None:
    """Describe/summary collect their base rows through inert nodes."""
    frame = spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
    described = frame.describe()
    assert described.columns == ["summary", "id", "v"]
    assert [row[0] for row in described.collect()] == [
        "count",
        "mean",
        "stddev",
        "min",
        "max",
    ]
    summered = frame.summary("count")
    assert summered.columns == ["summary", "id", "v"]
    assert [tuple(row) for row in summered.collect()] == [("count", "2", "2")]


def test_sj2_never_stamped_frames_get_inert_empty_roots(spark: ReparkSession) -> None:
    """Describe outputs carry a root node with no outputs that never renews."""
    frame = spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
    described = frame.describe()
    assert described._frame_node.renews is False
    assert described._frame_node.output_count == 0
    child = described.filter("summary = 'count'")
    assert child._frame_node.output_count == 3
    assert child._frame_node.renews is False
    assert [tuple(row) for row in child.collect()] == [("count", "2", "2")]


def test_sj2_twin_sort_on_pass_through_child_still_refuses(spark: ReparkSession) -> None:
    """A twin Column sorted on a derived frame refuses ``AMBIGUOUS_REFERENCE``."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["id", "ID"])
    with pytest.raises(AnalysisException, match="AMBIGUOUS_REFERENCE"):
        frame.filter("1 > 0").orderBy(frame["id"]).collect()


def test_sj2_overlay_twin_sorts_hold(spark: ReparkSession) -> None:
    """Overlay-born twin sorts keep their base answers on every frame."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    wide = spark.createDataFrame([(1, 10, 100), (2, 20, 200)], ["v", "x", "V"])
    out = wide.withColumnsRenamed({"x": "v"})
    assert out.columns == ["v", "v", "V"]
    twin = out["V"]
    assert [tuple(row) for row in out.orderBy(twin).collect()] == [(1, 10, 100), (2, 20, 200)]
    assert [tuple(row) for row in out.filter("1 > 0").orderBy(twin).collect()] == [
        (1, 10, 100),
        (2, 20, 200),
    ]
    with pytest.raises(AnalysisException, match="AMBIGUOUS_REFERENCE"):
        wide.filter("1 > 0").orderBy(twin).collect()
