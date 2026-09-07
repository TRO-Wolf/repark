"""DFCORE-5 one-collect approxQuantile pins plus three inherited gap pins."""

from __future__ import annotations

import math
import re
from typing import Any
from unittest.mock import patch

import _live_parity as lp
import pytest

from repark.errors import (
    IllegalArgumentException,
    PySparkException,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark.dataframe.core import DataFrame
from repark.spark.session import ReparkSession, _reset_active_session_for_tests


@pytest.fixture()
def spark() -> ReparkSession:
    """A fresh session per test with the active-session registry isolated."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-dfcore-5").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _xy_frame(session: Any) -> Any:
    """The audit frame x = 1..1000 with y = 2x."""
    return session.range(1, 1001).selectExpr("id AS x", "id * 2 AS y")


def test_one_collect_per_frame_regardless_of_shape(spark: ReparkSession) -> None:
    """C-001: every non-trivial shape collects exactly once; empty shapes never."""
    frame = _xy_frame(spark)
    wide = frame.withColumn("z", frame["x"] + 1).withColumn("w", frame["y"] + 1)
    real_collect = DataFrame.collect
    with patch.object(DataFrame, "collect", autospec=True) as collect_mock:
        collect_mock.side_effect = real_collect
        assert frame.approxQuantile(["x", "y"], [0.25, 0.5, 0.75], 0.0) == [
            [250.0, 500.0, 750.0],
            [500.0, 1000.0, 1500.0],
        ]
        assert collect_mock.call_count == 1
        assert frame.approxQuantile(["x"], [0.5], 0.0) == [[500.0]]
        assert collect_mock.call_count == 2
        answered = wide.approxQuantile(["x", "y", "z", "w"], [0.0, 0.25, 0.5, 0.75, 1.0], 0.0)
        assert [len(row) for row in answered] == [5, 5, 5, 5]
        assert collect_mock.call_count == 3
        assert frame.approxQuantile("x", [0.25, 0.5, 0.75], 0.0) == [250.0, 500.0, 750.0]
        assert collect_mock.call_count == 4
        assert frame.approxQuantile(["x", "y"], [], 0.0) == [[], []]
        assert frame.approxQuantile([], [0.5], 0.0) == []
        assert collect_mock.call_count == 4


def test_both_doors_answer_identical_values(spark: ReparkSession) -> None:
    """C-002: df.approxQuantile and df.stat.approxQuantile agree on every shape."""
    frame = _xy_frame(spark)
    for columns in ("x", ["x"], ["x", "y"], ["y", "x", "x"]):
        for probabilities in ([0.5], [0.25, 0.5, 0.75], []):
            plain = frame.approxQuantile(columns, probabilities, 0.0)
            stat = frame.stat.approxQuantile(columns, probabilities, 0.0)
            assert plain == stat


def test_empty_probabilities_answer_empty_per_column(spark: ReparkSession) -> None:
    """C-002: an empty probability list answers [] per column on both doors."""
    frame = _xy_frame(spark)
    for door in (frame.approxQuantile, frame.stat.approxQuantile):
        assert door(["x", "y"], [], 0.0) == [[], []]
        assert door("x", [], 0.0) == []
        assert door([], [], 0.0) == []


def test_empty_columns_answer_empty(spark: ReparkSession) -> None:
    """C-002: an empty column list answers [] on both doors."""
    frame = _xy_frame(spark)
    for door in (frame.approxQuantile, frame.stat.approxQuantile):
        assert door([], [0.5], 0.0) == []


def test_single_string_returns_flat_float_list(spark: ReparkSession) -> None:
    """C-002: a single-string col answers a flat list of floats on both doors."""
    frame = _xy_frame(spark)
    for door in (frame.approxQuantile, frame.stat.approxQuantile):
        answered = door("x", [0.25, 0.5, 0.75], 0.0)
        assert answered == [250.0, 500.0, 750.0]
        assert all(isinstance(value, float) for value in answered)


def test_duplicate_columns_are_each_answered(spark: ReparkSession) -> None:
    """C-002: duplicate columns answer once per occurrence on both doors."""
    frame = _xy_frame(spark)
    for door in (frame.approxQuantile, frame.stat.approxQuantile):
        assert door(["x", "x"], [0.5], 0.0) == [[500.0], [500.0]]


def test_null_cells_are_ignored(spark: ReparkSession) -> None:
    """C-002: NULL cells do not move the quantiles on either door."""
    frame = spark.createDataFrame([(1.0,), (None,), (3.0,)], ["x"])
    for door in (frame.approxQuantile, frame.stat.approxQuantile):
        assert door("x", [0.5], 0.0) == [1.0]
        assert door("x", [0.0, 0.5, 1.0], 0.0) == [1.0, 1.0, 3.0]


def test_all_null_column_answers_nan_per_probability(spark: ReparkSession) -> None:
    """C-002: an all-NULL column answers NaN per probability on both doors."""
    frame = spark.sql("SELECT CAST(NULL AS DOUBLE) AS n FROM (VALUES (1), (2)) t(y)")
    for door in (frame.approxQuantile, frame.stat.approxQuantile):
        answered = door("n", [0.25, 0.5], 0.0)
        assert len(answered) == 2
        assert all(math.isnan(value) for value in answered)


def test_empty_frame_answers_nan_per_probability(spark: ReparkSession) -> None:
    """C-002: an empty frame answers NaN per probability on both doors."""
    frame = spark.sql("SELECT * FROM (SELECT CAST(1.0 AS DOUBLE) AS x) WHERE 1 = 0")
    for door in (frame.approxQuantile, frame.stat.approxQuantile):
        answered = door("x", [0.25, 0.5], 0.0)
        assert len(answered) == 2
        assert all(math.isnan(value) for value in answered)


def test_integer_decimal_and_float_columns(spark: ReparkSession) -> None:
    """C-002: integer, decimal and float columns answer floats on both doors."""
    frame = spark.range(1, 101)
    for door in (frame.approxQuantile, frame.stat.approxQuantile):
        answered = door("id", [0.5], 0.0)
        assert answered == [50.0]
        assert all(isinstance(value, float) for value in answered)
    decimals = spark.sql(
        "SELECT CAST(x AS DECIMAL(10, 2)) AS d FROM (VALUES (1.10), (2.20), (3.30)) t(x)"
    )
    for door in (decimals.approxQuantile, decimals.stat.approxQuantile):
        assert door("d", [0.5], 0.0) == [2.2]
    floats = spark.sql(
        "SELECT * FROM (VALUES (CAST(1.5 AS FLOAT)), (CAST(2.5 AS FLOAT)), "
        "(CAST(3.5 AS FLOAT))) t(x)"
    )
    for door in (floats.approxQuantile, floats.stat.approxQuantile):
        assert door("x", [0.5], 0.0) == [2.5]


def test_non_numeric_column_raises_engine_error(spark: ReparkSession) -> None:
    """C-002: a non-numeric column fails loud with the engine message on both doors."""
    frame = spark.createDataFrame([(1.0, "a", True)], ["x", "s", "b"])
    for door in (frame.approxQuantile, frame.stat.approxQuantile):
        with pytest.raises(PySparkException, match="percentile_approx does not support Utf8"):
            door("s", [0.5], 0.0)
        with pytest.raises(PySparkException, match="percentile_approx does not support Boolean"):
            door("b", [0.5], 0.0)
        with pytest.raises(PySparkException, match="percentile_approx does not support Utf8"):
            door(["x", "s"], [0.5], 0.0)
        with pytest.raises(PySparkException, match="percentile_approx does not support Utf8"):
            door(["s", "b"], [0.5], 0.0)
        with pytest.raises(PySparkException, match="percentile_approx does not support Boolean"):
            door(["b", "s"], [0.5], 0.0)


def test_validation_order_and_error_contract(spark: ReparkSession) -> None:
    """C-002: type errors precede value errors with exact classes and parameters."""
    frame = _xy_frame(spark)
    for door in (frame.approxQuantile, frame.stat.approxQuantile):
        with pytest.raises(PySparkTypeError) as caught:
            door("x", [0.5], "bad")
        assert caught.value.getErrorClass() == "NOT_FLOAT_OR_INT"
        assert caught.value.getMessageParameters() == {
            "arg_name": "relativeError",
            "arg_type": "str",
        }
        with pytest.raises(PySparkTypeError) as caught_bool:
            door("x", [0.5], True)
        assert caught_bool.value.getErrorClass() == "NOT_FLOAT_OR_INT"
        with pytest.raises(PySparkValueError) as caught_negative:
            door("x", [0.5], -0.1)
        assert caught_negative.value.getErrorClass() == "NEGATIVE_VALUE"
        assert caught_negative.value.getMessageParameters() == {
            "arg_name": "relativeError",
            "arg_value": "-0.1",
        }
        with pytest.raises(PySparkValueError) as caught_nan:
            door("x", [0.5], float("nan"))
        assert caught_nan.value.getErrorClass() == "NEGATIVE_VALUE"
        with pytest.raises(PySparkTypeError) as caught_col:
            door(123, [0.5], 0.0)
        assert caught_col.value.getErrorClass() == "NOT_LIST_OR_STR_OR_TUPLE"
        assert caught_col.value.getMessageParameters() == {
            "arg_name": "col",
            "arg_type": "int",
        }
        with pytest.raises(PySparkTypeError) as caught_item:
            door(["x", 5], [0.5], 0.0)
        assert caught_item.value.getErrorClass() == "DISALLOWED_TYPE_FOR_CONTAINER"
        assert caught_item.value.getMessageParameters() == {
            "arg_name": "col",
            "arg_type": "list",
            "allowed_types": "str",
            "item_type": "int",
        }
        with pytest.raises(PySparkTypeError) as caught_probs:
            door("x", "0.5", 0.0)
        assert caught_probs.value.getErrorClass() == "NOT_LIST_OR_TUPLE"
        assert caught_probs.value.getMessageParameters() == {
            "arg_name": "probabilities",
            "arg_type": "str",
        }
        with pytest.raises(PySparkTypeError) as caught_prob_item:
            door("x", [0.5, "bad"], 0.0)
        assert caught_prob_item.value.getErrorClass() == "NOT_LIST_OF_FLOAT_OR_INT"
        assert caught_prob_item.value.getMessageParameters() == {
            "arg_name": "probabilities",
            "arg_type": "str",
        }
        with pytest.raises(PySparkValueError) as caught_bound:
            door("x", [1.5], 0.0)
        assert caught_bound.value.getErrorClass() == "VALUE_OUT_OF_BOUND"
        assert caught_bound.value.getMessageParameters() == {
            "arg_name": "probabilities",
            "arg_value": "1.5",
        }
        with pytest.raises(PySparkValueError):
            door("x", [float("nan")], 0.0)
        with pytest.raises(PySparkTypeError) as caught_order:
            door(123, "bad", "bad")
        assert caught_order.value.getErrorClass() == "NOT_FLOAT_OR_INT"
        with pytest.raises(PySparkTypeError) as caught_col_first:
            door(123, "bad", 0.0)
        assert caught_col_first.value.getErrorClass() == "NOT_LIST_OR_STR_OR_TUPLE"
        with pytest.raises(PySparkValueError) as caught_seq:
            door("x", [1.5, "bad"], 0.0)
        assert caught_seq.value.getErrorClass() == "VALUE_OUT_OF_BOUND"


def test_relative_error_is_validated_but_ignored(spark: ReparkSession) -> None:
    """C-002: relativeError shapes the errors, never the answers, on both doors."""
    frame = _xy_frame(spark)
    for door in (frame.approxQuantile, frame.stat.approxQuantile):
        assert door(["x", "y"], [0.25, 0.5, 0.75], 0.0) == door(["x", "y"], [0.25, 0.5, 0.75], 0.5)


def test_result_shape_is_nested_float_lists(spark: ReparkSession) -> None:
    """C-002: a column list answers list[list[float]] on both doors."""
    frame = _xy_frame(spark)
    for door in (frame.approxQuantile, frame.stat.approxQuantile):
        answered = door(["x", "y"], [0.25, 0.5, 0.75], 0.0)
        assert isinstance(answered, list)
        assert len(answered) == 2
        for row in answered:
            assert isinstance(row, list)
            assert len(row) == 3
            assert all(isinstance(value, float) for value in row)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_values_match_spark_on_audit_frame(spark: ReparkSession, spark_engine: Any) -> None:
    """C-003: both engines answer the audit values on both doors."""
    expected = [[250.0, 500.0, 750.0], [500.0, 1000.0, 1500.0]]
    mine = _xy_frame(spark)
    oracle = _xy_frame(spark_engine.session)
    for frame in (mine, oracle):
        for door in (frame.approxQuantile, frame.stat.approxQuantile):
            assert door(["x", "y"], [0.25, 0.5, 0.75], 0.0) == expected
            assert door("x", [0.25, 0.5, 0.75], 0.0) == expected[0]


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_null_and_empty_divergence_stays_pinned(
    spark: ReparkSession, spark_engine: Any
) -> None:
    """C-003: repark answers NaN per probability where live Spark answers []."""
    null_sql = "SELECT CAST(NULL AS DOUBLE) AS n FROM (VALUES (1), (2)) t(y)"
    empty_sql = "SELECT * FROM (SELECT CAST(1.0 AS DOUBLE) AS x) WHERE 1 = 0"
    mine_null = spark.sql(null_sql)
    oracle_null = spark_engine.session.sql(null_sql)
    for door in (mine_null.approxQuantile, mine_null.stat.approxQuantile):
        answered = door("n", [0.25, 0.5], 0.0)
        assert len(answered) == 2
        assert all(math.isnan(value) for value in answered)
    for door in (oracle_null.approxQuantile, oracle_null.stat.approxQuantile):
        assert door("n", [0.25, 0.5], 0.0) == []
    mine_empty = spark.sql(empty_sql)
    oracle_empty = spark_engine.session.sql(empty_sql)
    for door in (mine_empty.approxQuantile, mine_empty.stat.approxQuantile):
        answered = door("x", [0.25, 0.5], 0.0)
        assert len(answered) == 2
        assert all(math.isnan(value) for value in answered)
    for door in (oracle_empty.approxQuantile, oracle_empty.stat.approxQuantile):
        assert door("x", [0.25, 0.5], 0.0) == []
    mixed_sql = (
        "SELECT * FROM (VALUES (CAST(1.0 AS DOUBLE), CAST(NULL AS DOUBLE)), "
        "(CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)), "
        "(CAST(3.0 AS DOUBLE), CAST(NULL AS DOUBLE))) t(x, n)"
    )
    mine_mixed = spark.sql(mixed_sql)
    oracle_mixed = spark_engine.session.sql(mixed_sql)
    for door in (mine_mixed.approxQuantile, mine_mixed.stat.approxQuantile):
        answered = door(["x", "n"], [0.5], 0.0)
        assert answered[0] == [1.0]
        assert len(answered[1]) == 1
        assert math.isnan(answered[1][0])
    for door in (oracle_mixed.approxQuantile, oracle_mixed.stat.approxQuantile):
        assert door(["x", "n"], [0.5], 0.0) == [[1.0], []]


def test_crosstab_absent_pair_fills_zero(spark: ReparkSession) -> None:
    """C-004: a sparse crosstab fixture carries literal 0 cells (DFCORE-3 F1)."""
    frame = spark.createDataFrame([("a", "x"), ("a", "y"), ("b", "x")], ["g", "v"])
    for table in (
        frame.crosstab("g", "v"),
        frame.stat.crosstab("g", "v"),
    ):
        assert table.columns[0] == "g_v"
        missing = table.columns.index("y")
        rows = {row[0]: row for row in table.collect()}
        assert rows["a"][missing] == 1
        assert rows["b"][missing] == 0


def test_freq_items_refusal_matches_full_message(spark: ReparkSession) -> None:
    """C-004: the freqItems refusal pins its full text (DFCORE-3 F2)."""
    frame = spark.range(3)
    with pytest.raises(
        UnsupportedOperationException,
        match=re.escape("DataFrame.stat.freqItems is not supported yet (disclosed R-DF-BATCH2)"),
    ):
        frame.stat.freqItems(["id"])


def test_sample_fraction_refusal_matches_full_message(spark: ReparkSession) -> None:
    """C-004: the sample fraction refusal pins its full text (DFCORE-4a F1)."""
    with pytest.raises(
        IllegalArgumentException,
        match=re.escape("requirement failed: Fraction must be in [0, 1], but got -1.0"),
    ):
        spark.range(1).sample(-1.0)
