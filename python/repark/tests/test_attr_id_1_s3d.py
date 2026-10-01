from __future__ import annotations

from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-attr-id-1-s3d").getOrCreate()


@pytest.fixture(params=[False, True], ids=["insensitive", "sensitive"])
def ruled_spark(spark: ReparkSession, request: Any) -> ReparkSession:
    spark.conf.set("spark.sql.caseSensitive", "true" if request.param else "false")
    return spark


def _frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, 30), (2, None)], "id BIGINT, v BIGINT")


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    return sorted((tuple(row) for row in frame.collect()), key=repr)


def _twins_one_attribute(spark: ReparkSession) -> Any:
    return _frame(spark).select("v", "v")


def _twins_two_attributes(spark: ReparkSession) -> Any:
    frame = _frame(spark)
    return frame.select(frame.v, frame.v.alias("v"))


def _self_join(spark: ReparkSession) -> Any:
    frame = _frame(spark)
    return frame.join(frame, frame.id == frame.id)


def test_drop_str_drops_twin_pair_of_one_attribute(ruled_spark: ReparkSession) -> None:
    dropped = _twins_one_attribute(ruled_spark).drop("v")
    assert dropped.columns == []


def test_drop_str_drops_two_attributes_sharing_a_display(ruled_spark: ReparkSession) -> None:
    dropped = _twins_two_attributes(ruled_spark).drop("v")
    assert dropped.columns == []


def test_drop_str_miss_is_noop(ruled_spark: ReparkSession) -> None:
    dropped = _frame(ruled_spark).drop("zzz")
    assert dropped.columns == ["id", "v"]
    assert _rows(dropped) == [(1, 30), (2, None)]


def test_drop_str_variant_drops_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    dropped = _frame(spark).drop("V")
    assert dropped.columns == ["id"]
    assert _rows(dropped) == [(1,), (2,)]


def test_drop_str_variant_misses_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    dropped = _frame(spark).drop("V")
    assert dropped.columns == ["id", "v"]
    assert _rows(dropped) == [(1, 30), (2, None)]


def test_drop_col_drops_twin_pair_of_one_attribute(ruled_spark: ReparkSession) -> None:
    dropped = _twins_one_attribute(ruled_spark).drop(functions.col("v"))
    assert dropped.columns == []


def test_drop_col_on_two_attributes_is_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as dropped:
        _twins_two_attributes(ruled_spark).drop(functions.col("v"))
    assert dropped.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_drop_col_miss_is_noop(ruled_spark: ReparkSession) -> None:
    dropped = _frame(ruled_spark).drop(functions.col("zzz"))
    assert dropped.columns == ["id", "v"]
    assert _rows(dropped) == [(1, 30), (2, None)]


def test_drop_col_variant_drops_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    dropped = _frame(spark).drop(functions.col("V"))
    assert dropped.columns == ["id"]
    assert _rows(dropped) == [(1,), (2,)]


def test_drop_col_variant_misses_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    dropped = _frame(spark).drop(functions.col("V"))
    assert dropped.columns == ["id", "v"]
    assert _rows(dropped) == [(1, 30), (2, None)]


def test_drop_parent_column_drops_only_its_position(ruled_spark: ReparkSession) -> None:
    parent = _frame(ruled_spark)
    child = parent.select(parent.v, parent.v.alias("v"))
    dropped = child.drop(parent.v)
    assert dropped.columns == ["v"]
    assert _rows(dropped) == [(30,), (None,)]


def test_drop_parent_column_on_one_attribute_twins_drops_both(
    ruled_spark: ReparkSession,
) -> None:
    parent = _frame(ruled_spark)
    child = parent.select("v", "v")
    assert child.drop(parent.v).columns == []


def test_drop_compound_alias_is_noop(ruled_spark: ReparkSession) -> None:
    frame = _twins_two_attributes(ruled_spark)
    dropped = frame.drop((functions.col("id") + 1).alias("v"))
    assert dropped.columns == ["v", "v"]
    assert _rows(dropped) == [(30, 30), (None, None)]


def test_drop_literal_is_noop(ruled_spark: ReparkSession) -> None:
    dropped = _frame(ruled_spark).drop(functions.lit(1))
    assert dropped.columns == ["id", "v"]
    assert _rows(dropped) == [(1, 30), (2, None)]


def test_drop_alias_of_own_column_is_noop(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    dropped = frame.drop(frame.v.alias("v"))
    assert dropped.columns == ["id", "v"]
    assert _rows(dropped) == [(1, 30), (2, None)]


def test_drop_other_frame_column_is_noop(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    other = ruled_spark.createDataFrame([(9, 99)], "id BIGINT, v BIGINT")
    dropped = frame.drop(other.v)
    assert dropped.columns == ["id", "v"]
    assert _rows(dropped) == [(1, 30), (2, None)]


def test_drop_parent_after_self_join_drops_only_parent_side(
    ruled_spark: ReparkSession,
) -> None:
    frame = _frame(ruled_spark)
    joined = frame.join(frame, frame.id == frame.id)
    dropped = joined.drop(frame.v)
    assert dropped.columns == ["id", "id", "v"]
    assert _rows(dropped) == [(1, 1, 30), (2, 2, None)]


def test_drop_parent_after_join_keeps_other_side_values(ruled_spark: ReparkSession) -> None:
    left = ruled_spark.createDataFrame([(1, 30)], "id BIGINT, v BIGINT")
    right = ruled_spark.createDataFrame([(1, 300)], "id BIGINT, v BIGINT")
    joined = left.join(right, left.id == right.id)
    assert _rows(joined.drop(left.v)) == [(1, 1, 300)]
    assert _rows(joined.drop(right.v)) == [(1, 30, 1)]


def test_drop_uses_live_rule_after_build_insensitive_then_sensitive(
    spark: ReparkSession,
) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = _frame(spark).select("v", "v")
    spark.conf.set("spark.sql.caseSensitive", "true")
    dropped = frame.drop("V")
    assert dropped.columns == ["v", "v"]
    assert _rows(dropped) == [(30, 30), (None, None)]


def test_drop_uses_live_rule_after_build_sensitive_then_insensitive(
    spark: ReparkSession,
) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    frame = _frame(spark)
    spark.conf.set("spark.sql.caseSensitive", "false")
    dropped = frame.drop("V")
    assert dropped.columns == ["id"]
    assert _rows(dropped) == [(1,), (2,)]


def test_drop_str_on_join_dup_drops_both(ruled_spark: ReparkSession) -> None:
    dropped = _self_join(ruled_spark).drop("v")
    assert dropped.columns == ["id", "id"]
    assert _rows(dropped) == [(1, 1), (2, 2)]


def test_drop_col_on_join_dup_is_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as dropped:
        _self_join(ruled_spark).drop(functions.col("v"))
    assert dropped.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_drop_without_args_is_noop(ruled_spark: ReparkSession) -> None:
    dropped = _frame(ruled_spark).drop()
    assert dropped.columns == ["id", "v"]
    assert _rows(dropped) == [(1, 30), (2, None)]


def test_drop_duplicates_on_twins_runs(ruled_spark: ReparkSession) -> None:
    deduped = _twins_one_attribute(ruled_spark).dropDuplicates(["v"])
    assert deduped.columns == ["v", "v"]
    assert _rows(deduped) == [(30, 30), (None, None)]


def test_drop_duplicates_on_two_attributes_runs(ruled_spark: ReparkSession) -> None:
    deduped = _twins_two_attributes(ruled_spark).dropDuplicates(["v"])
    assert deduped.columns == ["v", "v"]
    assert _rows(deduped) == [(30, 30), (None, None)]


def test_drop_duplicates_miss_raises_legacy(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as deduped:
        _frame(ruled_spark).dropDuplicates(["zzz"])
    assert deduped.value.getCondition() == "_LEGACY_ERROR_TEMP_1201"


def test_drop_duplicates_variant_runs_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    deduped = _frame(spark).dropDuplicates(["V"])
    assert deduped.columns == ["id", "v"]
    assert _rows(deduped) == [(1, 30), (2, None)]


def test_drop_duplicates_variant_raises_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    with pytest.raises(AnalysisException) as deduped:
        _frame(spark).dropDuplicates(["V"])
    assert deduped.value.getCondition() == "_LEGACY_ERROR_TEMP_1201"


def test_drop_duplicates_on_join_dup_runs(ruled_spark: ReparkSession) -> None:
    deduped = _self_join(ruled_spark).dropDuplicates(["v"])
    assert deduped.columns == ["id", "v", "id", "v"]
    assert _rows(deduped) == [(1, 30, 1, 30), (2, None, 2, None)]


def test_fillna_subset_fills_twin_pair(ruled_spark: ReparkSession) -> None:
    filled = _twins_one_attribute(ruled_spark).fillna(0, subset=["v"])
    assert filled.columns == ["v", "v"]
    assert _rows(filled) == [(0, 0), (30, 30)]


def test_fillna_subset_on_two_attributes_is_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as filled:
        _twins_two_attributes(ruled_spark).fillna(0, subset=["v"])
    assert filled.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_fillna_subset_miss_is_unresolved(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as filled:
        _frame(ruled_spark).fillna(0, subset=["zzz"])
    assert filled.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_fillna_subset_variant_fills_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    filled = _frame(spark).fillna(0, subset=["V"])
    assert filled.columns == ["id", "v"]
    assert _rows(filled) == [(1, 30), (2, 0)]


def test_fillna_subset_variant_is_unresolved_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    with pytest.raises(AnalysisException) as filled:
        _frame(spark).fillna(0, subset=["V"])
    assert filled.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_fillna_dict_fills_twin_pair(ruled_spark: ReparkSession) -> None:
    filled = _twins_one_attribute(ruled_spark).fillna({"v": 0})
    assert filled.columns == ["v", "v"]
    assert _rows(filled) == [(0, 0), (30, 30)]


def test_fillna_dict_on_two_attributes_is_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as filled:
        _twins_two_attributes(ruled_spark).fillna({"v": 0})
    assert filled.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_fillna_dict_miss_is_unresolved(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as filled:
        _frame(ruled_spark).fillna({"zzz": 0})
    assert filled.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_fillna_dict_variant_fills_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    filled = _frame(spark).fillna({"V": 0})
    assert filled.columns == ["id", "v"]
    assert _rows(filled) == [(1, 30), (2, 0)]


def test_fillna_dict_variant_is_unresolved_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    with pytest.raises(AnalysisException) as filled:
        _frame(spark).fillna({"V": 0})
    assert filled.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_fillna_dict_folded_keys_last_wins_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    filled = _frame(spark).fillna({"v": 0, "V": 1})
    assert filled.columns == ["id", "v"]
    assert _rows(filled) == [(1, 30), (2, 1)]


def test_fillna_on_join_dup_is_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as filled:
        _self_join(ruled_spark).fillna(0, subset=["v"])
    assert filled.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_dropna_subset_filters_twin_pair(ruled_spark: ReparkSession) -> None:
    dropped = _twins_one_attribute(ruled_spark).dropna(subset=["v"])
    assert dropped.columns == ["v", "v"]
    assert _rows(dropped) == [(30, 30)]


def test_dropna_subset_on_two_attributes_is_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as dropped:
        _twins_two_attributes(ruled_spark).dropna(subset=["v"])
    assert dropped.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_dropna_subset_miss_is_unresolved(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as dropped:
        _frame(ruled_spark).dropna(subset=["zzz"])
    assert dropped.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_dropna_subset_variant_filters_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    dropped = _frame(spark).dropna(subset=["V"])
    assert dropped.columns == ["id", "v"]
    assert _rows(dropped) == [(1, 30)]


def test_dropna_subset_variant_is_unresolved_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    with pytest.raises(AnalysisException) as dropped:
        _frame(spark).dropna(subset=["V"])
    assert dropped.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_dropna_thresh_counts_each_twin_position(ruled_spark: ReparkSession) -> None:
    dropped = _twins_one_attribute(ruled_spark).dropna(thresh=2, subset=["v"])
    assert dropped.columns == ["v", "v"]
    assert _rows(dropped) == [(30, 30)]


def test_dropna_duplicate_keys_count_each_time(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    dropped = _frame(spark).dropna(thresh=2, subset=["v", "V"])
    assert dropped.columns == ["id", "v"]
    assert _rows(dropped) == [(1, 30)]


def test_dropna_empty_subset_is_noop(ruled_spark: ReparkSession) -> None:
    dropped = _frame(ruled_spark).dropna(subset=[])
    assert dropped.columns == ["id", "v"]
    assert _rows(dropped) == [(1, 30), (2, None)]


def test_dropna_on_join_dup_is_ambiguous(ruled_spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as dropped:
        _self_join(ruled_spark).dropna(subset=["v"])
    assert dropped.value.getCondition() == "AMBIGUOUS_REFERENCE"
