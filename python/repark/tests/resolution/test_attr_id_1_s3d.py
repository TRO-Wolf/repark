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


def _pair_frame(spark: ReparkSession, held: str) -> Any:
    return spark.createDataFrame([(1, 2)], [held, "v"])


def test_fold_a_fillna_subset_misses_upper_only_pairs(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    for held, written in [("I", "\u0131"), ("\u00b5", "\u03bc"), ("s", "\u017f")]:
        with pytest.raises(AnalysisException) as filled:
            _pair_frame(spark, held).fillna(0, subset=[written])
        assert filled.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_fold_a_fillna_dict_misses_upper_only_pairs(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    for held, written in [("I", "\u0131"), ("\u00b5", "\u03bc"), ("s", "\u017f")]:
        with pytest.raises(AnalysisException) as filled:
            _pair_frame(spark, held).fillna({written: 0})
        assert filled.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_fold_a_dropna_subset_misses_upper_only_pairs(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    for held, written in [("I", "\u0131"), ("\u00b5", "\u03bc"), ("s", "\u017f")]:
        with pytest.raises(AnalysisException) as dropped:
            _pair_frame(spark, held).dropna(subset=[written])
        assert dropped.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_fold_a_drop_col_misses_upper_only_pairs(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    for held, written in [("I", "\u0131"), ("\u00b5", "\u03bc"), ("s", "\u017f")]:
        dropped = _pair_frame(spark, held).drop(functions.col(written))
        assert dropped.columns == [held, "v"]


def test_fold_a_fillna_subset_hits_simple_lower_pairs(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 2), (None, 2)], ["\u00df", "v"])
    filled = frame.fillna(7, subset=["\u1e9e"])
    assert filled.columns == ["\u00df", "v"]
    assert _rows(filled) == [(1, 2), (7, 2)]
    kelvin = spark.createDataFrame([(1, 2), (None, 2)], ["k", "v"])
    assert _rows(kelvin.fillna(7, subset=["\u212a"])) == [(1, 2), (7, 2)]


def test_fold_b_drop_str_hits_upper_only_pairs(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    for held, written in [("I", "\u0131"), ("\u00b5", "\u03bc"), ("s", "\u017f")]:
        dropped = _pair_frame(spark, held).drop(written)
        assert dropped.columns == ["v"]


def test_fold_b_drop_duplicates_hits_upper_only_pairs(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    for held, written in [("I", "\u0131"), ("\u00b5", "\u03bc"), ("s", "\u017f")]:
        deduped = _pair_frame(spark, held).dropDuplicates([written])
        assert deduped.columns == [held, "v"]
        assert _rows(deduped) == [(1, 2)]


def test_fold_b_drop_str_misses_newer_scripts(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    for held, written in [("\U00010570", "\U00010597"), ("\u0390", "\u1fd3"), ("\ua7c0", "\ua7c1")]:
        dropped = _pair_frame(spark, held).drop(written)
        assert dropped.columns == [held, "v"]


def test_fold_b_drop_duplicates_misses_newer_scripts(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    for held, written in [("\U00010570", "\U00010597"), ("\u0390", "\u1fd3"), ("\ua7c0", "\ua7c1")]:
        with pytest.raises(AnalysisException) as deduped:
            _pair_frame(spark, held).dropDuplicates([written])
        assert deduped.value.getCondition() == "_LEGACY_ERROR_TEMP_1201"


def _expansion_pairs() -> list[tuple[str, str]]:
    return [("\u00df", "SS"), ("\u0130", "i\u0307"), ("\ufb00", "FF")]


def test_fold_b_drop_str_misses_expansion_pairs(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    for held, written in _expansion_pairs():
        dropped = _pair_frame(spark, held).drop(written)
        assert dropped.columns == [held, "v"]


def test_fold_b_drop_duplicates_misses_expansion_pairs(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    for held, written in _expansion_pairs():
        with pytest.raises(AnalysisException) as deduped:
            _pair_frame(spark, held).dropDuplicates([written])
        assert deduped.value.getCondition() == "_LEGACY_ERROR_TEMP_1201"


def test_fold_b_drop_str_hits_sharp_s_capital_pair(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    assert _pair_frame(spark, "\u00df").drop("\u1e9e").columns == ["v"]
    assert _pair_frame(spark, "\u1e9e").drop("\u00df").columns == ["v"]


def test_fold_b_drop_duplicates_hits_sharp_s_capital_pair(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    deduped = _pair_frame(spark, "\u00df").dropDuplicates(["\u1e9e"])
    assert deduped.columns == ["\u00df", "v"]
    assert _rows(deduped) == [(1, 2)]


def test_fold_a_drop_col_misses_expansion_pairs(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    for held, written in _expansion_pairs():
        dropped = _pair_frame(spark, held).drop(functions.col(written))
        assert dropped.columns == [held, "v"]


def test_fold_a_fillna_subset_misses_expansion_pairs(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    for held, written in _expansion_pairs():
        with pytest.raises(AnalysisException) as filled:
            _pair_frame(spark, held).fillna(0, subset=[written])
        assert filled.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_fold_b_drop_str_hits_deseret_supplementary_pair(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    assert _pair_frame(spark, "\U00010400").drop("\U00010428").columns == ["v"]
    assert _pair_frame(spark, "\U00010428").drop("\U00010400").columns == ["v"]


def _union_source(spark: ReparkSession) -> Any:
    spark.conf.set("spark.sql.caseSensitive", "false")
    return spark.createDataFrame([(1, 10), (2, None)], "id INT, v INT")


def _resolved_union(spark: ReparkSession) -> Any:
    source = _union_source(spark)
    casef = source.select("id", "v", functions.col("V"))
    return casef.union(casef)


def _aliased_union(spark: ReparkSession) -> Any:
    source = _union_source(spark)
    casef = source.select("id", "v", functions.col("v").alias("V"))
    return casef.union(casef)


def _same_display_union(spark: ReparkSession) -> Any:
    source = _union_source(spark)
    twins = source.select("id", "v", "v")
    return twins.union(twins)


def _distinct_frame(spark: ReparkSession) -> Any:
    spark.conf.set("spark.sql.caseSensitive", "false")
    return spark.createDataFrame([(1, None), (None, 2)], ["v", "V"])


def _reversed_frame(spark: ReparkSession) -> Any:
    spark.conf.set("spark.sql.caseSensitive", "false")
    return spark.createDataFrame([(1, None), (None, 2)], ["V", "v"])


def test_union_fillna_subset_binds_first_position_only(spark: ReparkSession) -> None:
    wanted = [(1, 10, 10), (1, 10, 10), (2, 0, None), (2, 0, None)]
    assert _rows(_resolved_union(spark).fillna(0, subset=["v"])) == wanted
    assert _rows(_resolved_union(spark).fillna(0, subset=["V"])) == wanted


def test_union_fillna_dict_last_key_wins_first_position(spark: ReparkSession) -> None:
    first = [(1, 10, 10), (1, 10, 10), (2, 1, None), (2, 1, None)]
    second = [(1, 10, 10), (1, 10, 10), (2, 0, None), (2, 0, None)]
    assert _rows(_resolved_union(spark).fillna({"v": 0, "V": 1})) == first
    assert _rows(_resolved_union(spark).fillna({"V": 1, "v": 0})) == second


def test_union_dropna_subset_binds_first_position_only(spark: ReparkSession) -> None:
    wanted = [(1, 10, 10), (1, 10, 10)]
    assert _rows(_resolved_union(spark).dropna(subset=["v"])) == wanted
    assert _rows(_resolved_union(spark).dropna(subset=["V"])) == wanted


def test_union_drop_col_binds_first_position_only(spark: ReparkSession) -> None:
    assert _resolved_union(spark).drop(functions.col("v")).columns == ["id", "V"]
    assert _resolved_union(spark).drop(functions.col("V")).columns == ["id", "V"]


def test_union_drop_str_fans_out_to_every_hit(spark: ReparkSession) -> None:
    assert _resolved_union(spark).drop("v").columns == ["id"]
    assert _resolved_union(spark).drop("V").columns == ["id"]


def test_union_same_display_twins_bind_first_position_only(spark: ReparkSession) -> None:
    wanted = [(1, 10, 10), (1, 10, 10), (2, 0, None), (2, 0, None)]
    assert _rows(_same_display_union(spark).fillna(0, subset=["v"])) == wanted
    assert _rows(_same_display_union(spark).fillna(0, subset=["V"])) == wanted
    assert _rows(_same_display_union(spark).dropna(subset=["v"])) == [
        (1, 10, 10),
        (1, 10, 10),
    ]
    assert _rows(_same_display_union(spark).dropna(subset=["V"])) == [
        (1, 10, 10),
        (1, 10, 10),
    ]
    assert _same_display_union(spark).drop(functions.col("v")).columns == ["id", "v"]
    assert _same_display_union(spark).drop(functions.col("V")).columns == ["id", "v"]


def _alias_dup_union(spark: ReparkSession) -> Any:
    source = _union_source(spark)
    twins = source.select("id", functions.col("v").alias("v"), functions.col("v").alias("v"))
    return twins.union(twins)


def test_union_multi_id_same_display_twins_refuse_subset_and_col(
    spark: ReparkSession,
) -> None:
    with pytest.raises(AnalysisException) as failed:
        _alias_dup_union(spark).fillna(0, subset=["v"])
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as failed:
        _alias_dup_union(spark).fillna(0, subset=["V"])
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as failed:
        _alias_dup_union(spark).dropna(subset=["v"])
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as failed:
        _alias_dup_union(spark).drop(functions.col("v"))
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as failed:
        _alias_dup_union(spark).drop(functions.col("V"))
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"


def _casef_frame(spark: ReparkSession) -> Any:
    source = _union_source(spark)
    return source.select("id", "v", functions.col("V"))


def test_project_dup_twins_fill_fans_out_without_union(spark: ReparkSession) -> None:
    wanted = [(1, 10, 10), (2, 0, 0)]
    assert _rows(_casef_frame(spark).fillna(0, subset=["v"])) == wanted
    assert _rows(_casef_frame(spark).fillna(0, subset=["V"])) == wanted
    assert _rows(_casef_frame(spark).dropna(subset=["V"])) == [(1, 10, 10)]


def test_project_dup_twins_drop_col_fans_out_without_union(spark: ReparkSession) -> None:
    assert _casef_frame(spark).drop(functions.col("V")).columns == ["id"]


def test_union_drop_duplicates_fans_out_to_divergent_twins(spark: ReparkSession) -> None:
    source = _union_source(spark)
    sided = source.select("id", "v", functions.lit(20).alias("V"))
    doubled = sided.union(sided)
    assert _rows(doubled.dropDuplicates(["V"])) == [(1, 10, 20), (2, None, 20)]
    assert _rows(doubled.dropDuplicates(["v"])) == [(1, 10, 20), (2, None, 20)]
    assert doubled.drop("V").columns == ["id"]


def test_fillna_subset_misses_dotted_capital_i_variants(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 2)], ["\u0130d", "v"])
    for key in ["id", "\u0131d"]:
        with pytest.raises(AnalysisException) as failed:
            frame.fillna(0, subset=[key])
        assert failed.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
        with pytest.raises(AnalysisException) as failed:
            frame.dropna(subset=[key])
        assert failed.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_fillna_subset_hits_dotted_capital_i_exact_fold(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 2)], ["\u0130d", "v"])
    assert _rows(frame.fillna(0, subset=["\u0130D"])) == [(1, 2)]
    assert _rows(frame.dropna(subset=["\u0130D"])) == [(1, 2)]


def test_drop_str_and_duplicates_hit_dotted_capital_i_variants(
    spark: ReparkSession,
) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 2)], ["\u0130d", "v"])
    assert frame.drop("id").columns == ["v"]
    assert frame.drop("\u0131d").columns == ["v"]
    assert _rows(frame.dropDuplicates(["\u0131d"])) == [(1, 2)]


def test_drop_col_misses_dotted_capital_i_variants(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 2)], ["\u0130d", "v"])
    assert frame.drop(functions.col("id")).columns == ["\u0130d", "v"]
    assert frame.drop(functions.col("\u0131d")).columns == ["\u0130d", "v"]


def test_fillna_subset_hits_final_sigma_fold(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1,), (None,)], ["a\u03a3"])
    assert _rows(frame.fillna(7, subset=["A\u03c2"])) == [(1,), (7,)]


def test_fillna_subset_misses_dotted_capital_i_expansion(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1,), (None,)], ["\u0130"])
    with pytest.raises(AnalysisException) as failed:
        frame.fillna(7, subset=["i\u0307"])
    assert failed.value.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"


def test_union_distinct_ids_refuse_subset_and_col(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as failed:
        _aliased_union(spark).fillna(0, subset=["v"])
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as failed:
        _aliased_union(spark).fillna(0, subset=["V"])
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as failed:
        _aliased_union(spark).dropna(subset=["V"])
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as failed:
        _aliased_union(spark).drop(functions.col("V"))
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_reversed_display_order_refuses_subset_and_col(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as failed:
        _reversed_frame(spark).fillna(0, subset=["V"])
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as failed:
        _reversed_frame(spark).fillna(0, subset=["v"])
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as failed:
        _reversed_frame(spark).dropna(subset=["V"])
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as failed:
        _reversed_frame(spark).drop(functions.col("V"))
    assert failed.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_sensitive_exact_hit_fans_out_to_same_id(spark: ReparkSession) -> None:
    source = _union_source(spark)
    casef = source.select("id", "v", functions.col("V"))
    spark.conf.set("spark.sql.caseSensitive", "true")
    wanted = [(1, 10, 10), (2, 0, 0)]
    assert _rows(casef.fillna(0, subset=["v"])) == wanted
    assert _rows(casef.fillna(0, subset=["V"])) == wanted
    assert casef.drop(functions.col("v")).columns == ["id"]


def test_sensitive_exact_only_on_distinct_ids(spark: ReparkSession) -> None:
    frame = _distinct_frame(spark)
    spark.conf.set("spark.sql.caseSensitive", "true")
    assert _rows(frame.fillna(0, subset=["V"])) == [(1, 0), (None, 2)]
    assert _rows(frame.fillna(0, subset=["v"])) == [(0, 2), (1, None)]
    assert _rows(frame.dropna(subset=["V"])) == [(None, 2)]
    assert frame.drop(functions.col("V")).columns == ["v"]
    assert frame.drop(functions.col("v")).columns == ["V"]


def test_sensitive_drop_str_has_no_id_closure(spark: ReparkSession) -> None:
    source = _union_source(spark)
    casef = source.select("id", "v", functions.col("V"))
    spark.conf.set("spark.sql.caseSensitive", "true")
    assert casef.drop("v").columns == ["id", "V"]
