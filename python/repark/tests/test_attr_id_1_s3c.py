from __future__ import annotations

from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-attr-id-1-s3c").getOrCreate()


@pytest.fixture(params=[False, True], ids=["insensitive", "sensitive"])
def ruled_spark(spark: ReparkSession, request: Any) -> ReparkSession:
    spark.conf.set("spark.sql.caseSensitive", "true" if request.param else "false")
    return spark


def _frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, 30), (2, 10)], "id BIGINT, v BIGINT")


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    return sorted(tuple(row) for row in frame.collect())


def _twins_one_attribute(spark: ReparkSession) -> Any:
    return _frame(spark).select("v", "v")


def _twins_two_attributes(spark: ReparkSession) -> Any:
    frame = _frame(spark)
    return frame.select(frame.v, frame.v.alias("v"))


def test_with_column_replaces_twin_pair_of_one_attribute(ruled_spark: ReparkSession) -> None:
    replaced = _twins_one_attribute(ruled_spark).withColumn("v", functions.lit(99))
    assert replaced.columns == ["v", "v"]
    assert _rows(replaced) == [(99, 99), (99, 99)]


def test_with_column_replaces_two_attributes_sharing_a_display(
    ruled_spark: ReparkSession,
) -> None:
    replaced = _twins_two_attributes(ruled_spark).withColumn("v", functions.lit(99))
    assert replaced.columns == ["v", "v"]
    assert _rows(replaced) == [(99, 99), (99, 99)]


def test_with_column_replace_then_filter_select_str_select_col_are_ambiguous(
    ruled_spark: ReparkSession,
) -> None:
    replaced = _twins_one_attribute(ruled_spark).withColumn("v", functions.lit(99))
    with pytest.raises(AnalysisException) as filtered:
        replaced.filter("v > 50")
    assert filtered.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as selected:
        replaced.select("v")
    assert selected.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as col_selected:
        replaced.select(functions.col("v"))
    assert col_selected.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_with_column_bare_value_replacement_mints_per_position(
    ruled_spark: ReparkSession,
) -> None:
    frame = _frame(ruled_spark)
    replaced = frame.select("v", "v").withColumn("v", frame.v)
    assert replaced.columns == ["v", "v"]
    assert _rows(replaced) == [(10, 10), (30, 30)]
    with pytest.raises(AnalysisException) as filtered:
        replaced.filter("v > 5")
    assert filtered.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as selected:
        replaced.select("v")
    assert selected.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_with_column_variant_spelling_replaces_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    replaced = _frame(spark).withColumn("V", functions.lit(99))
    assert replaced.columns == ["id", "V"]
    assert _rows(replaced) == [(1, 99), (2, 99)]


def test_with_column_variant_spelling_appends_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    appended = _frame(spark).withColumn("V", functions.lit(99))
    assert appended.columns == ["id", "v", "V"]
    assert _rows(appended) == [(1, 30, 99), (2, 10, 99)]


def test_with_column_on_folded_rival_columns_replaces_both_insensitive(
    spark: ReparkSession,
) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 10)], "a BIGINT, A BIGINT")
    replaced = frame.withColumn("a", functions.lit(99))
    assert replaced.columns == ["a", "a"]
    assert _rows(replaced) == [(99, 99)]


def test_with_column_on_folded_rival_columns_replaces_exact_sensitive(
    spark: ReparkSession,
) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    frame = spark.createDataFrame([(1, 10)], "a BIGINT, A BIGINT")
    replaced = frame.withColumn("a", functions.lit(99))
    assert replaced.columns == ["a", "A"]
    assert _rows(replaced) == [(99, 10)]


def test_with_columns_replaces_hits_and_appends_misses_in_order(
    ruled_spark: ReparkSession,
) -> None:
    frame = _frame(ruled_spark)
    updated = frame.withColumns({"v": functions.lit(1), "x": functions.lit(2)})
    assert updated.columns == ["id", "v", "x"]
    assert _rows(updated) == [(1, 1, 2), (2, 1, 2)]


def test_with_columns_folded_keys_last_key_wins_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    updated = _frame(spark).withColumns({"v": functions.lit(1), "V": functions.lit(2)})
    assert updated.columns == ["id", "V"]
    assert _rows(updated) == [(1, 2), (2, 2)]


def test_with_columns_folded_keys_replace_and_append_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    updated = _frame(spark).withColumns({"v": functions.lit(1), "V": functions.lit(2)})
    assert updated.columns == ["id", "v", "V"]
    assert _rows(updated) == [(1, 1, 2), (2, 1, 2)]


def test_with_column_renamed_renames_twin_pair(ruled_spark: ReparkSession) -> None:
    renamed = _twins_one_attribute(ruled_spark).withColumnRenamed("v", "w")
    assert renamed.columns == ["w", "w"]
    assert _rows(renamed) == [(10, 10), (30, 30)]


def test_with_column_renamed_renames_two_attributes_sharing_a_display(
    ruled_spark: ReparkSession,
) -> None:
    renamed = _twins_two_attributes(ruled_spark).withColumnRenamed("v", "w")
    assert renamed.columns == ["w", "w"]
    assert _rows(renamed) == [(10, 10), (30, 30)]


def test_with_column_renamed_miss_is_noop(ruled_spark: ReparkSession) -> None:
    frame = _frame(ruled_spark)
    renamed = frame.withColumnRenamed("zzz", "w")
    assert renamed.columns == ["id", "v"]
    assert _rows(renamed) == [(1, 30), (2, 10)]


def test_with_column_renamed_variant_renames_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    renamed = _twins_one_attribute(spark).withColumnRenamed("V", "w")
    assert renamed.columns == ["w", "w"]
    assert _rows(renamed) == [(10, 10), (30, 30)]


def test_with_column_renamed_variant_misses_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    renamed = _twins_one_attribute(spark).withColumnRenamed("V", "w")
    assert renamed.columns == ["v", "v"]
    assert _rows(renamed) == [(10, 10), (30, 30)]


def test_renamed_twins_then_filter_select_str_select_col_are_ambiguous(
    ruled_spark: ReparkSession,
) -> None:
    renamed = _twins_one_attribute(ruled_spark).withColumnRenamed("v", "w")
    with pytest.raises(AnalysisException) as filtered:
        renamed.filter("w > 5")
    assert filtered.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as selected:
        renamed.select("w")
    assert selected.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as col_selected:
        renamed.select(functions.col("w"))
    assert col_selected.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_plural_renamed_twins_then_filter_select_str_select_col_are_ambiguous(
    ruled_spark: ReparkSession,
) -> None:
    renamed = _twins_one_attribute(ruled_spark).withColumnsRenamed({"v": "w"})
    with pytest.raises(AnalysisException) as filtered:
        renamed.filter("w > 5")
    assert filtered.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as selected:
        renamed.select("w")
    assert selected.value.getCondition() == "AMBIGUOUS_REFERENCE"
    with pytest.raises(AnalysisException) as col_selected:
        renamed.select(functions.col("w"))
    assert col_selected.value.getCondition() == "AMBIGUOUS_REFERENCE"


def test_with_columns_renamed_folded_keys_match_sequentially(ruled_spark: ReparkSession) -> None:
    renamed = _frame(ruled_spark).withColumnsRenamed({"v": "x", "V": "y"})
    assert renamed.columns == ["id", "x"]
    assert _rows(renamed) == [(1, 30), (2, 10)]


def test_with_columns_renamed_sequential_chain_refuses_duplicate_finals(
    ruled_spark: ReparkSession,
) -> None:
    frame = ruled_spark.createDataFrame([(1, 10)], "a BIGINT, b BIGINT")
    with pytest.raises(AnalysisException, match="duplicate column names"):
        frame.withColumnsRenamed({"a": "b", "b": "c"})


def test_with_columns_renamed_folded_match_refuses_duplicate_finals_insensitive(
    spark: ReparkSession,
) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 10)], "a BIGINT, A BIGINT")
    with pytest.raises(AnalysisException, match="duplicate column names"):
        frame.withColumnsRenamed({"a": "x"})


def test_with_columns_renamed_exact_match_keeps_rival_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    frame = spark.createDataFrame([(1, 10)], "a BIGINT, A BIGINT")
    renamed = frame.withColumnsRenamed({"a": "x"})
    assert renamed.columns == ["x", "A"]
    assert _rows(renamed) == [(1, 10)]


def test_live_rule_decides_after_build_insensitive_then_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = _frame(spark)
    spark.conf.set("spark.sql.caseSensitive", "true")
    appended = frame.withColumn("V", functions.lit(99))
    assert appended.columns == ["id", "v", "V"]
    assert _rows(appended) == [(1, 30, 99), (2, 10, 99)]


def test_live_rule_decides_after_build_sensitive_then_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    frame = _frame(spark)
    spark.conf.set("spark.sql.caseSensitive", "false")
    replaced = frame.withColumn("V", functions.lit(99))
    assert replaced.columns == ["id", "V"]
    assert _rows(replaced) == [(1, 99), (2, 99)]
