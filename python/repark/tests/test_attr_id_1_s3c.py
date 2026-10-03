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


def test_with_columns_folded_keys_refuse_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    with pytest.raises(AnalysisException) as refused:
        _frame(spark).withColumns({"v": functions.lit(1), "V": functions.lit(2)})
    assert refused.value.getCondition() == "COLUMN_ALREADY_EXISTS"
    assert refused.value.getSqlState() == "42711"
    assert "[COLUMN_ALREADY_EXISTS] The column `v` already exists." in str(refused.value)


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


def test_with_columns_renamed_sequential_chain_materializes_duplicate_finals(
    ruled_spark: ReparkSession,
) -> None:
    frame = ruled_spark.createDataFrame([(1, 10)], "a BIGINT, b BIGINT")
    renamed = frame.withColumnsRenamed({"a": "b", "b": "c"})
    assert renamed.columns == ["c", "c"]
    assert _rows(renamed) == [(1, 10)]


def test_with_columns_renamed_folded_match_materializes_duplicate_finals_insensitive(
    spark: ReparkSession,
) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 10)], "a BIGINT, A BIGINT")
    renamed = frame.withColumnsRenamed({"a": "x"})
    assert renamed.columns == ["x", "x"]
    assert _rows(renamed) == [(1, 10)]


def test_with_columns_renamed_exact_match_keeps_rival_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    frame = spark.createDataFrame([(1, 10)], "a BIGINT, A BIGINT")
    renamed = frame.withColumnsRenamed({"a": "x"})
    assert renamed.columns == ["x", "A"]
    assert _rows(renamed) == [(1, 10)]


def test_casefold_pair_without_java_match_appends_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 2)], ["stra\u00dfe", "v"])
    appended = frame.withColumn("STRASSE", functions.lit(0))
    assert appended.columns == ["stra\u00dfe", "v", "STRASSE"]
    assert _rows(appended) == [(1, 2, 0)]


def test_casefold_pair_without_java_match_noops_rename_insensitive(
    spark: ReparkSession,
) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 2)], ["stra\u00dfe", "v"])
    assert frame.withColumnRenamed("STRASSE", "z").columns == ["stra\u00dfe", "v"]
    assert frame.withColumnsRenamed({"STRASSE": "z"}).columns == ["stra\u00dfe", "v"]


def test_capital_sharp_s_replaces_small_sharp_s_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 2)], ["\u00df", "v"])
    replaced = frame.withColumn("\u1e9e", functions.lit(0))
    assert replaced.columns == ["\u1e9e", "v"]
    assert _rows(replaced) == [(0, 2)]


def test_java_match_without_casefold_match_renames_insensitive(
    spark: ReparkSession,
) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 2)], ["\u0130d", "v"])
    renamed = frame.withColumnsRenamed({"id": "z"})
    assert renamed.columns == ["z", "v"]
    assert _rows(renamed) == [(1, 2)]


def test_java_match_without_casefold_match_misses_sensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    frame = spark.createDataFrame([(1, 2)], ["\u0130d", "v"])
    renamed = frame.withColumnsRenamed({"id": "z"})
    assert renamed.columns == ["\u0130d", "v"]
    assert _rows(renamed) == [(1, 2)]


def test_dotless_i_replaces_dotted_capital_i_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 2)], ["\u0130d", "v"])
    replaced = frame.withColumn("\u0131d", functions.lit(0))
    assert replaced.columns == ["\u0131d", "v"]
    assert _rows(replaced) == [(0, 2)]


def test_dotless_i_renames_dotted_capital_i_insensitive(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "false")
    frame = spark.createDataFrame([(1, 2)], ["\u0130d", "v"])
    renamed = frame.withColumnRenamed("\u0131d", "z")
    assert renamed.columns == ["z", "v"]
    assert _rows(renamed) == [(1, 2)]


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
