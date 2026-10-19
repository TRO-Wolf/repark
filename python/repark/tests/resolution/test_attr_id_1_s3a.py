from __future__ import annotations

from typing import Any

import pytest

from repark import ReparkSession, _native
from repark.errors import AnalysisException
from repark.spark import functions


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-attr-id-1-s3a").getOrCreate()


@pytest.fixture(params=[False, True], ids=["insensitive", "sensitive"])
def ruled_spark(spark: ReparkSession, request: Any) -> ReparkSession:
    spark.conf.set("spark.sql.caseSensitive", "true" if request.param else "false")
    return spark


def _frame(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, "a"), (2, "b")], "id BIGINT, v STRING")


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    return sorted(tuple(row) for row in frame.collect())


def _ids(frame: Any) -> list[str | None]:
    return list(_native.attribute_ids(frame._inner))


def test_select_of_twin_pair_binds_the_one_attribute(ruled_spark: ReparkSession) -> None:
    twins = _frame(ruled_spark).select("v", "v")
    assert twins.columns == ["v", "v"]
    selected = twins.select("v")
    assert selected.columns == ["v"]
    assert selected.dtypes == [("v", "string")]
    assert _rows(selected) == [("a",), ("b",)]


def test_getitem_of_twin_pair_binds_the_one_attribute(ruled_spark: ReparkSession) -> None:
    twins = _frame(ruled_spark).select("v", "v")
    ref = twins["v"]
    assert ref._attr_id is not None
    assert ref._attr_id == _ids(twins)[0] == _ids(twins)[1]
    selected = twins.select(ref)
    assert selected.columns == ["v"]
    assert _rows(selected) == [("a",), ("b",)]


def test_getattr_of_twin_pair_binds_the_one_attribute(ruled_spark: ReparkSession) -> None:
    twins = _frame(ruled_spark).select("v", "v")
    selected = twins.select(twins.v)
    assert selected.columns == ["v"]
    assert _rows(selected) == [("a",), ("b",)]


def test_two_attributes_one_display_refuses(ruled_spark: ReparkSession) -> None:
    renamed = _frame(ruled_spark).select(
        functions.col("v").alias("v"), functions.col("v").alias("v")
    )
    assert renamed.columns == ["v", "v"]
    assert _ids(renamed)[0] != _ids(renamed)[1]
    with pytest.raises(AnalysisException) as refused:
        renamed.select("v")
    assert "[AMBIGUOUS_REFERENCE]" in str(refused.value)


def test_folded_spelling_binds_exact_only_when_sensitive(spark: ReparkSession) -> None:
    twins = spark.createDataFrame([(1, 2)], ["id", "ID"])
    with pytest.raises(AnalysisException) as refused:
        twins.select("id")
    assert "[AMBIGUOUS_REFERENCE]" in str(refused.value)
    spark.conf.set("spark.sql.caseSensitive", "true")
    with pytest.raises(AnalysisException) as missing:
        twins.select("Id")
    assert "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in str(missing.value)


def test_sensitive_born_frame_binds_exact_folded_spelling(spark: ReparkSession) -> None:
    spark.conf.set("spark.sql.caseSensitive", "true")
    twins = spark.createDataFrame([(1, 2)], ["id", "ID"])
    assert _rows(twins.select("ID")) == [(2,)]
    assert _rows(twins.select("id")) == [(1,)]


def test_parent_column_binds_after_filter(ruled_spark: ReparkSession) -> None:
    base = _frame(ruled_spark)
    filtered = base.filter("id > 1")
    selected = filtered.select(base["v"])
    assert selected.columns == ["v"]
    assert _rows(selected) == [("b",)]


def test_parent_column_binds_after_drop(ruled_spark: ReparkSession) -> None:
    base = _frame(ruled_spark)
    dropped = base.drop("v")
    selected = dropped.select(base["id"])
    assert selected.columns == ["id"]
    assert _rows(selected) == [(1,), (2,)]


def test_alias_mints_a_fresh_id(ruled_spark: ReparkSession) -> None:
    base = _frame(ruled_spark)
    mixed = base.select(base.v.alias("v"), base.v)
    assert mixed.columns == ["v", "v"]
    assert _ids(mixed)[0] != _ids(mixed)[1]
    assert _ids(mixed)[1] == _ids(base)[1]
    with pytest.raises(AnalysisException) as refused:
        mixed.select("v")
    assert "[AMBIGUOUS_REFERENCE]" in str(refused.value)


def _aliased_join(spark: ReparkSession) -> Any:
    left = spark.createDataFrame([(1, "a"), (2, "b")], "id BIGINT, v STRING").alias("a")
    right = spark.createDataFrame([(1, "x"), (2, "y")], "id BIGINT, v STRING").alias("b")
    return left.join(right, left["id"] == right["id"])


def test_qualified_select_binds_one_join_side(ruled_spark: ReparkSession) -> None:
    joined = _aliased_join(ruled_spark)
    selected = joined.select("a.v")
    assert selected.columns == ["v"]
    assert selected.dtypes == [("v", "string")]
    assert _rows(selected) == [("a",), ("b",)]
    other = joined.select("b.v")
    assert _rows(other) == [("x",), ("y",)]


def test_qualified_miss_refuses_unresolved(ruled_spark: ReparkSession) -> None:
    joined = _aliased_join(ruled_spark)
    with pytest.raises(AnalysisException) as refused:
        joined.select("x.v")
    assert "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in str(refused.value)


def test_unicode_spelling_binds_by_casefold(ruled_spark: ReparkSession) -> None:
    sp = ruled_spark.createDataFrame([(2,)], ["Ünï"])
    if ruled_spark.conf.get("spark.sql.caseSensitive") == "true":
        with pytest.raises(AnalysisException) as refused:
            sp.select("ünï")
        assert "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in str(refused.value)
    else:
        assert _rows(sp.select("ünï")) == [(2,)]


def test_quoted_dup_display_misses_like_before(ruled_spark: ReparkSession) -> None:
    frame = ruled_spark.createDataFrame([(1, "x", "y")], ["id", "a`b", "c"])
    twins = frame.select("id", "a`b", "a`b")
    with pytest.raises(AnalysisException) as refused:
        twins.select("`a`b`")
    assert "cannot be resolved" in str(refused.value)
    assert "[AMBIGUOUS_REFERENCE]" not in str(refused.value)


def test_live_case_rule_decides_after_creation(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1,)], ["id"])
    spark.conf.set("spark.sql.caseSensitive", "true")
    with pytest.raises(AnalysisException) as refused:
        frame.select("ID")
    assert "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in str(refused.value)
    assert _rows(frame.select("id")) == [(1,)]
    spark.conf.set("spark.sql.caseSensitive", "false")
    assert _rows(frame.select("ID")) == [(1,)]
