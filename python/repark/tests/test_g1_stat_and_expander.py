"""G1 — DataFrame.stat family + UPDATE/DELETE expander e2e pins.

Apache FAIL-MISSING TOP-1 family (blocked-count 4): ``df.stat.corr/cov/crosstab/sampleBy``.
Expander UPDATE/DELETE unit pins live in ``test_f1_sql_expander.py``; this file owns the
stat value/type surface + a thin e2e that free-SQL DML resolves bare names.
"""

from __future__ import annotations

import math
from pathlib import Path

import pytest

from repark.errors import (
    IllegalArgumentException,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark.session import ReparkSession, _reset_active_session_for_tests


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("pytest-g1-stat")
        .config("repark.sql.autoMemoryCatalog", "false")
        .getOrCreate()
    )
    session.register_memory_catalog("glue_catalog", tmp_path)
    session.create_namespace("glue_catalog", "default")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def test_stat_is_property_not_method(spark: ReparkSession) -> None:
    """Apache suite uses ``df.stat.corr`` — property form (not ``df.stat()``)."""
    frame = spark.range(3)
    handle = frame.stat
    assert not callable(handle)
    assert hasattr(handle, "corr")
    assert hasattr(handle, "cov")
    assert hasattr(handle, "crosstab")
    assert hasattr(handle, "sampleBy")
    assert hasattr(handle, "approxQuantile")


def test_stat_corr_pearson(spark: ReparkSession) -> None:
    rows = [(float(index), math.sqrt(float(index))) for index in range(10)]
    frame = spark.createDataFrame(rows, ["a", "b"])
    corr = frame.stat.corr("a", "b")
    assert abs(corr - 0.95734012) < 1e-6
    assert abs(frame.corr("a", "b") - corr) < 1e-12


def test_stat_corr_type_errors(spark: ReparkSession) -> None:
    frame = spark.range(3).withColumnRenamed("id", "a")
    with pytest.raises(PySparkTypeError) as caught:
        frame.stat.corr(10, "a")  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_STR"
    assert caught.value.getMessageParameters()["arg_name"] == "col1"


def test_stat_cov_sample(spark: ReparkSession) -> None:
    rows = [(float(index), 2.0 * float(index)) for index in range(10)]
    frame = spark.createDataFrame(rows, ["a", "b"])
    cov = frame.stat.cov("a", "b")
    # Sample cov of i vs 2i for i=0..9 is 55/3.
    assert abs(cov - 55.0 / 3.0) < 1e-6


def test_stat_cov_type_errors(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1.0, 2.0)], ["a", "b"])
    with pytest.raises(PySparkTypeError) as caught:
        frame.stat.cov(10, "b")  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_STR"
    with pytest.raises(PySparkTypeError) as caught_bool:
        frame.stat.cov("a", True)  # type: ignore[arg-type]
    assert caught_bool.value.getCondition() == "NOT_STR"
    assert caught_bool.value.getMessageParameters()["arg_name"] == "col2"


def test_stat_crosstab_counts(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(index % 3, index % 2) for index in range(1, 7)], ["a", "b"])
    table = frame.stat.crosstab("a", "b")
    assert table.columns[0] == "a_b"
    collected = sorted(table.collect(), key=lambda row: str(row[0]))
    assert len(collected) == 3
    for row in collected:
        # Counts for the two b strata are positive (Apache uses assertTrue truthiness).
        assert row[1] >= 1
        assert row[2] >= 1


def test_stat_sample_by_type_errors(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(index, index % 3) for index in range(20)], ["a", "b"])
    with pytest.raises(PySparkTypeError) as caught:
        frame.sampleBy(10, fractions={0: 0.5})  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_COLUMN_OR_STR"
    with pytest.raises(PySparkTypeError) as caught_dict:
        frame.sampleBy("b", fractions=[0.5, 0.5])  # type: ignore[arg-type]
    assert caught_dict.value.getCondition() == "NOT_DICT"


def test_stat_sample_by_fraction_range(spark: ReparkSession) -> None:
    """octo C1-Q-001: fractions must be in [0, 1] (Spark sampleBy; NaN included)."""
    frame = spark.createDataFrame([(index, index % 3) for index in range(20)], ["a", "b"])
    with pytest.raises(IllegalArgumentException, match=r"Fraction must be in \[0, 1\]"):
        frame.sampleBy("b", fractions={0: 1.5})
    with pytest.raises(IllegalArgumentException, match=r"Fraction must be in \[0, 1\]"):
        frame.stat.sampleBy("b", fractions={0: -0.01})
    with pytest.raises(IllegalArgumentException, match=r"Fraction must be in \[0, 1\]"):
        frame.sampleBy("b", fractions={0: float("nan")})


def test_stat_sample_by_stratified_bounds(spark: ReparkSession) -> None:
    """Loose count pin — engine random() is not Spark XORShift seed-stable."""
    frame = spark.createDataFrame([(index, index % 3) for index in range(100)], ["a", "b"])
    sampled = frame.stat.sampleBy("b", fractions={0: 0.5, 1: 0.5}, seed=0)
    count = sampled.count()
    # Stratum 2 fully excluded (~33 rows); 0+1 ~67 * 0.5 ≈ 33. Allow wide band.
    assert 10 <= count <= 60


def test_stat_approx_quantile_list(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(float(index),) for index in range(10)], ["a"])
    quantiles = frame.stat.approxQuantile("a", [0.1, 0.5, 0.9], 0.1)
    assert isinstance(quantiles, list)
    assert len(quantiles) == 3
    assert all(isinstance(value, float) for value in quantiles)
    multi = frame.stat.approxQuantile(["a", "a"], [0.5], 0.1)
    assert isinstance(multi, list) and len(multi) == 2
    assert len(multi[0]) == 1


def test_stat_approx_quantile_relative_error(spark: ReparkSession) -> None:
    """octo C1-Q-002 / C2-Q-001: relativeError must be a non-negative number (not NaN)."""
    frame = spark.createDataFrame([(float(index),) for index in range(10)], ["a"])
    with pytest.raises(PySparkValueError) as caught:
        frame.approxQuantile("a", [0.5], -0.1)
    assert caught.value.getCondition() == "NEGATIVE_VALUE"
    with pytest.raises(PySparkValueError) as caught_nan:
        frame.approxQuantile("a", [0.5], float("nan"))
    assert caught_nan.value.getCondition() == "NEGATIVE_VALUE"
    with pytest.raises(PySparkTypeError) as caught_type:
        frame.stat.approxQuantile("a", [0.5], "bad")  # type: ignore[arg-type]
    assert caught_type.value.getCondition() == "NOT_FLOAT_OR_INT"


def test_stat_approx_quantile_probability_domain(spark: ReparkSession) -> None:
    """octo C2-Q-002: probability domain is value-class, not type-class."""
    frame = spark.createDataFrame([(float(index),) for index in range(10)], ["a"])
    with pytest.raises(PySparkValueError) as caught:
        frame.approxQuantile("a", [1.5], 0.0)
    assert caught.value.getCondition() == "VALUE_OUT_OF_BOUND"
    with pytest.raises(PySparkValueError):
        frame.stat.approxQuantile("a", [float("nan")], 0.0)


def test_stat_freq_items_still_loud(spark: ReparkSession) -> None:
    frame = spark.range(3)
    with pytest.raises(UnsupportedOperationException, match="freqItems"):
        frame.stat.freqItems(["id"])


def test_e2e_bare_update_delete_public_sql(spark: ReparkSession) -> None:
    spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "name"]).write.saveAsTable(
        "g1_bare_dml"
    )
    spark.sql("UPDATE g1_bare_dml SET name = 'z' WHERE id = 1")
    spark.sql("DELETE FROM g1_bare_dml WHERE id = 2")
    rows = spark.sql("SELECT id, name FROM g1_bare_dml ORDER BY id").to_arrow().to_pylist()
    assert rows == [{"id": 1, "name": "z"}, {"id": 3, "name": "c"}]


# Group H attempt — SubqueryAlias both join sides (partial)


def test_group_h_self_join_on_name(spark: ReparkSession) -> None:
    """``df.join(df, on=key)`` — both sides get distinct SubqueryAlias (G1 attempt)."""
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    joined = frame.join(frame, on="a")
    assert joined.columns[0] == "a"
    assert joined.count() == 2


def test_group_h_equi_join_independent_ranges(spark: ReparkSession) -> None:
    from repark.spark.functions import lit

    left = spark.range(5).withColumn("v1", lit(1))
    right = spark.range(5).withColumn("v2", lit(2))
    joined = left.join(right, "id")
    assert joined.columns == ["id", "v1", "v2"]
    assert joined.count() == 5


def test_group_h_condition_join_duplicate_nonkey_is_stop(spark: ReparkSession) -> None:
    """H1 flips G1 STOP: condition join with duplicate non-key resolves (self_join_II)."""
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b == frame.b)
    assert joined.columns == ["aa", "b", "a", "b"]
    assert joined.count() == 2


def test_group_h_join_no_alias_when_column_sets_disjoint(spark: ReparkSession) -> None:
    """octo C1-Q-003: disjoint schemas skip SubqueryAlias temp views (no session pollution)."""
    left = spark.createDataFrame([(1, "L")], ["lid", "lv"])
    right = spark.createDataFrame([(1, "R")], ["rid", "rv"])
    before = {table.name for table in spark.catalog.listTables() if table.isTemporary}
    joined = left.join(right, left.lid == right.rid)
    assert joined.count() == 1
    after = {table.name for table in spark.catalog.listTables() if table.isTemporary}
    leaked = {
        name
        for name in (after - before)
        if name.startswith("_repark_jl_") or name.startswith("_repark_jr_")
    }
    assert leaked == set()


def test_join_empty_on_list_respects_cross_join_gate(spark: ReparkSession) -> None:
    """octo C3-Q-001: join(on=[]) must not silent-cartesian without conf."""
    from repark.errors import AnalysisException

    left = spark.range(2)
    right = spark.range(2)
    # Default conf path: empty key list is cartesian — refuse like on=None when disabled.
    spark.conf.set("spark.sql.crossJoin.enabled", "false")
    try:
        with pytest.raises(AnalysisException, match="cartesian"):
            left.join(right, []).count()
    finally:
        spark.conf.set("spark.sql.crossJoin.enabled", "true")
    # When enabled, empty keys are an allowed cross product.
    assert left.join(right, []).count() == 4


# H1: join/identity (extends the Group H attempt)


def test_h1_condition_join_duplicate_nonkey(spark: ReparkSession) -> None:
    """Apache test_self_join_II: condition join with same non-key name on both sides."""
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b == frame.b)
    assert joined.columns == ["aa", "b", "a", "b"]
    assert joined.count() == 2


def test_h1_self_join_range_condition(spark: ReparkSession) -> None:
    """Apache test_self_join: range self-join on renamed column condition."""
    from repark.spark.functions import lit

    df1 = spark.range(10).withColumn("a", lit(0))
    df2 = df1.withColumnRenamed("a", "b")
    joined = df1.join(df2, df1["a"] == df2["b"])
    assert joined.count() == 100
    assert joined.columns.count("id") == 2


def test_h1_self_join_left_duplicate_names(spark: ReparkSession) -> None:
    """Apache test_self_join_III: left join keeps duplicate display names."""
    from repark.spark.functions import lit

    df1 = spark.range(10).withColumn("value", lit(1))
    df2 = df1.union(df1)
    joined = df1.join(df2, df1.id == df2.id, "left")
    assert joined.columns == ["id", "value", "id", "value"]
    assert joined.count() == 20


def test_h1_self_join_right(spark: ReparkSession) -> None:
    """Apache test_self_join_IV: right join + multi-name output."""
    from repark.spark.functions import lit

    df1 = spark.range(10).withColumn("value", lit(1))
    df2 = df1.withColumn("value", lit(2)).union(df1.withColumn("value", lit(3)))
    joined = df1.join(df2, df1.id == df2.id, "right")
    assert joined.columns == ["id", "value", "id", "value"]
    assert joined.count() == 20


def test_h1_ambiguous_reference_on_joined(spark: ReparkSession) -> None:
    """Bare joined['b'] raises AMBIGUOUS_REFERENCE (Spark 4.1.2 class)."""
    from repark.errors import AnalysisException

    frame = spark.createDataFrame([(1, 2)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b == frame.b)
    with pytest.raises(AnalysisException, match=r"AMBIGUOUS_REFERENCE"):
        _ = joined["b"]


def test_h1_drop_by_column_correct_side(spark: ReparkSession) -> None:
    """drop(left['b']) drops only the left-side engine field."""
    frame = spark.createDataFrame([(1, 2)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b == frame.b)
    dropped = joined.drop(left["b"])
    assert dropped.columns == ["aa", "a", "b"]
    assert dropped.count() == 1


def test_h1_select_parent_columns_both_sides(spark: ReparkSession) -> None:
    """joined.select(left['b'], right['b']) yields both bare display names."""
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b == frame.b)
    both = joined.select(left["b"], frame["b"])
    assert both.columns == ["b", "b"]
    assert both.count() == 2


def test_h1_select_join_keys_all_how(spark: ReparkSession) -> None:
    """Apache test_select_join_keys: select parent id after name equi-join for each how."""
    from repark.spark.functions import lit

    df1 = spark.range(10).withColumn("v1", lit(1))
    df2 = spark.range(10).withColumn("v2", lit(2))
    for how in ["inner", "left", "right", "full"]:
        left_keys = df1.join(df2, "id", how).select(df1["id"])
        right_keys = df1.join(df2, "id", how).select(df2["id"])
        assert left_keys.columns == ["id"]
        assert right_keys.columns == ["id"]
        assert left_keys.count() == 10
        assert right_keys.count() == 10


def test_h1_select_star_multi_name(spark: ReparkSession) -> None:
    """octo H1-C1-002: select('*') on multi-name join keeps display names + row count."""
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b == frame.b)
    starred = joined.select("*")
    assert starred.columns == ["aa", "b", "a", "b"]
    assert starred.count() == 2


def test_h1_chained_condition_join_multi_name(spark: ReparkSession) -> None:
    """octo H1-C1-001: chained condition join after dup display names plans cleanly."""
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b == frame.b)
    extra = spark.createDataFrame([(2, "x"), (4, "y")], ["b", "z"])
    chained = joined.join(extra, left["b"] == extra["b"])
    assert "z" in chained.columns
    assert chained.count() == 2


def test_h1_filter_order_by_parent_columns(spark: ReparkSession) -> None:
    """octo H1-C1-003: post-join filter/orderBy on parent Columns for dup field names."""
    frame = spark.createDataFrame([(1, 2), (3, 4), (5, 6)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b == frame.b)
    filtered = joined.filter(left["b"] > 2)
    assert filtered.count() == 2
    ordered = joined.orderBy(left["b"].desc())
    assert ordered.count() == 3
    # Value pin: desc on left b → 6, then 4, then 2 (not just count).
    top_left_b = ordered.select(left["b"]).collect()
    assert [row[0] for row in top_left_b] == [6, 4, 2]
    null_checked = joined.filter(left["b"].isNotNull())
    assert null_checked.count() == 3


def test_h1_select_cast_parent_column(spark: ReparkSession) -> None:
    """octo H1-C1-004: select(parent.col.cast(...)) after multi-name join."""
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b == frame.b)
    casted = joined.select(left["b"].cast("double").alias("bd"))
    assert casted.columns == ["bd"]
    assert casted.count() == 2
    values = [row[0] for row in casted.collect()]
    assert values == [2.0, 4.0]


def test_h1_rename_dropna_fillna_when_multi_name(spark: ReparkSession) -> None:
    """octo H1-C2-001..004: rename / na / when on multi-name condition joins."""
    from repark import functions as functions_mod

    frame = spark.createDataFrame([(1, 2), (3, None)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b.eqNullSafe(frame.b))
    assert joined.columns == ["aa", "b", "a", "b"]
    renamed = joined.withColumnRenamed("aa", "AA")
    assert renamed.columns == ["AA", "b", "a", "b"]
    assert joined.dropna().count() == 1
    filled = joined.fillna(0)
    assert filled.columns == ["aa", "b", "a", "b"]
    # Both sides' b filled; row with null→0.
    left_b_vals = [row[0] for row in filled.select(left["b"]).collect()]
    assert 0 in left_b_vals
    flags = joined.select(functions_mod.when(left["b"] > 0, 1).otherwise(0).alias("f")).collect()
    assert [row[0] for row in flags] == [1, 0]


def test_h1_todf_alias_union_sample_multi_name(spark: ReparkSession) -> None:
    """octo H1-C3: toDF / alias / union / sample keep multi-name display identity."""
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b == frame.b)
    renamed = joined.toDF("w", "x", "y", "z")
    assert renamed.columns == ["w", "x", "y", "z"]
    assert renamed.count() == 2
    aliased = joined.alias("j")
    assert aliased.columns == ["aa", "b", "a", "b"]
    united = joined.union(joined)
    assert united.columns == ["aa", "b", "a", "b"]
    assert united.count() == 4
    sampled = joined.sample(False, 1.0, seed=1)
    assert sampled.columns == ["aa", "b", "a", "b"]
    assert sampled.count() == 2


def test_h1_withcolumns_describe_dropdup_multi_name(spark: ReparkSession) -> None:
    """octo H1-C4: withColumns / describe / dropDuplicates on multi-name joins."""
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b == frame.b)
    widened = joined.withColumns({"z": left["b"]})
    assert widened.columns == ["aa", "b", "a", "b", "z"]
    assert widened.count() == 2
    described = joined.describe()
    assert described.columns[0] == "summary"
    assert described.columns[1:] == ["aa", "b", "a", "b"]
    assert described.count() == 5
    deduped = joined.dropDuplicates(["b"])
    assert deduped.columns == ["aa", "b", "a", "b"]
    assert deduped.count() == 2
    assert joined.intersect(joined).columns == ["aa", "b", "a", "b"]


def test_h1_rename_replace_rsplit_dtypes_multi_name(spark: ReparkSession) -> None:
    """octo H1-C5/C6: withColumnsRenamed / replace / randomSplit / dtypes overlay."""
    frame = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    left = frame.select(frame.a.alias("aa"), frame.b)
    joined = left.join(frame, left.b == frame.b)
    renamed = joined.withColumnsRenamed({"aa": "AA"})
    assert renamed.columns == ["AA", "b", "a", "b"]
    replaced = joined.replace(1, 9, subset=["aa"])
    assert [row[0] for row in replaced.select("aa").collect()] == [9, 3]
    parts = joined.randomSplit([0.5, 0.5], seed=1)
    assert all(part.columns == ["aa", "b", "a", "b"] for part in parts)
    assert sum(part.count() for part in parts) == 2
    assert [name for name, _type in joined.dtypes] == ["aa", "b", "a", "b"]
    assert [field.name for field in joined.schema.fields] == ["aa", "b", "a", "b"]
    assert joined.selectExpr("*").columns == ["aa", "b", "a", "b"]
