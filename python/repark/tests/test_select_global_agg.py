"""R-SELECT-GLOBAL-AGG: df.select(<aggregates only>) is Spark global aggregate."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark.errors import AnalysisException


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("select-global-agg").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def frame(spark: ReparkSession) -> object:
    return spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "x"])


def test_select_sum_matches_agg(frame: object) -> None:
    via_select = frame.select(F.sum("x")).to_arrow()
    via_agg = frame.agg(F.sum("x")).to_arrow()
    assert via_select.column_names == via_agg.column_names == ["sum(x)"]
    assert via_select.to_pylist() == via_agg.to_pylist() == [{"sum(x)": 60}]
    assert via_select.schema.field("sum(x)").type in (pa.int64(), pa.int32(), pa.decimal128(38, 0))


def test_select_count_star_empty_frame(spark: ReparkSession) -> None:
    empty = spark.createDataFrame([], "x INT")
    table = empty.select(F.count("*")).to_arrow()
    assert table.num_rows == 1
    # name may be count(1) or count(*) depending on engine display
    assert table.num_columns == 1
    assert table.column(0).to_pylist() == [0]


def test_select_multi_aggregates(frame: object) -> None:
    table = frame.select(F.sum("x"), F.count("*"), F.max("id")).to_arrow()
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["sum(x)"] == 60
    assert list(row.values())[1] == 3  # count
    assert row["max(id)"] == 3


def test_select_mixed_aggregate_and_column_missing_group_by(frame: object) -> None:
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught:
        frame.select(F.sum("x"), F.col("id")).collect()
    assert "GROUP BY" in str(caught.value)


def test_select_non_aggregate_unchanged(frame: object) -> None:
    table = frame.select("id", "x").to_arrow()
    assert table.column_names == ["id", "x"]
    assert table.num_rows == 3


def test_select_agg_alias(frame: object) -> None:
    table = frame.select(F.sum("x").alias("total")).to_arrow()
    assert table.column_names == ["total"]
    assert table.to_pylist() == [{"total": 60}]


def test_is_aggregate_sticky_on_alias_cast_and_binary() -> None:
    """Mutation-proof: sticky ``_is_aggregate`` across alias / cast / arithmetic (C1-Q-001)."""
    bare = F.sum("x")
    assert bare._is_aggregate is True
    assert (bare + 1)._is_aggregate is True
    assert bare.cast("double")._is_aggregate is True
    assert bare.alias("total")._is_aggregate is True
    assert bare.alias("total").cast("double")._is_aggregate is True
    assert (-bare)._is_aggregate is True
    # Non-aggregate stays non-aggregate.
    assert F.col("x")._is_aggregate is False
    assert (F.col("x") + 1)._is_aggregate is False
    assert F.lit(1)._is_aggregate is False
    assert F.lit(1)._is_foldable is True


def test_select_sum_plus_one_is_global_agg(frame: object) -> None:
    """``select(sum(x)+1)`` is one global-agg row (Spark); must not fall to row-wise select."""
    table = frame.select((F.sum("x") + 1).alias("s1")).to_arrow()
    assert table.num_rows == 1
    assert table.column_names == ["s1"]
    assert table.to_pylist() == [{"s1": 61}]


def test_select_cast_sum_is_global_agg(frame: object) -> None:
    """``select(sum(x).cast("double"))`` is global agg with floating type (C1-Q-001)."""
    table = frame.select(F.sum("x").cast("double").alias("total")).to_arrow()
    assert table.num_rows == 1
    assert table.column_names == ["total"]
    assert table.to_pylist() == [{"total": 60.0}]
    assert table.schema.field("total").type == pa.float64()


def test_select_sum_with_lit_is_global_agg(frame: object) -> None:
    """Spark allows foldable constants with aggregates — not ``[MISSING_GROUP_BY]`` (C1-Q-002)."""
    table = frame.select(F.sum("x"), F.lit(1).alias("one")).to_arrow()
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["sum(x)"] == 60
    assert row["one"] == 1


def test_select_composed_agg_with_bare_column_missing_group_by(frame: object) -> None:
    """Composed agg + bare col must still raise ``[MISSING_GROUP_BY]`` (C1-L-002)."""
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught:
        frame.select(F.sum("x") + 1, F.col("id")).collect()
    assert "GROUP BY" in str(caught.value)


def test_select_sum_plus_bare_column_missing_group_by(frame: object) -> None:
    """Nested free attr inside sticky-OR agg expr → ``[MISSING_GROUP_BY]`` (C2-Q-001 / C2-L-001)."""
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught:
        frame.select(F.sum("x") + F.col("id")).collect()
    assert "GROUP BY" in str(caught.value)


def test_select_coalesce_sum_and_bare_missing_group_by(frame: object) -> None:
    """``coalesce(sum(x), id)`` is mixed free+agg, not pure global (C2-Q-001 / C2-L-001)."""
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught:
        frame.select(F.coalesce(F.sum("x"), F.col("id"))).collect()
    assert "GROUP BY" in str(caught.value)


def test_select_when_bare_and_sum_missing_group_by(frame: object) -> None:
    """``when(id > 0, sum(x))`` free condition + agg → ``[MISSING_GROUP_BY]`` (C2-L-001)."""
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught:
        frame.select(F.when(F.col("id") > 0, F.sum("x")).otherwise(0)).collect()
    assert "GROUP BY" in str(caught.value)


def test_select_abs_sum_is_global_agg(frame: object) -> None:
    """``select(abs(sum(x)))`` is global agg — scalar wrappers keep sticky (C2-Q-002 / C2-L-002)."""
    table = frame.select(F.abs(F.sum("x")).alias("a")).to_arrow()
    assert table.num_rows == 1
    assert table.column_names == ["a"]
    assert table.to_pylist() == [{"a": 60}]


def test_select_round_sum_is_global_agg(frame: object) -> None:
    """``select(round(sum(x)))`` is global agg via ``_scalar`` sticky (C2-Q-002)."""
    table = frame.select(F.round(F.sum("x")).alias("r")).to_arrow()
    assert table.num_rows == 1
    assert table.column_names == ["r"]
    assert table.to_pylist() == [{"r": 60.0}] or table.to_pylist() == [{"r": 60}]


def test_select_sum_with_hostile_count_lit_string(frame: object) -> None:
    """Foldable lit must not be corrupted by count SQL rewrite (C2-SAF-001)."""
    table = frame.select(
        F.sum("x"),
        F.lit("count(Int64(1))").alias("token"),
    ).to_arrow()
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["sum(x)"] == 60
    assert row["token"] == "count(Int64(1))"


def test_select_sum_with_nonfinite_float_lit(frame: object) -> None:
    """Free-SQL global-agg must embed NaN/Inf as CAST floats, not bare identifiers (C6-SAF-002).

    Pre-fix: ``_lit_sql_expr`` used ``repr(float)`` → bare ``nan``/``inf`` which DataFusion
    treats as column refs (Schema error / binds a column named nan).
    """
    import math

    from repark.spark.functions import _lit_sql_expr

    assert _lit_sql_expr(float("nan")) == "CAST('NaN' AS DOUBLE)"
    assert _lit_sql_expr(float("inf")) == "CAST('Infinity' AS DOUBLE)"
    assert _lit_sql_expr(float("-inf")) == "CAST('-Infinity' AS DOUBLE)"
    assert _lit_sql_expr(1.5) == "1.5"
    table = frame.select(
        F.sum("x"),
        F.lit(float("nan")).alias("n"),
        F.lit(float("inf")).alias("pinf"),
        F.lit(float("-inf")).alias("ninf"),
    ).to_arrow()
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["sum(x)"] == 60
    assert math.isnan(row["n"])
    assert row["pinf"] == math.inf
    assert row["ninf"] == -math.inf
    # Column named ``nan`` must not capture the lit (identifier vs float constant).
    frame_nan = frame.select(F.col("x").alias("nan"))
    row2 = (
        frame_nan.select(F.sum("nan"), F.lit(float("nan")).alias("lit_n")).to_arrow().to_pylist()[0]
    )
    assert row2["sum(nan)"] == 60
    assert math.isnan(row2["lit_n"])


def test_is_aggregate_sticky_on_null_when_coalesce_abs() -> None:
    """Mutation-proof sticky ``_is_aggregate`` / free-attr on null/when/coalesce/abs (C2-Q-003)."""
    bare = F.sum("x")
    free = F.col("id")
    assert bare.is_null()._is_aggregate is True
    assert bare.is_null()._has_free_attribute is False
    assert F.coalesce(bare, F.lit(0))._is_aggregate is True
    assert F.coalesce(bare, F.lit(0))._has_free_attribute is False
    assert F.coalesce(bare, free)._is_aggregate is True
    assert F.coalesce(bare, free)._has_free_attribute is True
    when_col = F.when(F.lit(True), bare).otherwise(0)
    assert when_col._is_aggregate is True
    assert when_col._has_free_attribute is False
    when_free = F.when(free > 0, bare).otherwise(0)
    assert when_free._is_aggregate is True
    assert when_free._has_free_attribute is True
    assert F.abs(bare)._is_aggregate is True
    assert F.abs(bare)._has_free_attribute is False
    assert F.round(bare)._is_aggregate is True
    assert F.concat(bare.cast("string"), F.lit("x"))._is_aggregate is True
    # Free-attribute sticky on bare col compositions (mutation-proof for C2 free bit).
    assert free._has_free_attribute is True
    assert (bare + free)._has_free_attribute is True
    assert (bare + free)._is_aggregate is True
    assert (bare + 1)._has_free_attribute is False


def test_select_sum_compound_with_lit_is_global_agg(frame: object) -> None:
    """``select(sum(x+1), lit)`` uses structural sql_expr, not Int64 schema_name (C3-Q-001)."""
    table = frame.select(F.sum(F.col("x") + 1), F.lit(0).alias("z")).to_arrow()
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["sum((x + 1))"] == 63
    assert row["z"] == 0


def test_select_sum_compound_plus_one_and_cast(frame: object) -> None:
    """``sum(x+1)+1`` / ``cast(sum(x+1))`` stay global-agg via structural sql_expr (C3-001)."""
    plus = frame.select((F.sum(F.col("x") + 1) + 1).alias("s")).to_arrow()
    assert plus.num_rows == 1
    assert plus.to_pylist() == [{"s": 64}]
    casted = frame.select(F.sum(F.col("x") + 1).cast("double").alias("s")).to_arrow()
    assert casted.num_rows == 1
    assert casted.to_pylist() == [{"s": 63.0}]
    assert casted.schema.field("s").type == pa.float64()


def test_select_sum_with_current_timestamp_is_global_agg(frame: object) -> None:
    """``current_timestamp()`` with aggregates is global agg, not MISSING_GROUP_BY (C3-Q-002)."""
    table = frame.select(F.sum("x"), F.current_timestamp().alias("ts")).to_arrow()
    assert table.num_rows == 1
    assert table.column_names == ["sum(x)", "ts"]
    assert table.to_pylist()[0]["sum(x)"] == 60
    assert table.to_pylist()[0]["ts"] is not None


def test_select_sum_with_current_date_is_global_agg(frame: object) -> None:
    """``current_date()`` companion with aggregates is global agg (C3-Q-002 companion)."""
    table = frame.select(F.sum("x"), F.current_date().alias("d")).to_arrow()
    assert table.num_rows == 1
    assert table.to_pylist()[0]["sum(x)"] == 60
    assert table.to_pylist()[0]["d"] is not None


def test_select_alias_then_arithmetic_is_global_agg(frame: object) -> None:
    """``sum(x).alias(t)+1`` must not embed intermediate AS into SQL (C3-002)."""
    table = frame.select((F.sum("x").alias("t") + 1).alias("s")).to_arrow()
    assert table.num_rows == 1
    assert table.to_pylist() == [{"s": 61}]


def test_select_alias_then_cast_is_global_agg(frame: object) -> None:
    """``sum(x).alias(t).cast("double")`` is CAST(sum) not CAST(... AS t AS DOUBLE) (C3-SAF-001)."""
    table = frame.select(F.sum("x").alias("t").cast("double")).to_arrow()
    assert table.num_rows == 1
    assert table.column_names == ["t"]
    assert table.to_pylist() == [{"t": 60.0}]
    assert table.schema.field("t").type == pa.float64()


def test_select_hostile_count_name_does_not_retarget_from(frame: object) -> None:
    """Quoted count identifier — hostile name must not rewrite FROM (C3-SEC-001)."""
    hostile = "x) FROM secret --"
    with pytest.raises(Exception) as caught:
        frame.select(F.count(hostile), F.lit(1).alias("one")).collect()
    message = str(caught.value).lower()
    # Fail on missing field (quoted), never on unresolved external table ``secret``.
    assert "no field" in message
    assert "table 'datafusion.public.secret'" not in message
    assert 'table "secret"' not in message
    # Structural pin: sql_expr is quoted (mutation-proof for the quoting fix).
    assert F.count(hostile).sql_expr_part() == f'count("{hostile}")'


def test_select_case_preserved_sum_with_lit(frame: object) -> None:
    """SQL path rebinds case-preserved ``sum("X")`` like native (C3-003)."""
    preserved = frame.select("X")  # type: ignore[attr-defined]
    via_native = preserved.select(F.sum("X")).to_arrow()
    via_sql = preserved.select(F.sum("X"), F.lit(1).alias("one")).to_arrow()
    assert via_native.num_rows == via_sql.num_rows == 1
    assert via_native.column_names == ["sum(X)"]
    assert via_sql.column_names == ["sum(X)", "one"]
    assert via_native.to_pylist()[0]["sum(X)"] == via_sql.to_pylist()[0]["sum(X)"] == 60
    assert via_sql.to_pylist()[0]["one"] == 1


def test_aggregate_structural_sql_expr_quoted() -> None:
    """Mutation-proof: AF builders carry structural quoted sql_expr (C3-Q-001 / C3-SEC-001)."""
    assert F.sum("x").sql_expr_part() == 'sum("x")'
    assert F.avg("x").sql_expr_part() == 'avg("x")'
    assert F.min("x").sql_expr_part() == 'min("x")'
    assert F.max("x").sql_expr_part() == 'max("x")'
    assert F.count("x").sql_expr_part() == 'count("x")'
    assert F.sum(F.col("x") + 1).sql_expr_part() == 'sum(("x" + 1))'
    # alias does not embed AS into sql_expr (C3-002).
    assert F.sum("x").alias("total").sql_expr_part() == 'sum("x")'
    assert (F.sum("x").alias("total") + 1).sql_expr_part() == '(sum("x") + 1)'
    assert F.sum("x").alias("total").cast("double").sql_expr_part() == 'CAST(sum("x") AS DOUBLE)'
    # current_timestamp is free of attributes / foldable for classifier (C3-Q-002).
    ts = F.current_timestamp()
    assert ts._has_free_attribute is False
    assert ts._is_foldable is True
    assert ts._is_aggregate is False


# ---- Cycle-4 pins (alias rebind / batch-4 sql_expr / IGNORE NULLS / sticky wrappers) ----------


def test_select_case_preserved_sum_alias_and_alias_lit(frame: object) -> None:
    """``.alias`` clears ``_agg_name`` but rebind still binds case-preserved leaves (C4-Q-001)."""
    preserved = frame.select("X")  # type: ignore[attr-defined]
    pure = preserved.select(F.sum("X").alias("total")).to_arrow()
    assert pure.num_rows == 1
    assert pure.column_names == ["total"]
    assert pure.to_pylist() == [{"total": 60}]
    via_sql = preserved.select(F.sum("x").alias("t"), F.lit(1).alias("one")).to_arrow()
    assert via_sql.num_rows == 1
    assert via_sql.column_names == ["t", "one"]
    assert via_sql.to_pylist() == [{"t": 60, "one": 1}]


def test_select_batch4_af_sql_expr_and_case_preserved(frame: object) -> None:
    """Batch-4 AFs: structural sql_expr + rebind allowlist (C4-Q-002 / C4-SEC-001 / C4-L-002)."""
    assert F.stddev("x").sql_expr_part() == 'stddev("x")'
    assert F.variance("x").sql_expr_part() == 'var_samp("x")'
    assert F.median("x").sql_expr_part() == 'median("x")'
    assert F.bit_and("x").sql_expr_part() == 'bit_and("x")'
    assert F.corr("x", "id").sql_expr_part() == 'corr("x", "id")'
    # Companion lit forces free-SQL path — must not fall back to unquoted schema_name.
    table = frame.select(F.stddev("x"), F.lit(1).alias("one")).to_arrow()
    assert table.num_rows == 1
    assert table.column_names == ["stddev(x)", "one"]
    assert table.to_pylist()[0]["one"] == 1
    assert abs(table.to_pylist()[0]["stddev(x)"] - 10.0) < 1e-9
    # Case-preserved pure + lit (rebind allowlist for stddev).
    preserved = frame.select("X")  # type: ignore[attr-defined]
    pure = preserved.select(F.stddev("X")).to_arrow()
    via_sql = preserved.select(F.stddev("X"), F.lit(0).alias("z")).to_arrow()
    assert pure.num_rows == via_sql.num_rows == 1
    assert pure.to_pylist()[0]["stddev(X)"] == via_sql.to_pylist()[0]["stddev(X)"]
    # Hostile identifier stays quoted (C4-SEC-001).
    hostile = "x) FROM secret --"
    assert F.stddev(hostile).sql_expr_part() == f'stddev("{hostile}")'
    with pytest.raises(Exception) as caught:
        frame.select(F.stddev(hostile), F.lit(1).alias("one")).collect()
    message = str(caught.value).lower()
    assert "no field" in message
    assert "table 'datafusion.public.secret'" not in message


def test_select_asc_preserves_sql_expr() -> None:
    """``Column.asc``/``desc`` keep structural sql_expr (C4-SEC-002)."""
    bare = F.sum("x")
    assert bare.asc().sql_expr_part() == 'sum("x")'
    assert bare.desc().sql_expr_part() == 'sum("x")'
    assert bare.asc()._is_aggregate is True
    assert bare.asc()._sql_expr == bare._sql_expr
    hostile = "x) FROM secret --"
    assert F.sum(hostile).asc().sql_expr_part() == f'sum("{hostile}")'


def test_select_first_ignorenulls_sql_path(spark: ReparkSession) -> None:
    """``first/last(ignorenulls=True)`` free-SQL global-agg value parity (C4-L-001 / C5-Q-003)."""
    source = spark.createDataFrame([(None,), (20,), (30,)], ["v"])
    assert F.first("v", ignorenulls=True).sql_expr_part() == 'first_value("v") IGNORE NULLS'
    assert F.last("v", ignorenulls=True).sql_expr_part() == 'last_value("v") IGNORE NULLS'
    assert F.first("v", ignorenulls=False).sql_expr_part() == 'first_value("v")'
    # lit companion forces SQL path — must match native 20, not leading NULL.
    via_sql = source.select(F.first("v", ignorenulls=True), F.lit(1).alias("one")).to_arrow()
    via_native = source.agg(F.first("v", ignorenulls=True)).to_arrow()
    assert via_sql.num_rows == 1
    assert via_sql.to_pylist()[0]["first(v)"] == via_native.to_pylist()[0]["first(v)"] == 20
    casted = source.select(F.first("v", ignorenulls=True).cast("int").alias("f")).to_arrow()
    assert casted.to_pylist() == [{"f": 20}]
    # last value pin: SQL + pure native both skip leading NULL to 30 (C5-Q-003).
    last_sql = source.select(F.last("v", ignorenulls=True), F.lit(1).alias("one")).to_arrow()
    last_native = source.agg(F.last("v", ignorenulls=True)).to_arrow()
    last_pure = source.select(F.last("v", ignorenulls=True)).to_arrow()
    assert last_sql.to_pylist()[0]["last(v)"] == last_native.to_pylist()[0]["last(v)"] == 30
    assert last_pure.to_pylist()[0]["last(v)"] == 30


def test_select_collect_list_sql_nulls_and_empty(spark: ReparkSession) -> None:
    """``collect_list/set`` SQL path excludes nulls and empty→[] (C4-L-002 / C5-Q-002)."""
    assert "IGNORE NULLS" in F.collect_list("x").sql_expr_part()
    assert "make_array()" in F.collect_list("x").sql_expr_part()
    # collect_set uses array_distinct(array_agg … IGNORE NULLS) — DF DISTINCT+IGNORE NULLS
    # keeps NULL (C5-Q-002).
    set_sql = F.collect_set("x").sql_expr_part()
    assert "array_distinct" in set_sql
    assert "IGNORE NULLS" in set_sql
    assert "make_array()" in set_sql
    with_nulls = spark.createDataFrame([(None,), (20,), (None,), (30,), (20,)], ["v"])
    table = with_nulls.select(F.collect_list("v"), F.lit(1).alias("one")).to_arrow()
    assert table.num_rows == 1
    values = table.to_pylist()[0]["collect_list(v)"]
    assert values is not None
    assert sorted(values) == [20, 20, 30]
    # collect_set value path: null-exclude + de-dupe (not just DISTINCT substring).
    set_table = with_nulls.select(F.collect_set("v"), F.lit(1).alias("one")).to_arrow()
    set_values = set_table.to_pylist()[0]["collect_set(v)"]
    assert set_values is not None
    assert None not in set_values
    assert sorted(set_values) == [20, 30]
    empty = spark.createDataFrame([], "x INT")
    empty_table = empty.select(F.collect_list("x"), F.lit(1).alias("one")).to_arrow()
    assert empty_table.to_pylist()[0]["collect_list(x)"] == []
    empty_set = empty.select(F.collect_set("x"), F.lit(1).alias("one")).to_arrow()
    assert empty_set.to_pylist()[0]["collect_set(x)"] == []


def test_select_isnull_and_date_family_sticky(frame: object, spark: ReparkSession) -> None:
    """``isnull`` / date_* keep free+agg sticky for select routing (C4-L-003)."""
    free = F.col("id")
    bare = F.sum("x")
    assert F.isnull(free)._has_free_attribute is True
    assert F.isnull(free)._is_aggregate is False
    assert F.isnull(bare)._is_aggregate is True
    assert F.isnull(bare)._has_free_attribute is False
    assert F.isnull(bare).sql_expr_part() == '(sum("x") IS NULL)'
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY"):
        frame.select(F.sum("x"), F.isnull(F.col("id"))).collect()
    # isnull(sum) is pure global (one row).
    isnull_sum = frame.select(F.isnull(F.sum("x")).alias("n")).to_arrow()
    assert isnull_sum.num_rows == 1
    assert isnull_sum.to_pylist() == [{"n": False}]
    # date_add on free attr beside agg → MISSING_GROUP_BY.
    from datetime import date

    dated = spark.createDataFrame([(date(2024, 1, 15), 10)], ["d", "x"])
    assert F.date_add(F.col("d"), 1)._has_free_attribute is True
    assert F.date_add(F.max("d"), 1)._is_aggregate is True
    assert F.date_add(F.max("d"), 1)._has_free_attribute is False
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY"):
        dated.select(F.sum("x"), F.date_add(F.col("d"), 1)).collect()
    # Post-agg date wrapper keeps global-agg routing.
    wrapped = dated.select(F.date_add(F.max("d"), 1).alias("next")).to_arrow()
    assert wrapped.num_rows == 1
    assert wrapped.to_pylist() == [{"next": date(2024, 1, 16)}]
    # Mutation-proof sticky on the rest of the date family named in C4-L-003.
    assert F.add_months(F.col("d"), 1)._has_free_attribute is True
    assert F.date_format(F.col("d"), "yyyy")._has_free_attribute is True
    assert F.trunc(F.col("d"), "month")._has_free_attribute is True
    assert F.date_trunc("month", F.col("d"))._has_free_attribute is True
    assert F.add_months(F.max("d"), 1)._is_aggregate is True
    assert F.date_format(F.max("d"), "yyyy")._is_aggregate is True


def test_select_case_preserved_rebind_extended_afs(spark: ReparkSession) -> None:
    """Pure native rebind covers first/last/collect_*/count_distinct/corr (C5-Q-001 / C5-L-002).

    After ``select("X")`` the field is case-preserved; unquoted AF leaves fail without rebind.
    Pure ``select(AF)`` (native path) must match lit-companion SQL path and ``agg``.
    """
    preserved = spark.createDataFrame([(10,), (20,), (30,)], ["X"]).select("X")
    # first / last pure rebind
    first_pure = preserved.select(F.first("X")).to_arrow()
    first_sql = preserved.select(F.first("X"), F.lit(1).alias("one")).to_arrow()
    assert first_pure.to_pylist()[0]["first(X)"] == first_sql.to_pylist()[0]["first(X)"] == 10
    last_pure = preserved.select(F.last("X")).to_arrow()
    assert last_pure.to_pylist()[0]["last(X)"] == 30
    # collect_list / collect_set pure rebind
    cl_pure = preserved.select(F.collect_list("X")).to_arrow()
    cl_sql = preserved.select(F.collect_list("X"), F.lit(0).alias("z")).to_arrow()
    assert (
        sorted(cl_pure.to_pylist()[0]["collect_list(X)"])
        == sorted(cl_sql.to_pylist()[0]["collect_list(X)"])
        == [10, 20, 30]
    )
    cs_pure = preserved.select(F.collect_set("X")).to_arrow()
    assert sorted(cs_pure.to_pylist()[0]["collect_set(X)"]) == [10, 20, 30]
    # count_distinct pure rebind
    cd_pure = preserved.select(F.count_distinct("X")).to_arrow()
    cd_native = preserved.agg(F.count_distinct("X")).to_arrow()
    assert cd_pure.to_pylist() == cd_native.to_pylist() == [{"count(DISTINCT X)": 3}]
    # binary corr / covar on case-preserved pair
    pair = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["A", "B"]).select("A", "B")
    corr_pure = pair.select(F.corr("A", "B")).to_arrow()
    corr_sql = pair.select(F.corr("A", "B"), F.lit(1).alias("one")).to_arrow()
    assert corr_pure.num_rows == corr_sql.num_rows == 1
    assert abs(corr_pure.to_pylist()[0]["corr(A, B)"] - 1.0) < 1e-9
    assert abs(corr_sql.to_pylist()[0]["corr(A, B)"] - 1.0) < 1e-9
    covar_pure = pair.select(F.covar_samp("A", "B")).to_arrow()
    assert covar_pure.num_rows == 1
    assert abs(covar_pure.to_pylist()[0]["covar_samp(A, B)"] - 10.0) < 1e-9


def test_select_count_distinct_multi_sql_null_if_any(spark: ReparkSession) -> None:
    """Multi-col ``count_distinct`` free-SQL path keeps null-if-any pack (C5-L-001).

    Fixture: ``(1,NULL),(1,1),(1,1),(NULL,2),(2,2)`` → count 2 (tuples ``(1,1)`` and ``(2,2)``).
    SQL path (lit companion) must match pure native / ``agg``, not bare multi-arg COUNT DISTINCT.
    """
    source = spark.createDataFrame(
        [(1, None), (1, 1), (1, 1), (None, 2), (2, 2)],
        ["a", "b"],
    )
    sql_expr = F.count_distinct("a", "b").sql_expr_part()
    assert "struct(" in sql_expr
    assert "IS NOT NULL" in sql_expr
    assert 'count(DISTINCT "a", "b")' not in sql_expr
    via_sql = source.select(F.count_distinct("a", "b"), F.lit(1).alias("one")).to_arrow()
    via_pure = source.select(F.count_distinct("a", "b")).to_arrow()
    via_native = source.agg(F.count_distinct("a", "b")).to_arrow()
    assert via_sql.num_rows == via_pure.num_rows == via_native.num_rows == 1
    assert via_sql.to_pylist()[0]["count(DISTINCT a, b)"] == 2
    assert via_pure.to_pylist()[0]["count(DISTINCT a, b)"] == 2
    assert via_native.to_pylist()[0]["count(DISTINCT a, b)"] == 2
    assert via_sql.to_pylist()[0]["one"] == 1
    # Case-preserved multi-col pure rebind + SQL path.
    upper = spark.createDataFrame(
        [(1, None), (1, 1), (1, 1), (None, 2), (2, 2)],
        ["A", "B"],
    ).select("A", "B")
    multi_pure = upper.select(F.count_distinct("A", "B")).to_arrow()
    multi_sql = upper.select(F.count_distinct("A", "B"), F.lit(0).alias("z")).to_arrow()
    assert multi_pure.to_pylist()[0]["count(DISTINCT A, B)"] == 2
    assert multi_sql.to_pylist()[0]["count(DISTINCT A, B)"] == 2


# ---- Cycle-6 pins (free-OR scalar/concat, pure_global vs window, compound rebind) ------------


def test_select_free_scalar_concat_greatest_missing_group_by(frame: object) -> None:
    """Free (non-agg) args through ``_scalar``/``concat``/``greatest`` → MISSING_GROUP_BY.

    Mutation-proof for free-OR on builders (octo C6-Q-001): deleting
    ``has_free_attribute=any(...)`` must fail these select-boundary pins, not only metadata.
    """
    free_upper = F.upper(F.col("id"))
    free_concat = F.concat(F.col("id").cast("string"), F.lit("x"))
    free_greatest = F.greatest(F.sum("x"), F.col("id"))
    # Metadata asserts (free-OR cannot be deleted while only select pins fail later).
    assert free_upper._has_free_attribute is True
    assert free_upper._is_aggregate is False
    assert free_concat._has_free_attribute is True
    assert free_concat._is_aggregate is False
    assert free_greatest._has_free_attribute is True
    assert free_greatest._is_aggregate is True
    # Select boundary — each must raise, never silent group / global-agg route.
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught_upper:
        frame.select(F.sum("x"), free_upper).collect()
    assert "GROUP BY" in str(caught_upper.value)
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught_concat:
        frame.select(F.sum("x"), free_concat).collect()
    assert "GROUP BY" in str(caught_concat.value)
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught_greatest:
        frame.select(free_greatest).collect()
    assert "GROUP BY" in str(caught_greatest.value)
    # Pure post-agg scalar (no free) still global-agg — free-OR is not "always free".
    pure_upper = frame.select(F.upper(F.sum("x").cast("string")).alias("u")).to_arrow()
    assert pure_upper.num_rows == 1
    assert pure_upper.to_pylist() == [{"u": "60"}]


def test_select_sum_with_window_over_missing_group_by(frame: object) -> None:
    """``select(sum, row_number().over(...))`` must not take pure_global SQL (C6-L-001).

    Window ``.over`` clears agg/free/foldable; pure_global = all(¬free) alone mis-routed
    into global-agg SQL. Predicate requires aggregate|foldable + not free.
    """
    from repark.spark.window import Window

    windowed = F.row_number().over(Window.orderBy("id"))
    assert windowed._is_aggregate is False
    assert windowed._is_foldable is False
    assert windowed._has_free_attribute is False
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught:
        frame.select(F.sum("x"), windowed).collect()
    assert "GROUP BY" in str(caught.value)
    # Foldable companion still allowed (predicate not over-tightened to all-aggregate).
    with_lit = frame.select(F.sum("x"), F.lit(1).alias("one")).to_arrow()
    assert with_lit.num_rows == 1
    assert with_lit.to_pylist()[0]["sum(x)"] == 60
    assert with_lit.to_pylist()[0]["one"] == 1


def test_select_case_preserved_sum_compound_pure_and_sql(frame: object) -> None:
    """Case-preserved ``sum(col(X)+1)`` pure ≡ SQL lit-companion path (C6-L-002).

    Nested structural sql_expr forces free-SQL (native rebind is simple-name only).
    """
    preserved = frame.select("X")  # type: ignore[attr-defined]
    compound = F.sum(F.col("X") + 1)
    assert compound._is_aggregate_function is True
    assert compound.sql_expr_part() == 'sum(("X" + 1))'
    # Nested paren → not native-pure (mutation-proof for the compound-routing fix).
    from repark.spark.dataframe import _is_native_pure_global_aggregate

    assert _is_native_pure_global_aggregate(compound) is False
    assert _is_native_pure_global_aggregate(F.sum("X")) is True
    pure = preserved.select(compound).to_arrow()
    via_sql = preserved.select(compound, F.lit(0).alias("z")).to_arrow()
    assert pure.num_rows == via_sql.num_rows == 1
    assert pure.to_pylist()[0]["sum((X + 1))"] == via_sql.to_pylist()[0]["sum((X + 1))"] == 63
    assert via_sql.to_pylist()[0]["z"] == 0


def test_select_pure_collect_set_excludes_nulls(spark: ReparkSession) -> None:
    """Pure ``select(collect_set)`` excludes nulls like SQL/agg paths (C6-L-003)."""
    with_nulls = spark.createDataFrame([(None,), (20,), (None,), (30,), (20,)], ["v"])
    pure = with_nulls.select(F.collect_set("v")).to_arrow()
    via_sql = with_nulls.select(F.collect_set("v"), F.lit(1).alias("one")).to_arrow()
    via_agg = with_nulls.agg(F.collect_set("v")).to_arrow()
    pure_values = pure.to_pylist()[0]["collect_set(v)"]
    sql_values = via_sql.to_pylist()[0]["collect_set(v)"]
    agg_values = via_agg.to_pylist()[0]["collect_set(v)"]
    assert pure_values is not None and sql_values is not None and agg_values is not None
    assert None not in pure_values
    assert sorted(pure_values) == sorted(sql_values) == sorted(agg_values) == [20, 30]


def test_polars_sort_key_preserves_sql_expr(frame: object) -> None:
    """``polars._sort_key`` keeps structural sql_expr + generator sticky (C6-Q-002).

    Pre-fix: only sql_expr was copied; ``_generator``/``_generator_cast`` were stripped
    while Column.asc/desc keep them — hollow pin for generator refuse on pl.sort /
    orderBy after sort-key wrapping (combine octo C6-Q-002).
    """
    from repark.spark.polars import _sort_key

    bare = F.sum("x")
    keyed = _sort_key(bare, ascending=True)
    assert keyed.sql_expr_part() == bare.sql_expr_part() == 'sum("x")'
    assert keyed._is_aggregate is True
    assert keyed._sql_expr == bare._sql_expr
    hostile = "x) FROM secret --"
    keyed_hostile = _sort_key(F.sum(hostile), ascending=False)
    assert keyed_hostile.sql_expr_part() == f'sum("{hostile}")'
    # Generator sticky (parity with Column.asc/desc) — not only sql_expr.
    gen = F.explode(F.col("a")).cast("INT")
    assert gen._generator == "explode"
    assert gen._generator_cast is not None
    gen_keyed = _sort_key(gen, ascending=False)
    assert gen_keyed._generator == gen._generator == "explode"
    assert gen_keyed._generator_cast == gen._generator_cast
    assert gen_keyed._sort_ascending is False
    # orderBy on sticky-keyed generator still refuses (not array-placeholder sort).
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.orderBy(gen_keyed).collect()
    # pl.sort(explode) refuses (isNull and/or orderBy), never sorts the array placeholder.
    with pytest.raises(AnalysisException, match=r"UNSUPPORTED_GENERATOR|nested"):
        frame.pl.sort(F.explode(F.col("a"))).spark.collect()


# ---- Cycle-7 pins (rand non-foldable; sticky ungroupable for nested window) --------------------


def test_select_sum_with_rand_missing_group_by(frame: object) -> None:
    """``select(sum, rand)`` must not pure_global — Rand is non-foldable (C7-L-001).

    Mutation-proof: vacuous ``all([])`` in ``_scalar`` must not mark nullary random foldable.
    Foldable companions (lit / current_date) still allowed so the fix is not over-tight.
    """
    rand_col = F.rand()
    assert rand_col._is_foldable is False
    assert rand_col._is_aggregate is False
    assert rand_col._has_free_attribute is False
    assert rand_col._has_ungroupable is True
    # current_date remains foldable after vacuous-all fix (explicit foldable=True).
    assert F.current_date()._is_foldable is True
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught:
        frame.select(F.sum("x"), rand_col.alias("r")).collect()
    assert "GROUP BY" in str(caught.value)
    # Nested sum∘rand also ungroupable (not only list-level companion).
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught_nested:
        frame.select((F.sum("x") + F.rand()).alias("s")).collect()
    assert "GROUP BY" in str(caught_nested.value)
    # Foldable companions still global-agg (predicate not over-tightened).
    with_lit = frame.select(F.sum("x"), F.lit(1).alias("one")).to_arrow()
    assert with_lit.num_rows == 1
    assert with_lit.to_pylist()[0]["sum(x)"] == 60
    with_date = frame.select(F.sum("x"), F.current_date().alias("d")).to_arrow()
    assert with_date.num_rows == 1
    assert with_date.to_pylist()[0]["sum(x)"] == 60


def test_select_nested_window_with_aggregate_missing_group_by(frame: object) -> None:
    """Nested window∘aggregate must raise ``[MISSING_GROUP_BY]`` (C7-L-002).

    C6 only fixed list-level ``select(sum, window)``. Sticky ``_has_ungroupable`` from
    ``.over`` OR-propagates through binary / coalesce / when so nested composition cannot
    pure_global via sticky ``_is_aggregate`` alone.
    """
    from repark.spark.window import Window

    windowed = F.row_number().over(Window.orderBy("id"))
    assert windowed._has_ungroupable is True
    assert windowed._is_aggregate is False
    assert windowed._is_foldable is False
    sum_plus = F.sum("x") + windowed
    assert sum_plus._is_aggregate is True
    assert sum_plus._has_ungroupable is True
    assert sum_plus._has_free_attribute is False
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught_add:
        frame.select(sum_plus.alias("s")).collect()
    assert "GROUP BY" in str(caught_add.value)
    coalesced = F.coalesce(F.sum("x"), windowed)
    assert coalesced._is_aggregate is True
    assert coalesced._has_ungroupable is True
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught_coalesce:
        frame.select(coalesced.alias("s")).collect()
    assert "GROUP BY" in str(caught_coalesce.value)
    when_col = F.when(F.lit(True), F.sum("x")).otherwise(windowed)
    assert when_col._is_aggregate is True
    assert when_col._has_ungroupable is True
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught_when:
        frame.select(when_col.alias("s")).collect()
    assert "GROUP BY" in str(caught_when.value)
    # List-level window companion still raises (C6-L-001 regression guard).
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY"):
        frame.select(F.sum("x"), windowed).collect()


def test_select_generator_plus_aggregate_missing_group_by(spark: ReparkSession) -> None:
    """Generator sibling of sticky aggregate refuses before unnest (combine octo C1-Q-002).

    Cross-unit pin with R-EXPLODE-REWRITE: aggregate classification must run before the
    generator short-circuit so ``select(explode, sum)`` never mid-projects aggregates.
    """
    frame = spark.sql(
        """
        SELECT 1 AS id, 10 AS x, make_array(1, 2) AS a
        UNION ALL SELECT 2, 20, make_array(3)
        """
    )
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY") as caught:
        frame.select(F.explode(frame.a).alias("e"), F.sum("x").alias("total")).collect()
    assert "GROUP BY" in str(caught.value)
    # Alias / cast sticky aggregate still classified (mutation-proof with F1 sticky bits).
    sticky = F.sum("x").alias("total").cast("double")
    assert sticky._is_aggregate is True
    with pytest.raises(AnalysisException, match=r"MISSING_GROUP_BY"):
        frame.select(F.explode(frame.a).alias("e"), sticky).collect()


# ---- Combine cycle-3 pins (rebind sort sticky / withColumns refuse aggregate) ----------------


def test_grouping_col_sql_quotes_hostile_string_keys(spark: ReparkSession) -> None:
    """CUBE/ROLLUP/GROUPING SETS str keys use _quote_ident (combine octo C4-SEC-001).

    Pre-fix: non-identifier keys used f'\"{item}\"' without doubling embedded quotes, so
    ``a\") UNION ALL SELECT 1 --`` broke out of free-SQL GROUP BY. Mutation that reverts to
    naive quoting fails the doubled-quote pin and/or injection refuse.
    """
    from repark.spark._idents import quote_ident as _quote_ident

    frame = spark.createDataFrame([(1, 10), (2, 20)], ["order", "x"])
    hostile = 'a") UNION ALL SELECT 1 --'
    quoted = frame._grouping_col_sql(hostile)
    assert quoted == _quote_ident(hostile)
    assert quoted == '"a"") UNION ALL SELECT 1 --"'
    # Naive f-string would leave a single closing quote after a — pin escape.
    assert quoted != f'"{hostile}"'
    assert '""' in quoted
    # Identifier-looking keys are also always quoted (not bare isidentifier passthrough).
    assert frame._grouping_col_sql("order") == _quote_ident("order") == '"order"'
    assert frame._grouping_col_sql("g") == _quote_ident("g") == '"g"'
    # Behavioral: cube on reserved-name string key still works; hostile key cannot inject.
    cube_table = frame.cube("order").agg(F.sum("x")).to_arrow()
    by_order = {
        row["order"]: next(iter(value for key, value in row.items() if key != "order"))
        for row in cube_table.to_pylist()
    }
    assert by_order[1] == 10
    assert by_order[2] == 20
    assert by_order[None] == 30
    with pytest.raises((AnalysisException, Exception)):
        frame.cube(hostile).agg(F.count("*")).collect()
    with pytest.raises((AnalysisException, Exception)):
        frame.rollup(hostile).agg(F.count("*")).collect()
    with pytest.raises((AnalysisException, Exception)):
        frame.groupingSets(hostile).agg(F.count("*")).collect()


def test_rebind_sort_marker_preserves_sticky_bits(spark: ReparkSession) -> None:
    """``_rebind_stable_name_column`` sort branch keeps sql_expr/AF/generator (C3-Q-001).

    ``Column.asc`` preserves sticky bits; rebind at select/group/order must not drop them.
    Reserved name ``order`` must stay quoted for cube free-SQL SELECT.
    """
    from repark.spark.column import Column

    frame = spark.createDataFrame([(1, 10), (2, 20)], ["order", "x"])
    # Public path: bare col + asc rebinds and keeps schema-quoted sql_expr.
    sorted_order = F.col("order").asc()
    assert sorted_order._sql_expr == '"order"'
    rebound = frame._rebind_stable_name_column(sorted_order)
    assert rebound._sort_ascending is True
    assert rebound.sql_expr_part() == '"order"'
    assert rebound._sql_expr == '"order"'
    assert rebound._has_free_attribute is True
    # Synthetic sticky AF + generator: mutation-proof vs omitting fields in the sort branch.
    base = frame._bind_schema_column("x")
    synth = Column(
        base._inner,
        sort_ascending=True,
        sort_nulls_first=True,
        spark_display=base._spark_display,
        projection_name=base._projection_name,
        stable_name=True,
        is_aggregate=True,
        is_aggregate_function=True,
        sql_expr='sum("x")',
        agg_name="sum(x)",
        generator="explode",
        generator_cast="INT",
        has_free_attribute=True,
    )
    synth_rebound = frame._rebind_stable_name_column(synth)
    assert synth_rebound._is_aggregate is True
    assert synth_rebound._is_aggregate_function is True
    assert synth_rebound._generator == "explode"
    assert synth_rebound._generator_cast == "INT"
    assert synth_rebound._sql_expr == base._sql_expr == '"x"'
    # Behavioral: cube on reserved-name sort key still free-SQL SELECT-quoted.
    cube_table = frame.cube(F.col("order").asc()).agg(F.sum("x")).to_arrow()
    rows = cube_table.to_pylist()
    by_order = {row["order"]: next(iter(v for k, v in row.items() if k != "order")) for row in rows}
    assert by_order[1] == 10
    assert by_order[2] == 20
    assert by_order[None] == 30


def test_with_columns_pure_aggregate_refused(frame: object) -> None:
    """``withColumns({x: sum(x)})`` must not pure_global collapse N→1 (combine octo C3-001).

    Spark rejects aggregates in withColumn/withColumns; global agg stays on select/agg.
    """
    with pytest.raises(AnalysisException, match=r"INVALID_USAGE_OF_AGGREGATE") as caught:
        frame.withColumns({"x": F.sum("x")}).collect()
    assert "withColumns" in str(caught.value)
    # Aggregate + foldable map is still pure_global if it reached select — refuse too.
    with pytest.raises(AnalysisException, match=r"INVALID_USAGE_OF_AGGREGATE"):
        frame.withColumns({"id": F.lit(0), "x": F.sum("x")}).collect()
    # withColumn single sticky aggregate (not only the withColumns→select path).
    with pytest.raises(AnalysisException, match=r"INVALID_USAGE_OF_AGGREGATE") as caught_one:
        frame.withColumn("x", F.sum("x")).collect()
    assert "withColumn" in str(caught_one.value)
    # Alias / cast sticky aggregate still refused (mutation-proof sticky bit).
    sticky = F.sum("x").alias("total").cast("double")
    assert sticky._is_aggregate is True
    with pytest.raises(AnalysisException, match=r"INVALID_USAGE_OF_AGGREGATE"):
        frame.withColumns({"x": sticky}).collect()
    # Non-aggregate withColumns still works (refuse not over-broad).
    ok = frame.withColumns({"x": F.col("x") + 1}).to_arrow()
    assert ok.num_rows == 3
    assert sorted(ok.column("x").to_pylist()) == [11, 21, 31]
    # select pure global still allowed (contrast withColumns refuse).
    via_select = frame.select(F.sum("x")).to_arrow()
    assert via_select.num_rows == 1
    assert via_select.to_pylist() == [{"sum(x)": 60}]
