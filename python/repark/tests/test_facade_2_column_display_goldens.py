"""FACADE-2 column display-string goldens against the committed JSON file."""

from __future__ import annotations

import datetime
import json
import os
from collections.abc import Iterator
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pytest

import repark
from repark import Column, ReparkSession
from repark import functions as F  # noqa: N812
from repark.spark.column import Column as SparkColumn
from repark.spark.dataframe import DataFrame
from repark.spark.types import (
    BooleanType,
    ByteType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
)

RECORD_ENV = "REPARK_FACADE_2_RECORD_GOLDENS"
GOLDEN_PATH = Path(__file__).with_name("facade_2_column_display_goldens.json")
NESTED_SCHEMA = (
    "x int, y int, s string, flag boolean, "
    "st struct<a:int,b:string>, m map<string,int>, arr array<int>"
)
NESTED_ROWS = [(1, 2, "ab", True, (10, "z"), {"ab": 7, "k": 8}, [9, 8, 7])]
ARITH_VALUES: tuple[int, ...] = (0, 1, 2, -1, 4)
DIV_VALUES: tuple[int, ...] = (1, 2, -1, 4)
CMP_VALUES: tuple[int, ...] = (0, 1, -1)
CAST_DDL: tuple[str, ...] = (
    "double",
    "string",
    "int",
    "long",
    "boolean",
    "float",
    "byte",
    "short",
    "decimal(10,2)",
    "decimal(38,10)",
)


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """Session for FACADE-2 display goldens."""
    session = ReparkSession.builder.appName("facade-2-column-display").getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def _running_in_ci() -> bool:
    """True when GitHub Actions or generic CI is set."""
    return os.environ.get("CI") == "true" or os.environ.get("GITHUB_ACTIONS") == "true"


def _record_requested() -> bool:
    """True when the explicit record environment variable is `1`."""
    return os.environ.get(RECORD_ENV, "") == "1"


def _assert_record_mode_allowed() -> None:
    """Refuse record mode when CI is set so a golden cannot be rewritten in CI."""
    if _record_requested() and _running_in_ci():
        raise AssertionError(f"{RECORD_ENV} is set while CI is set; refusing to rewrite goldens")


def _canonical_json(payload: dict[str, dict[str, object]]) -> str:
    """Stable JSON bytes used both to record and to compare."""
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _nested_frame(spark: ReparkSession) -> DataFrame:
    """Small in-memory frame with scalar, struct, map, and array columns."""
    return spark.createDataFrame(NESTED_ROWS, NESTED_SCHEMA)


def _stable_output_name(name: str) -> str:
    """Drop the createDataFrame temp-view qualifier; keep the output field name."""
    if "__repark_cdf_" in name:
        return name.rsplit(".", 1)[-1]
    return name


def _snapshot(column: Column, frame: DataFrame, case_id: str) -> dict[str, object]:
    """Six render strings plus `df.select(c).columns` for one case."""
    try:
        names = [_stable_output_name(name) for name in frame.select(column).columns]
    except Exception as error:
        raise AssertionError(f"{case_id}: select failed: {error}") from error
    return {
        "join_sql": column.join_sql_part(),
        "projection_name": column._projection_name,
        "select_columns": names,
        "spark_display": column.spark_display_part(),
        "sql_expr": column.sql_expr_part(),
        "sql_expr_without_alias": column.sql_expr_without_alias(),
    }


def _put(out: dict[str, Column], case_id: str, column: Column) -> None:
    """Insert one case; duplicate ids are a builder bug."""
    if case_id in out:
        raise AssertionError(f"duplicate case id {case_id}")
    out[case_id] = column


def _group1_cases(out: dict[str, Column]) -> None:
    """Audit §4 Group-1 `PyColumn.sql` entry points."""
    _put(out, "g1_lit_datetime_naive", F.lit(datetime.datetime(2024, 1, 2, 3, 4, 5)))
    _put(
        out,
        "g1_lit_datetime_micro",
        F.lit(datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)),
    )
    _put(
        out,
        "g1_lit_datetime_micro_trailing",
        F.lit(datetime.datetime(2024, 1, 2, 3, 4, 5, 123000)),
    )
    _put(
        out,
        "g1_lit_datetime_tz_utc",
        F.lit(datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=ZoneInfo("UTC"))),
    )
    _put(
        out,
        "g1_lit_datetime_tz_ny",
        F.lit(datetime.datetime(2024, 1, 2, 8, 4, 5, tzinfo=ZoneInfo("America/New_York"))),
    )
    _put(out, "g1_lit_date", F.lit(datetime.date(2024, 1, 2)))
    _put(out, "g1_lit_date_leap", F.lit(datetime.date(2024, 2, 29)))
    _put(out, "g1_lit_time", F.lit(datetime.time(3, 4, 5)))
    _put(out, "g1_lit_time_micro", F.lit(datetime.time(3, 4, 5, 7000)))
    _put(out, "g1_cast_numpy_int32", F.lit(np.array([1, 2, 3], dtype=np.int32)))
    _put(out, "g1_cast_numpy_int64", F.lit(np.array([1, 2], dtype=np.int64)))
    _put(out, "g1_cast_numpy_float64", F.lit(np.array([1.5, 2.0], dtype=np.float64)))
    _put(out, "g1_expr_passthrough_pi", F.expr("pi()"))
    _put(out, "g1_expr_infix", F.expr("1 + 1"))
    _put(out, "g1_expr_infix_mul", F.expr("2 * 3"))
    _put(out, "g1_expr_paren", F.expr("(1 + 1)"))
    _put(out, "g1_expr_cast_sql", F.expr("CAST(1 AS DOUBLE)"))
    _put(out, "g1_pi", F.pi())
    _put(out, "g1_uuid", F.uuid())


def _group2_binary(out: dict[str, Column], x: Column, y: Column) -> None:
    """Binary arithmetic, comparison, and reflected forms."""
    _put(out, "g2_binary_arith__add_col", x + y)
    _put(out, "g2_binary_arith__sub_col", x - y)
    _put(out, "g2_binary_arith__mul_col", x * y)
    _put(out, "g2_binary_arith__div_col", x / y)
    _put(out, "g2_binary_arith__mod_col", x % y)
    _put(out, "g2_binary_arith__pow_col", x**y)
    for value in ARITH_VALUES:
        _put(out, f"g2_binary_arith__add_lit_{value}", x + value)
        _put(out, f"g2_binary_arith__radd_lit_{value}", value + x)
        _put(out, f"g2_binary_arith__sub_lit_{value}", x - value)
        _put(out, f"g2_binary_arith__rsub_lit_{value}", value - x)
        _put(out, f"g2_binary_arith__mul_lit_{value}", x * value)
        _put(out, f"g2_binary_arith__rmul_lit_{value}", value * x)
    for value in DIV_VALUES:
        _put(out, f"g2_binary_arith__div_lit_{value}", x / value)
        _put(out, f"g2_binary_arith__rdiv_lit_{value}", value / x)
        _put(out, f"g2_binary_arith__mod_lit_{value}", x % value)
        _put(out, f"g2_binary_arith__rmod_lit_{value}", value % x)
        _put(out, f"g2_binary_arith__pow_lit_{value}", x**value)
        _put(out, f"g2_binary_arith__rpow_lit_{value}", value**x)
    _put(out, "g2_binary_cmp__eq_col", x == y)
    _put(out, "g2_binary_cmp__ne_col", x != y)
    _put(out, "g2_binary_cmp__lt_col", x < y)
    _put(out, "g2_binary_cmp__gt_col", x > y)
    _put(out, "g2_binary_cmp__le_col", x <= y)
    _put(out, "g2_binary_cmp__ge_col", x >= y)
    for value in CMP_VALUES:
        _put(out, f"g2_binary_cmp__eq_lit_{value}", x == value)
        _put(out, f"g2_binary_cmp__ne_lit_{value}", x != value)
        _put(out, f"g2_binary_cmp__lt_lit_{value}", x < value)
        _put(out, f"g2_binary_cmp__gt_lit_{value}", x > value)
        _put(out, f"g2_binary_cmp__le_lit_{value}", x <= value)
        _put(out, f"g2_binary_cmp__ge_lit_{value}", x >= value)
        _put(out, f"g2_binary_cmp__req_lit_{value}", value == x)
        _put(out, f"g2_binary_cmp__rne_lit_{value}", value != x)
        _put(out, f"g2_binary_cmp__rlt_lit_{value}", value < x)
        _put(out, f"g2_binary_cmp__rgt_lit_{value}", value > x)
        _put(out, f"g2_binary_cmp__rle_lit_{value}", value <= x)
        _put(out, f"g2_binary_cmp__rge_lit_{value}", value >= x)
    _put(out, "g2_binary_arith__add_chain", (x + y) * 2)
    _put(out, "g2_binary_arith__add_float", x + 1.5)


def _group2_predicates(
    out: dict[str, Column],
    x: Column,
    y: Column,
    s: Column,
    flag: Column,
) -> None:
    """Unary, null-safe, string, bitwise, invert, null tests, CASE, alias."""
    negated = -x
    _put(out, "g2_unary__neg", negated)
    _put(out, "g2_unary__neg_neg", -negated)
    _put(out, "g2_unary__neg_add", -(x + 1))
    _put(out, "g2_eqnullsafe__col", x.eqNullSafe(y))
    _put(out, "g2_eqnullsafe__lit", x.eqNullSafe(1))
    _put(out, "g2_eqnullsafe__none", x.eqNullSafe(None))
    _put(out, "g2_eqnullsafe__zero", x.eqNullSafe(0))
    _put(out, "g2_substr__int", s.substr(1, 2))
    _put(out, "g2_substr__int_long", s.substr(1, 10))
    _put(out, "g2_substr__col", s.substr(F.lit(1), F.lit(2)))
    _put(out, "g2_string_pred__startswith_str", s.startswith("a"))
    _put(out, "g2_string_pred__endswith_str", s.endswith("b"))
    _put(out, "g2_string_pred__contains_str", s.contains("a"))
    _put(out, "g2_string_pred__like_str", s.like("a%"))
    _put(out, "g2_string_pred__rlike_str", s.rlike("^a"))
    _put(out, "g2_string_pred__startswith_col", s.startswith(F.lit("a")))
    _put(out, "g2_string_pred__endswith_col", s.endswith(F.lit("b")))
    _put(out, "g2_string_pred__contains_col", s.contains(F.lit("a")))
    _put(out, "g2_string_pred__like_col", s.like(F.lit("a%")))
    _put(out, "g2_string_pred__rlike_col", s.rlike(F.lit("^a")))
    _put(out, "g2_bitwise__and_col", x.bitwiseAND(y))
    _put(out, "g2_bitwise__or_col", x.bitwiseOR(y))
    _put(out, "g2_bitwise__xor_col", x.bitwiseXOR(y))
    _put(out, "g2_bitwise__and_lit", x.bitwiseAND(1))
    _put(out, "g2_bitwise__or_lit", x.bitwiseOR(1))
    _put(out, "g2_bitwise__xor_lit", x.bitwiseXOR(1))
    _put(out, "g2_invert__flag", ~flag)
    _put(out, "g2_invert__cmp", ~(x > 0))
    _put(out, "g2_logical__and", flag & (x > 0))
    _put(out, "g2_logical__or", flag | (x > 0))
    _put(out, "g2_logical__rand", True & flag)
    _put(out, "g2_logical__ror", False | flag)
    _put(out, "g2_null__isnull", x.isNull())
    _put(out, "g2_null__isnotnull", x.isNotNull())
    _put(out, "g2_null__isnull_alias", x.is_null())
    _put(out, "g2_null__isnotnull_alias", x.is_not_null())
    _put(out, "g2_when__len1", F.when(x > 0, 1).otherwise(0))
    _put(out, "g2_when__len1_open", F.when(x > 0, 1))
    _put(
        out,
        "g2_when__len3",
        F.when(x > 0, 1).when(x < 0, -1).when(x == 0, 9).otherwise(8),
    )
    _put(out, "g2_when__len3_open", F.when(x > 0, 1).when(x < 0, -1).when(x == 0, 9))
    _put(out, "g2_alias__simple", x.alias("z"))
    _put(out, "g2_alias__multi", x.alias("a", "b"))
    _put(out, "g2_alias__metadata", x.alias("z", metadata={"k": "v"}))
    _put(out, "g2_alias__add", (x + 1).alias("sum1"))


def _group2_access_cast_sort(
    out: dict[str, Column],
    x: Column,
    s: Column,
    st: Column,
    mapped: Column,
    arr: Column,
) -> None:
    """__getitem__, cast/try_cast, sort order, nesting, aggregate."""
    _put(out, "g2_getitem__struct", st["a"])
    _put(out, "g2_getitem__struct_b", st["b"])
    _put(out, "g2_getitem__map", mapped["k"])
    _put(out, "g2_getitem__map_ab", mapped["ab"])
    _put(out, "g2_getitem__array", arr[0])
    _put(out, "g2_getitem__array_1", arr[1])
    _put(out, "g2_getitem__map_col", mapped[s])
    _put(out, "g2_getitem__array_col", arr[x])
    _put(out, "g2_getfield__a", st.getField("a"))
    _put(out, "g2_getfield__b", st.getField("b"))
    _put(out, "g2_getitem_method__0", arr.getItem(0))
    _put(out, "g2_getitem_method__map", mapped.getItem("k"))
    for ddl in CAST_DDL:
        token = ddl.replace("(", "_").replace(")", "").replace(",", "_")
        _put(out, f"g2_cast__ddl_{token}", x.cast(ddl))
        _put(out, f"g2_try_cast__ddl_{token}", x.try_cast(ddl))
    _put(out, "g2_cast__IntegerType", x.cast(IntegerType()))
    _put(out, "g2_cast__DoubleType", x.cast(DoubleType()))
    _put(out, "g2_cast__StringType", x.cast(StringType()))
    _put(out, "g2_cast__DecimalType_10_4", x.cast(DecimalType(10, 4)))
    _put(out, "g2_cast__LongType", x.cast(LongType()))
    _put(out, "g2_cast__BooleanType", x.cast(BooleanType()))
    _put(out, "g2_cast__FloatType", x.cast(FloatType()))
    _put(out, "g2_cast__ByteType", x.cast(ByteType()))
    _put(out, "g2_cast__ShortType", x.cast(ShortType()))
    _put(out, "g2_try_cast__IntegerType", x.try_cast(IntegerType()))
    _put(out, "g2_try_cast__DoubleType", x.try_cast(DoubleType()))
    _put(out, "g2_try_cast__StringType", x.try_cast(StringType()))
    _put(out, "g2_try_cast__DecimalType_10_4", x.try_cast(DecimalType(10, 4)))
    _put(out, "g2_try_cast__LongType", x.try_cast(LongType()))
    _put(out, "g2_sort__asc", x.asc())
    _put(out, "g2_sort__desc", x.desc())
    _put(out, "g2_sort__asc_nulls_first", x.asc_nulls_first())
    _put(out, "g2_sort__desc_nulls_last", x.desc_nulls_last())
    _put(out, "g2_sort__asc_nulls_last", x.asc_nulls_last())
    _put(out, "g2_sort__desc_nulls_first", x.desc_nulls_first())
    _put(
        out,
        "g2_nesting__op_cast_alias_getitem",
        (st["a"].alias("inner").cast("double") + 1),
    )
    _put(out, "g2_agg__sum_plus_one", F.sum(x) + 1)


def _all_cases() -> dict[str, Column]:
    """Every Group-1 and Group-2 golden case."""
    x = F.col("x")
    y = F.col("y")
    s = F.col("s")
    flag = F.col("flag")
    st = F.col("st")
    mapped = F.col("m")
    arr = F.col("arr")
    out: dict[str, Column] = {}
    _group1_cases(out)
    _group2_binary(out, x, y)
    _group2_predicates(out, x, y, s, flag)
    _group2_access_cast_sort(out, x, s, st, mapped, arr)
    return out


def _build_payload(spark: ReparkSession) -> dict[str, dict[str, object]]:
    """Snapshot every case against the nested in-memory frame."""
    frame = _nested_frame(spark)
    cases = _all_cases()
    return {case_id: _snapshot(column, frame, case_id) for case_id, column in cases.items()}


def test_column_display_goldens_match_committed_bytes(spark: ReparkSession) -> None:
    """Byte-identical Group-1/2 display goldens. pins: facade-2/C-001, C-002, C-003"""
    _assert_record_mode_allowed()
    payload = _build_payload(spark)
    assert len(payload) >= 150, f"need 150+ cases, got {len(payload)}"
    encoded = _canonical_json(payload)
    if _record_requested():
        GOLDEN_PATH.write_text(encoded, encoding="utf-8")
    if not GOLDEN_PATH.is_file():
        raise AssertionError(f"missing golden {GOLDEN_PATH.name}; set {RECORD_ENV}=1 to record")
    expected = GOLDEN_PATH.read_text(encoding="utf-8")
    if encoded != expected:
        actual_ids = set(payload)
        want = json.loads(expected)
        want_ids = set(want)
        missing = sorted(want_ids - actual_ids)
        extra = sorted(actual_ids - want_ids)
        changed = sorted(
            case_id
            for case_id in sorted(actual_ids & want_ids)
            if payload[case_id] != want[case_id]
        )
        raise AssertionError(
            f"golden byte mismatch missing={missing[:12]} extra={extra[:12]} changed={changed[:12]}"
        )


def test_isinstance_column_is_repark_column() -> None:
    """isinstance(c, repark.Column) holds. pins: facade-2/C-004"""
    assert repark.Column is SparkColumn
    column = F.col("x")
    assert isinstance(column, repark.Column)
    assert isinstance(F.lit(1), repark.Column)
    assert isinstance(column + 1, repark.Column)
    assert isinstance(column.alias("z"), repark.Column)
    assert isinstance(column.cast("double"), repark.Column)
    assert isinstance(F.when(column > 0, 1).otherwise(0), repark.Column)


def test_record_mode_fails_when_ci_is_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """Record env is refused in CI. pins: facade-2/C-003"""
    monkeypatch.setenv(RECORD_ENV, "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    with pytest.raises(AssertionError, match="refusing to rewrite goldens"):
        _assert_record_mode_allowed()
    monkeypatch.delenv("CI")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    with pytest.raises(AssertionError, match="refusing to rewrite goldens"):
        _assert_record_mode_allowed()
