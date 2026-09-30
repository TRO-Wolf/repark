from __future__ import annotations

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.column import Column
from repark.spark.dataframe.core import DataFrame

_MAX_ARRAY_ELEMENTS_KEY = "repark.sql.maxArrayElements"

RAISES = (
    AnalysisException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)


@pytest.fixture
def utc() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn10")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


@pytest.fixture
def legacy() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn10-legacy")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def ceiling100() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn10-ceil")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config(_MAX_ARRAY_ELEMENTS_KEY, "100")
        .getOrCreate()
    )


@pytest.fixture
def ceiling100_legacy() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn10-ceil-legacy")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config(_MAX_ARRAY_ELEMENTS_KEY, "100")
        .getOrCreate()
    )


def build_vn10_t(session: ReparkSession) -> None:
    session.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn10_t AS SELECT 1 AS c, CAST(NULL AS INT) AS n, 0 AS z"
    ).collect()


def vn10_df_count(session: ReparkSession, count: Column) -> DataFrame:
    return session.range(1).select(F.size(F.array_repeat(F.lit(1), count)).alias("v"))


VN10_R1_SQL_REFUSALS: list[tuple[str, str]] = [
    (
        "castfirst_trycast",
        "SELECT size(array_repeat(1, nullif(try_cast(150.5D AS INT), 0))) AS v",
    ),
    (
        "castfirst_trycast_coalesce",
        "SELECT size(array_repeat(1, coalesce(nullif(try_cast(150.5D AS INT), 0), 5))) AS v",
    ),
]

VN10_R1_SQL_REFUSAL_IDS = [cell[0] for cell in VN10_R1_SQL_REFUSALS]


@pytest.mark.parametrize("cell", VN10_R1_SQL_REFUSALS, ids=VN10_R1_SQL_REFUSAL_IDS)
def test_vn10_r1_sql_refuses(ceiling100: ReparkSession, cell: tuple[str, str]) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql(cell[1]).collect()


@pytest.mark.parametrize("cell", VN10_R1_SQL_REFUSALS, ids=VN10_R1_SQL_REFUSAL_IDS)
def test_vn10_r1_sql_refuses_legacy(
    ceiling100_legacy: ReparkSession, cell: tuple[str, str]
) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100_legacy.sql(cell[1]).collect()


@pytest.mark.parametrize("cell", VN10_R1_SQL_REFUSALS, ids=VN10_R1_SQL_REFUSAL_IDS)
def test_vn10_r1_fexpr_refuses(ceiling100: ReparkSession, cell: tuple[str, str]) -> None:
    inner = cell[1].removeprefix("SELECT ").removesuffix(" AS v")
    frame = ceiling100.range(1).select(F.expr(inner).alias("v"))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


@pytest.mark.parametrize("cell", VN10_R1_SQL_REFUSALS, ids=VN10_R1_SQL_REFUSAL_IDS)
def test_vn10_r1_fexpr_refuses_legacy(
    ceiling100_legacy: ReparkSession, cell: tuple[str, str]
) -> None:
    frame = ceiling100_legacy.range(1).select(
        F.expr(cell[1].removeprefix("SELECT ").removesuffix(" AS v")).alias("v")
    )
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


def test_vn10_r1_df_trycast_first_refuses(ceiling100: ReparkSession) -> None:
    count = F.nullif(F.lit(150.5).try_cast("int"), F.lit(0))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        vn10_df_count(ceiling100, count).collect()


def test_vn10_r1_df_trycast_first_refuses_legacy(ceiling100_legacy: ReparkSession) -> None:
    count = F.nullif(F.lit(150.5).try_cast("int"), F.lit(0))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        vn10_df_count(ceiling100_legacy, count).collect()


def test_vn10_r1_df_trycast_first_coalesce_refuses(ceiling100: ReparkSession) -> None:
    count = F.coalesce(F.nullif(F.lit(150.5).try_cast("int"), F.lit(0)), F.lit(5))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        vn10_df_count(ceiling100, count).collect()


def test_vn10_r1_df_trycast_first_coalesce_refuses_legacy(
    ceiling100_legacy: ReparkSession,
) -> None:
    count = F.coalesce(F.nullif(F.lit(150.5).try_cast("int"), F.lit(0)), F.lit(5))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        vn10_df_count(ceiling100_legacy, count).collect()


VN10_R1_EQUAL_NULLS: list[tuple[str, str]] = [
    (
        "cast_second",
        "SELECT size(array_repeat(1, nullif(101, CAST(101.5D AS INT)))) AS v",
    ),
    (
        "trycast_second",
        "SELECT size(array_repeat(1, nullif(101, try_cast(101.5D AS INT)))) AS v",
    ),
]

VN10_R1_EQUAL_NULL_IDS = [cell[0] for cell in VN10_R1_EQUAL_NULLS]


@pytest.mark.parametrize("cell", VN10_R1_EQUAL_NULLS, ids=VN10_R1_EQUAL_NULL_IDS)
def test_vn10_r1_sql_equal_cast_answers_null(
    ceiling100: ReparkSession, cell: tuple[str, str]
) -> None:
    assert [row.asDict() for row in ceiling100.sql(cell[1]).collect()] == [{"v": None}]


@pytest.mark.parametrize("cell", VN10_R1_EQUAL_NULLS, ids=VN10_R1_EQUAL_NULL_IDS)
def test_vn10_r1_sql_equal_cast_answers_null_legacy(
    ceiling100_legacy: ReparkSession, cell: tuple[str, str]
) -> None:
    assert [row.asDict() for row in ceiling100_legacy.sql(cell[1]).collect()] == [{"v": None}]


@pytest.mark.parametrize("cell", VN10_R1_EQUAL_NULLS, ids=VN10_R1_EQUAL_NULL_IDS)
def test_vn10_r1_fexpr_equal_cast_answers_null(
    ceiling100: ReparkSession, cell: tuple[str, str]
) -> None:
    frame = ceiling100.range(1).select(
        F.expr(cell[1].removeprefix("SELECT ").removesuffix(" AS v")).alias("v")
    )
    assert [row.asDict() for row in frame.collect()] == [{"v": None}]


@pytest.mark.parametrize("cell", VN10_R1_EQUAL_NULLS, ids=VN10_R1_EQUAL_NULL_IDS)
def test_vn10_r1_fexpr_equal_cast_answers_null_legacy(
    ceiling100_legacy: ReparkSession, cell: tuple[str, str]
) -> None:
    frame = ceiling100_legacy.range(1).select(
        F.expr(cell[1].removeprefix("SELECT ").removesuffix(" AS v")).alias("v")
    )
    assert [row.asDict() for row in frame.collect()] == [{"v": None}]


def test_vn10_r1_df_equal_cast_answers_null(ceiling100: ReparkSession) -> None:
    count = F.nullif(F.lit(101), F.lit(101.5).cast("int"))
    assert [row.asDict() for row in vn10_df_count(ceiling100, count).collect()] == [{"v": None}]


def test_vn10_r1_df_equal_trycast_answers_null(ceiling100_legacy: ReparkSession) -> None:
    count = F.nullif(F.lit(101), F.lit(101.5).try_cast("int"))
    assert [row.asDict() for row in vn10_df_count(ceiling100_legacy, count).collect()] == [
        {"v": None}
    ]


def test_vn10_r1_sequence_equal_cast_answers_null(ceiling100: ReparkSession) -> None:
    frame = ceiling100.sql("SELECT size(sequence(1, nullif(101, CAST(101.5D AS INT)))) AS v")
    assert [row.asDict() for row in frame.collect()] == [{"v": None}]


def test_vn10_r1_fexpr_inexact_float_still_refuses(ceiling100: ReparkSession) -> None:
    frame = ceiling100.range(1).select(
        F.expr("size(array_repeat(1, nullif(101, 101.4)))").alias("v")
    )
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


def test_vn10_r1_fexpr_inexact_float_still_refuses_legacy(
    ceiling100_legacy: ReparkSession,
) -> None:
    frame = ceiling100_legacy.range(1).select(
        F.expr("size(array_repeat(1, nullif(101, 101.4)))").alias("v")
    )
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


def test_vn10_r2_df_greatest_zeroifnull_column_refuses(ceiling100: ReparkSession) -> None:
    build_vn10_t(ceiling100)
    frame = ceiling100.table("vn10_t").select(
        F.size(F.array_repeat(F.lit(1), F.greatest(F.zeroifnull(F.col("c")), F.lit(1000)))).alias(
            "v"
        )
    )
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


def test_vn10_r2_df_greatest_zeroifnull_column_refuses_legacy(
    ceiling100_legacy: ReparkSession,
) -> None:
    build_vn10_t(ceiling100_legacy)
    frame = ceiling100_legacy.table("vn10_t").select(
        F.size(F.array_repeat(F.lit(1), F.greatest(F.zeroifnull(F.col("c")), F.lit(1000)))).alias(
            "v"
        )
    )
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


def test_vn10_r2_df_greatest_zeroifnull_literal_refuses(ceiling100: ReparkSession) -> None:
    count = F.greatest(F.zeroifnull(F.lit(0)), F.lit(1000))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        vn10_df_count(ceiling100, count).collect()


def test_vn10_r2_df_greatest_zeroifnull_literal_refuses_legacy(
    ceiling100_legacy: ReparkSession,
) -> None:
    count = F.greatest(F.zeroifnull(F.lit(0)), F.lit(1000))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        vn10_df_count(ceiling100_legacy, count).collect()


VN10_R2_ZERO_ANSWERS: list[tuple[str, Column]] = [
    ("coalesce_zero", F.coalesce(F.zeroifnull(F.lit(0)), F.lit(1000))),
    ("coalesce_null", F.coalesce(F.zeroifnull(F.lit(None).cast("int")), F.lit(1000))),
    ("nvl_zero", F.nvl(F.zeroifnull(F.lit(0)), F.lit(1000))),
    ("nvl_null", F.nvl(F.zeroifnull(F.lit(None).cast("int")), F.lit(1000))),
]

VN10_R2_ZERO_ANSWER_IDS = [cell[0] for cell in VN10_R2_ZERO_ANSWERS]


@pytest.mark.parametrize("cell", VN10_R2_ZERO_ANSWERS, ids=VN10_R2_ZERO_ANSWER_IDS)
def test_vn10_r2_df_zeroifnull_answers_zero(
    ceiling100: ReparkSession, cell: tuple[str, Column]
) -> None:
    assert [row.asDict() for row in vn10_df_count(ceiling100, cell[1]).collect()] == [{"v": 0}]


@pytest.mark.parametrize("cell", VN10_R2_ZERO_ANSWERS, ids=VN10_R2_ZERO_ANSWER_IDS)
def test_vn10_r2_df_zeroifnull_answers_zero_legacy(
    ceiling100_legacy: ReparkSession, cell: tuple[str, Column]
) -> None:
    assert [row.asDict() for row in vn10_df_count(ceiling100_legacy, cell[1]).collect()] == [
        {"v": 0}
    ]


def test_vn10_r2_df_zeroifnull_column_answers_zero(ceiling100: ReparkSession) -> None:
    build_vn10_t(ceiling100)
    frame = ceiling100.table("vn10_t").select(
        F.size(F.array_repeat(F.lit(1), F.coalesce(F.zeroifnull(F.col("z")), F.lit(1000)))).alias(
            "v"
        )
    )
    assert [row.asDict() for row in frame.collect()] == [{"v": 0}]


def test_vn10_r2_df_nvl2_zeroifnull_answers_one(ceiling100: ReparkSession) -> None:
    count = F.nvl2(F.zeroifnull(F.lit(0)), F.lit(1), F.lit(1000))
    assert [row.asDict() for row in vn10_df_count(ceiling100, count).collect()] == [{"v": 1}]


def test_vn10_r2_df_nvl2_zeroifnull_answers_one_legacy(ceiling100_legacy: ReparkSession) -> None:
    count = F.nvl2(F.zeroifnull(F.lit(0)), F.lit(1), F.lit(1000))
    assert [row.asDict() for row in vn10_df_count(ceiling100_legacy, count).collect()] == [{"v": 1}]


def test_vn10_r2_sql_coalesce_zeroifnull_answers_zero(ceiling100: ReparkSession) -> None:
    frame = ceiling100.sql("SELECT size(array_repeat(1, coalesce(zeroifnull(0), 1000))) AS v")
    assert [row.asDict() for row in frame.collect()] == [{"v": 0}]


def test_vn10_r2_sql_sequence_zeroifnull_answers_two(ceiling100: ReparkSession) -> None:
    frame = ceiling100.sql("SELECT size(sequence(zeroifnull(0), 1)) AS v")
    assert [row.asDict() for row in frame.collect()] == [{"v": 2}]


VN10_R3_WIDEN: list[tuple[str, str, str]] = [
    ("int", "5", "5"),
    ("bigint", "CAST(5 AS BIGINT)", "5"),
    ("double", "CAST(1.5 AS DOUBLE)", "1.5"),
    ("float", "CAST(1.5 AS FLOAT)", "1.5"),
    ("decimal", "CAST(5.25 AS DECIMAL(10,2))", "5.25"),
    ("date", "DATE '2024-01-01'", "2024-01-01"),
    ("ts", "TIMESTAMP '2024-01-01 00:00:00'", "2024-01-01 00:00:00"),
]

VN10_R3_WIDEN_IDS = [cell[0] for cell in VN10_R3_WIDEN]


@pytest.mark.parametrize("cell", VN10_R3_WIDEN, ids=VN10_R3_WIDEN_IDS)
def test_vn10_r3_nvl_string_first_answers_string_legacy(
    legacy: ReparkSession, cell: tuple[str, str, str]
) -> None:
    frame = legacy.sql(f"SELECT typeof(nvl('a', {cell[1]})) AS t, nvl('a', {cell[1]}) AS v")
    assert [row.asDict() for row in frame.collect()] == [{"t": "string", "v": "a"}]


@pytest.mark.parametrize("cell", VN10_R3_WIDEN, ids=VN10_R3_WIDEN_IDS)
def test_vn10_r3_nvl_typed_first_answers_string_legacy(
    legacy: ReparkSession, cell: tuple[str, str, str]
) -> None:
    frame = legacy.sql(f"SELECT typeof(nvl({cell[1]}, 'a')) AS t, nvl({cell[1]}, 'a') AS v")
    assert [row.asDict() for row in frame.collect()] == [{"t": "string", "v": cell[2]}]


@pytest.mark.parametrize("cell", VN10_R3_WIDEN, ids=VN10_R3_WIDEN_IDS)
def test_vn10_r3_ifnull_answers_string_legacy(
    legacy: ReparkSession, cell: tuple[str, str, str]
) -> None:
    frame = legacy.sql(f"SELECT typeof(ifnull('a', {cell[1]})) AS t, ifnull({cell[1]}, 'a') AS v")
    assert [row.asDict() for row in frame.collect()] == [{"t": "string", "v": cell[2]}]


@pytest.mark.parametrize("cell", VN10_R3_WIDEN, ids=VN10_R3_WIDEN_IDS)
def test_vn10_r3_nvl2_answers_string_legacy(
    legacy: ReparkSession, cell: tuple[str, str, str]
) -> None:
    frame = legacy.sql(
        f"SELECT typeof(nvl2(1, 'a', {cell[1]})) AS t, nvl2(NULL, 'a', {cell[1]}) AS v"
    )
    assert [row.asDict() for row in frame.collect()] == [{"t": "string", "v": cell[2]}]


@pytest.mark.parametrize("cell", VN10_R3_WIDEN, ids=VN10_R3_WIDEN_IDS)
def test_vn10_r3_nvl2_swapped_answers_string_legacy(
    legacy: ReparkSession, cell: tuple[str, str, str]
) -> None:
    frame = legacy.sql(f"SELECT typeof(nvl2(1, {cell[1]}, 'a')) AS t, nvl2(1, {cell[1]}, 'a') AS v")
    assert [row.asDict() for row in frame.collect()] == [{"t": "string", "v": cell[2]}]


def test_vn10_r3_nvl_bool_refuses_legacy(legacy: ReparkSession) -> None:
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.DATA_DIFF_TYPES"):
        legacy.sql("SELECT nvl('a', true) AS v").collect()
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.DATA_DIFF_TYPES"):
        legacy.sql("SELECT nvl(true, 'a') AS v").collect()


def test_vn10_r3_ifnull_bool_refuses_legacy(legacy: ReparkSession) -> None:
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.DATA_DIFF_TYPES"):
        legacy.sql("SELECT ifnull('a', true) AS v").collect()


def test_vn10_r3_nvl2_bool_refuses_legacy(legacy: ReparkSession) -> None:
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.DATA_DIFF_TYPES"):
        legacy.sql("SELECT nvl2(1, 'a', true) AS v").collect()
    with pytest.raises(AnalysisException, match=r"DATATYPE_MISMATCH.DATA_DIFF_TYPES"):
        legacy.sql("SELECT nvl2(NULL, true, 'a') AS v").collect()


def test_vn10_r3_zeroifnull_string_answers_string_legacy(legacy: ReparkSession) -> None:
    frame = legacy.sql("SELECT typeof(zeroifnull('a')) AS t, zeroifnull('a') AS v")
    assert [row.asDict() for row in frame.collect()] == [{"t": "string", "v": "a"}]


def test_vn10_r3_zeroifnull_null_string_answers_zero_legacy(legacy: ReparkSession) -> None:
    frame = legacy.sql(
        "SELECT typeof(zeroifnull(CAST(NULL AS STRING))) AS t, "
        "zeroifnull(CAST(NULL AS STRING)) AS v"
    )
    assert [row.asDict() for row in frame.collect()] == [{"t": "string", "v": "0"}]


def test_vn10_r3_nvl_null_string_first_answers_string_legacy(legacy: ReparkSession) -> None:
    frame = legacy.sql(
        "SELECT typeof(nvl(CAST(NULL AS STRING), 5)) AS t, nvl(CAST(NULL AS STRING), 5) AS v"
    )
    assert [row.asDict() for row in frame.collect()] == [{"t": "string", "v": "5"}]


def test_vn10_r3_nvl_fexpr_twin_answers_string_legacy(legacy: ReparkSession) -> None:
    frame = legacy.range(1).select(
        F.expr("typeof(nvl('a', 5))").alias("t"), F.expr("nvl('a', 5)").alias("v")
    )
    assert [row.asDict() for row in frame.collect()] == [{"t": "string", "v": "a"}]


def test_vn10_r3_nvl_df_twin_answers_string_legacy(legacy: ReparkSession) -> None:
    frame = legacy.range(1).select(F.nvl(F.lit("a"), F.lit(5)).alias("v"))
    assert [row.asDict() for row in frame.collect()] == [{"v": "a"}]
    assert [field.dataType.simpleString() for field in frame.schema.fields] == ["string"]


def test_vn10_r3_nvl_on_guards_kept(utc: ReparkSession) -> None:
    with pytest.raises(RAISES, match="CAST_INVALID_INPUT"):
        utc.sql("SELECT nvl('a', 5) AS v").collect()
    frame = utc.sql("SELECT typeof(nvl(5, 'a')) AS t, nvl(5, 'a') AS v")
    assert [row.asDict() for row in frame.collect()] == [{"t": "bigint", "v": 5}]


VN10_R3_NULLIF_LEGACY: list[tuple[str, str, object]] = [
    ("str_int", "SELECT nullif('a', 5) AS v", [{"v": "a"}]),
    ("int_str", "SELECT nullif(5, 'a') AS v", [{"v": 5}]),
    ("num_int_null", "SELECT nullif('05', 5) AS v", [{"v": None}]),
    ("int_num_null", "SELECT nullif(5, '05') AS v", [{"v": None}]),
    ("num_double", "SELECT nullif('05', CAST(1.5 AS DOUBLE)) AS v", [{"v": "05"}]),
    ("str_bool", "SELECT nullif('a', true) AS v", [{"v": "a"}]),
    ("bool_str", "SELECT nullif(true, 'a') AS v", [{"v": True}]),
    ("num_bool", "SELECT nullif('05', true) AS v", [{"v": "05"}]),
    ("str_date", "SELECT nullif('a', DATE '2024-01-01') AS v", [{"v": "a"}]),
    ("num_date", "SELECT nullif('05', DATE '2024-01-01') AS v", [{"v": "05"}]),
    ("str_decimal", "SELECT nullif('a', CAST(5.25 AS DECIMAL(10,2))) AS v", [{"v": "a"}]),
    ("num_decimal", "SELECT nullif('05', CAST(5.25 AS DECIMAL(10,2))) AS v", [{"v": "05"}]),
]


@pytest.mark.parametrize(
    "cell", VN10_R3_NULLIF_LEGACY, ids=[cell[0] for cell in VN10_R3_NULLIF_LEGACY]
)
def test_vn10_r3_nullif_legacy_matches_spark(
    legacy: ReparkSession, cell: tuple[str, str, object]
) -> None:
    assert [row.asDict() for row in legacy.sql(cell[1]).collect()] == cell[2]


def test_vn10_r3_nullif_float_precision_legacy(legacy: ReparkSession) -> None:
    frame = legacy.sql("SELECT CAST(nullif('0.1', CAST(0.1 AS FLOAT)) AS STRING) AS v")
    assert [row.asDict() for row in frame.collect()] == [{"v": None}]
    frame = legacy.sql("SELECT CAST(nullif('16777217', CAST(16777216 AS FLOAT)) AS STRING) AS v")
    assert [row.asDict() for row in frame.collect()] == [{"v": None}]


def test_vn10_r3_nullif_decimal_precision_legacy(legacy: ReparkSession) -> None:
    frame = legacy.sql("SELECT CAST(nullif('5d', CAST(5 AS DECIMAL(38,0))) AS STRING) AS v")
    assert [row.asDict() for row in frame.collect()] == [{"v": "5d"}]
    frame = legacy.sql("SELECT CAST(nullif(CAST(5 AS DECIMAL(38,0)), '5d') AS STRING) AS v")
    assert [row.asDict() for row in frame.collect()] == [{"v": "5"}]


def test_vn10_r3_nullif_decimal_fullwidth_divergence(legacy: ReparkSession) -> None:
    frame = legacy.sql("SELECT CAST(nullif('\uff15', CAST(5 AS DECIMAL(38,0))) AS STRING) AS v")
    assert [row.asDict() for row in frame.collect()] == [{"v": "\uff15"}]


def test_vn10_r3_nullif_float_on_guard_kept(utc: ReparkSession) -> None:
    frame = utc.sql("SELECT CAST(nullif('0.1', CAST(0.1 AS FLOAT)) AS STRING) AS v")
    assert [row.asDict() for row in frame.collect()] == [{"v": "0.1"}]


def test_vn10_r3_nullifzero_string_legacy(legacy: ReparkSession) -> None:
    frame = legacy.sql("SELECT nullifzero('a') AS v")
    assert [row.asDict() for row in frame.collect()] == [{"v": "a"}]
