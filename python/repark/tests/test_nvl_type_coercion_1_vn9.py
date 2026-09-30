from __future__ import annotations

from datetime import date, datetime

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.column import Column
from repark.spark.dataframe.core import DataFrame

_MAX_ARRAY_ELEMENTS_KEY = "repark.sql.maxArrayElements"


@pytest.fixture
def utc() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn9")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


@pytest.fixture
def ceiling100() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn9-ceil")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config(_MAX_ARRAY_ELEMENTS_KEY, "100")
        .getOrCreate()
    )


@pytest.fixture
def ceiling100_legacy() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn9-legacy")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config(_MAX_ARRAY_ELEMENTS_KEY, "100")
        .getOrCreate()
    )


def build_vn9_c(session: ReparkSession) -> None:
    session.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn9_c AS SELECT * FROM VALUES (1), (2) AS t(c)"
    ).collect()


def build_vn9_eq(session: ReparkSession) -> None:
    session.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn9_eq AS SELECT * FROM VALUES (101) AS t(c)"
    ).collect()


def build_vn9_eq1(session: ReparkSession) -> None:
    session.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn9_eq1 AS SELECT * FROM VALUES (1) AS t(c)"
    ).collect()


VN9_TABLE_SQL_REFUSALS: list[tuple[str, str]] = [
    (
        "coalesce_null",
        "SELECT size(array_repeat(1, coalesce(nullif(1, 1), 1000))) AS v",
    ),
    (
        "inexact_float",
        "SELECT size(array_repeat(1, nullif(101, 101.4))) AS v",
    ),
    (
        "abs_first",
        "SELECT size(array_repeat(1, nullif(abs(101), 0))) AS v",
    ),
    (
        "trycast_first",
        "SELECT size(array_repeat(1, nullif(try_cast('101' AS INT), 0))) AS v",
    ),
    (
        "simple_case_first",
        "SELECT size(array_repeat(1, nullif(CASE '1' WHEN '1' THEN 101 ELSE 1 END, 0))) AS v",
    ),
]

VN9_TABLE_SQL_REFUSAL_IDS = [cell[0] for cell in VN9_TABLE_SQL_REFUSALS]


@pytest.mark.parametrize("cell", VN9_TABLE_SQL_REFUSALS, ids=VN9_TABLE_SQL_REFUSAL_IDS)
def test_vn9_table_sql_refuses(ceiling100: ReparkSession, cell: tuple[str, str]) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql(cell[1]).collect()


def test_vn9_table_sql_column_second_refuses(ceiling100: ReparkSession) -> None:
    build_vn9_c(ceiling100)
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql("SELECT size(array_repeat(1, nullif(101, c))) AS v FROM vn9_c").collect()


VN9_TABLE_SQL_ANSWERS: list[tuple[str, str, object]] = [
    (
        "coalesce_value",
        "SELECT size(array_repeat(1, coalesce(nullif(1, 0), 1000))) AS v",
        [{"v": 1}],
    ),
    (
        "null_start",
        "SELECT sequence(nullif(1, 1), 1000) AS v",
        [{"v": None}],
    ),
    (
        "null_step_coalesce",
        "SELECT size(sequence(1, 1000, coalesce(nullif(1, 1), 10))) AS v",
        [{"v": 100}],
    ),
    (
        "equal_str",
        "SELECT size(array_repeat(1, nullif(101, '101'))) AS v",
        [{"v": None}],
    ),
    (
        "equal_dbl",
        "SELECT size(array_repeat(1, nullif(101, 101.0D))) AS v",
        [{"v": None}],
    ),
    (
        "equal_big",
        "SELECT size(array_repeat(1, nullif(101, 101L))) AS v",
        [{"v": None}],
    ),
    (
        "small_equal",
        "SELECT size(array_repeat(1, nullif(5, 5))) AS v",
        [{"v": None}],
    ),
    (
        "small_unequal",
        "SELECT size(array_repeat(1, nullif(50, 0))) AS v",
        [{"v": 50}],
    ),
]

VN9_TABLE_SQL_ANSWER_IDS = [cell[0] for cell in VN9_TABLE_SQL_ANSWERS]


@pytest.mark.parametrize("cell", VN9_TABLE_SQL_ANSWERS, ids=VN9_TABLE_SQL_ANSWER_IDS)
def test_vn9_table_sql_answers(ceiling100: ReparkSession, cell: tuple[str, str, object]) -> None:
    assert [row.asDict() for row in ceiling100.sql(cell[1]).collect()] == cell[2]


VN9_TABLE_FEXPR_REFUSALS: list[tuple[str, str]] = [
    ("coalesce_null", "array_repeat(1, coalesce(nullif(1, 1), 1000))"),
    ("inexact_float", "array_repeat(1, nullif(101, 101.4))"),
    ("abs_first", "array_repeat(1, nullif(abs(101), 0))"),
    ("trycast_first", "array_repeat(1, nullif(try_cast('101' AS INT), 0))"),
    (
        "simple_case_first",
        "array_repeat(1, nullif(CASE '1' WHEN '1' THEN 101 ELSE 1 END, 0))",
    ),
]

VN9_TABLE_FEXPR_REFUSAL_IDS = [cell[0] for cell in VN9_TABLE_FEXPR_REFUSALS]


@pytest.mark.parametrize("cell", VN9_TABLE_FEXPR_REFUSALS, ids=VN9_TABLE_FEXPR_REFUSAL_IDS)
def test_vn9_table_fexpr_refuses(ceiling100: ReparkSession, cell: tuple[str, str]) -> None:
    frame = ceiling100.range(1).select(F.size(F.expr(cell[1])).alias("v"))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


def test_vn9_table_fexpr_column_second_refuses(ceiling100: ReparkSession) -> None:
    frame = ceiling100.range(2).select(
        F.size(F.expr("array_repeat(1, nullif(101, id))")).alias("v")
    )
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


VN9_TABLE_FEXPR_ANSWERS: list[tuple[str, str, object]] = [
    (
        "coalesce_value",
        "array_repeat(1, coalesce(nullif(1, 0), 1000))",
        [{"v": 1}],
    ),
    ("null_start", "sequence(nullif(1, 1), 1000)", [{"v": None}]),
    (
        "null_step_coalesce",
        "sequence(1, 1000, coalesce(nullif(1, 1), 10))",
        [{"v": 100}],
    ),
    ("equal_str", "array_repeat(1, nullif(101, '101'))", [{"v": None}]),
    ("equal_dbl", "array_repeat(1, nullif(101, 101.0D))", [{"v": None}]),
    ("equal_big", "array_repeat(1, nullif(101, 101L))", [{"v": None}]),
    ("small_equal", "array_repeat(1, nullif(5, 5))", [{"v": None}]),
    ("small_unequal", "array_repeat(1, nullif(50, 0))", [{"v": 50}]),
]

VN9_TABLE_FEXPR_ANSWER_IDS = [cell[0] for cell in VN9_TABLE_FEXPR_ANSWERS]


@pytest.mark.parametrize("cell", VN9_TABLE_FEXPR_ANSWERS, ids=VN9_TABLE_FEXPR_ANSWER_IDS)
def test_vn9_table_fexpr_answers(ceiling100: ReparkSession, cell: tuple[str, str, object]) -> None:
    frame = ceiling100.range(1).select(F.size(F.expr(cell[1])).alias("v"))
    assert [row.asDict() for row in frame.collect()] == cell[2]


def vn9_df_count(ceiling100: ReparkSession, count: Column) -> DataFrame:
    return ceiling100.range(1).select(F.size(F.array_repeat(F.lit(1), count)).alias("v"))


def test_vn9_table_df_coalesce_null_refuses(ceiling100: ReparkSession) -> None:
    count = F.coalesce(F.nullif(F.lit(1), F.lit(1)), F.lit(1000))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        vn9_df_count(ceiling100, count).collect()


def test_vn9_table_df_inexact_float_refuses(ceiling100: ReparkSession) -> None:
    count = F.nullif(F.lit(101), F.lit(101.4))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        vn9_df_count(ceiling100, count).collect()


def test_vn9_table_df_abs_first_refuses(ceiling100: ReparkSession) -> None:
    count = F.nullif(F.abs(F.lit(101)), F.lit(0))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        vn9_df_count(ceiling100, count).collect()


def test_vn9_table_df_trycast_first_refuses(ceiling100: ReparkSession) -> None:
    count = F.nullif(F.lit("101").try_cast("int"), F.lit(0))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        vn9_df_count(ceiling100, count).collect()


def test_vn9_table_df_simple_case_first_refuses(ceiling100: ReparkSession) -> None:
    count = F.nullif(F.expr("CASE '1' WHEN '1' THEN 101 ELSE 1 END"), F.lit(0))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        vn9_df_count(ceiling100, count).collect()


def test_vn9_table_df_column_second_refuses(ceiling100: ReparkSession) -> None:
    frame = ceiling100.range(2).select(
        F.size(F.array_repeat(F.lit(1), F.nullif(F.lit(101), F.col("id")))).alias("v")
    )
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


def test_vn9_table_df_coalesce_value_answers(ceiling100: ReparkSession) -> None:
    count = F.coalesce(F.nullif(F.lit(1), F.lit(0)), F.lit(1000))
    assert [row.asDict() for row in vn9_df_count(ceiling100, count).collect()] == [{"v": 1}]


def test_vn9_table_df_null_start_answers(ceiling100: ReparkSession) -> None:
    frame = ceiling100.range(1).select(
        F.sequence(F.nullif(F.lit(1), F.lit(1)), F.lit(1000)).alias("v")
    )
    assert [row.asDict() for row in frame.collect()] == [{"v": None}]


def test_vn9_table_df_null_step_coalesce_answers(ceiling100: ReparkSession) -> None:
    step = F.coalesce(F.nullif(F.lit(1), F.lit(1)), F.lit(10))
    frame = ceiling100.range(1).select(F.size(F.sequence(F.lit(1), F.lit(1000), step)).alias("v"))
    assert [row.asDict() for row in frame.collect()] == [{"v": 100}]


def test_vn9_table_df_equal_str_answers(ceiling100: ReparkSession) -> None:
    count = F.nullif(F.lit(101), F.lit("101"))
    assert [row.asDict() for row in vn9_df_count(ceiling100, count).collect()] == [{"v": None}]


def test_vn9_table_df_equal_dbl_answers(ceiling100: ReparkSession) -> None:
    count = F.nullif(F.lit(101), F.lit(101.0))
    assert [row.asDict() for row in vn9_df_count(ceiling100, count).collect()] == [{"v": None}]


def test_vn9_table_df_equal_big_answers(ceiling100: ReparkSession) -> None:
    count = F.nullif(F.lit(101), F.lit(101).cast("bigint"))
    assert [row.asDict() for row in vn9_df_count(ceiling100, count).collect()] == [{"v": None}]


def test_vn9_table_df_small_equal_answers(ceiling100: ReparkSession) -> None:
    count = F.nullif(F.lit(5), F.lit(5))
    assert [row.asDict() for row in vn9_df_count(ceiling100, count).collect()] == [{"v": None}]


def test_vn9_table_df_small_unequal_answers(ceiling100: ReparkSession) -> None:
    count = F.nullif(F.lit(50), F.lit(0))
    assert [row.asDict() for row in vn9_df_count(ceiling100, count).collect()] == [{"v": 50}]


VN9_DOOR_REFUSALS: list[tuple[str, str]] = [
    (
        "repeat",
        "SELECT length(repeat('a', coalesce(nullif(1, 1), 1000))) AS v",
    ),
    (
        "generate_series",
        "SELECT size(generate_series(1, coalesce(nullif(1, 1), 1000))) AS v",
    ),
    (
        "lambda",
        "SELECT transform(array(1), x -> size(array_repeat(1, coalesce(nullif(1, 1), 1000)))) AS v",
    ),
    (
        "second_big",
        "SELECT size(array_repeat(1, coalesce(nullif(5, 5L), 1000))) AS v",
    ),
    (
        "second_dbl",
        "SELECT size(array_repeat(1, coalesce(nullif(1, 1.0D), 1000))) AS v",
    ),
    (
        "second_str",
        "SELECT size(array_repeat(1, coalesce(nullif(1, '1'), 1000))) AS v",
    ),
    (
        "second_subquery",
        "SELECT size(array_repeat(1, coalesce(nullif(1, (SELECT 1)), 1000))) AS v",
    ),
]

VN9_DOOR_REFUSAL_IDS = [cell[0] for cell in VN9_DOOR_REFUSALS]


@pytest.mark.parametrize("cell", VN9_DOOR_REFUSALS, ids=VN9_DOOR_REFUSAL_IDS)
def test_vn9_doors_refuse(ceiling100: ReparkSession, cell: tuple[str, str]) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql(cell[1]).collect()


def test_vn9_door_column_second_refuses(ceiling100: ReparkSession) -> None:
    build_vn9_c(ceiling100)
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql(
            "SELECT size(array_repeat(1, coalesce(nullif(1, c), 1000))) AS v FROM vn9_c"
        ).collect()


VN9_TEMPORAL_ON: list[tuple[str, str, str, object]] = [
    (
        "d_s",
        "sequence(DATE'2024-01-01', '2024-01-03')",
        "array<date>",
        [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
    ),
    (
        "s_d",
        "sequence('2024-01-01', DATE'2024-01-03')",
        "array<date>",
        [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
    ),
    (
        "d_s_ws",
        "sequence(DATE'2024-01-01', ' 2024-01-03 ')",
        "array<date>",
        [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
    ),
    (
        "d_s_int",
        "sequence(DATE'2024-01-01', '2024-01-05', INTERVAL 2 DAY)",
        "array<date>",
        [date(2024, 1, 1), date(2024, 1, 3), date(2024, 1, 5)],
    ),
    (
        "ts_s",
        "sequence(TIMESTAMP'2024-01-01 00:00:00', '2024-01-01 03:00:00', INTERVAL 1 HOUR)",
        "array<timestamp>",
        [
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 1, 0, 0),
            datetime(2024, 1, 1, 2, 0, 0),
            datetime(2024, 1, 1, 3, 0, 0),
        ],
    ),
    (
        "ntz_s",
        "sequence(TIMESTAMP_NTZ'2024-01-01 00:00:00', '2024-01-01 02:00:00', INTERVAL 1 HOUR)",
        "array<timestamp_ntz>",
        [
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 1, 0, 0),
            datetime(2024, 1, 1, 2, 0, 0),
        ],
    ),
]

VN9_TEMPORAL_ON_IDS = [cell[0] for cell in VN9_TEMPORAL_ON]


@pytest.mark.parametrize("cell", VN9_TEMPORAL_ON, ids=VN9_TEMPORAL_ON_IDS)
def test_vn9_temporal_string_answers(
    utc: ReparkSession, cell: tuple[str, str, str, object]
) -> None:
    rows = utc.sql(f"SELECT typeof({cell[1]}) AS t, {cell[1]} AS v").collect()
    assert [row.asDict()["t"] for row in rows] == [cell[2]]
    assert [row.asDict()["v"] for row in rows] == [cell[3]]


VN9_TEMPORAL_OFF: list[tuple[str, str]] = [
    ("d_s", "SELECT sequence(DATE'2024-01-01', '2024-01-03') AS v"),
    ("s_d", "SELECT sequence('2024-01-01', DATE'2024-01-03') AS v"),
    ("d_s_ws", "SELECT sequence(DATE'2024-01-01', ' 2024-01-03 ') AS v"),
    ("d_s_int", "SELECT sequence(DATE'2024-01-01', '2024-01-05', INTERVAL 2 DAY) AS v"),
    (
        "ts_s",
        "SELECT sequence(TIMESTAMP'2024-01-01 00:00:00', '2024-01-01 03:00:00', "
        "INTERVAL 1 HOUR) AS v",
    ),
    (
        "ntz_s",
        "SELECT sequence(TIMESTAMP_NTZ'2024-01-01 00:00:00', '2024-01-01 02:00:00', "
        "INTERVAL 1 HOUR) AS v",
    ),
]

VN9_TEMPORAL_OFF_IDS = [cell[0] for cell in VN9_TEMPORAL_OFF]


@pytest.mark.parametrize("cell", VN9_TEMPORAL_OFF, ids=VN9_TEMPORAL_OFF_IDS)
def test_vn9_temporal_string_refuses_without_ansi(
    ceiling100_legacy: ReparkSession, cell: tuple[str, str]
) -> None:
    with pytest.raises(AnalysisException, match="SEQUENCE_WRONG_INPUT_TYPES"):
        ceiling100_legacy.sql(cell[1]).collect()


def test_vn9_temporal_string_fexpr_answers(utc: ReparkSession) -> None:
    frame = utc.range(1).select(F.expr("sequence(DATE'2024-01-01', '2024-01-03')").alias("v"))
    assert [row.asDict() for row in frame.collect()] == [
        {"v": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]}
    ]


def test_vn9_temporal_string_dataframe_answers(utc: ReparkSession) -> None:
    frame = utc.range(1).select(
        F.sequence(F.expr("DATE'2024-01-01'"), F.lit("2024-01-03")).alias("v")
    )
    assert [row.asDict() for row in frame.collect()] == [
        {"v": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]}
    ]


def test_vn9_temporal_string_timestamp_reads_session_zone(utc: ReparkSession) -> None:
    sql = (
        "SELECT sequence(TIMESTAMP'2024-01-01 00:00:00', '2024-01-01 03:00:00', "
        "INTERVAL 1 HOUR) AS v"
    )
    utc_rows = [row.asDict()["v"] for row in utc.sql(sql).collect()]
    assert len(utc_rows[0]) == 4
    utc.stop()
    ny = (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn9-ny")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    try:
        ny_rows = [row.asDict()["v"] for row in ny.sql(sql).collect()]
    finally:
        ny.stop()
    assert len(ny_rows[0]) == 4
    assert utc_rows == ny_rows


def test_vn9_residue_inexact_column_second_refuses(ceiling100: ReparkSession) -> None:
    build_vn9_eq(ceiling100)
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql("SELECT size(array_repeat(1, nullif(101, c))) AS v FROM vn9_eq").collect()


def test_vn9_residue_negative_step_stop_refuses(ceiling100: ReparkSession) -> None:
    build_vn9_eq1(ceiling100)
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql("SELECT size(sequence(101, nullif(1, c), -1)) AS v FROM vn9_eq1").collect()
