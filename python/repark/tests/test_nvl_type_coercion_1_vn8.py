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

_MAX_ARRAY_ELEMENTS_KEY = "repark.sql.maxArrayElements"

_RAISES = (
    AnalysisException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)


@pytest.fixture
def utc() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


@pytest.fixture
def ceiling100() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn8-ceil")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config(_MAX_ARRAY_ELEMENTS_KEY, "100")
        .getOrCreate()
    )


@pytest.fixture
def ceiling100_legacy() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn8-legacy")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config(_MAX_ARRAY_ELEMENTS_KEY, "100")
        .getOrCreate()
    )


VN8_R1_SQL_REFUSALS: list[tuple[str, str]] = [
    (
        "repeat_trycast_first",
        "SELECT size(array_repeat(1, nullif(try_cast('101' AS INT), 0))) AS v",
    ),
    (
        "seq_trycast_first",
        "SELECT size(sequence(1, nullif(try_cast('101' AS INT), 0))) AS v",
    ),
    (
        "repeat_coalesce_first",
        "SELECT size(array_repeat(1, nullif(coalesce(try_cast('101' AS INT), 1), 0))) AS v",
    ),
    (
        "seq_coalesce_first",
        "SELECT size(sequence(1, nullif(coalesce(try_cast('101' AS INT), 1), 0))) AS v",
    ),
    (
        "repeat_simple_case",
        "SELECT size(array_repeat(1, nullif(CASE '1' WHEN '1' THEN 101 ELSE 1 END, 0))) AS v",
    ),
    (
        "seq_simple_case",
        "SELECT size(sequence(1, nullif(CASE '1' WHEN '1' THEN 101 ELSE 1 END, 0))) AS v",
    ),
    (
        "repeat_q_coalesce",
        "SELECT size(array_repeat(1, nullif(101, coalesce(try_cast('0' AS INT), 101)))) AS v",
    ),
    (
        "seq_q_coalesce",
        "SELECT size(sequence(1, nullif(101, coalesce(try_cast('0' AS INT), 101)))) AS v",
    ),
]

VN8_R1_SQL_REFUSAL_IDS = [cell[0] for cell in VN8_R1_SQL_REFUSALS]


@pytest.mark.parametrize("cell", VN8_R1_SQL_REFUSALS, ids=VN8_R1_SQL_REFUSAL_IDS)
def test_vn8_r1_bypass_shapes_refuse(ceiling100: ReparkSession, cell: tuple[str, str]) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql(cell[1]).collect()


VN8_R1_SQL_EQUAL_ANSWERS: list[tuple[str, str]] = [
    (
        "seq_big_str",
        "SELECT size(sequence(1, nullif(101L, '101'))) AS v",
    ),
    (
        "seq_cast_si_str",
        "SELECT size(sequence(1, nullif(CAST(101 AS SMALLINT), '101'))) AS v",
    ),
    (
        "repeat_subquery_str",
        "SELECT size(array_repeat(1, nullif((SELECT 101), '101'))) AS v",
    ),
    (
        "seq_subquery_str",
        "SELECT size(sequence(1, nullif((SELECT 101), '101'))) AS v",
    ),
]

VN8_R1_SQL_EQUAL_ANSWER_IDS = [cell[0] for cell in VN8_R1_SQL_EQUAL_ANSWERS]


@pytest.mark.parametrize("cell", VN8_R1_SQL_EQUAL_ANSWERS, ids=VN8_R1_SQL_EQUAL_ANSWER_IDS)
def test_vn8_r1_exact_equal_shapes_answer_null(
    ceiling100: ReparkSession, cell: tuple[str, str]
) -> None:
    assert [row.asDict() for row in ceiling100.sql(cell[1]).collect()] == [{"v": None}]


VN8_R1_SQL_WRAPPED_NULLS: list[tuple[str, str]] = [
    (
        "repeat_greatest_str",
        "SELECT size(array_repeat(1, nullif(101, greatest(CAST('0' AS INT), 101)))) AS v",
    ),
    (
        "seq_greatest_str",
        "SELECT size(sequence(1, nullif(101, greatest(CAST('0' AS INT), 101)))) AS v",
    ),
]

VN8_R1_SQL_WRAPPED_NULL_IDS = [cell[0] for cell in VN8_R1_SQL_WRAPPED_NULLS]


@pytest.mark.parametrize("cell", VN8_R1_SQL_WRAPPED_NULLS, ids=VN8_R1_SQL_WRAPPED_NULL_IDS)
def test_vn8_r1_wrapped_cast_second_answers_null(
    ceiling100: ReparkSession, cell: tuple[str, str]
) -> None:
    assert [row.asDict() for row in ceiling100.sql(cell[1]).collect()] == [{"v": None}]


VN8_R1_FEXPR_REFUSALS: list[tuple[str, str, str]] = [
    ("trycast_first", "array_repeat", "nullif(try_cast('101' AS INT), 0)"),
    ("trycast_first_seq", "sequence", "nullif(try_cast('101' AS INT), 0)"),
    ("coalesce_first", "array_repeat", "nullif(coalesce(try_cast('101' AS INT), 1), 0)"),
    ("coalesce_first_seq", "sequence", "nullif(coalesce(try_cast('101' AS INT), 1), 0)"),
    ("simple_case", "array_repeat", "nullif(CASE '1' WHEN '1' THEN 101 ELSE 1 END, 0)"),
    ("simple_case_seq", "sequence", "nullif(CASE '1' WHEN '1' THEN 101 ELSE 1 END, 0)"),
    ("q_coalesce", "array_repeat", "nullif(101, coalesce(try_cast('0' AS INT), 101))"),
    ("q_coalesce_seq", "sequence", "nullif(101, coalesce(try_cast('0' AS INT), 101))"),
    ("trunc_101_4", "array_repeat", "nullif(101, 101.4)"),
    ("trunc_101_4_seq", "sequence", "nullif(101, 101.4)"),
    ("trunc_101_9", "array_repeat", "nullif(101, 101.9)"),
    ("trunc_101_9_seq", "sequence", "nullif(101, 101.9)"),
    ("trunc_exp", "array_repeat", "nullif(101, 1.014e2)"),
    ("trunc_exp_seq", "sequence", "nullif(101, 1.014e2)"),
    ("trunc_sqrt", "array_repeat", "nullif(101, sqrt(10201.5D))"),
    ("trunc_sqrt_seq", "sequence", "nullif(101, sqrt(10201.5D))"),
]

VN8_R1_FEXPR_REFUSAL_IDS = [cell[0] for cell in VN8_R1_FEXPR_REFUSALS]


@pytest.mark.parametrize("cell", VN8_R1_FEXPR_REFUSALS, ids=VN8_R1_FEXPR_REFUSAL_IDS)
def test_vn8_r1_bypass_shapes_fexpr_refuse(
    ceiling100: ReparkSession, cell: tuple[str, str, str]
) -> None:
    frame = ceiling100.range(1).select(F.size(F.expr(f"{cell[1]}(1, {cell[2]})")).alias("v"))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


VN8_R1_FEXPR_EQUAL_ANSWERS: list[tuple[str, str, str]] = [
    ("big_str", "sequence", "nullif(101L, '101')"),
    ("cast_si_str", "sequence", "nullif(CAST(101 AS SMALLINT), '101')"),
    ("subquery_str", "array_repeat", "nullif((SELECT 101), '101')"),
    ("greatest_str", "array_repeat", "nullif(101, greatest(CAST('0' AS INT), 101))"),
]

VN8_R1_FEXPR_EQUAL_ANSWER_IDS = [cell[0] for cell in VN8_R1_FEXPR_EQUAL_ANSWERS]


@pytest.mark.parametrize("cell", VN8_R1_FEXPR_EQUAL_ANSWERS, ids=VN8_R1_FEXPR_EQUAL_ANSWER_IDS)
def test_vn8_r1_exact_equal_shapes_fexpr_answer_null(
    ceiling100: ReparkSession, cell: tuple[str, str, str]
) -> None:
    frame = ceiling100.range(1).select(F.size(F.expr(f"{cell[1]}(1, {cell[2]})")).alias("v"))
    assert [row.asDict() for row in frame.collect()] == [{"v": None}]


@pytest.mark.parametrize("func", ["array_repeat", "sequence"])
def test_vn8_r1_df_coalesce_first_refuses(ceiling100: ReparkSession, func: str) -> None:
    first = F.coalesce(F.lit("101").try_cast("int"), F.lit(1))
    if func == "array_repeat":
        call = F.array_repeat(F.lit(1), F.nullif(first, F.lit(0)))
    else:
        call = F.sequence(F.lit(1), F.nullif(first, F.lit(0)))
    frame = ceiling100.range(1).select(F.size(call).alias("v"))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


@pytest.mark.parametrize("func", ["array_repeat", "sequence"])
def test_vn8_r1_df_q_coalesce_refuses(ceiling100: ReparkSession, func: str) -> None:
    second = F.coalesce(F.lit("0").try_cast("int"), F.lit(101))
    if func == "array_repeat":
        call = F.array_repeat(F.lit(1), F.nullif(F.lit(101), second))
    else:
        call = F.sequence(F.lit(1), F.nullif(F.lit(101), second))
    frame = ceiling100.range(1).select(F.size(call).alias("v"))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


def test_vn8_r1_df_trycast_first_refuses(ceiling100: ReparkSession) -> None:
    first = F.lit("101").try_cast("int")
    frame = ceiling100.range(1).select(
        F.size(F.array_repeat(F.lit(1), F.nullif(first, F.lit(0)))).alias("v")
    )
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


VN8_R1_DF_FIRSTS: list[tuple[str, str]] = [
    ("abs", "array_repeat"),
    ("greatest", "array_repeat"),
    ("searched_case", "array_repeat"),
    ("nested", "array_repeat"),
    ("outer_cast", "array_repeat"),
    ("int_big", "array_repeat"),
    ("int_big_seq", "sequence"),
    ("si_cast", "array_repeat"),
]

VN8_R1_DF_FIRST_IDS = [cell[0] for cell in VN8_R1_DF_FIRSTS]


@pytest.mark.parametrize("cell", VN8_R1_DF_FIRSTS, ids=VN8_R1_DF_FIRST_IDS)
def test_vn8_r1_df_foldable_firsts_refuse(ceiling100: ReparkSession, cell: tuple[str, str]) -> None:
    kind = cell[0]
    if kind == "abs":
        first = F.abs(F.lit(-101))
    elif kind == "greatest":
        first = F.greatest(F.lit(101), F.lit(1))
    elif kind == "searched_case":
        first = F.when(F.lit(True), F.lit(101))
    elif kind == "nested":
        first = F.nullif(F.nullif(F.lit(101), F.lit(0)), F.lit(0))
    elif kind == "outer_cast":
        first = F.nullif(F.lit(101.0), F.lit(0)).cast("int")
        frame = ceiling100.range(1).select(F.size(F.array_repeat(F.lit(1), first)).alias("v"))
        with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
            frame.collect()
        return
    elif kind in ("int_big", "int_big_seq"):
        first = F.nullif(F.lit(101), F.lit(0).cast("bigint"))
        if cell[1] == "array_repeat":
            call = F.array_repeat(F.lit(1), first)
        else:
            call = F.sequence(F.lit(1), first)
        frame = ceiling100.range(1).select(F.size(call).alias("v"))
        with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
            frame.collect()
        return
    else:
        first = F.lit(101).cast("smallint")
    if cell[1] == "array_repeat":
        call = F.array_repeat(F.lit(1), F.nullif(first, F.lit(0)))
    else:
        call = F.sequence(F.lit(1), F.nullif(first, F.lit(0)))
    frame = ceiling100.range(1).select(F.size(call).alias("v"))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


VN8_R1_MIXED_EQUAL_ANSWERS: list[tuple[str, str]] = [
    (
        "repeat_int_big_equal",
        "SELECT size(array_repeat(1, nullif(101, 101L))) AS v",
    ),
    (
        "seq_int_big_equal",
        "SELECT size(sequence(1, nullif(101, 101L))) AS v",
    ),
]

VN8_R1_MIXED_EQUAL_ANSWER_IDS = [cell[0] for cell in VN8_R1_MIXED_EQUAL_ANSWERS]


@pytest.mark.parametrize("cell", VN8_R1_MIXED_EQUAL_ANSWERS, ids=VN8_R1_MIXED_EQUAL_ANSWER_IDS)
def test_vn8_r1_equal_mixed_width_answers_null(
    ceiling100: ReparkSession, cell: tuple[str, str]
) -> None:
    assert [row.asDict() for row in ceiling100.sql(cell[1]).collect()] == [{"v": None}]


def test_vn8_r1_small_equal_pair_answers_null(utc: ReparkSession) -> None:
    rows = utc.sql("SELECT size(array_repeat(1, nullif(5, 5))) AS v").collect()
    assert [row.asDict() for row in rows] == [{"v": None}]


VN8_R2_ANSI_ON: list[tuple[str, str, int, int, int]] = [
    ("stop", "sequence(1, '5')", 5, 1, 5),
    ("overflow", "sequence(2147483647, '2147483648')", 2, 2147483647, 2147483648),
    ("padded", "sequence(1, ' 5 ')", 5, 1, 5),
    ("start", "sequence('1', 5)", 5, 1, 5),
    ("bigint_start", "sequence(1L, '101')", 101, 1, 101),
    ("step", "sequence(1, 101, '1')", 101, 1, 101),
    ("nullif_stop", "sequence(1, nullif('101', 0))", 101, 1, 101),
]

VN8_R2_ANSI_ON_IDS = [cell[0] for cell in VN8_R2_ANSI_ON]


@pytest.mark.parametrize("cell", VN8_R2_ANSI_ON, ids=VN8_R2_ANSI_ON_IDS)
def test_vn8_r2_string_bound_casts_to_bigint(
    utc: ReparkSession, cell: tuple[str, str, int, int, int]
) -> None:
    rows = utc.sql(f"SELECT typeof({cell[1]}) AS t, {cell[1]} AS v").collect()
    assert [row.asDict()["t"] for row in rows] == ["array<bigint>"]
    values = next(row.asDict()["v"] for row in rows)
    assert len(values) == cell[2]
    assert (values[0], values[-1]) == (cell[3], cell[4])


VN8_R2_ANSI_OFF: list[tuple[str, str]] = [
    ("stop", "SELECT sequence(1, '5') AS v"),
    ("overflow", "SELECT sequence(2147483647, '2147483648') AS v"),
    ("padded", "SELECT sequence(1, ' 5 ') AS v"),
    ("start", "SELECT sequence('1', 5) AS v"),
    ("bigint_start", "SELECT sequence(1L, '101') AS v"),
    ("step", "SELECT sequence(1, 101, '1') AS v"),
    ("nullif_stop", "SELECT sequence(1, nullif('101', 0)) AS v"),
    ("typeof", "SELECT typeof(sequence(1, '5')) AS t"),
]

VN8_R2_ANSI_OFF_IDS = [cell[0] for cell in VN8_R2_ANSI_OFF]


@pytest.mark.parametrize("cell", VN8_R2_ANSI_OFF, ids=VN8_R2_ANSI_OFF_IDS)
def test_vn8_r2_string_bound_refuses_without_ansi(
    ceiling100_legacy: ReparkSession, cell: tuple[str, str]
) -> None:
    with pytest.raises(AnalysisException, match="SEQUENCE_WRONG_INPUT_TYPES"):
        ceiling100_legacy.sql(cell[1]).collect()


def test_vn8_r2_garbage_string_raises_cast_invalid_input(utc: ReparkSession) -> None:
    with pytest.raises(_RAISES, match="CAST_INVALID_INPUT"):
        utc.sql("SELECT sequence(1, 'abc') AS v").collect()


def test_vn8_r2_garbage_string_typeof_is_bigint(utc: ReparkSession) -> None:
    rows = utc.sql("SELECT typeof(sequence(1, 'abc')) AS t").collect()
    assert [row.asDict() for row in rows] == [{"t": "array<bigint>"}]


def test_vn8_r2_garbage_string_refuses_without_ansi(
    ceiling100_legacy: ReparkSession,
) -> None:
    with pytest.raises(AnalysisException, match="SEQUENCE_WRONG_INPUT_TYPES"):
        ceiling100_legacy.sql("SELECT sequence(1, 'abc') AS v").collect()


def test_vn8_r2_string_column_answers_bigint(utc: ReparkSession) -> None:
    utc.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn8_sv AS SELECT * FROM VALUES "
        "(1, '5'), (2, CAST(NULL AS STRING)), (3, ' 3 ') AS t(id, s)"
    ).collect()
    rows = utc.sql("SELECT id, sequence(1, s) AS v FROM vn8_sv ORDER BY id").collect()
    assert [row.asDict() for row in rows] == [
        {"id": 1, "v": [1, 2, 3, 4, 5]},
        {"id": 2, "v": None},
        {"id": 3, "v": [1, 2, 3]},
    ]
    rows = utc.sql("SELECT typeof(sequence(1, s)) AS t FROM vn8_sv LIMIT 1").collect()
    assert [row.asDict() for row in rows] == [{"t": "array<bigint>"}]


def test_vn8_r2_dataframe_sequence_answers_bigint(utc: ReparkSession) -> None:
    frame = utc.range(1).select(F.sequence(F.lit(1), F.lit("5")).alias("v"))
    assert [field.dataType.simpleString() for field in frame.schema.fields] == ["array<bigint>"]
    assert [row.asDict() for row in frame.collect()] == [{"v": [1, 2, 3, 4, 5]}]
