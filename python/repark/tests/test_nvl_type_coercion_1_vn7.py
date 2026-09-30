from __future__ import annotations

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)
from repark.spark import functions as F  # noqa: N812

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
        ReparkSession.builder.appName("nvl-type-coercion-1-vn7")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


@pytest.fixture
def ceiling100() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn7-ceil")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config(_MAX_ARRAY_ELEMENTS_KEY, "100")
        .getOrCreate()
    )


@pytest.fixture
def ceiling100_legacy() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn7-3-legacy")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config(_MAX_ARRAY_ELEMENTS_KEY, "100")
        .getOrCreate()
    )


VN7_R1_REFUSALS: list[tuple[str, str]] = [
    ("repeat_abs", "SELECT size(array_repeat(1, nullif(abs(-101), 0))) AS v"),
    ("repeat_greatest", "SELECT size(array_repeat(1, nullif(greatest(101, 1), 0))) AS v"),
    (
        "repeat_case",
        "SELECT size(array_repeat(1, nullif(CASE WHEN true THEN 101 END, 0))) AS v",
    ),
    ("repeat_subquery", "SELECT size(array_repeat(1, nullif((SELECT 101), 0))) AS v"),
    (
        "repeat_nested",
        "SELECT size(array_repeat(1, nullif(nullif(101, 0), 0))) AS v",
    ),
    (
        "repeat_nested_mixed",
        "SELECT size(array_repeat(1, nullif(nullif(101, 0L), 0))) AS v",
    ),
    (
        "repeat_dbl_cast",
        "SELECT size(array_repeat(1, CAST(nullif(101.0D, 0) AS INT))) AS v",
    ),
    ("seq_abs", "SELECT size(sequence(1, nullif(abs(-101), 0))) AS v"),
    ("seq_greatest", "SELECT size(sequence(1, nullif(greatest(101, 1), 0))) AS v"),
    (
        "seq_case",
        "SELECT size(sequence(1, nullif(CASE WHEN true THEN 101 END, 0))) AS v",
    ),
    ("seq_subquery", "SELECT size(sequence(1, nullif((SELECT 101), 0))) AS v"),
    (
        "seq_nested_mixed",
        "SELECT size(sequence(1, nullif(nullif(101, 0L), 0))) AS v",
    ),
    (
        "seq_dbl_cast",
        "SELECT size(sequence(1, CAST(nullif(101.0D, 0) AS INT))) AS v",
    ),
]

VN7_R1_REFUSAL_IDS = [cell[0] for cell in VN7_R1_REFUSALS]


@pytest.mark.parametrize("cell", VN7_R1_REFUSALS, ids=VN7_R1_REFUSAL_IDS)
def test_vn7_r1_foldable_first_refuses(ceiling100: ReparkSession, cell: tuple[str, str]) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql(cell[1]).collect()


VN7_R1_FEXPR_REFUSALS: list[tuple[str, str, str]] = [
    ("abs", "array_repeat", "nullif(abs(-101), 0)"),
    ("greatest", "array_repeat", "nullif(greatest(101, 1), 0)"),
    ("case", "array_repeat", "nullif(CASE WHEN true THEN 101 END, 0)"),
    ("subquery", "array_repeat", "nullif((SELECT 101), 0)"),
    ("nested", "array_repeat", "nullif(nullif(101, 0L), 0)"),
    ("dbl_cast", "array_repeat", "CAST(nullif(101.0D, 0) AS INT)"),
    ("int_big", "array_repeat", "nullif(101, 0L)"),
    ("int_big_seq", "sequence", "nullif(101, 0L)"),
    ("si_cast_seq", "sequence", "nullif(CAST(101 AS SMALLINT), 0)"),
]

VN7_R1_FEXPR_REFUSAL_IDS = [cell[0] for cell in VN7_R1_FEXPR_REFUSALS]


@pytest.mark.parametrize("cell", VN7_R1_FEXPR_REFUSALS, ids=VN7_R1_FEXPR_REFUSAL_IDS)
def test_vn7_r1_foldable_first_fexpr_refuses(
    ceiling100: ReparkSession, cell: tuple[str, str, str]
) -> None:
    frame = ceiling100.range(1).select(F.size(F.expr(f"{cell[1]}(1, {cell[2]})")).alias("v"))
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        frame.collect()


VN7_R1_EQUAL_ANSWERS: list[tuple[str, str]] = [
    ("repeat_eq_dbl", "SELECT size(array_repeat(1, nullif(101, 101.0D))) AS v"),
    ("seq_eq_dbl", "SELECT size(sequence(1, nullif(101, 101.0D))) AS v"),
]

VN7_R1_EQUAL_ANSWER_IDS = [cell[0] for cell in VN7_R1_EQUAL_ANSWERS]


@pytest.mark.parametrize("cell", VN7_R1_EQUAL_ANSWERS, ids=VN7_R1_EQUAL_ANSWER_IDS)
def test_vn7_r1_equal_pairs_answer_null(ceiling100: ReparkSession, cell: tuple[str, str]) -> None:
    assert [row.asDict() for row in ceiling100.sql(cell[1]).collect()] == [{"v": None}]


VN7_R1_WRAPPED_DEC_NULLS: list[tuple[str, str]] = [
    (
        "repeat_second_dec",
        "SELECT size(array_repeat(1, nullif(101, CAST(101 AS DECIMAL(5,0))))) AS v",
    ),
    (
        "seq_second_dec",
        "SELECT size(sequence(1, nullif(101, CAST(101 AS DECIMAL(5,0))))) AS v",
    ),
]

VN7_R1_WRAPPED_DEC_NULL_IDS = [cell[0] for cell in VN7_R1_WRAPPED_DEC_NULLS]


@pytest.mark.parametrize("cell", VN7_R1_WRAPPED_DEC_NULLS, ids=VN7_R1_WRAPPED_DEC_NULL_IDS)
def test_vn7_r1_wrapped_decimal_second_answers_null(
    ceiling100: ReparkSession, cell: tuple[str, str]
) -> None:
    assert [row.asDict() for row in ceiling100.sql(cell[1]).collect()] == [{"v": None}]


VN7_3_STRING_EQUAL_ANSWERS: list[tuple[str, str]] = [
    (
        "repeat_second_str",
        "SELECT size(array_repeat(1, nullif(101, '101'))) AS v",
    ),
    ("seq_second_str", "SELECT size(sequence(1, nullif(101, '101'))) AS v"),
    (
        "repeat_second_str_padded",
        "SELECT size(array_repeat(1, nullif(101, ' 101 '))) AS v",
    ),
    ("seq_second_str_padded", "SELECT size(sequence(1, nullif(101, ' 101 '))) AS v"),
]

VN7_3_STRING_EQUAL_ANSWER_IDS = [cell[0] for cell in VN7_3_STRING_EQUAL_ANSWERS]


@pytest.mark.parametrize("cell", VN7_3_STRING_EQUAL_ANSWERS, ids=VN7_3_STRING_EQUAL_ANSWER_IDS)
def test_vn7_3_equal_string_second_answers_null(
    ceiling100: ReparkSession, cell: tuple[str, str]
) -> None:
    assert [row.asDict() for row in ceiling100.sql(cell[1]).collect()] == [{"v": None}]


VN7_3_STRING_UNEQUAL_REFUSALS: list[tuple[str, str]] = [
    (
        "repeat_second_str_unequal",
        "SELECT size(array_repeat(1, nullif(101, '102'))) AS v",
    ),
    ("seq_second_str_unequal", "SELECT size(sequence(1, nullif(101, '102'))) AS v"),
]

VN7_3_STRING_UNEQUAL_REFUSAL_IDS = [cell[0] for cell in VN7_3_STRING_UNEQUAL_REFUSALS]


@pytest.mark.parametrize(
    "cell", VN7_3_STRING_UNEQUAL_REFUSALS, ids=VN7_3_STRING_UNEQUAL_REFUSAL_IDS
)
def test_vn7_3_unequal_string_second_refuses(
    ceiling100: ReparkSession, cell: tuple[str, str]
) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql(cell[1]).collect()


def test_vn7_3_garbage_string_second_raises_cast_error_when_ansi(
    ceiling100: ReparkSession,
) -> None:
    with pytest.raises(RAISES, match="CAST_INVALID_INPUT"):
        ceiling100.sql("SELECT nullif(101, 'abc') AS v").collect()


def test_vn7_3_garbage_string_second_answers_without_ansi(
    ceiling100_legacy: ReparkSession,
) -> None:
    rows = ceiling100_legacy.sql("SELECT nullif(101, 'abc') AS v").collect()
    assert [row.asDict() for row in rows] == [{"v": 101}]


def test_vn7_3_garbage_string_second_under_ceiling_refuses_divergence(
    ceiling100: ReparkSession,
) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql("SELECT size(array_repeat(1, nullif(101, 'abc'))) AS v").collect()


VN7_R1_INT_CAST_DOUBLE: list[tuple[str, str]] = [
    (
        "repeat_cast_dbl",
        "SELECT size(array_repeat(1, nullif(CAST(101.9D AS INT), 0))) AS v",
    ),
    (
        "seq_cast_dbl",
        "SELECT size(sequence(1, nullif(CAST(101.9D AS INT), 0))) AS v",
    ),
    (
        "seq_cast_dbl_big",
        "SELECT size(sequence(1, nullif(CAST(101.9D AS BIGINT), 0L))) AS v",
    ),
]

VN7_R1_INT_CAST_DOUBLE_IDS = [cell[0] for cell in VN7_R1_INT_CAST_DOUBLE]


@pytest.mark.parametrize("cell", VN7_R1_INT_CAST_DOUBLE, ids=VN7_R1_INT_CAST_DOUBLE_IDS)
def test_vn7_r1_int_cast_double_first_refuses(
    ceiling100: ReparkSession, cell: tuple[str, str]
) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql(cell[1]).collect()


def test_vn7_r1_unsigned_count_over_ceiling_refuses(
    ceiling100: ReparkSession,
) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql(
            "SELECT size(array_repeat(1, nullif(arrow_cast(101, 'UInt8'), 0L))) AS v"
        ).collect()


def test_vn7_r2_string_first_sequence_answers(utc: ReparkSession) -> None:
    rows = utc.sql(
        "SELECT typeof(sequence(1, nullif('101', 0))) AS t, "
        "size(sequence(1, nullif('101', 0))) AS v"
    ).collect()
    assert [row.asDict() for row in rows] == [{"t": "array<bigint>", "v": 101}]


def test_vn7_r2_string_first_sequence_bigint_answers(utc: ReparkSession) -> None:
    rows = utc.sql(
        "SELECT typeof(sequence(1L, nullif('101', 0))) AS t, "
        "size(sequence(1L, nullif('101', 0))) AS v"
    ).collect()
    assert [row.asDict() for row in rows] == [{"t": "array<bigint>", "v": 101}]


def test_vn7_r2_string_first_sequence_ceiling100_residue(
    ceiling100: ReparkSession,
) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql("SELECT size(sequence(1, nullif('101', 0))) AS v").collect()


def test_vn7_r2_string_first_sequence_fexpr(utc: ReparkSession) -> None:
    frame = utc.range(1).select(F.size(F.expr("sequence(1, nullif('101', 0))")).alias("v"))
    assert [row.asDict() for row in frame.collect()] == [{"v": 101}]


def test_vn7_r2_string_first_sequence_column_api(utc: ReparkSession) -> None:
    frame = utc.range(1).select(
        F.size(F.sequence(F.lit(1), F.nullif(F.lit("101"), F.lit(0)))).alias("v")
    )
    assert [row.asDict() for row in frame.collect()] == [{"v": 101}]


def test_vn7_r2_array_repeat_string_first_still_refuses(utc: ReparkSession) -> None:
    with pytest.raises(RAISES):
        utc.sql("SELECT size(array_repeat(1, nullif('101', 0))) AS v").collect()


def test_vn7_r2_sequence_garbage_string_still_refuses(utc: ReparkSession) -> None:
    with pytest.raises(RAISES):
        utc.sql("SELECT size(sequence(1, 'abc')) AS v").collect()


def build_vn7_cc(session: ReparkSession) -> None:
    session.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn7_cc AS SELECT * FROM VALUES "
        "(1, CAST(NULL AS INT), 'abc', CAST(NULL AS DATE), 'junk'), "
        "(2, 5, '5', DATE '2024-01-01', '2024-01-01') AS t(id, x, s, d, ds)"
    ).collect()


def test_vn7_r3_null_first_skips_bad_cast(utc: ReparkSession) -> None:
    build_vn7_cc(utc)
    rows = utc.sql("SELECT id, nullif(x, s) AS v FROM vn7_cc ORDER BY id").collect()
    assert [row.asDict() for row in rows] == [{"id": 1, "v": None}, {"id": 2, "v": None}]


def test_vn7_r3_null_date_first_skips_bad_cast(utc: ReparkSession) -> None:
    build_vn7_cc(utc)
    rows = utc.sql("SELECT id, nullif(d, ds) AS v FROM vn7_cc ORDER BY id").collect()
    assert [row.asDict() for row in rows] == [{"id": 1, "v": None}, {"id": 2, "v": None}]


def test_vn7_r3_null_first_where_counts_both(utc: ReparkSession) -> None:
    build_vn7_cc(utc)
    rows = utc.sql("SELECT count(*) AS v FROM vn7_cc WHERE nullif(x, s) IS NULL").collect()
    assert [row.asDict() for row in rows] == [{"v": 2}]


def test_vn7_r3_non_null_invalid_still_raises(utc: ReparkSession) -> None:
    build_vn7_cc(utc)
    with pytest.raises(RAISES, match="CAST_INVALID_INPUT"):
        utc.sql("SELECT id, nullif(x, 'abc') AS v FROM vn7_cc ORDER BY id").collect()


def test_vn7_r3_non_null_first_cast_still_raises(utc: ReparkSession) -> None:
    build_vn7_cc(utc)
    with pytest.raises(RAISES, match="CAST_INVALID_INPUT"):
        utc.sql("SELECT id, nullif(s, x) AS v FROM vn7_cc ORDER BY id").collect()


def build_vn7_sc(session: ReparkSession) -> None:
    session.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn7_sc AS SELECT CAST(NULL AS INT) AS ni FROM range(2)"
    ).collect()


def test_vn7_r3_all_null_column_skips_scalar_cast(utc: ReparkSession) -> None:
    build_vn7_sc(utc)
    rows = utc.sql("SELECT nullif(ni, 'abc') AS v FROM vn7_sc").collect()
    assert [row.asDict() for row in rows] == [{"v": None}, {"v": None}]


def test_vn7_r3_all_null_column_count_and_where(utc: ReparkSession) -> None:
    build_vn7_sc(utc)
    rows = utc.sql("SELECT count(nullif(ni, 'abc')) AS v FROM vn7_sc").collect()
    assert [row.asDict() for row in rows] == [{"v": 0}]
    rows = utc.sql("SELECT count(*) AS v FROM vn7_sc WHERE nullif(ni, 'abc') IS NULL").collect()
    assert [row.asDict() for row in rows] == [{"v": 2}]
