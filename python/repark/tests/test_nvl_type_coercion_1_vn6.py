from __future__ import annotations

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

_MAX_ARRAY_ELEMENTS_KEY = "repark.sql.maxArrayElements"


@pytest.fixture
def utc() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn6")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


@pytest.fixture
def ceiling100() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn6-ceil")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config(_MAX_ARRAY_ELEMENTS_KEY, "100")
        .getOrCreate()
    )


VN6_R1_FIRSTS: list[tuple[str, str, str, str]] = [
    (
        "try_cast",
        "try_cast(concat('x', CAST(rand() AS STRING)) AS INT)",
        "x",
        "int",
    ),
    (
        "try_add",
        "try_add(CAST(2147483647 AS INT), CAST(rand() + 1 AS INT))",
        "x",
        "int",
    ),
    (
        "try_divide",
        "try_divide(CAST(rand() AS INT), 0)",
        "CAST(x AS DOUBLE)",
        "double",
    ),
    (
        "try_element_at",
        "try_element_at(array(CAST(rand() AS INT)), 5)",
        "x",
        "int",
    ),
    (
        "try_to_number",
        "try_to_number(concat('x', CAST(rand() AS STRING)), '999')",
        "CAST(x AS DECIMAL(3,0))",
        "decimal(3,0)",
    ),
]

VN6_R1_FIRST_IDS = [cell[0] for cell in VN6_R1_FIRSTS]

VN6_R1_SECONDS: dict[str, str] = {
    "try_cast": "x",
    "try_add": "x",
    "try_divide": "double",
    "try_element_at": "x",
    "try_to_number": "decimal(3,0)",
}


def build_vn6_nb(session: ReparkSession) -> None:
    session.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn6_nb AS SELECT * FROM VALUES "
        "(1, CAST(NULL AS INT)), (2, CAST(NULL AS INT)) AS t(id, x)"
    ).collect()


@pytest.mark.parametrize("fn", ["nvl", "ifnull"])
@pytest.mark.parametrize("cell", VN6_R1_FIRSTS, ids=VN6_R1_FIRST_IDS)
def test_vn6_r1_try_first_over_nullable_fallback_collects_null(
    utc: ReparkSession, fn: str, cell: tuple[str, str, str, str]
) -> None:
    build_vn6_nb(utc)
    frame = utc.sql(f"SELECT {fn}({cell[1]}, {cell[2]}) AS v FROM vn6_nb ORDER BY id")
    assert [(field.dataType.simpleString(), field.nullable) for field in frame.schema.fields] == [
        (cell[3], True)
    ]
    assert [row.asDict() for row in frame.collect()] == [{"v": None}, {"v": None}]


@pytest.mark.parametrize("fn", ["nvl", "ifnull"])
@pytest.mark.parametrize("cell", VN6_R1_FIRSTS, ids=VN6_R1_FIRST_IDS)
def test_vn6_r1_try_first_dataframe_collects_null(
    utc: ReparkSession, fn: str, cell: tuple[str, str, str, str]
) -> None:
    build_vn6_nb(utc)
    first = F.expr(cell[1])
    cast = VN6_R1_SECONDS[cell[0]]
    second = F.col("x") if cast == "x" else F.col("x").cast(cast)
    apply = F.nvl if fn == "nvl" else F.ifnull
    frame = utc.table("vn6_nb").select(apply(first, second).alias("v")).orderBy("v")
    assert [(field.dataType.simpleString(), field.nullable) for field in frame.schema.fields] == [
        (cell[3], True)
    ]
    assert [row.asDict() for row in frame.collect()] == [{"v": None}, {"v": None}]


VN6_R2_REFUSALS: list[tuple[str, str]] = [
    ("repeat_int_big", "SELECT size(array_repeat(1, nullif(101, 0L))) AS v"),
    (
        "repeat_cast_big",
        "SELECT size(array_repeat(1, nullif(CAST(101 AS BIGINT), 0))) AS v",
    ),
    ("sequence_int_big", "SELECT size(sequence(1, nullif(101, 0L))) AS v"),
    ("repeat_int_big_1000", "SELECT size(array_repeat(1, nullif(1000, 1L))) AS v"),
]

VN6_R2_REFUSAL_IDS = [cell[0] for cell in VN6_R2_REFUSALS]


@pytest.mark.parametrize("cell", VN6_R2_REFUSALS, ids=VN6_R2_REFUSAL_IDS)
def test_vn6_r2_mixed_width_nullif_over_ceiling_refuses(
    ceiling100: ReparkSession, cell: tuple[str, str]
) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql(cell[1]).collect()


def test_vn6_r2_equal_mixed_width_nullif_yields_null(ceiling100: ReparkSession) -> None:
    rows = ceiling100.sql(
        "SELECT typeof(nullif(101, 101L)) AS t, CAST(nullif(101, 101L) AS STRING) AS v"
    ).collect()
    assert [row.asDict() for row in rows] == [{"t": "int", "v": None}]


def test_vn6_r2_lossy_nullif_in_sequence_refuses_residue(
    ceiling100: ReparkSession,
) -> None:
    with pytest.raises(AnalysisException, match=_MAX_ARRAY_ELEMENTS_KEY):
        ceiling100.sql(
            "SELECT typeof(sequence(0L, nullif(9007199254740993L, 9007199254740992D))) AS t, "
            "CAST(sequence(0L, nullif(9007199254740993L, 9007199254740992D)) AS STRING) AS v"
        ).collect()
