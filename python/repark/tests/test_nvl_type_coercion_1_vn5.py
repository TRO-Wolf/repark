from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)

RAISES = (
    AnalysisException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)


@pytest.fixture
def utc() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn5")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


@pytest.fixture
def utc_off() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn5-off")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


VN5_R4_ON_VALUES: list[tuple[str, str, str, str | None]] = [
    ("big_first", "nullif(5L, ' 5 ')", "bigint", None),
    ("str_first", "nullif(' 5 ', 5L)", "string", None),
]

VN5_R4_ON_VALUE_IDS = [cell[0] for cell in VN5_R4_ON_VALUES]


def check_vn5_select(session: ReparkSession, cell: tuple[str, str, str, str | None]) -> None:
    rows = session.sql(f"SELECT typeof({cell[1]}) AS t, CAST({cell[1]} AS STRING) AS v").collect()
    assert [row.asDict() for row in rows] == [{"t": cell[2], "v": cell[3]}]


@pytest.mark.parametrize("cell", VN5_R4_ON_VALUES, ids=VN5_R4_ON_VALUE_IDS)
def test_vn5_r4_bigint_string_both_orders(
    utc: ReparkSession, cell: tuple[str, str, str, str | None]
) -> None:
    check_vn5_select(utc, cell)


def test_vn5_r4_bigint_string_column_both_orders(utc: ReparkSession) -> None:
    rows = utc.sql(
        "SELECT typeof(nullif(a, b)) AS t, CAST(nullif(a, b) AS STRING) AS v "
        "FROM VALUES (5L, ' 5 ') AS v(a, b)"
    ).collect()
    assert [row.asDict() for row in rows] == [{"t": "bigint", "v": None}]
    rows = utc.sql(
        "SELECT typeof(nullif(a, b)) AS t, CAST(nullif(a, b) AS STRING) AS v "
        "FROM VALUES (' 5 ', 5L) AS v(a, b)"
    ).collect()
    assert [row.asDict() for row in rows] == [{"t": "string", "v": None}]


VN5_R4_ON_ERRORS: list[tuple[str, str]] = [
    ("bool_on", "SELECT CAST(nullif(true, 'on') AS STRING) AS v"),
    ("bool_off", "SELECT CAST(nullif(true, 'off') AS STRING) AS v"),
    ("bool_tr", "SELECT CAST(nullif(true, 'tr') AS STRING) AS v"),
    ("on_bool", "SELECT CAST(nullif('on', true) AS STRING) AS v"),
    ("off_bool", "SELECT CAST(nullif('off', true) AS STRING) AS v"),
    ("tr_bool", "SELECT CAST(nullif('tr', true) AS STRING) AS v"),
    ("big_p50", "SELECT CAST(nullif(5L, '5.0') AS STRING) AS v"),
    ("p50_big", "SELECT CAST(nullif('5.0', 5L) AS STRING) AS v"),
]

VN5_R4_ON_ERROR_IDS = [cell[0] for cell in VN5_R4_ON_ERRORS]


@pytest.mark.parametrize("cell", VN5_R4_ON_ERRORS, ids=VN5_R4_ON_ERROR_IDS)
def test_vn5_r4_ansi_on_invalid_raises(utc: ReparkSession, cell: tuple[str, str]) -> None:
    with pytest.raises(RAISES, match="CAST_INVALID_INPUT"):
        utc.sql(cell[1]).collect()


VN5_R4_OFF_VALUES: list[tuple[str, str, str, str | None]] = [
    ("bool_on", "nullif(true, 'on')", "boolean", "true"),
    ("bool_off", "nullif(true, 'off')", "boolean", "true"),
    ("bool_tr", "nullif(true, 'tr')", "boolean", "true"),
    ("on_bool", "nullif('on', true)", "string", "on"),
    ("off_bool", "nullif('off', true)", "string", "off"),
    ("tr_bool", "nullif('tr', true)", "string", "tr"),
    ("big_ws", "nullif(5L, ' 5 ')", "bigint", None),
    ("big_p50", "nullif(5L, '5.0')", "bigint", None),
    ("ws_big", "nullif(' 5 ', 5L)", "string", None),
    ("p50_big", "nullif('5.0', 5L)", "string", None),
]

VN5_R4_OFF_VALUE_IDS = [cell[0] for cell in VN5_R4_OFF_VALUES]


@pytest.mark.parametrize("cell", VN5_R4_OFF_VALUES, ids=VN5_R4_OFF_VALUE_IDS)
def test_vn5_r4_ansi_off_values(
    utc_off: ReparkSession, cell: tuple[str, str, str, str | None]
) -> None:
    check_vn5_select(utc_off, cell)


def test_vn5_r4_dictionary_string_column(tmp_path: Path) -> None:
    items = pa.array(["5", " 5 ", "6", None], type=pa.dictionary(pa.int32(), pa.string()))
    table = pa.table({"id": pa.array([1, 2, 3, 4], type=pa.int32()), "ds": items})
    pq.write_table(table, tmp_path / "dict.parquet")
    session = (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn5-dict")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    try:
        session.read.parquet(str(tmp_path / "dict.parquet")).createOrReplaceTempView("vn5_dt")
        rows = session.sql(
            "SELECT CAST(nullif(ds, 5) AS STRING) AS v FROM vn5_dt ORDER BY id"
        ).collect()
        assert [row.asDict() for row in rows] == [
            {"v": None},
            {"v": None},
            {"v": "6"},
            {"v": None},
        ]
        rows = session.sql(
            "SELECT CAST(nullif(5, ds) AS STRING) AS v FROM vn5_dt ORDER BY id"
        ).collect()
        assert [row.asDict() for row in rows] == [
            {"v": None},
            {"v": None},
            {"v": "5"},
            {"v": "5"},
        ]
        rows = session.sql(
            "SELECT CAST(nullifzero(ds) AS STRING) AS v FROM vn5_dt ORDER BY id"
        ).collect()
        assert [row.asDict() for row in rows] == [
            {"v": "5"},
            {"v": " 5 "},
            {"v": "6"},
            {"v": None},
        ]
    finally:
        session.stop()


def test_vn5_r5_volatile_non_nullable_first_reports_non_nullable(utc: ReparkSession) -> None:
    utc.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn5_big AS SELECT id, "
        "CASE WHEN id % 4 = 0 THEN NULL ELSE CAST(id AS DOUBLE) END AS xd FROM range(20000)"
    ).collect()
    frame = utc.sql(
        "SELECT nvl(rand(), xd) AS v, nvl(rand(), 1D) AS w, ifnull(rand(), xd) AS u FROM vn5_big"
    )
    assert [
        (field.name, field.dataType.simpleString(), field.nullable) for field in frame.schema.fields
    ] == [
        ("v", "double", False),
        ("w", "double", False),
        ("u", "double", False),
    ]


@pytest.mark.parametrize("fn", ["nvl", "ifnull"])
def test_vn5_r5_if_first_over_nullable_column_collects(utc: ReparkSession, fn: str) -> None:
    utc.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn5_big AS SELECT id, "
        "CASE WHEN id % 3 = 0 THEN NULL ELSE id END AS x FROM range(20000)"
    ).collect()
    rows = utc.sql(f"SELECT {fn}(IF(rand() < 0.5, NULL, 1), x) AS v FROM vn5_big").collect()
    assert len(rows) == 20000
    nulls = sum(1 for row in rows if row["v"] is None)
    assert 2800 <= nulls <= 3900


def test_vn5_r2_nan_payload_float_kernel_answers_null(utc: ReparkSession) -> None:
    rows = utc.sql(
        "SELECT CAST(nullif(CAST(sqrt(a) AS FLOAT), CAST('NaN' AS DOUBLE)) AS STRING) AS v "
        "FROM VALUES (-1.0D) AS t(a)"
    ).collect()
    assert [row.asDict() for row in rows] == [{"v": None}]


def test_vn5_r2_nan_payload_string_kernel_answers_null(utc: ReparkSession) -> None:
    rows = utc.sql(
        "SELECT CAST(nullif('NaN', sqrt(a)) AS STRING) AS v FROM VALUES (-1.0D) AS t(a)"
    ).collect()
    assert [row.asDict() for row in rows] == [{"v": None}]


def test_vn5_r7_lossy_nullif_in_sequence_answers_null(utc: ReparkSession) -> None:
    rows = utc.sql(
        "SELECT typeof(sequence(0L, nullif(9007199254740993L, 9007199254740992D))) AS t, "
        "CAST(sequence(0L, nullif(9007199254740993L, 9007199254740992D)) AS STRING) AS v"
    ).collect()
    assert [row.asDict() for row in rows] == [{"t": "array<bigint>", "v": None}]


def test_vn5_r7_lossy_nullif_cast_form_in_sequence_answers_null(utc: ReparkSession) -> None:
    rows = utc.sql(
        "SELECT typeof(sequence(0L, nullif(CAST(9007199254740993 AS BIGINT), "
        "CAST(9007199254740992 AS DOUBLE)))) AS t, "
        "CAST(sequence(0L, nullif(CAST(9007199254740993 AS BIGINT), "
        "CAST(9007199254740992 AS DOUBLE))) AS STRING) AS v"
    ).collect()
    assert [row.asDict() for row in rows] == [{"t": "array<bigint>", "v": None}]
