from __future__ import annotations

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

PLUS_NVL = "nvl('5', 5) + 1"
LEN_NVL_LTZ = "length(nvl(CAST(NULL AS STRING), TIMESTAMP'2024-01-02 03:04:05.5'))"
NULLIF_S_LTZ = "nullif('2024-01-02 03:04:05', TIMESTAMP'2024-01-02 03:04:05')"
NULLIF_LTZ_S = "nullif(TIMESTAMP'2024-01-02 03:04:05', '2024-01-02 03:04:05')"

ZONES = ["UTC", "America/New_York", "Asia/Kolkata"]


def build_session(zone: str, ansi: bool) -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn11")
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.ansi.enabled", "true" if ansi else "false")
        .getOrCreate()
    )


@pytest.mark.parametrize("zone", ZONES)
def test_vn11_plus_nvl_sql_answers(zone: str) -> None:
    """Q1: nvl('5', 5) + 1 answers 6 on the SQL door."""
    session = build_session(zone, True)
    rows = session.sql(f"SELECT {PLUS_NVL} AS v").collect()
    assert [row["v"] for row in rows] == [6]


@pytest.mark.parametrize("zone", ZONES)
def test_vn11_plus_nvl_sql_types(zone: str) -> None:
    """Q1: nvl('5', 5) + 1 types as bigint on the SQL door."""
    session = build_session(zone, True)
    rows = session.sql(f"SELECT typeof({PLUS_NVL}) AS v").collect()
    assert [row["v"] for row in rows] == ["bigint"]


@pytest.mark.parametrize("zone", ZONES)
def test_vn11_plus_nvl_fexpr_answers(zone: str) -> None:
    """Q1: nvl('5', 5) + 1 answers 6 on the F.expr door."""
    session = build_session(zone, True)
    rows = session.range(1).select(F.expr(PLUS_NVL).alias("v")).collect()
    assert [row["v"] for row in rows] == [6]


@pytest.mark.parametrize("zone", ZONES)
def test_vn11_plus_nvl_fexpr_types(zone: str) -> None:
    """Q1: nvl('5', 5) + 1 types as bigint on the F.expr door."""
    session = build_session(zone, True)
    rows = session.range(1).select(F.expr(f"typeof({PLUS_NVL})").alias("v")).collect()
    assert [row["v"] for row in rows] == ["bigint"]


@pytest.mark.parametrize("zone", ZONES)
def test_vn11_len_nvl_ltz_fexpr_answers(zone: str) -> None:
    """Q1: length(nvl(NULL, LTZ)) answers 21 on the F.expr door."""
    session = build_session(zone, True)
    rows = session.range(1).select(F.expr(LEN_NVL_LTZ).alias("v")).collect()
    assert [row["v"] for row in rows] == [21]


@pytest.mark.parametrize("zone", ["America/New_York", "Asia/Kolkata"])
def test_vn11_fold_nullif_s_ltz_fexpr_answers_null_legacy(zone: str) -> None:
    """Q2(a): string-versus-LTZ nullif answers NULL with ANSI off."""
    session = build_session(zone, False)
    rows = session.range(1).select(F.expr(NULLIF_S_LTZ).alias("v")).collect()
    assert [row["v"] for row in rows] == [None]


@pytest.mark.parametrize("zone", ["America/New_York", "Asia/Kolkata"])
def test_vn11_fold_nullif_ltz_s_fexpr_answers_null_legacy(zone: str) -> None:
    """Q2(a): LTZ-versus-string nullif answers NULL with ANSI off."""
    session = build_session(zone, False)
    rows = session.range(1).select(F.expr(NULLIF_LTZ_S).alias("v")).collect()
    assert [row["v"] for row in rows] == [None]
