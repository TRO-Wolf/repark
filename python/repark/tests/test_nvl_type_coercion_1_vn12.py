from __future__ import annotations

import re

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

NVL2_BAD = "nvl2(1, 'a', X'0102')"
NVL2_GOOD = "nvl2(1, 'ab', X'0102')"
CAST_CLASS = "CAST_WITH_CONF_SUGGESTION"
CAST_TEXT = "CAST(Int32(1) AS BINARY)"


@pytest.fixture
def legacy() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn12")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def test_vn12_nvl2_binary_bad_fexpr_value_refuses(legacy: ReparkSession) -> None:
    """Q3 fold: F.expr nvl2(1, 'a', binary) refuses with base's cast class."""
    frame = legacy.range(1).select(F.expr(NVL2_BAD).alias("v"))
    with pytest.raises(AnalysisException, match=re.escape(CAST_CLASS)):
        frame.collect()


def test_vn12_nvl2_binary_bad_fexpr_typeof_refuses(legacy: ReparkSession) -> None:
    """Q3 fold: typeof of F.expr nvl2(1, 'a', binary) names base's cast."""
    frame = legacy.range(1).select(F.expr(f"typeof({NVL2_BAD})").alias("v"))
    with pytest.raises(AnalysisException, match=re.escape(CAST_TEXT)):
        frame.collect()


def test_vn12_nvl2_binary_good_fexpr_value_refuses(legacy: ReparkSession) -> None:
    """Q3 fold: F.expr nvl2(1, 'ab', binary) refuses with base's cast class."""
    frame = legacy.range(1).select(F.expr(NVL2_GOOD).alias("v"))
    with pytest.raises(AnalysisException, match=re.escape(CAST_CLASS)):
        frame.collect()


def test_vn12_nvl2_binary_good_fexpr_typeof_refuses(legacy: ReparkSession) -> None:
    """Q3 fold: typeof of F.expr nvl2(1, 'ab', binary) names base's cast."""
    frame = legacy.range(1).select(F.expr(f"typeof({NVL2_GOOD})").alias("v"))
    with pytest.raises(AnalysisException, match=re.escape(CAST_TEXT)):
        frame.collect()
