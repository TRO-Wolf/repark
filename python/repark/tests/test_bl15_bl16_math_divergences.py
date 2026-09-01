"""BL-15/BL-16 today: composed expm1 and overflowing hypot (registry §7)."""

import math

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _one_double(expr, name: str) -> float:
    repark = ReparkSession.builder.appName("bl15-16").master("local[1]").getOrCreate()
    frame = repark.createDataFrame([(1.0,)], "x double").select(expr.alias(name))
    return frame.collect()[0][name]


def test_bl15_expm1_composes_exp_minus_one_today() -> None:
    """Today: bit-equal to ``exp(x) - 1``; Spark ``Math.expm1(1e-08)`` is ``1.0000000050000001e-08``."""
    got = _one_double(F.expm1(F.lit(1e-08)), "y")
    assert got == math.exp(1e-08) - 1.0
    assert got != math.expm1(1e-08)


def test_bl16_hypot_overflows_to_inf_today() -> None:
    """Today: ``hypot(1e200, 1e200)`` is ``inf``; Spark rescales to 1.4142135623730951e+200."""
    got = _one_double(F.hypot(F.lit(1e200), F.lit(1e200)), "y")
    assert math.isinf(got)
