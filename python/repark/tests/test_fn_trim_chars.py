"""FN-TRIM-CHARS-1 today: trim/ltrim/rtrim have no two-arg charset overload (registry §7).

pins: ex-4-functions-strings-a/C-001
"""

import pytest

from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def test_fn_trim_two_arg_is_typeerror_today() -> None:
    """Today: F.trim/ltrim/rtrim reject a trim-character argument; Spark 4.1.2 trims it."""
    with pytest.raises(TypeError, match="takes 1 positional argument but 2 were given"):
        F.trim(F.col("s"), F.lit("x"))
    with pytest.raises(TypeError, match="takes 1 positional argument but 2 were given"):
        F.ltrim(F.col("s"), F.lit("x"))
    with pytest.raises(TypeError, match="takes 1 positional argument but 2 were given"):
        F.rtrim(F.col("s"), F.lit("x"))
