"""FN-CHR-1 today: chr/char take a Unicode scalar, not n % 256 (registry §7).

pins: ex-4-functions-strings-a/C-001
"""

import pytest

from repark.errors import PySparkException
from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def test_fn_chr_300_is_unicode_letter_today() -> None:
    """Today: chr(300) and char(300) are 'Ĭ'; Spark 4.1.2 answers ',' (300 % 256)."""
    repark = ReparkSession.builder.appName("fn-chr").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(300,)], ["n"])
        chr_values = [row[0] for row in frame.select(F.chr(F.col("n"))).collect()]
        char_values = [row[0] for row in frame.select(F.char(F.col("n"))).collect()]
        assert chr_values == ["Ĭ"]
        assert char_values == ["Ĭ"]
    finally:
        repark.stop()


def test_fn_chr_negative_raises_today() -> None:
    """Today: chr(-1) raises; Spark 4.1.2 answers an empty string."""
    repark = ReparkSession.builder.appName("fn-chr-neg").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(-1,)], ["n"])
        with pytest.raises(PySparkException, match="invalid Unicode scalar value: -1"):
            frame.select(F.chr(F.col("n"))).collect()
        with pytest.raises(PySparkException, match="invalid Unicode scalar value: -1"):
            frame.select(F.char(F.col("n"))).collect()
    finally:
        repark.stop()
