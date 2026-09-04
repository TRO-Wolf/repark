"""FN-CHR-1: chr/char take n % 256 and empty string for negatives (registry §7)."""

import pytest

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812


def test_fn_chr_modulo_256_and_negative_empty() -> None:
    """FN-CHR-1: chr(300) is ',' and chr(-1) is ''. pins: fn-fix-2-string-rows/C-003"""
    repark = ReparkSession.builder.appName("fn-chr").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [(0,), (65,), (255,), (256,), (300,), (321,), (65601,), (-1,), (-256,), (None,)],
            ["n"],
        )
        chr_values = [row[0] for row in frame.select(F.chr(F.col("n"))).collect()]
        char_values = [row[0] for row in frame.select(F.char(F.col("n"))).collect()]
        expected = ["\x00", "A", "ÿ", "\x00", ",", "A", "A", "", "", None]
        assert chr_values == expected
        assert char_values == expected
        sql_row = repark.sql("SELECT chr(300) AS a, chr(65601) AS b, chr(-1) AS c").collect()[0]
        assert [sql_row["a"], sql_row["b"], sql_row["c"]] == [",", "A", ""]
    finally:
        repark.stop()


@pytest.mark.parametrize(
    ("number", "expected"),
    [
        (0, "\x00"),
        (256, "\x00"),
        (65536, "\x00"),
        (1114112, "\x00"),
        (-1, ""),
        (-256, ""),
        (None, None),
    ],
)
def test_fn_chr_zero_large_negative_null(number: int | None, expected: str | None) -> None:
    """FN-CHR-1: zero/huge wrap; negatives '', NULL NULL. pins: fn-fix-2-ctrl-1-controls/C-002"""
    repark = ReparkSession.builder.appName("fn-chr-ctrl").master("local[1]").getOrCreate()
    try:
        literal = F.lit(number).cast("bigint") if number is None else F.lit(number)
        values = [row[0] for row in repark.range(1).select(F.chr(literal)).collect()]
        assert values == [expected]
    finally:
        repark.stop()
