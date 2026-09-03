"""FN-INITCAP-1 today: initcap starts a word at any non-alphanumeric (registry §7).

pins: ex-4-functions-strings-a/C-001
"""

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def test_fn_initcap_starts_word_at_any_non_alnum_today() -> None:
    """Today: 'a-b' is 'A-B' and 'foo.bar' is 'Foo.Bar'; Spark 4.1.2 is 'A-b' / 'Foo.bar'."""
    repark = ReparkSession.builder.appName("fn-initcap").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("hello world",),
                ("a-b",),
                ("foo.bar",),
                ("1abc",),
                ("o'neil",),
                ("ab_cd",),
                ("x\ty",),
                ("a-b c.d",),
            ],
            ["s"],
        )
        values = [row[0] for row in frame.select(F.initcap(F.col("s"))).collect()]
        assert values == [
            "Hello World",
            "A-B",
            "Foo.Bar",
            "1abc",
            "O'Neil",
            "Ab_Cd",
            "X\tY",
            "A-B C.D",
        ]
    finally:
        repark.stop()
