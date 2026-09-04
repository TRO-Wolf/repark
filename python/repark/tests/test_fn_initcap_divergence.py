"""FN-INITCAP-1: initcap starts a word only after a space (registry §7)."""

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812


def test_fn_initcap_starts_word_only_after_space() -> None:
    """FN-INITCAP-1: 'a-b' is 'A-b' and 'foo.bar' is 'Foo.bar'. pins: fn-fix-2-string-rows/C-003"""
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
                ("  leading",),
                ("",),
                ("Ünï",),
                ("SPARK",),
                ("a  b",),
                ("a\nb",),
                (None,),
            ],
            ["s"],
        )
        values = [row[0] for row in frame.select(F.initcap(F.col("s"))).collect()]
        assert values == [
            "Hello World",
            "A-b",
            "Foo.bar",
            "1abc",
            "O'neil",
            "Ab_cd",
            "X\ty",
            "A-b C.d",
            "  Leading",
            "",
            "Ünï",
            "Spark",
            "A  B",
            "A\nb",
            None,
        ]
        sql_values = [row[0] for row in repark.sql("SELECT initcap('a-b') AS r").collect()]
        assert sql_values == ["A-b"]
    finally:
        repark.stop()
