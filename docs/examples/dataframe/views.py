"""Register frames under SQL names: self-aliasing and the two temp-view spellings.

pins: ex-15-dataframe-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.alias",
    "DataFrame.createOrReplaceTempView",
    "DataFrame.create_or_replace_temp_view",
    "DataFrame.createTempView",
    "DataFrame.create_temp_view",
]


def main() -> None:
    """Run the measured view answers: alias reads, replace, and fresh-name creation."""
    repark = ReparkSession.builder.appName("ex-df-views").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 1, 10.0),
                ("a", 2, 20.0),
                ("a", 2, 30.0),
                ("a", 3, 40.0),
                ("b", 1, 50.0),
                ("b", 2, None),
            ],
            ["g", "k", "v"],
        )
        side = frame.alias("side")
        assert side.columns == ["g", "k", "v"]
        assert set(side.filter(F.col("k") == 2).collect()) == {
            ("a", 2, 20.0),
            ("a", 2, 30.0),
            ("b", 2, None),
        }

        frame.createOrReplaceTempView("tv_ex15")
        assert sorted(repark.sql("SELECT k FROM tv_ex15").collect(), key=tuple) == [
            (1,),
            (1,),
            (2,),
            (2,),
            (2,),
            (3,),
        ]
        repark.createDataFrame([(99,)], ["k"]).createOrReplaceTempView("tv_ex15")
        assert repark.sql("SELECT k FROM tv_ex15").collect() == [(99,)]

        repark.createDataFrame([(7,)], ["k"]).create_or_replace_temp_view("tv_snake_ex15")
        assert repark.sql("SELECT k FROM tv_snake_ex15").collect() == [(7,)]

        repark.createDataFrame([(7,)], ["k"]).createTempView("tv_fresh_ex15")
        assert repark.sql("SELECT k FROM tv_fresh_ex15").collect() == [(7,)]
        repark.createDataFrame([(8,)], ["k"]).create_temp_view("tv_fresh_snake_ex15")
        assert repark.sql("SELECT k FROM tv_fresh_snake_ex15").collect() == [(8,)]
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
