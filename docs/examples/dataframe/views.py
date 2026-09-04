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
        if side.columns != ["g", "k", "v"]:
            raise SystemExit(f"DataFrame.alias columns {side.columns!r} != ['g', 'k', 'v']")
        alias_rows = set(side.filter(F.col("k") == 2).collect())
        alias_expected = {("a", 2, 20.0), ("a", 2, 30.0), ("b", 2, None)}
        if alias_rows != alias_expected:
            raise SystemExit(f"DataFrame.alias rows {alias_rows!r} != {alias_expected!r}")

        frame.createOrReplaceTempView("tv_ex15")
        replaced = sorted(repark.sql("SELECT k FROM tv_ex15").collect(), key=tuple)
        replaced_expected = [(1,), (1,), (2,), (2,), (2,), (3,)]
        if replaced != replaced_expected:
            raise SystemExit(
                f"DataFrame.createOrReplaceTempView rows {replaced!r} != {replaced_expected!r}"
            )
        repark.createDataFrame([(99,)], ["k"]).createOrReplaceTempView("tv_ex15")
        swapped = repark.sql("SELECT k FROM tv_ex15").collect()
        if swapped != [(99,)]:
            raise SystemExit(f"DataFrame.createOrReplaceTempView rows {swapped!r} != [(99,)]")

        repark.createDataFrame([(7,)], ["k"]).create_or_replace_temp_view("tv_snake_ex15")
        snake_swapped = repark.sql("SELECT k FROM tv_snake_ex15").collect()
        if snake_swapped != [(7,)]:
            raise SystemExit(
                f"DataFrame.create_or_replace_temp_view rows {snake_swapped!r} != [(7,)]"
            )

        repark.createDataFrame([(7,)], ["k"]).createTempView("tv_fresh_ex15")
        fresh = repark.sql("SELECT k FROM tv_fresh_ex15").collect()
        if fresh != [(7,)]:
            raise SystemExit(f"DataFrame.createTempView rows {fresh!r} != [(7,)]")
        repark.createDataFrame([(8,)], ["k"]).create_temp_view("tv_fresh_snake_ex15")
        fresh_snake = repark.sql("SELECT k FROM tv_fresh_snake_ex15").collect()
        if fresh_snake != [(8,)]:
            raise SystemExit(f"DataFrame.create_temp_view rows {fresh_snake!r} != [(8,)]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
