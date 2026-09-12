"""Demonstrate the Python UDF doors: ``F.udf``, ``F.pandas_udf``, and ``F.udtf``."""

from __future__ import annotations

from collections.abc import Iterator

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession
from repark.spark.types import LongType

COVERS: list[str] = [
    "F.udf",
    "F.pandas_udf",
    "F.udtf",
    "F.UserDefinedFunction",
    "F.UserDefinedTableFunction",
    "F.PandasUDFType",
    "F.lit",
]


def tag(value: object) -> str:
    """Tag one value with a ``u`` prefix."""
    return f"u{value}"


def mark(value: object) -> str:
    """Tag one value with a ``w`` prefix."""
    return f"w{value}"


@F.udtf(returnType="c1: int")
class Pair:
    """Emit the value and its successor as two rows."""

    def eval(self, value: int) -> Iterator[tuple[int]]:
        """Yield ``value`` then ``value + 1``."""
        yield (value,)
        yield (value + 1,)


def main() -> None:
    """Run the measured scalar, direct-construction, pandas, and table UDF arms."""
    repark = ReparkSession.builder.appName("ex-functions-udf").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1,), (2,), (3,)], "n INT")

        tagged = frame.select(F.udf(tag)("n").alias("u")).collect()
        tag_values = [row["u"] for row in tagged]
        print(f"F.udf: {tag_values!r}")
        if tag_values != ["u1", "u2", "u3"]:
            raise SystemExit(f"F.udf {tag_values!r} != ['u1', 'u2', 'u3']")

        direct = F.UserDefinedFunction(mark, "STRING")
        marked = frame.select(direct("n").alias("w")).collect()
        mark_values = [row["w"] for row in marked]
        print(f"F.UserDefinedFunction: {mark_values!r}")
        if mark_values != ["w1", "w2", "w3"]:
            raise SystemExit(f"F.UserDefinedFunction {mark_values!r} != ['w1', 'w2', 'w3']")

        doubled = F.pandas_udf(
            lambda series: series * 2,
            returnType=LongType(),
            functionType=F.PandasUDFType.SCALAR,
        )
        doubled_rows = frame.select(doubled("n").alias("d")).collect()
        doubled_values = [row["d"] for row in doubled_rows]
        print(f"F.pandas_udf: {doubled_values!r}")
        if doubled_values != [2, 4, 6]:
            raise SystemExit(f"F.pandas_udf {doubled_values!r} != [2, 4, 6]")

        if not isinstance(Pair, F.UserDefinedTableFunction):
            raise SystemExit(f"F.udtf decorated {type(Pair).__name__} != UserDefinedTableFunction")
        pair_rows = [tuple(row) for row in Pair(F.lit(5)).collect()]
        print(f"F.udtf: {pair_rows!r}")
        if pair_rows != [(5,), (6,)]:
            raise SystemExit(f"F.udtf {pair_rows!r} != [(5,), (6,)]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
