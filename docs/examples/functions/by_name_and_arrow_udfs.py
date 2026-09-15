"""Demonstrate by-name routine calls and the Arrow UDF and UDTF factories."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.call_function", "F.call_udf", "F.arrow_udf", "F.arrow_udtf"]


class CountUp:
    """Emit ``i`` from zero up to the requested count."""

    def eval(self, count: pa.Array) -> object:
        """Yield one Arrow table with the counted rows."""
        limit = count[0].as_py() if len(count) else 0
        yield pa.table({"i": pa.array(list(range(limit)), pa.int64())})


def plus_one(value: int | None) -> int | None:
    """Add one to a nullable integer."""
    return None if value is None else value + 1


def arrow_plus_one(values: pa.Array) -> pa.Array:
    """Add one to every value of an Arrow array, keeping nulls."""
    return pc.add(values, 1)


def main() -> None:
    """Match each factory and by-name call to the equivalent direct spelling."""
    repark = ReparkSession.builder.appName("ex-by-name-arrow").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1, "a"), (-2, "b"), (None, None)], "x int, s string")
        repark.udf.register("plus_one_ex", plus_one, "int")
        by_name = frame.select(
            F.call_function("abs", F.col("x")).alias("a"),
            F.call_function("upper", F.col("s")).alias("u"),
            F.call_udf("plus_one_ex", F.col("x")).alias("p"),
        ).collect()
        direct = frame.select(
            F.abs(F.col("x")).alias("a"),
            F.upper(F.col("s")).alias("u"),
            (F.col("x") + 1).alias("p"),
        ).collect()
        if [row.asDict() for row in by_name] != [row.asDict() for row in direct]:
            raise SystemExit(f"by-name rows {by_name!r} != direct rows {direct!r}")
        adder = F.arrow_udf(arrow_plus_one, "int")
        arrow_values = [row["y"] for row in frame.select(adder(F.col("x")).alias("y")).collect()]
        if arrow_values != [2, -1, None]:
            raise SystemExit(f"arrow_udf values {arrow_values!r} != [2, -1, None]")
        counter = F.arrow_udtf(CountUp, returnType="i long")
        counted = [row["i"] for row in counter(F.lit(3)).collect()]
        if counted != [0, 1, 2]:
            raise SystemExit(f"arrow_udtf rows {counted!r} != [0, 1, 2]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
