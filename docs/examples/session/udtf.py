"""Register a Python table function and read it through FROM with literal args."""

from __future__ import annotations

from collections.abc import Iterator

from repark.spark import ReparkSession
from repark.spark.functions import udtf as make_udtf

COVERS: list[str] = [
    "SparkSession.udtf",
]


@make_udtf(returnType="a INT, b INT")
class PlusOne:
    def eval(self, x: int) -> Iterator[tuple[int, int]]:
        yield (x, x + 1)


def main() -> None:
    """Run the measured table-function register and FROM-read arms."""
    repark = ReparkSession.builder.appName("ex26-udtf").master("local[1]").getOrCreate()
    try:
        repark.udtf.register("plus_ex26", PlusOne)
        rows = repark.sql("SELECT * FROM plus_ex26(1)")
        rows_dtypes = [("a", "int"), ("b", "int")]
        if rows.dtypes != rows_dtypes:
            raise SystemExit(f"udtf dtypes {rows.dtypes!r} != {rows_dtypes!r}")
        rows_values = [tuple(row) for row in rows.collect()]
        if rows_values != [(1, 2)]:
            raise SystemExit(f"udtf rows {rows_values!r} != [(1, 2)]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
