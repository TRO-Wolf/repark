"""Register a scalar Python UDF and answer with it in SQL and on a frame."""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.udf",
]


def tag_with_u(value: object) -> str:
    return f"u{value}"


def main() -> None:
    repark = ReparkSession.builder.appName("ex26-udf").master("local[1]").getOrCreate()
    try:
        registered = repark.udf.register("fn_ex26", tag_with_u)
        sql_rows = [tuple(row) for row in repark.sql("SELECT fn_ex26(4)").collect()]
        if sql_rows != [("u4",)]:
            raise SystemExit(f"udf sql rows {sql_rows!r} != [('u4',)]")
        seed = repark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
        picked = seed.select(registered("id").alias("u"))
        picked_rows = [tuple(row) for row in picked.collect()]
        picked_expected = [("u1",), ("u2",)]
        if picked_rows != picked_expected:
            raise SystemExit(f"udf frame rows {picked_rows!r} != {picked_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
