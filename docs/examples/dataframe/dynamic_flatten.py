"""Flatten nested structs and lists natively; a repark extension with no Spark analog."""

from __future__ import annotations

from repark.spark import ReparkSession
from repark.spark.types import ArrayType, LongType, StringType, StructField, StructType

COVERS: list[str] = [
    "DataFrame.dynamicFlatten",
    "DataFrame.dynamic_flatten",
]


def main() -> None:
    """Run the measured struct-expansion and list-explosion arms."""
    repark = ReparkSession.builder.appName("ex26-flatten").master("local[1]").getOrCreate()
    try:
        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField(
                    "payload",
                    StructType(
                        [
                            StructField("x", LongType(), True),
                            StructField("label", StringType(), True),
                        ]
                    ),
                    True,
                ),
                StructField("tags", ArrayType(LongType()), True),
            ]
        )
        nested = repark.createDataFrame(
            [
                {"id": 1, "payload": {"x": 10, "label": "p"}, "tags": [1, 2]},
                {"id": 2, "payload": {"x": 20, "label": "q"}, "tags": [3]},
            ],
            schema=schema,
        )
        flat = nested.dynamicFlatten()
        flat_columns = ["id", "payload_x", "payload_label", "tags"]
        if flat.columns != flat_columns:
            raise SystemExit(f"dynamicFlatten columns {flat.columns!r} != {flat_columns!r}")
        flat_dtypes = [
            ("id", "bigint"),
            ("payload_x", "bigint"),
            ("payload_label", "string"),
            ("tags", "bigint"),
        ]
        if flat.dtypes != flat_dtypes:
            raise SystemExit(f"dynamicFlatten dtypes {flat.dtypes!r} != {flat_dtypes!r}")
        flat_rows = sorted(tuple(row) for row in flat.collect())
        flat_expected = [(1, 10, "p", 1), (1, 10, "p", 2), (2, 20, "q", 3)]
        if flat_rows != flat_expected:
            raise SystemExit(f"dynamicFlatten rows {flat_rows!r} != {flat_expected!r}")
        snake = nested.dynamic_flatten()
        if snake.columns != flat_columns:
            raise SystemExit(f"dynamic_flatten columns {snake.columns!r} != {flat_columns!r}")
        snake_rows = sorted(tuple(row) for row in snake.collect())
        if snake_rows != flat_expected:
            raise SystemExit(f"dynamic_flatten rows {snake_rows!r} != {flat_expected!r}")
        kept = nested.dynamicFlatten(explode_lists=False)
        if kept.columns != flat_columns:
            raise SystemExit(f"kept columns {kept.columns!r} != {flat_columns!r}")
        kept_rows = sorted(tuple(row) for row in kept.collect())
        kept_expected = [(1, 10, "p", [1, 2]), (2, 20, "q", [3])]
        if kept_rows != kept_expected:
            raise SystemExit(f"kept rows {kept_rows!r} != {kept_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
