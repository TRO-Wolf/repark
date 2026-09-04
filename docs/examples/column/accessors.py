"""Read array elements by position and struct fields by name.

pins: ex-17-column-a/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession
from repark.spark.types import DoubleType, StringType, StructField, StructType

COVERS: list[str] = ["Column.getItem", "Column.getField"]


def main() -> None:
    """Run the measured array-element and struct-field answers on local frames."""
    repark = ReparkSession.builder.appName("ex-col-accessors").master("local[1]").getOrCreate()
    try:
        arrays = repark.createDataFrame([([1, 2, 3],), ([4, 5],)], ["arr"])
        seconds = arrays.select(arrays.arr.getItem(1))
        if seconds.columns != ["arr[1]"]:
            raise SystemExit(f"Column.getItem columns {seconds.columns!r} != ['arr[1]']")
        second_rows = set(seconds.collect())
        if second_rows != {(2,), (5,)}:
            raise SystemExit(f"Column.getItem rows {second_rows!r} != {(2,), (5,)}")

        schema = StructType(
            [
                StructField(
                    "r",
                    StructType(
                        [
                            StructField("a", StringType()),
                            StructField("b", DoubleType()),
                        ]
                    ),
                )
            ]
        )
        records = repark.createDataFrame([(("x", 2.0),), (("y", 3.0),)], schema)
        labels = records.select(records.r.getField("a").alias("a"))
        if labels.columns != ["a"]:
            raise SystemExit(f"Column.getField columns {labels.columns!r} != ['a']")
        label_rows = set(labels.collect())
        if label_rows != {("x",), ("y",)}:
            raise SystemExit(f"Column.getField rows {label_rows!r} != {('x',), ('y',)}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
