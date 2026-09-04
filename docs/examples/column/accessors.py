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
        item_columns = ["arr[1]"]
        if seconds.columns != item_columns:
            raise SystemExit(f"Column.getItem columns {seconds.columns!r} != {item_columns!r}")
        item_rows = set(seconds.collect())
        item_expected = {(2,), (5,)}
        if item_rows != item_expected:
            raise SystemExit(f"Column.getItem rows {item_rows!r} != {item_expected!r}")

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
        field_columns = ["a"]
        if labels.columns != field_columns:
            raise SystemExit(f"Column.getField columns {labels.columns!r} != {field_columns!r}")
        label_rows = set(labels.collect())
        label_expected = {("x",), ("y",)}
        if label_rows != label_expected:
            raise SystemExit(f"Column.getField rows {label_rows!r} != {label_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
