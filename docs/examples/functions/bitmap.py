"""Demonstrate the ``bitmap_*`` bit-position helpers and binary popcount."""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.bitmap_bit_position",
    "F.bitmap_bucket_number",
    "F.bitmap_count",
]


def main() -> None:
    """Run the measured bit-position, bucket, and popcount arms."""
    repark = ReparkSession.builder.appName("ex-bitmap").master("local[1]").getOrCreate()
    try:
        marks = repark.createDataFrame(
            [(-1,), (0,), (1,), (32767,), (32768,), (65536,), (None,)], "m BIGINT"
        )
        rows = marks.select(
            F.bitmap_bit_position("m").alias("bit"),
            F.bitmap_bucket_number("m").alias("bucket"),
        ).collect()
        checked = (
            ("bit", [1, 0, 0, 32766, 32767, 32767, None]),
            ("bucket", [0, 0, 1, 1, 1, 2, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.bitmap_{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.bitmap_{name} {values!r} != {expected!r}")

        blobs = repark.createDataFrame([(b"Spark",), (b"A",), (None,)], "b BINARY")
        counts = [
            row["count"] for row in blobs.select(F.bitmap_count("b").alias("count")).collect()
        ]
        print(f"F.bitmap_count: {counts!r}")
        if counts != [19, 2, None]:
            raise SystemExit(f"F.bitmap_count {counts!r} != [19, 2, None]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
