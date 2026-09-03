"""Demonstrate ``F.unbase64`` decoding a base64 string into bytes.

pins: ex-4-functions-strings-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.unbase64", "F.col"]


def main() -> None:
    """Check the decoded bytes on valid base64 input, NULL included."""
    repark = ReparkSession.builder.appName("ex-unbase64").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([("U3Bhcms=",), ("QXBhY2hl",), ("QQ==",), (None,)], ["b"])
        rows = frame.select(F.unbase64(F.col("b")).alias("unbase64")).collect()
        values = [row["unbase64"] for row in rows]
        print(f"F.unbase64: {values!r}")
        if values != [b"Spark", b"Apache", b"A", None]:
            raise SystemExit(f"F.unbase64 values {values!r} != [b'Spark', b'Apache', b'A', None]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
