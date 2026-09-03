"""Demonstrate the ``F.*`` digest and checksum names on a small local frame.

pins: ex-11-functions-hash-url-random/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.md5", "F.sha", "F.sha1", "F.crc32", "F.xxhash64", "F.col"]


def main() -> None:
    """Check the digests, the CRC32 checksum, and xxHash64 on strings and NULL."""
    repark = ReparkSession.builder.appName("ex-hashing").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([("hello",), ("hello world",), ("",), (None,)], ["s"])
        rows = frame.select(
            F.col("s"),
            F.md5(F.col("s")).alias("md5"),
            F.sha(F.col("s")).alias("sha"),
            F.sha1(F.col("s")).alias("sha1"),
            F.crc32(F.col("s")).alias("crc32"),
            F.xxhash64(F.col("s")).alias("xxhash64"),
        ).collect()
        checked = (
            (
                "md5",
                [
                    "5d41402abc4b2a76b9719d911017c592",
                    "5eb63bbbe01eeed093cb22bb8f5acdc3",
                    "d41d8cd98f00b204e9800998ecf8427e",
                    None,
                ],
            ),
            (
                "sha",
                [
                    "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                    "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                    "da39a3ee5e6b4b0d3255bfef95601890afd80709",
                    None,
                ],
            ),
            (
                "sha1",
                [
                    "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                    "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                    "da39a3ee5e6b4b0d3255bfef95601890afd80709",
                    None,
                ],
            ),
            ("crc32", [907060870, 222957957, 0, None]),
            ("xxhash64", [-4367754540140381902, 7620854247404556961, -7444071767201028348, 42]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
        if [row["sha"] for row in rows] != [row["sha1"] for row in rows]:
            raise SystemExit("F.sha is Spark's SHA-1 digest spelling and must equal F.sha1")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
