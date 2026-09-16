"""Demonstrate the to_char family: formatting, parsing and binary decoding."""

from __future__ import annotations

import datetime
from decimal import Decimal

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession
from repark.spark.column import Column
from repark.spark.dataframe import DataFrame

COVERS: list[str] = [
    "F.to_char",
    "F.to_varchar",
    "F.to_number",
    "F.to_binary",
    "F.lit",
]


def _refuses(frame: DataFrame, label: str, column: Column, condition: str) -> None:
    """Run one refusing call and check it raises the recorded Spark condition."""
    try:
        frame.select(column.alias("v")).collect()
    except Exception as error:
        print(f"{label} raises: {error}")
        if condition not in str(error):
            raise SystemExit(f"{label} raised without {condition}: {error}") from error
    else:
        raise SystemExit(f"{label} did not raise")


def main() -> None:
    """Check the to_char family answers PySpark 4.1.2 rows on a small frame."""
    builder = (
        ReparkSession.builder.appName("ex-to-char-family")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
    )
    repark = builder.getOrCreate()
    try:
        frame = repark.createDataFrame(
            [(Decimal("12345.6789"), 2.5, "100", datetime.datetime(2024, 1, 1, 10, 7, 30))],
            "dec DECIMAL(10,4), d DOUBLE, num STRING, ts TIMESTAMP",
        )
        row = frame.select(
            F.to_char("dec", F.lit("000000.0000")).alias("padded"),
            F.to_varchar("dec", F.lit("000000.0000")).alias("same"),
            F.to_char("d", F.lit("$9.99")).alias("money"),
            F.to_char("ts", F.lit("yyyy-MM-dd")).alias("day"),
            F.to_binary("num", F.lit("utf-8")).alias("raw"),
            F.to_binary(F.lit("616263"), F.lit("hex")).alias("hexed"),
            F.to_number(F.lit("$1,234.56"), F.lit("$9,999.99")).alias("parsed"),
        ).collect()[0]
        print(f"F.to_char: {row['padded']!r} {row['money']!r} {row['day']!r}")
        if row["padded"] != "012345.6789" or row["same"] != "012345.6789":
            raise SystemExit(f"to_char/to_varchar disagree: {row!r}")
        if row["money"] != "$2.50" or row["day"] != "2024-01-01":
            raise SystemExit(f"to_char arms disagree: {row!r}")
        if bytes(row["raw"]) != b"100" or bytes(row["hexed"]) != b"abc":
            raise SystemExit(f"to_binary disagrees: {row!r}")
        if row["parsed"] != Decimal("1234.56"):
            raise SystemExit(f"to_number disagrees: {row!r}")
        _refuses(
            frame,
            "F.to_number",
            F.to_number(F.lit("zz"), F.lit("999")),
            "[INVALID_FORMAT.MISMATCH_INPUT]",
        )
        _refuses(
            frame,
            "F.to_binary",
            F.to_binary(F.lit("zz"), F.lit("hex")),
            "[CONVERSION_INVALID_INPUT]",
        )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
