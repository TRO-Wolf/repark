"""Demonstrate the ``F.*`` UTF-8 byte-view names on a small local frame.

pins: ex-5-functions-strings-b-regex/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.bit_length",
    "F.octet_length",
    "F.is_valid_utf8",
    "F.make_valid_utf8",
    "F.try_validate_utf8",
    "F.validate_utf8",
    "F.col",
]


def main() -> None:
    """Check the byte counts, the invalid-sequence trio, and validate_utf8's loud path."""
    repark = ReparkSession.builder.appName("ex-utf8").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                (b"abc",),
                (b"\xff",),
                ("Café".encode(),),
                (b"a\xffb",),
                (b"",),
                (None,),
            ],
            ["b"],
        )
        rows = frame.select(
            F.bit_length(F.col("b")).alias("bits"),
            F.octet_length(F.col("b")).alias("octets"),
            F.is_valid_utf8(F.col("b")).alias("valid"),
            F.make_valid_utf8(F.col("b")).alias("repaired"),
            F.try_validate_utf8(F.col("b")).alias("tolerant"),
        ).collect()
        checked = (
            ("bits", [24, 8, 40, 24, 0, None]),
            ("octets", [3, 1, 5, 3, 0, None]),
            ("valid", [True, False, True, False, True, None]),
            ("repaired", ["abc", "\ufffd", "Café", "a\ufffdb", "", None]),
            ("tolerant", ["abc", None, "Café", None, "", None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")

        valid = repark.createDataFrame(
            [(b"abc",), ("Café".encode(),), (b"",), (None,)],
            ["b"],
        )
        rows = valid.select(F.validate_utf8(F.col("b")).alias("checked")).collect()
        values = [row["checked"] for row in rows]
        print(f"F.validate_utf8: {values!r}")
        if values != ["abc", "Café", "", None]:
            raise SystemExit(f"F.validate_utf8 values {values!r} != ['abc', 'Café', '', None]")
        bad = repark.createDataFrame([(b"\xff",)], ["b"])
        try:
            bad.select(F.validate_utf8(F.col("b")).alias("checked")).collect()
        except Exception as error:
            print(f"F.validate_utf8 invalid raises: {error}")
            if "INVALID_UTF8_STRING" not in str(error):
                raise SystemExit(
                    f"F.validate_utf8 raised without INVALID_UTF8_STRING: {error}"
                ) from error
        else:
            raise SystemExit("F.validate_utf8(b'\\xff') did not raise")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
