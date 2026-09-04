"""Atomic numeric types: construction, ``typeName``, ``simpleString``, ``jsonValue``, repr.

pins: ex-22-types-writerv2/C-001
"""

from __future__ import annotations

from repark.spark import types as T  # noqa: N812

COVERS: list[str] = [
    "types.IntegerType",
    "types.LongType",
    "types.ShortType",
    "types.ByteType",
    "types.FloatType",
    "types.DoubleType",
]


def expect(label: str, got: object, wanted: object) -> None:
    """Raise SystemExit when the measured answer differs."""
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Run the measured construction and display answers for the six numeric types."""
    integer = T.IntegerType()
    expect("IntegerType.typeName", integer.typeName(), "integer")
    expect("IntegerType.typeName class", T.IntegerType.typeName(), "integer")
    expect("IntegerType.simpleString", integer.simpleString(), "int")
    expect("IntegerType.jsonValue", integer.jsonValue(), "integer")
    expect("IntegerType.json", integer.json(), '"integer"')
    expect("IntegerType.repr", repr(integer), "IntegerType()")

    expect("LongType.simpleString", T.LongType().simpleString(), "bigint")
    expect("LongType.typeName class", T.LongType.typeName(), "long")
    expect("ShortType.simpleString", T.ShortType().simpleString(), "smallint")
    expect("ByteType.simpleString", T.ByteType().simpleString(), "tinyint")
    expect("FloatType.simpleString", T.FloatType().simpleString(), "float")
    expect("DoubleType.simpleString", T.DoubleType().simpleString(), "double")


if __name__ == "__main__":
    main()
