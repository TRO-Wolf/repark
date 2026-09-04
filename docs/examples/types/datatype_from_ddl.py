"""The DataType base: the typeName class method and DDL parsing through fromDDL.

pins: ex-22-types-writerv2/C-001
"""

from __future__ import annotations

from repark.spark import types as T  # noqa: N812

COVERS: list[str] = [
    "types.DataType",
]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Run the measured base-class and fromDDL answers."""
    expect("DataType.typeName class", T.DataType.typeName(), "data")
    expect(
        "fromDDL field list",
        T.DataType.fromDDL("k int, v string").simpleString(),
        "struct<k:int,v:string>",
    )
    expect(
        "fromDDL array",
        T.DataType.fromDDL("array<decimal(8,3)>").simpleString(),
        "array<decimal(8,3)>",
    )
    expect(
        "fromDDL struct",
        T.DataType.fromDDL("struct<a:int,b:string>").simpleString(),
        "struct<a:int,b:string>",
    )
    expect(
        "fromDDL repr",
        repr(T.DataType.fromDDL("k int, v string")),
        "StructType([StructField('k', IntegerType(), True), StructField('v', StringType(), True)])",
    )


if __name__ == "__main__":
    main()
