"""Arrow mapping helpers: repark DataType to pyarrow types and pyarrow.Schema to StructType.

pins: ex-22-types-writerv2/C-001
"""

from __future__ import annotations

import pyarrow as pa

from repark.spark import types as T  # noqa: N812

COVERS: list[str] = [
    "types.repark_type_to_arrow",
    "types.struct_type_from_arrow",
]


def expect(label: str, got: object, wanted: object) -> None:
    """Raise SystemExit when the measured answer differs."""
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Run the measured repark-to-arrow answers and the pyarrow-schema round trip."""
    expect("repark_type_to_arrow(ByteType)", T.repark_type_to_arrow(T.ByteType()), pa.int8())
    expect(
        "repark_type_to_arrow(ArrayType)",
        T.repark_type_to_arrow(T.ArrayType(T.IntegerType())),
        pa.list_(pa.int32()),
    )
    expect(
        "repark_type_to_arrow(DecimalType)",
        T.repark_type_to_arrow(T.DecimalType(10, 2)),
        pa.decimal128(10, 2),
    )
    expect(
        "repark_type_to_arrow(TimestampType)",
        T.repark_type_to_arrow(T.TimestampType()),
        pa.timestamp("us", tz="UTC"),
    )
    expect(
        "repark_type_to_arrow(TimestampNTZType)",
        T.repark_type_to_arrow(T.TimestampNTZType()),
        pa.timestamp("us"),
    )
    expect(
        "repark_type_to_arrow(MapType)",
        T.repark_type_to_arrow(T.MapType(T.StringType(), T.IntegerType())),
        pa.map_(pa.string(), pa.int32()),
    )

    arrow_schema = pa.schema(
        [pa.field("k", pa.int32(), nullable=False), pa.field("v", pa.string())]
    )
    rebuilt = T.struct_type_from_arrow(arrow_schema)
    expect("struct_type_from_arrow.simpleString", rebuilt.simpleString(), "struct<k:int,v:string>")
    expect("struct_type_from_arrow k nullable", rebuilt["k"].nullable, False)
    expect(
        "arrow round trip",
        T.repark_type_to_arrow(rebuilt),
        pa.struct([("k", pa.int32()), ("v", pa.string())]),
    )


if __name__ == "__main__":
    main()
