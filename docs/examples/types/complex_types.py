"""Complex types: arrays, maps, struct fields, schema building, and field access.

pins: ex-22-types-writerv2/C-001
"""

from __future__ import annotations

import decimal

from repark.spark import ReparkSession
from repark.spark import types as T  # noqa: N812

COVERS: list[str] = [
    "types.ArrayType",
    "types.MapType",
    "types.StructField",
    "types.StructType",
]

SCHEMA = T.StructType(
    [
        T.StructField("k", T.IntegerType(), False),
        T.StructField("v", T.DecimalType(10, 2)),
        T.StructField("w", T.ArrayType(T.StringType())),
    ]
)

ROWS = [(1, decimal.Decimal("1.50"), ["a"]), (2, decimal.Decimal("2.25"), ["b", "c"])]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Run the measured array/map/struct answers and the explicit-schema DataFrame."""
    expect("ArrayType.simpleString", T.ArrayType(T.IntegerType()).simpleString(), "array<int>")
    expect(
        "ArrayType.repr",
        repr(T.ArrayType(T.IntegerType(), False)),
        "ArrayType(IntegerType(), False)",
    )
    expect(
        "ArrayType.jsonValue",
        T.ArrayType(T.IntegerType()).jsonValue(),
        {"type": "array", "elementType": "integer", "containsNull": True},
    )
    nested = T.ArrayType(T.StructType([T.StructField("a", T.IntegerType())]))
    expect("nested ArrayType.simpleString", nested.simpleString(), "array<struct<a:int>>")
    mapping = T.MapType(T.StringType(), T.IntegerType())
    expect("MapType.simpleString", mapping.simpleString(), "map<string,int>")
    expect("MapType.repr", repr(mapping), "MapType(StringType(), IntegerType(), True)")

    field = T.StructField("k", T.IntegerType(), False)
    expect("StructField.repr", repr(field), "StructField('k', IntegerType(), False)")
    expect("StructField.simpleString", field.simpleString(), "k:int")
    expect(
        "StructField.jsonValue",
        field.jsonValue(),
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
    )

    schema_string = "struct<k:int,v:decimal(10,2),w:array<string>>"
    expect("schema.simpleString", SCHEMA.simpleString(), schema_string)
    expect(
        "schema.repr",
        repr(SCHEMA),
        "StructType([StructField('k', IntegerType(), False),"
        " StructField('v', DecimalType(10,2), True),"
        " StructField('w', ArrayType(StringType(), True), True)])",
    )
    expect("schema.fieldNames", SCHEMA.fieldNames(), ["k", "v", "w"])
    expect("schema['v'].repr", repr(SCHEMA["v"]), "StructField('v', DecimalType(10,2), True)")
    expect("schema['v'].simpleString", SCHEMA["v"].simpleString(), "v:decimal(10,2)")
    expect("schema['v'].nullable", SCHEMA["v"].nullable, True)
    expect("schema[0].name", SCHEMA[0].name, "k")
    expect("len(schema)", len(SCHEMA), 3)
    expect("schema.toDDL", SCHEMA.toDDL(), "k INT NOT NULL,v DECIMAL(10,2),w ARRAY<STRING>")
    expect(
        "schema.treeString",
        SCHEMA.treeString(),
        "root\n"
        " |-- k: integer (nullable = false)\n"
        " |-- v: decimal(10,2) (nullable = true)\n"
        " |-- w: array (nullable = true)\n"
        " |    |-- element: string (containsNull = true)\n",
    )
    built = T.StructType().add("k", T.IntegerType(), False).add("v", T.StringType())
    expect("add chain.simpleString", built.simpleString(), "struct<k:int,v:string>")
    expect("add chain.toDDL", built.toDDL(), "k INT NOT NULL,v STRING")
    clone = T.StructType(
        [
            T.StructField("k", T.IntegerType(), False),
            T.StructField("v", T.DecimalType(10, 2)),
            T.StructField("w", T.ArrayType(T.StringType())),
        ]
    )
    expect("schema equality", clone == SCHEMA, True)

    repark = ReparkSession.builder.appName("ex-types-schema").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(ROWS, SCHEMA)
        expect("df.schema.simpleString", frame.schema.simpleString(), schema_string)
        expect("df.schema.fieldNames", frame.schema.fieldNames(), ["k", "v", "w"])
        expect("df rows", sorted(tuple(row) for row in frame.collect()), ROWS)
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
