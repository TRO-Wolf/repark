"""Decimal, null, and variant types: parameterized precision and the void/variant markers.

pins: ex-22-types-writerv2/C-001
"""

from __future__ import annotations

from repark.spark import types as T  # noqa: N812

COVERS: list[str] = [
    "types.DecimalType",
    "types.NullType",
    "types.VariantType",
]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Run the measured construction and display answers for decimal, void, and variant."""
    expect("DecimalType.simpleString", T.DecimalType().simpleString(), "decimal(10,0)")
    expect("DecimalType(10,4).simpleString", T.DecimalType(10, 4).simpleString(), "decimal(10,4)")
    expect("DecimalType(10,4).repr", repr(T.DecimalType(10, 4)), "DecimalType(10,4)")
    expect("DecimalType(10,2).json", T.DecimalType(10, 2).json(), '"decimal(10,2)"')
    expect("DecimalType(10,4).precision", T.DecimalType(10, 4).precision, 10)
    expect("DecimalType(10,4).scale", T.DecimalType(10, 4).scale, 4)
    expect("DecimalType(39,0).simpleString", T.DecimalType(39, 0).simpleString(), "decimal(39,0)")
    expect("DecimalType(5,7).simpleString", T.DecimalType(5, 7).simpleString(), "decimal(5,7)")

    expect("NullType.simpleString", T.NullType().simpleString(), "void")
    expect("NullType.typeName class", T.NullType.typeName(), "void")

    expect("VariantType.simpleString", T.VariantType().simpleString(), "variant")
    expect("VariantType.typeName class", T.VariantType.typeName(), "variant")


if __name__ == "__main__":
    main()
