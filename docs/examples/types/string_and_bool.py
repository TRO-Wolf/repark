"""String-family types: collations, fixed and variable lengths, binary, boolean.

pins: ex-22-types-writerv2/C-001
"""

from __future__ import annotations

from repark.spark import types as T  # noqa: N812

COVERS: list[str] = [
    "types.StringType",
    "types.CharType",
    "types.VarcharType",
    "types.BinaryType",
    "types.BooleanType",
]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Run the measured construction and display answers for the string family."""
    string = T.StringType()
    expect("StringType.simpleString", string.simpleString(), "string")
    expect("StringType.collation", string.collation, "UTF8_BINARY")

    collated = T.StringType("UTF8_LCASE")
    expect("StringType collated.simpleString", collated.simpleString(), "string collate UTF8_LCASE")
    expect("StringType collated.repr", repr(collated), "StringType('UTF8_LCASE')")
    expect("StringType collated.isUTF8BinaryCollation", collated.isUTF8BinaryCollation(), False)

    expect("CharType.simpleString", T.CharType(5).simpleString(), "char(5)")
    expect("CharType.repr", repr(T.CharType(5)), "CharType(5)")
    expect("VarcharType.simpleString", T.VarcharType(10).simpleString(), "varchar(10)")
    expect("BinaryType.simpleString", T.BinaryType().simpleString(), "binary")
    expect("BooleanType.simpleString", T.BooleanType().simpleString(), "boolean")


if __name__ == "__main__":
    main()
