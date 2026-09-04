"""Combine integers bitwise and convert types with ``cast`` / ``try_cast``.

pins: ex-17-column-a/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Column.bitwiseAND",
    "Column.bitwiseOR",
    "Column.bitwiseXOR",
    "Column.cast",
    "Column.try_cast",
]


def main() -> None:
    """Run the measured bitwise and type-conversion answers on local frames."""
    repark = ReparkSession.builder.appName("ex-col-bitwise-cast").master("local[1]").getOrCreate()
    try:
        marks = repark.createDataFrame([(5,), (6,), (12,)], ["m"])
        anded = marks.select(marks.m.bitwiseAND(3))
        if anded.columns != ["(m & 3)"]:
            raise SystemExit(f"Column.bitwiseAND columns {anded.columns!r} != ['(m & 3)']")
        and_rows = sorted(anded.collect(), key=tuple)
        if and_rows != [(0,), (1,), (2,)]:
            raise SystemExit(f"Column.bitwiseAND rows {and_rows!r} != [(0,), (1,), (2,)]")
        ored = marks.select(marks.m.bitwiseOR(1))
        if ored.columns != ["(m | 1)"]:
            raise SystemExit(f"Column.bitwiseOR columns {ored.columns!r} != ['(m | 1)']")
        or_rows = sorted(ored.collect(), key=tuple)
        if or_rows != [(5,), (7,), (13,)]:
            raise SystemExit(f"Column.bitwiseOR rows {or_rows!r} != [(5,), (7,), (13,)]")
        xored = marks.select(marks.m.bitwiseXOR(3))
        if xored.columns != ["(m ^ 3)"]:
            raise SystemExit(f"Column.bitwiseXOR columns {xored.columns!r} != ['(m ^ 3)']")
        xor_rows = sorted(xored.collect(), key=tuple)
        if xor_rows != [(5,), (6,), (15,)]:
            raise SystemExit(f"Column.bitwiseXOR rows {xor_rows!r} != [(5,), (6,), (15,)]")

        plain = repark.createDataFrame([("a", 1, 10.0), ("b", 2, None)], ["g", "k", "v"])
        as_double = plain.select(plain.v.cast("double"))
        if as_double.columns != ["v"]:
            raise SystemExit(f"Column.cast columns {as_double.columns!r} != ['v']")
        double_rows = set(as_double.collect())
        if double_rows != {(10.0,), (None,)}:
            raise SystemExit(f"Column.cast rows {double_rows!r} != {(10.0,), (None,)}")
        as_string = plain.select(plain.k.cast("string"))
        string_rows = set(as_string.collect())
        if string_rows != {("1",), ("2",)}:
            raise SystemExit(f"Column.cast rows {string_rows!r} != {('1',), ('2',)}")

        dirty = repark.createDataFrame([("7",), ("x",), ("42",)], ["s"])
        forgiven = dirty.select(dirty.s.try_cast("int"))
        if forgiven.columns != ["s"]:
            raise SystemExit(f"Column.try_cast columns {forgiven.columns!r} != ['s']")
        forgiven_rows = set(forgiven.collect())
        forgiven_expected = {(7,), (42,), (None,)}
        if forgiven_rows != forgiven_expected:
            raise SystemExit(f"Column.try_cast rows {forgiven_rows!r} != {forgiven_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
