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
        and_columns = ["(m & 3)"]
        if anded.columns != and_columns:
            raise SystemExit(f"Column.bitwiseAND columns {anded.columns!r} != {and_columns!r}")
        and_rows = sorted(anded.collect(), key=tuple)
        and_expected = [(0,), (1,), (2,)]
        if and_rows != and_expected:
            raise SystemExit(f"Column.bitwiseAND rows {and_rows!r} != {and_expected!r}")
        ored = marks.select(marks.m.bitwiseOR(1))
        or_columns = ["(m | 1)"]
        if ored.columns != or_columns:
            raise SystemExit(f"Column.bitwiseOR columns {ored.columns!r} != {or_columns!r}")
        or_rows = sorted(ored.collect(), key=tuple)
        or_expected = [(5,), (7,), (13,)]
        if or_rows != or_expected:
            raise SystemExit(f"Column.bitwiseOR rows {or_rows!r} != {or_expected!r}")
        xored = marks.select(marks.m.bitwiseXOR(3))
        xor_columns = ["(m ^ 3)"]
        if xored.columns != xor_columns:
            raise SystemExit(f"Column.bitwiseXOR columns {xored.columns!r} != {xor_columns!r}")
        xor_rows = sorted(xored.collect(), key=tuple)
        xor_expected = [(5,), (6,), (15,)]
        if xor_rows != xor_expected:
            raise SystemExit(f"Column.bitwiseXOR rows {xor_rows!r} != {xor_expected!r}")

        plain = repark.createDataFrame([("a", 1, 10.0), ("b", 2, None)], ["g", "k", "v"])
        as_double = plain.select(plain.v.cast("double"))
        cast_columns = ["v"]
        if as_double.columns != cast_columns:
            raise SystemExit(f"Column.cast columns {as_double.columns!r} != {cast_columns!r}")
        double_rows = set(as_double.collect())
        double_expected = {(10.0,), (None,)}
        if double_rows != double_expected:
            raise SystemExit(f"Column.cast rows {double_rows!r} != {double_expected!r}")
        as_string = plain.select(plain.k.cast("string"))
        string_rows = set(as_string.collect())
        string_expected = {("1",), ("2",)}
        if string_rows != string_expected:
            raise SystemExit(f"Column.cast rows {string_rows!r} != {string_expected!r}")

        dirty = repark.createDataFrame([("7",), ("x",), ("42",)], ["s"])
        forgiven = dirty.select(dirty.s.try_cast("int"))
        try_columns = ["s"]
        if forgiven.columns != try_columns:
            raise SystemExit(f"Column.try_cast columns {forgiven.columns!r} != {try_columns!r}")
        forgiven_rows = set(forgiven.collect())
        forgiven_expected = {(7,), (42,), (None,)}
        if forgiven_rows != forgiven_expected:
            raise SystemExit(f"Column.try_cast rows {forgiven_rows!r} != {forgiven_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
