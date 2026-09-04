"""Test ranges, null-safe equality, and null presence on a column.

pins: ex-17-column-a/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Column.between",
    "Column.eqNullSafe",
    "Column.isNull",
    "Column.is_null",
    "Column.isNotNull",
    "Column.is_not_null",
]


def main() -> None:
    """Run the measured range, null-safe equality, and null-check answers."""
    repark = ReparkSession.builder.appName("ex-col-predicates").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 1, 10.0),
                ("a", 2, 20.0),
                ("a", 2, 30.0),
                ("a", 3, 40.0),
                ("b", 1, 50.0),
                ("b", 2, None),
            ],
            ["g", "k", "v"],
        )
        band = frame.filter(frame.k.between(2, 3))
        band_rows = set(band.collect())
        band_expected = {("a", 2, 20.0), ("a", 2, 30.0), ("a", 3, 40.0), ("b", 2, None)}
        if band_rows != band_expected:
            raise SystemExit(f"Column.between rows {band_rows!r} != {band_expected!r}")

        nulls = repark.createDataFrame([(None, 20), (20, 20), (30, None), (None, None)], ["n", "m"])
        safe = nulls.select(nulls.n.eqNullSafe(nulls.m))
        if safe.columns != ["(n <=> m)"]:
            raise SystemExit(f"Column.eqNullSafe columns {safe.columns!r} != ['(n <=> m)']")
        safe_rows = sorted(safe.collect(), key=tuple)
        safe_expected = [(False,), (False,), (True,), (True,)]
        if safe_rows != safe_expected:
            raise SystemExit(f"Column.eqNullSafe rows {safe_rows!r} != {safe_expected!r}")

        missing_rows = set(nulls.filter(nulls.n.isNull()).collect())
        missing_expected = {(None, 20), (None, None)}
        if missing_rows != missing_expected:
            raise SystemExit(f"Column.isNull rows {missing_rows!r} != {missing_expected!r}")
        missing_snake = set(nulls.filter(nulls.n.is_null()).collect())
        if missing_snake != missing_expected:
            raise SystemExit(f"Column.is_null rows {missing_snake!r} != {missing_expected!r}")
        present_rows = set(nulls.filter(nulls.n.isNotNull()).collect())
        present_expected = {(20, 20), (30, None)}
        if present_rows != present_expected:
            raise SystemExit(f"Column.isNotNull rows {present_rows!r} != {present_expected!r}")
        present_snake = set(nulls.filter(nulls.n.is_not_null()).collect())
        if present_snake != present_expected:
            raise SystemExit(f"Column.is_not_null rows {present_snake!r} != {present_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
