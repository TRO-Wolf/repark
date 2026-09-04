"""Order rows with the six sort markers, nulls placed explicitly.

pins: ex-17-column-a/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Column.asc",
    "Column.asc_nulls_first",
    "Column.asc_nulls_last",
    "Column.desc",
    "Column.desc_nulls_first",
    "Column.desc_nulls_last",
]


def main() -> None:
    """Run the measured six-marker ordering answers on one null-carrying frame."""
    repark = ReparkSession.builder.appName("ex-col-order").master("local[1]").getOrCreate()
    try:
        values = repark.createDataFrame([(2.0,), (None,), (1.0,)], ["x"])
        asc_rows = values.orderBy(values.x.asc()).collect()
        if [tuple(row) for row in asc_rows] != [(None,), (1.0,), (2.0,)]:
            raise SystemExit(f"Column.asc rows {asc_rows!r} != [(None,), (1.0,), (2.0,)]")
        first_rows = values.orderBy(values.x.asc_nulls_first()).collect()
        if [tuple(row) for row in first_rows] != [(None,), (1.0,), (2.0,)]:
            raise SystemExit(
                f"Column.asc_nulls_first rows {first_rows!r} != [(None,), (1.0,), (2.0,)]"
            )
        last_rows = values.orderBy(values.x.asc_nulls_last()).collect()
        if [tuple(row) for row in last_rows] != [(1.0,), (2.0,), (None,)]:
            raise SystemExit(
                f"Column.asc_nulls_last rows {last_rows!r} != [(1.0,), (2.0,), (None,)]"
            )
        desc_rows = values.orderBy(values.x.desc()).collect()
        if [tuple(row) for row in desc_rows] != [(2.0,), (1.0,), (None,)]:
            raise SystemExit(f"Column.desc rows {desc_rows!r} != [(2.0,), (1.0,), (None,)]")
        desc_first_rows = values.orderBy(values.x.desc_nulls_first()).collect()
        if [tuple(row) for row in desc_first_rows] != [(None,), (2.0,), (1.0,)]:
            raise SystemExit(
                f"Column.desc_nulls_first rows {desc_first_rows!r} != [(None,), (2.0,), (1.0,)]"
            )
        desc_last_rows = values.orderBy(values.x.desc_nulls_last()).collect()
        if [tuple(row) for row in desc_last_rows] != [(2.0,), (1.0,), (None,)]:
            raise SystemExit(
                f"Column.desc_nulls_last rows {desc_last_rows!r} != [(2.0,), (1.0,), (None,)]"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
