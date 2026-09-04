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
        asc_rows = [tuple(row) for row in values.orderBy(values.x.asc()).collect()]
        asc_expected = [(None,), (1.0,), (2.0,)]
        if asc_rows != asc_expected:
            raise SystemExit(f"Column.asc rows {asc_rows!r} != {asc_expected!r}")
        first_rows = [tuple(row) for row in values.orderBy(values.x.asc_nulls_first()).collect()]
        first_expected = [(None,), (1.0,), (2.0,)]
        if first_rows != first_expected:
            raise SystemExit(f"Column.asc_nulls_first rows {first_rows!r} != {first_expected!r}")
        last_rows = [tuple(row) for row in values.orderBy(values.x.asc_nulls_last()).collect()]
        last_expected = [(1.0,), (2.0,), (None,)]
        if last_rows != last_expected:
            raise SystemExit(f"Column.asc_nulls_last rows {last_rows!r} != {last_expected!r}")
        desc_rows = [tuple(row) for row in values.orderBy(values.x.desc()).collect()]
        desc_expected = [(2.0,), (1.0,), (None,)]
        if desc_rows != desc_expected:
            raise SystemExit(f"Column.desc rows {desc_rows!r} != {desc_expected!r}")
        desc_first_rows = [
            tuple(row) for row in values.orderBy(values.x.desc_nulls_first()).collect()
        ]
        desc_first_expected = [(None,), (2.0,), (1.0,)]
        if desc_first_rows != desc_first_expected:
            raise SystemExit(
                f"Column.desc_nulls_first rows {desc_first_rows!r} != {desc_first_expected!r}"
            )
        desc_last_rows = [
            tuple(row) for row in values.orderBy(values.x.desc_nulls_last()).collect()
        ]
        desc_last_expected = [(2.0,), (1.0,), (None,)]
        if desc_last_rows != desc_last_expected:
            raise SystemExit(
                f"Column.desc_nulls_last rows {desc_last_rows!r} != {desc_last_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
