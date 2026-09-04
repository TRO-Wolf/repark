"""Build frames from row lists and from integer ranges.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.create_dataframe",
    "SparkSession.range",
]


def main() -> None:
    """Run the measured frame-builder answers: row-list frames and the exclusive range."""
    repark = ReparkSession.builder.appName("ex21-ses-frames").master("local[1]").getOrCreate()
    try:
        rows = [tuple(row) for row in repark.create_dataframe([(3, "c"), (4, "d")], ["id", "s"]).collect()]
        rows_expected = [(3, "c"), (4, "d")]
        if rows != rows_expected:
            raise SystemExit(f"create_dataframe rows {rows!r} != {rows_expected!r}")

        ids = [row["id"] for row in repark.range(1, 4).collect()]
        ids_expected = [1, 2, 3]
        if ids != ids_expected:
            raise SystemExit(f"range(1, 4) ids {ids!r} != {ids_expected!r}")

        stepped = [row["id"] for row in repark.range(0, 6, 2).collect()]
        stepped_expected = [0, 2, 4]
        if stepped != stepped_expected:
            raise SystemExit(f"range(0, 6, 2) ids {stepped!r} != {stepped_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
