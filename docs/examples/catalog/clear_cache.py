"""Cache teardown: ``clearCache`` drops the session caches and reads keep answering.

pins: ex-20-window-catalog/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Catalog.clearCache",
    "Catalog.clear_cache",
]

SIX_ROWS = [
    ("a", 1, 10.0),
    ("a", 2, 20.0),
    ("a", 2, 30.0),
    ("a", 3, 40.0),
    ("b", 1, 50.0),
    ("b", 2, None),
]


def main() -> None:
    """Run the measured clear answers: a None return and the cached frame's rows still correct."""
    repark = ReparkSession.builder.appName("ex-cat-clear").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        cached = repark.createDataFrame(SIX_ROWS, ["g", "k", "v"]).cache()
        cleared = catalog.clearCache()
        cleared_expected = None
        if cleared != cleared_expected:
            raise SystemExit(f"Catalog.clearCache return {cleared!r} != {cleared_expected!r}")

        rows = sorted(
            (tuple(row) for row in cached.filter("k = 1").collect()),
            key=lambda row: (row[0], row[1]),
        )
        rows_expected = [("a", 1, 10.0), ("b", 1, 50.0)]
        if rows != rows_expected:
            raise SystemExit(f"Catalog.clearCache rows {rows!r} != {rows_expected!r}")

        cached_snake = repark.createDataFrame(SIX_ROWS, ["g", "k", "v"]).cache()
        snake_cleared = catalog.clear_cache()
        if snake_cleared != cleared_expected:
            raise SystemExit(
                f"Catalog.clear_cache return {snake_cleared!r} != {cleared_expected!r}"
            )

        snake_rows = sorted(
            (tuple(row) for row in cached_snake.filter("k = 1").collect()),
            key=lambda row: (row[0], row[1]),
        )
        if snake_rows != rows_expected:
            raise SystemExit(f"Catalog.clear_cache rows {snake_rows!r} != {rows_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
