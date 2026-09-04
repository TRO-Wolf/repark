"""Namespace probes and temp-view drops: the exists and dropTempView answers.

pins: ex-20-window-catalog/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Catalog.databaseExists",
    "Catalog.database_exists",
    "Catalog.dropTempView",
    "Catalog.drop_temp_view",
]


def main() -> None:
    """Run the measured answers: default True, missing False, and drop True then False."""
    repark = ReparkSession.builder.appName("ex-cat-views-exists").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        default_exists = catalog.databaseExists("default")
        default_exists_expected = True
        if default_exists != default_exists_expected:
            raise SystemExit(
                f"Catalog.databaseExists default {default_exists!r} != {default_exists_expected!r}"
            )

        snake_default = catalog.database_exists("default")
        if snake_default != default_exists_expected:
            raise SystemExit(
                f"Catalog.database_exists default {snake_default!r} != {default_exists_expected!r}"
            )

        missing = catalog.databaseExists("nope_ex20")
        missing_expected = False
        if missing != missing_expected:
            raise SystemExit(f"Catalog.databaseExists missing {missing!r} != {missing_expected!r}")

        snake_missing = catalog.database_exists("nope_ex20")
        if snake_missing != missing_expected:
            raise SystemExit(
                f"Catalog.database_exists missing {snake_missing!r} != {missing_expected!r}"
            )

        repark.createDataFrame([(1, "x")], ["k", "s"]).createTempView("ex20_tv")
        dropped = catalog.dropTempView("ex20_tv")
        dropped_expected = True
        if dropped != dropped_expected:
            raise SystemExit(f"Catalog.dropTempView existing {dropped!r} != {dropped_expected!r}")

        snake_dropped = catalog.drop_temp_view("ex20_tv")
        snake_dropped_expected = False
        if snake_dropped != snake_dropped_expected:
            raise SystemExit(
                f"Catalog.drop_temp_view again {snake_dropped!r} != {snake_dropped_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
