"""Probe table existence: a temp view answers True, a missing name answers False.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Catalog.tableExists",
    "Catalog.table_exists",
]


def main() -> None:
    """Run the measured tableExists answers: temp view True, missing False, both spellings."""
    repark = ReparkSession.builder.appName("ex21-cat-exists").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        repark.createDataFrame([(1, "x")], ["k", "s"]).createOrReplaceTempView("ex21_tv")

        exists = catalog.tableExists("ex21_tv")
        exists_expected = True
        if exists != exists_expected:
            raise SystemExit(f"Catalog.tableExists temp view {exists!r} != {exists_expected!r}")

        snake_exists = catalog.table_exists("ex21_tv")
        if snake_exists != exists_expected:
            raise SystemExit(f"Catalog.table_exists temp view {snake_exists!r} != {exists_expected!r}")

        missing = catalog.tableExists("nope_ex21")
        missing_expected = False
        if missing != missing_expected:
            raise SystemExit(f"Catalog.tableExists missing {missing!r} != {missing_expected!r}")

        snake_missing = catalog.table_exists("nope_ex21")
        if snake_missing != missing_expected:
            raise SystemExit(
                f"Catalog.table_exists missing {snake_missing!r} != {missing_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
