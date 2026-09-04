"""List tables per database: the MANAGED Iceberg row and the TEMPORARY view row.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Catalog.list_tables",
]


def main() -> None:
    """Run the measured listing answers: managed row, temp row, bare arm, and pattern arm."""
    warehouse = Path.cwd() / "ex21_wh_list"
    warehouse.mkdir(parents=True, exist_ok=True)
    repark = ReparkSession.builder.appName("ex21-cat-list-snake").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        repark.register_memory_catalog("ex21_cat", str(warehouse))
        repark.sql("CREATE NAMESPACE ex21_cat.ex21_db").collect()
        repark.sql(
            "CREATE TABLE ex21_cat.ex21_db.ex21_t AS SELECT 1 AS id UNION ALL SELECT 2 AS id"
        ).collect()
        repark.createDataFrame([(1, "x")], ["k", "s"]).createOrReplaceTempView("ex21_tv")

        rows = [tuple(row) for row in catalog.list_tables("ex21_db")]
        rows_expected = [
            ("ex21_t", "ex21_cat", ["ex21_db"], None, "MANAGED", False),
            ("ex21_tv", None, [], None, "TEMPORARY", True),
        ]
        if rows != rows_expected:
            raise SystemExit(f"Catalog.list_tables rows {rows!r} != {rows_expected!r}")

        bare_rows = [tuple(row) for row in catalog.list_tables()]
        bare_rows_expected = [("ex21_tv", None, [], None, "TEMPORARY", True)]
        if bare_rows != bare_rows_expected:
            raise SystemExit(f"Catalog.list_tables bare {bare_rows!r} != {bare_rows_expected!r}")

        pattern_rows = [tuple(row) for row in catalog.list_tables("ex21_db", "ex21_t")]
        pattern_rows_expected = [("ex21_t", "ex21_cat", ["ex21_db"], None, "MANAGED", False)]
        if pattern_rows != pattern_rows_expected:
            raise SystemExit(
                f"Catalog.list_tables pattern {pattern_rows!r} != {pattern_rows_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
