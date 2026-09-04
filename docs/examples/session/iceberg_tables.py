"""Create, list, read, and refresh a memory-catalog Iceberg table.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.read_iceberg_table",
    "SparkSession.list_iceberg_table_names",
    "SparkSession.list_df_schema_table_names",
    "SparkSession.refresh_catalog_provider",
]


def main() -> None:
    """Run the measured Iceberg answers: live listing, table read, provider directory, refresh."""
    warehouse = Path.cwd() / "ex21_wh_ice"
    warehouse.mkdir(parents=True, exist_ok=True)
    repark = ReparkSession.builder.appName("ex21-ses-iceberg").master("local[1]").getOrCreate()
    try:
        repark.register_memory_catalog("ex21_cat", str(warehouse))
        repark.sql("CREATE NAMESPACE ex21_cat.ex21_db").collect()
        repark.sql(
            "CREATE TABLE ex21_cat.ex21_db.ex21_t AS SELECT 1 AS id UNION ALL SELECT 2 AS id"
        ).collect()

        names = repark.list_iceberg_table_names("ex21_cat", "ex21_db")
        names_expected = ["ex21_t"]
        if names != names_expected:
            raise SystemExit(f"list_iceberg_table_names {names!r} != {names_expected!r}")

        rows = sorted(
            tuple(row) for row in repark.read_iceberg_table("ex21_cat.ex21_db.ex21_t").collect()
        )
        rows_expected = [(1,), (2,)]
        if rows != rows_expected:
            raise SystemExit(f"read_iceberg_table rows {rows!r} != {rows_expected!r}")

        provider_names = repark.list_df_schema_table_names("ex21_cat", "ex21_db")
        if provider_names != names_expected:
            raise SystemExit(
                f"list_df_schema_table_names {provider_names!r} != {names_expected!r}"
            )

        refreshed = repark.refresh_catalog_provider("ex21_cat")
        refreshed_expected = None
        if refreshed != refreshed_expected:
            raise SystemExit(
                f"refresh_catalog_provider return {refreshed!r} != {refreshed_expected!r}"
            )

        re_read = sorted(
            tuple(row) for row in repark.sql("SELECT id FROM ex21_cat.ex21_db.ex21_t").collect()
        )
        if re_read != rows_expected:
            raise SystemExit(f"rows after refresh {re_read!r} != {rows_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
