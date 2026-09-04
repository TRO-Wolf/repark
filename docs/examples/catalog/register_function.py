"""Register a scalar UDF through the catalog and answer with it in SQL.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Catalog.registerFunction",
    "Catalog.register_function",
]


def main() -> None:
    """Run the measured registration answers: registered names exist and answer inside SQL."""
    repark = ReparkSession.builder.appName("ex21-cat-register").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        catalog.register_function("ex21_fn", lambda value: f"u{value}")
        exists = catalog.functionExists("ex21_fn")
        exists_expected = True
        if exists != exists_expected:
            raise SystemExit(f"Catalog.functionExists {exists!r} != {exists_expected!r}")

        rows = [tuple(row) for row in repark.sql("SELECT ex21_fn(4) AS out").collect()]
        rows_expected = [("u4",)]
        if rows != rows_expected:
            raise SystemExit(f"register_function SQL rows {rows!r} != {rows_expected!r}")

        catalog.registerFunction("ex21_fn_c", lambda value: f"w{value}")
        snake_exists = catalog.functionExists("ex21_fn_c")
        if snake_exists != exists_expected:
            raise SystemExit(f"Catalog.functionExists camel {snake_exists!r} != {exists_expected!r}")

        camel_rows = [tuple(row) for row in repark.sql("SELECT ex21_fn_c(4) AS out").collect()]
        camel_rows_expected = [("w4",)]
        if camel_rows != camel_rows_expected:
            raise SystemExit(f"registerFunction SQL rows {camel_rows!r} != {camel_rows_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
