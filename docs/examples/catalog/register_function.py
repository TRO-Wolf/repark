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
        snake_exists = catalog.functionExists("ex21_fn")
        snake_exists_expected = True
        if snake_exists != snake_exists_expected:
            raise SystemExit(
                f"register_function functionExists {snake_exists!r} != {snake_exists_expected!r}"
            )

        snake_rows = [tuple(row) for row in repark.sql("SELECT ex21_fn(4) AS out").collect()]
        snake_rows_expected = [("u4",)]
        if snake_rows != snake_rows_expected:
            raise SystemExit(
                f"register_function SQL rows {snake_rows!r} != {snake_rows_expected!r}"
            )

        catalog.registerFunction("ex21_fn_c", lambda value: f"w{value}")
        camel_exists = catalog.functionExists("ex21_fn_c")
        if camel_exists != snake_exists_expected:
            raise SystemExit(
                f"registerFunction functionExists {camel_exists!r} != {snake_exists_expected!r}"
            )

        camel_rows = [tuple(row) for row in repark.sql("SELECT ex21_fn_c(4) AS out").collect()]
        camel_rows_expected = [("w4",)]
        if camel_rows != camel_rows_expected:
            raise SystemExit(f"registerFunction SQL rows {camel_rows!r} != {camel_rows_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
