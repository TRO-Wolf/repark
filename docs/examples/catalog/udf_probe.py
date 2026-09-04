"""Session UDF probes: ``functionExists`` against the registered temp function.

pins: ex-20-window-catalog/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Catalog.functionExists",
    "Catalog.function_exists",
]


def main() -> None:
    """Run the measured probe answers: a registered temp UDF exists, an unknown name does not."""
    repark = ReparkSession.builder.appName("ex-cat-udf").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        repark.udf.register("ex20_fn", lambda value: value)
        registered = catalog.functionExists("ex20_fn")
        registered_expected = True
        if registered != registered_expected:
            raise SystemExit(
                f"Catalog.functionExists registered {registered!r} != {registered_expected!r}"
            )

        snake_registered = catalog.function_exists("ex20_fn")
        if snake_registered != registered_expected:
            raise SystemExit(
                f"Catalog.function_exists registered {snake_registered!r}"
                f" != {registered_expected!r}"
            )

        unknown = catalog.functionExists("no_such_fn_ex20")
        unknown_expected = False
        if unknown != unknown_expected:
            raise SystemExit(f"Catalog.functionExists missing {unknown!r} != {unknown_expected!r}")

        snake_unknown = catalog.function_exists("no_such_fn_ex20")
        if snake_unknown != unknown_expected:
            raise SystemExit(
                f"Catalog.function_exists missing {snake_unknown!r} != {unknown_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
