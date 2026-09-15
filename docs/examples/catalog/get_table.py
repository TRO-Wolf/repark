"""getTable / listColumns / listFunctions / getFunction over a memory-catalog table.

pins: catalog-surface-1/C-001, C-002, C-003
"""

from __future__ import annotations

from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Catalog.getTable",
    "Catalog.get_table",
    "Catalog.listColumns",
    "Catalog.list_columns",
    "Catalog.listFunctions",
    "Catalog.list_functions",
    "Catalog.getFunction",
    "Catalog.get_function",
]


def main() -> None:
    """Read table, column, and function metadata through both spellings."""
    warehouse = Path.cwd() / "ex_cat_surface_wh"
    warehouse.mkdir(parents=True, exist_ok=True)
    repark = ReparkSession.builder.appName("ex-cat-get-table").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        repark.register_memory_catalog("ex_cat", str(warehouse))
        repark.create_namespace("ex_cat", "ex_db")
        repark.sql("CREATE TABLE ex_cat.ex_db.t1 (a INT, p STRING)")
        catalog.setCurrentCatalog("ex_cat")
        catalog.setCurrentDatabase("ex_db")
        repark.createDataFrame([(1,)], ["v"]).createOrReplaceTempView("tv")

        row = catalog.getTable("t1")
        expected = ("t1", "ex_cat", ["ex_db"], None, "MANAGED", False)
        if tuple(row) != expected:
            raise SystemExit(f"Catalog.getTable {tuple(row)!r} != {expected!r}")
        snake_row = catalog.get_table("tv")
        if (snake_row.name, snake_row.tableType, snake_row.isTemporary) != (
            "tv",
            "TEMPORARY",
            True,
        ):
            raise SystemExit(f"Catalog.get_table temp row {tuple(snake_row)!r}")

        columns = catalog.listColumns("t1")
        got = [(column.name, column.dataType, column.isPartition) for column in columns]
        expected_columns = [("a", "int", False), ("p", "string", False)]
        if got != expected_columns:
            raise SystemExit(f"Catalog.listColumns {got!r} != {expected_columns!r}")
        snake_columns = catalog.list_columns("tv")
        if [column.name for column in snake_columns] != ["v"]:
            raise SystemExit(f"Catalog.list_columns view {[tuple(c) for c in snake_columns]!r}")

        functions = catalog.listFunctions()
        names = [function.name for function in functions]
        if names != sorted(names) or "abs" not in names:
            raise SystemExit(f"Catalog.listFunctions names {names[:8]!r} …")
        snake_functions = catalog.list_functions(pattern="to_*")
        if not snake_functions or not all(
            function.name.startswith("to_") for function in snake_functions
        ):
            raise SystemExit(
                f"Catalog.list_functions pattern {[f.name for f in snake_functions]!r}"
            )

        builtin = catalog.getFunction("abs")
        if (builtin.name, builtin.className, builtin.isTemporary) != (
            "abs",
            "repark.builtin",
            True,
        ):
            raise SystemExit(f"Catalog.getFunction {tuple(builtin)!r}")
        repark.udf.register("ex_udf", lambda value: value, "int")
        udf = catalog.get_function("ex_udf")
        if (udf.name, udf.className, udf.isTemporary) != ("ex_udf", "repark.python_udf", True):
            raise SystemExit(f"Catalog.get_function {tuple(udf)!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
