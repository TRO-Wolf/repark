"""Subquery surface: scalar and EXISTS columns, lateral joins, and table arguments.

pins: df-subquery-1/C-001, C-002, C-004, C-005
"""

from __future__ import annotations

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812

COVERS: list[str] = [
    "DataFrame.asTable",
    "DataFrame.exists",
    "DataFrame.lateralJoin",
    "DataFrame.scalar",
]


def main() -> None:
    """Run the measured subquery answers against the oracle fixture frames."""
    repark = ReparkSession.builder.appName("ex-df-subquery").master("local[1]").getOrCreate()
    try:
        emp = repark.createDataFrame(
            [(1, "a", 10), (2, "b", 20), (3, "a", 30), (4, None, None)],
            "id int, dept string, sal int",
        )
        dept = repark.createDataFrame([("a", 100), ("c", 300)], "dept string, budget int")
        emp.createOrReplaceTempView("emp")
        dept.createOrReplaceTempView("dept")

        scalar = sorted(
            tuple(row)
            for row in emp.select("id", dept.select(F.max("budget")).scalar().alias("mb")).collect()
        )
        scalar_expected = [(1, 300), (2, 300), (3, 300), (4, 300)]
        if scalar != scalar_expected:
            raise SystemExit(f"DataFrame.scalar rows {scalar!r} != {scalar_expected!r}")

        exists_rows = sorted(
            tuple(row) for row in emp.where(repark.sql("SELECT 1 FROM dept").exists()).collect()
        )
        if len(exists_rows) != 4:
            raise SystemExit(f"DataFrame.exists kept {exists_rows!r}, expected all four rows")

        qualified = sorted(
            tuple(row)
            for row in emp.alias("e")
            .lateralJoin(
                dept.alias("d").where(F.col("d.dept") == F.col("e.dept").outer()).select("budget")
            )
            .collect()
        )
        qualified_expected = [(1, "a", 10, 100), (3, "a", 30, 100)]
        if qualified != qualified_expected:
            raise SystemExit(f"DataFrame.lateralJoin rows {qualified!r} != {qualified_expected!r}")

        arg = emp.asTable().partitionBy("dept").orderBy("id")
        if type(arg).__name__ != "TableArg":
            raise SystemExit(f"DataFrame.asTable gave {type(arg).__name__!r}")
    finally:
        repark.stop()
