"""Demonstrate ``F.current_catalog``, ``F.current_database`` and ``F.current_schema``.

pins: ex-10-functions-null-cond-misc/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.current_catalog", "F.current_database", "F.current_schema"]


def main() -> None:
    """Check the three session answers on a small local frame."""
    repark = ReparkSession.builder.appName("ex-session-context").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1,), (2,)], ["x"])
        rows = frame.select(
            F.current_catalog().alias("catalog"),
            F.current_database().alias("database"),
            F.current_schema().alias("schema"),
        ).collect()
        catalogs = [row["catalog"] for row in rows]
        databases = [row["database"] for row in rows]
        schemas = [row["schema"] for row in rows]
        print(f"F.current_catalog: {catalogs!r}")
        if catalogs != ["spark_catalog", "spark_catalog"]:
            raise SystemExit(
                f"F.current_catalog gave {catalogs!r}, expected ['spark_catalog', 'spark_catalog']"
            )
        print(f"F.current_database: {databases!r}")
        if databases != ["default", "default"]:
            raise SystemExit(f"F.current_database gave {databases!r}, expected ['default'] * 2")
        print(f"F.current_schema: {schemas!r}")
        if schemas != databases:
            raise SystemExit(
                f"F.current_schema gave {schemas!r}, F.current_database gave {databases!r}; "
                "must agree"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
