"""Demonstrate the boolean aggregates ``F.every``, ``F.some``, ``F.bool_and`` and ``F.bool_or``.

pins: ex-12-functions-aggregates-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.some", "F.every", "F.bool_and", "F.bool_or", "F.col"]


def main() -> None:
    """Collapse each group's booleans, with NULL groups answering NULL, not False."""
    repark = ReparkSession.builder.appName("ex-booleans").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", True),
                ("a", False),
                ("a", None),
                ("b", True),
                ("b", True),
                ("c", None),
                ("c", None),
                ("d", False),
                ("d", None),
            ],
            ["k", "b"],
        )
        aggregated = frame.groupBy("k").agg(
            F.bool_and(F.col("b")).alias("all_true"),
            F.bool_or("b").alias("any_true"),
            F.every("b").alias("every_value"),
            F.some("b").alias("some_value"),
        )
        rows = sorted(aggregated.collect(), key=lambda row: row["k"])
        all_true = [row["all_true"] for row in rows]
        if all_true != [False, True, None, False]:
            raise SystemExit(f"F.bool_and values {all_true!r} != [False, True, None, False]")
        any_true = [row["any_true"] for row in rows]
        if any_true != [True, True, None, False]:
            raise SystemExit(f"F.bool_or values {any_true!r} != [True, True, None, False]")
        every_value = [row["every_value"] for row in rows]
        if every_value != all_true:
            raise SystemExit(f"F.every values {every_value!r} != F.bool_and {all_true!r}")
        some_value = [row["some_value"] for row in rows]
        if some_value != any_true:
            raise SystemExit(f"F.some values {some_value!r} != F.bool_or {any_true!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
