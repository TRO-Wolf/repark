"""Demonstrate ``F.struct`` and ``F.named_struct`` on a small local frame.

pins: ex-9-functions-maps-structs-json/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.struct", "F.named_struct", "F.col", "F.lit"]


def main() -> None:
    """Build structs from columns, by argument name and by literal name, NULL fields included."""
    repark = ReparkSession.builder.appName("ex-structs").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1, "x"), (2, None), (None, "y")], "a int, s string")
        rows = frame.select(
            F.struct(F.col("a"), F.col("s")).alias("st"),
            F.named_struct(F.lit("x"), F.col("a"), F.lit("y"), F.col("s")).alias("ns"),
        ).collect()
        values = [row["st"]["a"] for row in rows]
        print(f"F.struct field a: {values!r}")
        if values != [1, 2, None]:
            raise SystemExit(f"F.struct field a gave {values!r}")
        values = [row["st"]["s"] for row in rows]
        print(f"F.struct field s: {values!r}")
        if values != ["x", None, "y"]:
            raise SystemExit(f"F.struct field s gave {values!r}; NULL fields stay NULL")
        values = [row["ns"]["x"] for row in rows]
        print(f"F.named_struct field x: {values!r}")
        if values != [1, 2, None]:
            raise SystemExit(f"F.named_struct field x gave {values!r}")
        values = [row["ns"]["y"] for row in rows]
        print(f"F.named_struct field y: {values!r}")
        if values != ["x", None, "y"]:
            raise SystemExit(f"F.named_struct field y gave {values!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
