"""Demonstrate ``F.inline`` and ``F.inline_outer``, one column per struct field.

pins: fnp-gen-1/C-002
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.inline", "F.inline_outer", "F.col"]


def main() -> None:
    """Fan an array of structs out to x/y columns; the outer spelling keeps NULL rows."""
    repark = ReparkSession.builder.appName("ex-inline").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [([(1, "p"), (2, "q")], "r1"), (None, "r2"), ([], "r3")],
            "a ARRAY<STRUCT<x: INT, y: STRING>>, tag STRING",
        )
        inlined = frame.select(F.col("tag"), F.inline(F.col("a"))).collect()
        values = [(row["tag"], row["x"], row["y"]) for row in inlined]
        print(f"F.inline: {values!r}")
        if values != [("r1", 1, "p"), ("r1", 2, "q")]:
            raise SystemExit(f"F.inline gave {values!r}, expected [('r1', 1, 'p'), ('r1', 2, 'q')]")
        outer = frame.select(F.col("tag"), F.inline_outer(F.col("a"))).collect()
        values = [(row["tag"], row["x"], row["y"]) for row in outer]
        print(f"F.inline_outer: {values!r}")
        expected = [
            ("r1", 1, "p"),
            ("r1", 2, "q"),
            ("r2", None, None),
            ("r3", None, None),
        ]
        if values != expected:
            raise SystemExit(f"F.inline_outer gave {values!r}, expected {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
