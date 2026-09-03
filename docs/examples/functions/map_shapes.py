"""Demonstrate the ``F.*`` map-construction names on a small local frame.

pins: ex-9-functions-maps-structs-json/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.map_from_arrays",
    "F.map_from_entries",
    "F.str_to_map",
    "F.struct",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Check the three ways a map column comes into being, NULL text included."""
    repark = ReparkSession.builder.appName("ex-map-shapes").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([("a:1,b:2",), (None,)], ["s"])
        rows = frame.select(
            F.map_from_arrays(F.array(F.lit("x"), F.lit("y")), F.array(F.lit(10), F.lit(20))).alias(
                "from_arrays"
            ),
            F.map_from_entries(
                F.array(
                    F.struct(F.lit("k1").alias("key"), F.lit(1).alias("value")),
                    F.struct(F.lit("k2").alias("key"), F.lit(2).alias("value")),
                )
            ).alias("from_entries"),
            F.str_to_map(F.col("s")).alias("from_text"),
            F.str_to_map(F.lit("a=1;b=2"), F.lit(";"), F.lit("=")).alias("from_text_delims"),
        ).collect()
        values = [row["from_arrays"] for row in rows]
        print(f"F.map_from_arrays: {values!r}")
        if values != [{"x": 10, "y": 20}, {"x": 10, "y": 20}]:
            raise SystemExit(f"F.map_from_arrays gave {values!r}")
        values = [row["from_entries"] for row in rows]
        print(f"F.map_from_entries: {values!r}")
        if values != [{"k1": 1, "k2": 2}, {"k1": 1, "k2": 2}]:
            raise SystemExit(f"F.map_from_entries gave {values!r}")
        values = [row["from_text"] for row in rows]
        print(f"F.str_to_map: {values!r}")
        if values != [{"a": "1", "b": "2"}, None]:
            raise SystemExit(f"F.str_to_map gave {values!r}; a NULL string answers NULL")
        values = [row["from_text_delims"] for row in rows]
        print(f"F.str_to_map custom delimiters: {values!r}")
        if values != [{"a": "1", "b": "2"}, {"a": "1", "b": "2"}]:
            raise SystemExit(f"F.str_to_map with delimiters gave {values!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
