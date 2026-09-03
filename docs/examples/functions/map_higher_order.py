"""Demonstrate the ``F.*`` higher-order map names on a small local frame.

pins: ex-9-functions-maps-structs-json/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.transform_keys",
    "F.transform_values",
    "F.map_filter",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Rewrite keys and values with a ``(k, v)`` lambda and filter entries, NULL map included."""
    repark = ReparkSession.builder.appName("ex-map-higher-order").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [({"a": 1, "b": 2},), ({"a": 3},), ({},), (None,)], "m map<string,int>"
        )
        rows = frame.select(
            F.transform_keys(F.col("m"), lambda k, v: F.concat(k, F.lit("_x"))).alias("tk"),
            F.transform_values(F.col("m"), lambda k, v: v + F.lit(1)).alias("tv"),
            F.map_filter(F.col("m"), lambda k, v: v > F.lit(1)).alias("mf"),
        ).collect()
        values = [row["tk"] for row in rows]
        print(f"F.transform_keys: {values!r}")
        if values != [{"b_x": 2, "a_x": 1}, {"a_x": 3}, {}, None]:
            raise SystemExit(f"F.transform_keys gave {values!r}; a NULL map answers NULL")
        values = [row["tv"] for row in rows]
        print(f"F.transform_values: {values!r}")
        if values != [{"a": 2, "b": 3}, {"a": 4}, {}, None]:
            raise SystemExit(f"F.transform_values gave {values!r}")
        values = [row["mf"] for row in rows]
        print(f"F.map_filter: {values!r}")
        if values != [{"b": 2}, {"a": 3}, {}, None]:
            raise SystemExit(f"F.map_filter gave {values!r}; an empty answer is an empty map")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
