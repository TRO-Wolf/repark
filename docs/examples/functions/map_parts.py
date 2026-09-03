"""Demonstrate the ``F.*`` map-inspection names on a small local frame.

pins: ex-9-functions-maps-structs-json/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.map_keys",
    "F.map_values",
    "F.map_entries",
    "F.map_contains_key",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Take one map column apart every way the facade offers, NULL map included."""
    repark = ReparkSession.builder.appName("ex-map-parts").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [({"a": 1, "b": 2},), ({"a": 3},), ({},), (None,)], "m map<string,int>"
        )
        rows = frame.select(
            F.map_keys(F.col("m")).alias("keys"),
            F.map_values(F.col("m")).alias("vals"),
            F.map_entries(F.col("m")).alias("entries"),
            F.map_contains_key(F.col("m"), F.lit("a")).alias("has_a"),
            F.map_contains_key(F.col("m"), F.lit("c")).alias("has_c"),
        ).collect()
        values = [row["keys"] for row in rows]
        print(f"F.map_keys: {values!r}")
        if values != [["a", "b"], ["a"], [], None]:
            raise SystemExit(f"F.map_keys gave {values!r}; a NULL map answers NULL")
        values = [row["vals"] for row in rows]
        print(f"F.map_values: {values!r}")
        if values != [[1, 2], [3], [], None]:
            raise SystemExit(f"F.map_values gave {values!r}")
        pairs = []
        for entries in [row["entries"] for row in rows]:
            if entries is None:
                pairs.append(None)
            else:
                pairs.append([(entry["key"], entry["value"]) for entry in entries])
        print(f"F.map_entries: {pairs!r}")
        if pairs != [[("a", 1), ("b", 2)], [("a", 3)], [], None]:
            raise SystemExit(f"F.map_entries gave {pairs!r}")
        values = [row["has_a"] for row in rows]
        print(f"F.map_contains_key 'a': {values!r}")
        if values != [True, True, False, None]:
            raise SystemExit(f"F.map_contains_key gave {values!r}; a NULL map answers NULL")
        values = [row["has_c"] for row in rows]
        print(f"F.map_contains_key 'c': {values!r}")
        if values != [False, False, False, None]:
            raise SystemExit(f"F.map_contains_key gave {values!r}; an empty map answers False")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
