"""Demonstrate the ``F.*`` random generators, asserted on shape and range only.

pins: ex-11-functions-hash-url-random/C-001
"""

from __future__ import annotations

import math
import re

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.uuid", "F.rand", "F.randn", "F.random"]

UUID4_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")


def main() -> None:
    """Check the uuid shape and the random ranges; no random value is ever asserted."""
    repark = ReparkSession.builder.appName("ex-random").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(index,) for index in range(32)], ["i"])
        uuids = [row["u"] for row in frame.select(F.uuid().alias("u")).collect()]
        print(f"F.uuid: {len(uuids)} values, first {uuids[0]!r}")
        for value in uuids:
            if not UUID4_PATTERN.fullmatch(value):
                raise SystemExit(f"F.uuid gave {value!r}, not a UUID4 shape")

        seeded = frame.select(F.rand().alias("rand"), F.rand(42).alias("rand_seeded"))
        randoms = frame.select(F.random().alias("random"))
        for name in ("rand", "rand_seeded", "random"):
            source = seeded if name != "random" else randoms
            values = [row[name] for row in source.collect()]
            print(f"F.{name}: {len(values)} values in [{min(values)}, {max(values)})")
            for value in values:
                if not isinstance(value, float) or not 0.0 <= value < 1.0:
                    raise SystemExit(f"F.{name} gave {value!r}, outside [0, 1)")

        gaussians = [row["n"] for row in frame.select(F.randn().alias("n")).collect()]
        low, high = min(gaussians), max(gaussians)
        print(f"F.randn: {len(gaussians)} finite floats between {low} and {high}")
        for value in gaussians:
            if not isinstance(value, float) or not math.isfinite(value):
                raise SystemExit(f"F.randn gave {value!r}, not a finite float")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
