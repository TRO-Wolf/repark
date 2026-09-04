"""Ask whether two frames carry one logical identity.

pins: ex-18-dataframe-c/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["DataFrame.sameSemantics", "DataFrame.same_semantics"]


def main() -> None:
    """Run the measured sameSemantics answers: one object yes, distinct plans no."""
    repark = ReparkSession.builder.appName("ex-df-same-semantics").master("local[1]").getOrCreate()
    try:
        rows = [
            ("a", 1, 10.0),
            ("a", 2, 20.0),
            ("a", 2, 30.0),
            ("a", 3, 40.0),
            ("b", 1, 50.0),
            ("b", 2, None),
        ]
        frame = repark.createDataFrame(rows, ["g", "k", "v"])
        self_answer = frame.sameSemantics(frame)
        if self_answer is not True:
            raise SystemExit(f"DataFrame.sameSemantics self {self_answer!r} != True")
        snake_answer = frame.same_semantics(frame)
        if snake_answer is not True:
            raise SystemExit(f"DataFrame.same_semantics self {snake_answer!r} != True")
        twin = repark.createDataFrame(rows, ["g", "k", "v"])
        twin_answer = frame.sameSemantics(twin)
        if twin_answer is not False:
            raise SystemExit(f"DataFrame.sameSemantics twin {twin_answer!r} != False")
        filtered_answer = frame.sameSemantics(frame.filter(F.col("k") > 1))
        if filtered_answer is not False:
            raise SystemExit(f"DataFrame.sameSemantics filter {filtered_answer!r} != False")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
