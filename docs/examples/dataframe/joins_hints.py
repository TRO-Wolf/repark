"""Join two frames, apply an optimizer hint, and intersect row sets.

pins: ex-16-dataframe-b/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.join",
    "DataFrame.hint",
    "DataFrame.intersect",
]


def main() -> None:
    """Run the measured join arms, the hint no-op, and the deduplicating intersect."""
    repark = ReparkSession.builder.appName("ex-df-b-joins").master("local[1]").getOrCreate()
    try:
        left = repark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c")],
            ["k", "name"],
        )
        right = repark.createDataFrame(
            [(1, 10.0), (2, 20.0), (4, 40.0)],
            ["k", "v"],
        )
        inner = left.join(right, "k")
        assert inner.columns == ["k", "name", "v"]
        assert set(inner.collect()) == {(1, "a", 10.0), (2, "b", 20.0)}

        leftj = left.join(right, ["k"], "left")
        assert leftj.columns == ["k", "name", "v"]
        assert set(leftj.collect()) == {(1, "a", 10.0), (2, "b", 20.0), (3, "c", None)}

        anti = left.join(right, ["k"], "left_anti")
        assert anti.columns == ["k", "name"]
        assert set(anti.collect()) == {(3, "c")}

        semi = left.join(right, ["k"], "left_semi")
        assert semi.columns == ["k", "name"]
        assert set(semi.collect()) == {(1, "a"), (2, "b")}

        cond = left.join(right, left["k"] == right["k"])
        assert cond.columns == ["k", "name", "k", "v"]
        assert set(cond.collect()) == {(1, "a", 1, 10.0), (2, "b", 2, 20.0)}

        hinted = left.hint("broadcast")
        assert hinted.columns == ["k", "name"]
        assert set(hinted.collect()) == {(1, "a"), (2, "b"), (3, "c")}

        common = left.select("k").intersect(right.select("k"))
        assert common.columns == ["k"]
        assert set(common.collect()) == {(1,), (2,)}
        duped = repark.createDataFrame([(1,), (2,), (1,)], ["k"])
        other = repark.createDataFrame([(1,), (3,)], ["k"])
        assert set(duped.intersect(other).collect()) == {(1,)}
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
