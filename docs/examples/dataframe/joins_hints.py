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
        inner_names = inner.columns
        if inner_names != ["k", "name", "v"]:
            raise SystemExit(f"DataFrame.join columns {inner_names!r} != ['k', 'name', 'v']")
        inner_rows = set(inner.collect())
        inner_expected = {(1, "a", 10.0), (2, "b", 20.0)}
        if inner_rows != inner_expected:
            raise SystemExit(f"DataFrame.join rows {inner_rows!r} != {inner_expected!r}")

        leftj = left.join(right, ["k"], "left")
        left_names = leftj.columns
        if left_names != ["k", "name", "v"]:
            raise SystemExit(f"DataFrame.join columns {left_names!r} != ['k', 'name', 'v']")
        left_rows = set(leftj.collect())
        left_expected = {(1, "a", 10.0), (2, "b", 20.0), (3, "c", None)}
        if left_rows != left_expected:
            raise SystemExit(f"DataFrame.join rows {left_rows!r} != {left_expected!r}")

        anti = left.join(right, ["k"], "left_anti")
        anti_rows = set(anti.collect())
        if anti_rows != {(3, "c")}:
            raise SystemExit(f"DataFrame.join rows {anti_rows!r} != {(3, 'c')}")
        anti_names = anti.columns
        if anti_names != ["k", "name"]:
            raise SystemExit(f"DataFrame.join columns {anti_names!r} != ['k', 'name']")

        semi = left.join(right, ["k"], "left_semi")
        semi_rows = set(semi.collect())
        semi_expected = {(1, "a"), (2, "b")}
        if semi_rows != semi_expected:
            raise SystemExit(f"DataFrame.join rows {semi_rows!r} != {semi_expected!r}")

        cond = left.join(right, left["k"] == right["k"])
        cond_names = cond.columns
        if cond_names != ["k", "name", "k", "v"]:
            raise SystemExit(f"DataFrame.join columns {cond_names!r} != ['k', 'name', 'k', 'v']")
        cond_rows = set(cond.collect())
        cond_expected = {(1, "a", 1, 10.0), (2, "b", 2, 20.0)}
        if cond_rows != cond_expected:
            raise SystemExit(f"DataFrame.join rows {cond_rows!r} != {cond_expected!r}")

        hinted = left.hint("broadcast")
        hinted_names = hinted.columns
        if hinted_names != ["k", "name"]:
            raise SystemExit(f"DataFrame.hint columns {hinted_names!r} != ['k', 'name']")
        hinted_rows = set(hinted.collect())
        hinted_expected = {(1, "a"), (2, "b"), (3, "c")}
        if hinted_rows != hinted_expected:
            raise SystemExit(f"DataFrame.hint rows {hinted_rows!r} != {hinted_expected!r}")

        common = left.select("k").intersect(right.select("k"))
        common_rows = set(common.collect())
        if common_rows != {(1,), (2,)}:
            raise SystemExit(f"DataFrame.intersect rows {common_rows!r} != {(1,), (2,)}")
        duped = repark.createDataFrame([(1,), (2,), (1,)], ["k"])
        other = repark.createDataFrame([(1,), (3,)], ["k"])
        deduped_rows = set(duped.intersect(other).collect())
        if deduped_rows != {(1,)}:
            raise SystemExit(f"DataFrame.intersect rows {deduped_rows!r} != {(1,)}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
