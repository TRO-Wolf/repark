"""Join two frames, merge a source into a target table, hint, and intersect row sets.

pins: ex-16-dataframe-b/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.join",
    "DataFrame.hint",
    "DataFrame.intersect",
    "DataFrame.mergeInto",
    "DataFrame.merge_into",
]


def main() -> None:
    """Run the measured join arms, the merge arms, the hint no-op, and the intersect."""
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
        inner_names_expected = ["k", "name", "v"]
        if inner_names != inner_names_expected:
            raise SystemExit(f"DataFrame.join columns {inner_names!r} != {inner_names_expected!r}")
        inner_rows = set(inner.collect())
        inner_expected = {(1, "a", 10.0), (2, "b", 20.0)}
        if inner_rows != inner_expected:
            raise SystemExit(f"DataFrame.join rows {inner_rows!r} != {inner_expected!r}")

        leftj = left.join(right, ["k"], "left")
        left_names = leftj.columns
        if left_names != inner_names_expected:
            raise SystemExit(f"DataFrame.join columns {left_names!r} != {inner_names_expected!r}")
        left_rows = set(leftj.collect())
        left_expected = {(1, "a", 10.0), (2, "b", 20.0), (3, "c", None)}
        if left_rows != left_expected:
            raise SystemExit(f"DataFrame.join rows {left_rows!r} != {left_expected!r}")

        anti = left.join(right, ["k"], "left_anti")
        anti_rows = set(anti.collect())
        anti_expected = {(3, "c")}
        if anti_rows != anti_expected:
            raise SystemExit(f"DataFrame.join rows {anti_rows!r} != {anti_expected!r}")
        anti_names = anti.columns
        anti_names_expected = ["k", "name"]
        if anti_names != anti_names_expected:
            raise SystemExit(f"DataFrame.join columns {anti_names!r} != {anti_names_expected!r}")

        semi = left.join(right, ["k"], "left_semi")
        semi_rows = set(semi.collect())
        semi_expected = {(1, "a"), (2, "b")}
        if semi_rows != semi_expected:
            raise SystemExit(f"DataFrame.join rows {semi_rows!r} != {semi_expected!r}")

        cond = left.join(right, left["k"] == right["k"])
        cond_names = cond.columns
        cond_names_expected = ["k", "name", "k", "v"]
        if cond_names != cond_names_expected:
            raise SystemExit(f"DataFrame.join columns {cond_names!r} != {cond_names_expected!r}")
        cond_rows = set(cond.collect())
        cond_expected = {(1, "a", 1, 10.0), (2, "b", 2, 20.0)}
        if cond_rows != cond_expected:
            raise SystemExit(f"DataFrame.join rows {cond_rows!r} != {cond_expected!r}")

        hinted = left.hint("broadcast")
        hinted_names = hinted.columns
        hinted_names_expected = ["k", "name"]
        if hinted_names != hinted_names_expected:
            raise SystemExit(
                f"DataFrame.hint columns {hinted_names!r} != {hinted_names_expected!r}"
            )
        hinted_rows = set(hinted.collect())
        hinted_expected = {(1, "a"), (2, "b"), (3, "c")}
        if hinted_rows != hinted_expected:
            raise SystemExit(f"DataFrame.hint rows {hinted_rows!r} != {hinted_expected!r}")

        common = left.select("k").intersect(right.select("k"))
        common_rows = set(common.collect())
        common_expected = {(1,), (2,)}
        if common_rows != common_expected:
            raise SystemExit(f"DataFrame.intersect rows {common_rows!r} != {common_expected!r}")
        duped = repark.createDataFrame([(1,), (2,), (1,)], ["k"])
        other = repark.createDataFrame([(1,), (3,)], ["k"])
        deduped_rows = set(duped.intersect(other).collect())
        deduped_expected = {(1,)}
        if deduped_rows != deduped_expected:
            raise SystemExit(f"DataFrame.intersect rows {deduped_rows!r} != {deduped_expected!r}")

        repark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).write.saveAsTable("people")
        merged_source = repark.createDataFrame([(1, "A"), (3, "c")], ["id", "name"])
        (
            merged_source.mergeInto("people", "id")
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .merge()
        )
        merged_rows = [
            tuple(row) for row in repark.sql("SELECT id, name FROM people ORDER BY id").collect()
        ]
        merged_expected = [(1, "A"), (2, "b"), (3, "c")]
        if merged_rows != merged_expected:
            raise SystemExit(f"DataFrame.mergeInto rows {merged_rows!r} != {merged_expected!r}")

        repark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).write.saveAsTable("accounts")
        condition_source = repark.createDataFrame([(1, "A"), (3, "c")], ["id", "name"])
        (
            condition_source.merge_into("accounts", F.col("target.id") == F.col("source.id"))
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .merge()
        )
        condition_rows = [
            tuple(row) for row in repark.sql("SELECT id, name FROM accounts ORDER BY id").collect()
        ]
        if condition_rows != merged_expected:
            raise SystemExit(f"DataFrame.merge_into rows {condition_rows!r} != {merged_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
