"""Deduplicate rows, drop null-bearing rows, fill nulls, and drop columns by name.

pins: ex-15-dataframe-a/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.distinct",
    "DataFrame.dropDuplicates",
    "DataFrame.drop_duplicates",
    "DataFrame.dropna",
    "DataFrame.fillna",
    "DataFrame.drop",
]


def main() -> None:
    """Run the measured deduplication, null-handling, and column-drop answers."""
    repark = ReparkSession.builder.appName("ex-df-dedup-nulls").master("local[1]").getOrCreate()
    try:
        dupes = repark.createDataFrame(
            [(1, "x"), (1, "x"), (2, "y"), (3, "z"), (2, "y")],
            ["n", "label"],
        )
        deduped = {(1, "x"), (2, "y"), (3, "z")}
        distinct_rows = set(dupes.distinct().collect())
        if distinct_rows != deduped:
            raise SystemExit(f"DataFrame.distinct rows {distinct_rows!r} != {deduped!r}")
        dup_all_rows = set(dupes.dropDuplicates().collect())
        if dup_all_rows != deduped:
            raise SystemExit(f"DataFrame.dropDuplicates rows {dup_all_rows!r} != {deduped!r}")
        dup_label_rows = set(dupes.dropDuplicates(["label"]).collect())
        if dup_label_rows != deduped:
            raise SystemExit(f"DataFrame.dropDuplicates rows {dup_label_rows!r} != {deduped!r}")
        dup_n_rows = set(dupes.drop_duplicates(("n",)).collect())
        if dup_n_rows != deduped:
            raise SystemExit(f"DataFrame.drop_duplicates rows {dup_n_rows!r} != {deduped!r}")

        sparse = repark.createDataFrame(
            [("a", 1, 10.0), ("a", None, 20.0), ("a", 2, None), ("b", 3, 30.0)],
            ["g", "k", "v"],
        )
        any_rows = set(sparse.dropna().collect())
        any_expected = {("a", 1, 10.0), ("b", 3, 30.0)}
        if any_rows != any_expected:
            raise SystemExit(f"DataFrame.dropna rows {any_rows!r} != {any_expected!r}")
        all_rows = set(sparse.dropna(how="all").collect())
        all_expected = {("a", 1, 10.0), ("a", 2, None), ("a", None, 20.0), ("b", 3, 30.0)}
        if all_rows != all_expected:
            raise SystemExit(f"DataFrame.dropna rows {all_rows!r} != {all_expected!r}")
        subset_rows = set(sparse.dropna(subset=["v"]).collect())
        subset_expected = {("a", 1, 10.0), ("a", None, 20.0), ("b", 3, 30.0)}
        if subset_rows != subset_expected:
            raise SystemExit(f"DataFrame.dropna rows {subset_rows!r} != {subset_expected!r}")
        thresh_rows = set(sparse.dropna(thresh=2).collect())
        if thresh_rows != all_expected:
            raise SystemExit(f"DataFrame.dropna rows {thresh_rows!r} != {all_expected!r}")

        scalar_rows = set(sparse.fillna(0.0).collect())
        scalar_expected = {("a", 0, 20.0), ("a", 1, 10.0), ("a", 2, 0.0), ("b", 3, 30.0)}
        if scalar_rows != scalar_expected:
            raise SystemExit(f"DataFrame.fillna rows {scalar_rows!r} != {scalar_expected!r}")
        dict_rows = set(sparse.fillna({"v": -1.0, "k": -2}).collect())
        dict_expected = {("a", -2, 20.0), ("a", 1, 10.0), ("a", 2, -1.0), ("b", 3, 30.0)}
        if dict_rows != dict_expected:
            raise SystemExit(f"DataFrame.fillna rows {dict_rows!r} != {dict_expected!r}")
        sub_rows = set(sparse.fillna(0.0, subset=["v"]).collect())
        sub_expected = {("a", 1, 10.0), ("a", 2, 0.0), ("a", None, 20.0), ("b", 3, 30.0)}
        if sub_rows != sub_expected:
            raise SystemExit(f"DataFrame.fillna rows {sub_rows!r} != {sub_expected!r}")

        frame = repark.createDataFrame(
            [("a", 1, 10.0), ("b", 2, 20.0)],
            ["g", "k", "v"],
        )
        dropped = frame.drop("v")
        if dropped.columns != ["g", "k"]:
            raise SystemExit(f"DataFrame.drop columns {dropped.columns!r} != ['g', 'k']")
        kept = frame.drop("nope")
        if kept.columns != ["g", "k", "v"]:
            raise SystemExit(f"DataFrame.drop columns {kept.columns!r} != ['g', 'k', 'v']")
        both = frame.drop("v", "k")
        if both.columns != ["g"]:
            raise SystemExit(f"DataFrame.drop columns {both.columns!r} != ['g']")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
