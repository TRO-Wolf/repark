"""Replace matching cells in one column and draw measured samples from one frame.

pins: ex-18-dataframe-c/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = ["DataFrame.replace", "DataFrame.sample", "DataFrame.sampleBy"]


def main() -> None:
    """Run the measured replace, sample, and sampleBy answers on one local frame."""
    repark = ReparkSession.builder.appName("ex-df-replace-sample").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 1, 10.0),
                ("a", 2, 20.0),
                ("a", 2, 30.0),
                ("a", 3, 40.0),
                ("b", 1, 50.0),
                ("b", 2, None),
            ],
            ["g", "k", "v"],
        )
        expected = {
            ("a", 1, 10.0),
            ("a", 2, 20.0),
            ("a", 2, 30.0),
            ("a", 3, 40.0),
            ("b", 1, 50.0),
            ("b", 2, None),
        }
        replaced = frame.replace(20.0, 99.0, "v")
        replaced_rows = set(replaced.collect())
        replaced_expected = {
            ("a", 1, 10.0),
            ("a", 2, 30.0),
            ("a", 2, 99.0),
            ("a", 3, 40.0),
            ("b", 1, 50.0),
            ("b", 2, None),
        }
        if replaced_rows != replaced_expected:
            raise SystemExit(f"DataFrame.replace rows {replaced_rows!r} != {replaced_expected!r}")
        dict_replaced = frame.replace({20.0: 99.0, 30.0: 33.0}, subset=["v"])
        dict_rows = set(dict_replaced.collect())
        dict_expected = {
            ("a", 1, 10.0),
            ("a", 2, 33.0),
            ("a", 2, 99.0),
            ("a", 3, 40.0),
            ("b", 1, 50.0),
            ("b", 2, None),
        }
        if dict_rows != dict_expected:
            raise SystemExit(f"DataFrame.replace rows {dict_rows!r} != {dict_expected!r}")

        whole = frame.sample(fraction=1.0, seed=1)
        whole_rows = set(whole.collect())
        if whole_rows != expected:
            raise SystemExit(f"DataFrame.sample rows {whole_rows!r} != {expected!r}")
        whole_count = whole.count()
        if whole_count != 6:
            raise SystemExit(f"DataFrame.sample count {whole_count!r} != 6")

        keep_a = frame.sampleBy("g", {"a": 1.0}, seed=0)
        keep_a_rows = set(keep_a.collect())
        keep_a_expected = {
            ("a", 1, 10.0),
            ("a", 2, 20.0),
            ("a", 2, 30.0),
            ("a", 3, 40.0),
        }
        if keep_a_rows != keep_a_expected:
            raise SystemExit(f"DataFrame.sampleBy rows {keep_a_rows!r} != {keep_a_expected!r}")
        keep_b = frame.sampleBy("g", {"a": 0.0, "b": 1.0}, seed=0)
        keep_b_rows = set(keep_b.collect())
        keep_b_expected = {
            ("b", 1, 50.0),
            ("b", 2, None),
        }
        if keep_b_rows != keep_b_expected:
            raise SystemExit(f"DataFrame.sampleBy rows {keep_b_rows!r} != {keep_b_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
