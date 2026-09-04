"""List the session temp-view names.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.list_temp_view_names",
]


def main() -> None:
    """Run the measured listing answer: the bare session lists none, two views list both."""
    repark = ReparkSession.builder.appName("ex21-ses-tempviews").master("local[1]").getOrCreate()
    try:
        before = repark.list_temp_view_names()
        before_expected: list[str] = []
        if before != before_expected:
            raise SystemExit(f"list_temp_view_names bare {before!r} != {before_expected!r}")

        repark.createDataFrame([(1, "x")], ["k", "s"]).createOrReplaceTempView("ex21_tv_a")
        repark.createDataFrame([(2, "y")], ["k", "s"]).createOrReplaceTempView("ex21_tv_b")

        names = sorted(repark.list_temp_view_names())
        names_expected = ["ex21_tv_a", "ex21_tv_b"]
        if names != names_expected:
            raise SystemExit(f"list_temp_view_names {names!r} != {names_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
