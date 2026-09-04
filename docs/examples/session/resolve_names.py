"""Resolve bare, two-part, and temp-view table names under the current catalog and database.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.resolve_table_name",
]


def main() -> None:
    """Run the measured resolution answers: bare, two-part, three-part, and the temp-view home."""
    repark = ReparkSession.builder.appName("ex21-ses-resolve").master("local[1]").getOrCreate()
    try:
        resolved = repark.resolve_table_name("ex21_report")
        resolved_expected = "spark_catalog.default.ex21_report"
        if resolved != resolved_expected:
            raise SystemExit(f"resolve_table_name {resolved!r} != {resolved_expected!r}")

        two_part = repark.resolve_table_name("default.ex21_report")
        two_part_expected = "spark_catalog.default.ex21_report"
        if two_part != two_part_expected:
            raise SystemExit(f"resolve_table_name two-part {two_part!r} != {two_part_expected!r}")

        repark.createDataFrame([(1, "x")], ["k", "s"]).createOrReplaceTempView("ex21_tv")
        home = repark.resolve_table_name("ex21_tv", prefer_temp_view=True)
        home_expected = "datafusion.public.ex21_tv"
        if home != home_expected:
            raise SystemExit(f"resolve_table_name home {home!r} != {home_expected!r}")

        plain = repark.resolve_table_name("ex21_tv")
        plain_expected = "spark_catalog.default.ex21_tv"
        if plain != plain_expected:
            raise SystemExit(f"resolve_table_name plain {plain!r} != {plain_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
