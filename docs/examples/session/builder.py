"""Build the session with the snake_case builder spellings and read the config back.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.Builder.app_name",
    "SparkSession.Builder.master",
    "SparkSession.Builder.config",
    "SparkSession.Builder.get_or_create",
]


def main() -> None:
    """Run the measured builder answers: app name, master, config readback, and the session."""
    builder = ReparkSession.builder
    repark = (
        builder.app_name("ex21-ses-builder")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "4")
        .get_or_create()
    )
    try:
        app_name = repark.conf.get("spark.app.name")
        app_name_expected = "ex21-ses-builder"
        if app_name != app_name_expected:
            raise SystemExit(f"spark.app.name {app_name!r} != {app_name_expected!r}")

        master = repark.conf.get("spark.master")
        master_expected = "local[1]"
        if master != master_expected:
            raise SystemExit(f"spark.master {master!r} != {master_expected!r}")

        partitions = repark.conf.get("spark.sql.shuffle.partitions")
        partitions_expected = "4"
        if partitions != partitions_expected:
            raise SystemExit(
                f"spark.sql.shuffle.partitions {partitions!r} != {partitions_expected!r}"
            )

        context_master = repark.sparkContext.master
        if context_master != master_expected:
            raise SystemExit(f"sparkContext.master {context_master!r} != {master_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
