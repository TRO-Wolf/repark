"""The legacy and pandas-on-Spark session names refuse loud with the supported route.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from repark.errors import UnsupportedOperationException
from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.registerTempTable",
    "SparkSession.pandas_api",
]


def main() -> None:
    """Run the measured refusals: each raises UnsupportedOperationException naming the route."""
    repark = ReparkSession.builder.appName("ex21-ses-refuse").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([(1, "a")], ["id", "s"])
        refused_view: str | None = None
        try:
            repark.registerTempTable("ex21_legacy", frame)
        except UnsupportedOperationException as error:
            refused_view = str(error)
        if refused_view is None or "createOrReplaceTempView" not in refused_view:
            raise SystemExit(f"registerTempTable refusal {refused_view!r} is not the loud answer")

        probed: object = None
        refused_pandas: str | None = None
        try:
            probed = repark.pandas_api
        except UnsupportedOperationException as error:
            refused_pandas = str(error)
        if probed is not None or refused_pandas is None or "to_pandas" not in refused_pandas:
            raise SystemExit(f"pandas_api refusal {refused_pandas!r} is not the loud answer")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
