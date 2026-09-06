"""Report the engine version and the session context identity fields."""

from __future__ import annotations

import repark as repark_native
from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.version",
    "SparkSession.sparkContext",
]


def main() -> None:
    repark = ReparkSession.builder.appName("ex26-identity").master("local[1]").getOrCreate()
    try:
        version = repark.version
        version_expected = f"repark-{repark_native.__version__}"
        if version != version_expected:
            raise SystemExit(f"version {version!r} != {version_expected!r}")
        if "4.1.2" in version:
            raise SystemExit(f"version {version!r} must not parse as a Spark release")
        context = repark.sparkContext
        if context.master != "local[1]":
            raise SystemExit(f"sparkContext.master {context.master!r} != 'local[1]'")
        application_id = context.applicationId
        if not isinstance(application_id, str) or not application_id:
            raise SystemExit(f"applicationId {application_id!r} is not a non-empty string")
        if context.setLogLevel("WARN") is not None:
            raise SystemExit("sparkContext.setLogLevel did not answer None")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
