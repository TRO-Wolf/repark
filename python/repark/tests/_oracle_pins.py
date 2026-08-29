"""Pinned oracle-environment coordinates shared by record drivers and live-tier helpers.

The MERGE record driver imports GAV from this module only, never from a ``test_`` module
(CP-8 / N-2b). Nothing here imports pyspark or starts a JVM; the pin is read from
``python/repark-parity/pyproject.toml``'s ``record`` extra so CI can assert the GAV's
Spark-minor without installing pyspark.
"""

from __future__ import annotations

import re
from pathlib import Path

# iceberg-spark-runtime whose Spark minor matches the pinned pyspark major.minor, derived at
# test time from python/repark-parity/pyproject.toml's record extra (CP-8), never a
# restated constant.

# Apache Iceberg publishes _2.13 for the Spark 4.x line; not derived from pyspark.
ICEBERG_SPARK_SCALA_BINARY = "2.13"
# Independent of the Spark-minor match duty.
ICEBERG_RUNTIME_VERSION = "1.11.0"

ICEBERG_SPARK_RUNTIME_GAV = (
    f"org.apache.iceberg:iceberg-spark-runtime-4.1_{ICEBERG_SPARK_SCALA_BINARY}"
    f":{ICEBERG_RUNTIME_VERSION}"
)
ICEBERG_SPARK_RUNTIME_NOTE = (
    "oracle: PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 (exact Spark-minor match)"
)


def _pinned_pyspark_version() -> str:
    """Return the exact ``pyspark==X.Y.Z`` pin from repark-parity's ``record`` extra (SSOT)."""
    pyproject = Path(__file__).resolve().parents[2] / "repark-parity" / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    match = re.search(r"pyspark==([0-9]+\.[0-9]+\.[0-9]+)", text)
    if match is None:
        raise AssertionError(
            f"could not find pyspark==X.Y.Z pin in {pyproject} (record extra SSOT)"
        )
    return match.group(1)


def _spark_major_minor(pyspark_version: str) -> str:
    """``4.1.2`` → ``4.1`` — the Spark minor the Iceberg runtime GAV must match (CP-8)."""
    parts = pyspark_version.split(".")
    if len(parts) < 2:
        raise AssertionError(f"pyspark version {pyspark_version!r} has no major.minor")
    return f"{parts[0]}.{parts[1]}"
