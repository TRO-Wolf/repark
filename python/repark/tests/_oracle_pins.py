"""Pinned oracle-environment coordinates shared by record drivers and live-tier helpers.

**One importable home** for the Iceberg Spark-runtime GAV and the pyspark-version helpers
that derive its Spark-minor token (CP-8 / N-2b). Both the MERGE differential corpus
(``test_merge_differential_parity.py``) and the live-tier lifecycle surface
(``_live_parity.build_spark_iceberg_engine``) consume these pins; the MERGE record driver
imports GAV from **this** module only (never from a ``test_`` module).

Nothing here imports pyspark or starts a JVM — the pin is read from
``python/repark-parity/pyproject.toml``'s ``record`` extra so routine CI can assert the
GAV's Spark-minor without installing pyspark.
"""

from __future__ import annotations

import re
from pathlib import Path

# ==================================================================================================
# Iceberg Spark-runtime GAV (record-time + lifecycle-live)
# ==================================================================================================

# Q2 ruling: iceberg-spark-runtime whose Spark minor matches the pinned pyspark major.minor
# (derived at test time from python/repark-parity/pyproject.toml's record extra — CP-8; never a
# restated constant). Published artifact uses Scala 2.13. Iceberg runtime version is the pin
# below; re-derive command is in the MERGE differential / record-driver module docstrings.

# Scala binary of the published iceberg-spark-runtime artifact for Spark 4.x (not derived from
# pyspark — Apache Iceberg publishes _2.13 for this line).
ICEBERG_SPARK_SCALA_BINARY = "2.13"
# Iceberg runtime version coordinate (independent of the Spark-minor match duty).
ICEBERG_RUNTIME_VERSION = "1.11.0"

ICEBERG_SPARK_RUNTIME_GAV = (
    f"org.apache.iceberg:iceberg-spark-runtime-4.1_{ICEBERG_SPARK_SCALA_BINARY}"
    f":{ICEBERG_RUNTIME_VERSION}"
)
ICEBERG_SPARK_RUNTIME_NOTE = (
    "oracle: PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 (exact Spark-minor match)"
)


def _pinned_pyspark_version() -> str:
    """Return the exact ``pyspark==X.Y.Z`` pin from repark-parity's ``record`` extra (SSOT).

    Routine CI does not install pyspark, so this reads the declared pin from
    ``python/repark-parity/pyproject.toml`` rather than ``importlib.metadata``.
    """
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
