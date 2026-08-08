"""PySpark-suite compatibility harness (C2 / R-PYSPARK-COMPAT).

Runs Apache's own ``pyspark.sql.tests`` modules against the repark facade via a
session/bootstrap redirect. Measurement-only: failing Apache tests are FINDINGS,
never mid-unit product fixes.

Public entry points:
- :func:`bootstrap.install_redirect` — patch map + test-package injection
- :func:`fetch.ensure_spark_tests` — cache Apache Spark tag sources
- :mod:`runner` — census CLI (``python -m compat.runner`` with PYTHONPATH)
"""

from __future__ import annotations

from .classify import CENSUS_CLASSES

__all__ = [
    "CENSUS_CLASSES",
    "__version__",
]

__version__ = "0.0.0"
