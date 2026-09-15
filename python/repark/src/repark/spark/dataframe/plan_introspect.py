"""Plan-introspection bodies bound on ``DataFrame`` (DF-PLAN-INTROSPECT-1)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame


def inputFiles(frame: DataFrame) -> list[str]:  # noqa: N802
    """List source files (PySpark ``DataFrame.inputFiles``). pins: df-plan-introspect-1/C-001"""
    frame._ensure_alive()
    from repark import _native

    native = frame._lineage_inner if frame._lineage_inner is not None else frame._plan()
    return list(_native.input_files(native))


def semanticHash(frame: DataFrame) -> int:  # noqa: N802
    """Hash the plan (PySpark ``DataFrame.semanticHash``). pins: df-plan-introspect-1/C-002"""
    frame._ensure_alive()
    from repark import _native

    return int(_native.semantic_hash(frame._plan()))
