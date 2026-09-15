"""Plan-introspection bodies bound on ``DataFrame`` (DF-PLAN-INTROSPECT-1)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame


def _cache_lineages(frame: DataFrame) -> dict[str, Any]:
    """Map each live cache-view name to its pre-cache native plan."""
    from repark.spark._temp_views import local_view_name

    lineages: dict[str, Any] = {}
    registry = frame._alive_token.get("cache_frames")
    if registry is None:
        return lineages
    for cached in list(registry):
        view = cached._cache_view
        lineage = cached._lineage_inner
        if view is None or lineage is None:
            continue
        lineages.setdefault(local_view_name(view), lineage)
    return lineages


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

    native = frame._lineage_inner if frame._lineage_inner is not None else frame._plan()
    return int(_native.semantic_hash(native, _cache_lineages(frame)))
