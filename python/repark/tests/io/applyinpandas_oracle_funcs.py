"""Picklable helpers for the live PySpark applyInPandas oracle (must be importable)."""

from __future__ import annotations

from typing import Any

import pandas as pd


def sum_v(pdf: pd.DataFrame) -> pd.DataFrame:
    """Sum column ``v`` for one already-grouped PDF; preserve non-``v`` key columns.

    applyInPandas delivers one group per call — do not re-groupby (null keys would
    float-upcast under pandas groupby).
    """
    keys = [name for name in pdf.columns if name != "v"]
    if not keys:
        return pd.DataFrame({"total": [int(pdf["v"].sum())]})
    row: dict[str, Any] = {}
    for name in keys:
        value = pdf[name].iloc[0]
        row[name] = None if pd.isna(value) else value
    row["total"] = int(pdf["v"].sum())
    return pd.DataFrame([row])


def count_rows(pdf: pd.DataFrame) -> pd.DataFrame:
    """One row per group with key columns + row count."""
    keys = [name for name in pdf.columns if name != "v"]
    if not keys:
        return pd.DataFrame({"n": [len(pdf)]})
    row: dict[str, Any] = {}
    for name in keys:
        value = pdf[name].iloc[0]
        row[name] = None if pd.isna(value) else value
    row["n"] = len(pdf)
    return pd.DataFrame([row])


def sum_v_global(pdf: pd.DataFrame) -> pd.DataFrame:
    """Global groupBy helper: single ``total`` column only (schema = ``total INT``)."""
    return pd.DataFrame({"total": [int(pdf["v"].sum())]})
