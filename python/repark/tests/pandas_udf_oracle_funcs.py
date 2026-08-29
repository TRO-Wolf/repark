"""Picklable helpers for the live PySpark pandas_udf oracle (importable module)."""

from __future__ import annotations

from collections.abc import Iterator

import pandas as pd


def double_long(series: pd.Series) -> pd.Series:
    """Multiply by two as int64 (Spark long)."""
    return series.astype("int64") * 2


def null_safe_double(series: pd.Series) -> pd.Series:
    """Null-preserving double (pandas nullable Int64)."""
    return series.astype("Int64") * 2


def add_long(left: pd.Series, right: pd.Series) -> pd.Series:
    """Element-wise sum as int64."""
    return left.astype("int64") + right.astype("int64")


def upper_str(series: pd.Series) -> pd.Series:
    """Uppercase strings."""
    return series.str.upper()


def boom(_series: pd.Series) -> pd.Series:
    """Always raise — error-surfacing pin."""
    raise ValueError("oracle-udf-boom")


def double_long_iter(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """SCALAR_ITER: double each batch Series as int64."""
    for series in batches:
        yield series.astype("int64") * 2


def mean_double_agg(series: pd.Series) -> float:
    """GROUPED_AGG: mean of a double Series."""
    return float(series.mean())
