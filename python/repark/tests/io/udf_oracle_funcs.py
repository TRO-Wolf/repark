"""Picklable helpers for the live PySpark classic scalar udf oracle (U8)."""

from __future__ import annotations


def double_long(value: int | None) -> int | None:
    """Multiply by two (null-preserving)."""
    if value is None:
        return None
    return int(value) * 2


def null_safe_double(value: int | None) -> int | None:
    """Null-preserving double (same as double_long; named for oracle clarity)."""
    if value is None:
        return None
    return int(value) * 2


def add_long(left: int | None, right: int | None) -> int | None:
    """Null-propagating sum."""
    if left is None or right is None:
        return None
    return int(left) + int(right)


def upper_str(value: str | None) -> str | None:
    """Uppercase strings (null-preserving)."""
    if value is None:
        return None
    return str(value).upper()


def boom(_value: object) -> None:
    """Always raise — error-surfacing pin."""
    raise ValueError("oracle-udf-boom")
