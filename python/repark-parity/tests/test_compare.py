"""Tests for the parity-harness comparison core (no Spark, no JVM, no repark required)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal


def _table(ids, names):
    return pa.table({"id": ids, "name": names})


def test_identical_frames_pass():
    assert_frames_equal(_table([1, 2, 3], ["a", "b", "c"]), _table([1, 2, 3], ["a", "b", "c"]))


def test_row_order_ignored_by_default():
    left = _table([1, 2, 3], ["a", "b", "c"])
    right = _table([3, 1, 2], ["c", "a", "b"])
    assert_frames_equal(left, right)


def test_order_sensitive_detects_reordering():
    left = _table([1, 2], ["a", "b"])
    right = _table([2, 1], ["b", "a"])
    with pytest.raises(FrameMismatchError):
        assert_frames_equal(left, right, order_sensitive=True)


def test_value_difference_raises():
    left = _table([1, 2], ["a", "b"])
    right = _table([1, 2], ["a", "X"])
    with pytest.raises(FrameMismatchError, match="value mismatch"):
        assert_frames_equal(left, right)


def test_schema_difference_raises():
    left = _table([1], ["a"])
    right = pa.table({"id": [1], "name": ["a"], "extra": [9]})
    with pytest.raises(FrameMismatchError, match="schema mismatch"):
        assert_frames_equal(left, right)


def test_row_count_difference_raises():
    with pytest.raises(FrameMismatchError, match="row count mismatch"):
        assert_frames_equal(_table([1, 2], ["a", "b"]), _table([1], ["a"]))


def test_nulls_at_matching_positions_are_equal():
    left = _table([1, None, 3], ["a", None, "c"])
    right = _table([1, None, 3], ["a", None, "c"])
    assert_frames_equal(left, right)


def test_null_versus_value_raises():
    with pytest.raises(FrameMismatchError):
        assert_frames_equal(_table([1, None], ["a", "b"]), _table([1, 2], ["a", "b"]))


def test_nullability_difference_raises():
    # Field nullability is part of the schema signature: same name/type/values but a differing
    # `nullable` flag is a parity failure (Spark's non-null guarantees are contractual).
    values = [1, 2, 3]
    nullable = pa.table(
        [pa.array(values, pa.int64())],
        schema=pa.schema([pa.field("id", pa.int64(), nullable=True)]),
    )
    non_nullable = pa.table(
        [pa.array(values, pa.int64())],
        schema=pa.schema([pa.field("id", pa.int64(), nullable=False)]),
    )
    with pytest.raises(FrameMismatchError, match="schema mismatch"):
        assert_frames_equal(nullable, non_nullable)
