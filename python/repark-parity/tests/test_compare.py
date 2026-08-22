"""Tests for the parity-harness comparison core (no Spark, no JVM, no repark required)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal


def _table(ids: list[int | None], names: list[str | None]) -> pa.Table:
    return pa.table({"id": ids, "name": names})


def test_identical_frames_pass() -> None:
    assert_frames_equal(_table([1, 2, 3], ["a", "b", "c"]), _table([1, 2, 3], ["a", "b", "c"]))


def test_row_order_ignored_by_default() -> None:
    left = _table([1, 2, 3], ["a", "b", "c"])
    right = _table([3, 1, 2], ["c", "a", "b"])
    assert_frames_equal(left, right)


def test_order_sensitive_detects_reordering() -> None:
    left = _table([1, 2], ["a", "b"])
    right = _table([2, 1], ["b", "a"])
    with pytest.raises(FrameMismatchError):
        assert_frames_equal(left, right, order_sensitive=True)


def test_value_difference_raises() -> None:
    left = _table([1, 2], ["a", "b"])
    right = _table([1, 2], ["a", "X"])
    with pytest.raises(FrameMismatchError, match="value mismatch"):
        assert_frames_equal(left, right)


def test_schema_difference_raises() -> None:
    left = _table([1], ["a"])
    right = pa.table({"id": [1], "name": ["a"], "extra": [9]})
    with pytest.raises(FrameMismatchError, match="schema mismatch"):
        assert_frames_equal(left, right)


def test_row_count_difference_raises() -> None:
    with pytest.raises(FrameMismatchError, match="row count mismatch"):
        assert_frames_equal(_table([1, 2], ["a", "b"]), _table([1], ["a"]))


def test_nulls_at_matching_positions_are_equal() -> None:
    left = _table([1, None, 3], ["a", None, "c"])
    right = _table([1, None, 3], ["a", None, "c"])
    assert_frames_equal(left, right)


def test_null_versus_value_raises() -> None:
    with pytest.raises(FrameMismatchError):
        assert_frames_equal(_table([1, None], ["a", "b"]), _table([1, 2], ["a", "b"]))


def test_nullability_difference_raises() -> None:
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


# ==================================================================================================
# G18 nested-type order-insensitive path (list / struct / map)
# ==================================================================================================


def _list_table(ids: list[int | None], arrays: list[list[int] | None]) -> pa.Table:
    """Flat key + list column (the classic collect_list shape)."""
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "items": pa.array(arrays, pa.list_(pa.int64())),
        }
    )


def _struct_table(ids: list[int], structs: list[dict[str, object] | None]) -> pa.Table:
    """Flat key + struct column."""
    struct_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "payload": pa.array(structs, struct_type),
        }
    )


def _map_table(ids: list[int], maps: list[list[tuple[str, int]] | None]) -> pa.Table:
    """Flat key + map column (pylist shape: list of (key, value) pairs)."""
    map_type = pa.map_(pa.string(), pa.int64())
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "attrs": pa.array(maps, map_type),
        }
    )


def test_flat_schema_sort_path_unchanged() -> None:
    """Invariant 1: flat tables still use Arrow sort_by — order matches historical path.

    Pins that a flat multiset still compares equal after permutation, and that the sorted
    row order of the private helper matches ``Table.sort_by`` (the pre-G18 mechanism).
    """
    from repark_parity.compare import _sorted_by_all_columns

    left = _table([3, 1, 2, None, 1], ["c", "a", "b", "z", "a"])
    right = _table([1, 1, 2, 3, None], ["a", "a", "b", "c", "z"])
    assert_frames_equal(left, right)

    # Direct pin: flat path row order ≡ Arrow sort_by (all columns ascending).
    sorted_helper = _sorted_by_all_columns(left)
    sorted_arrow = left.sort_by([("id", "ascending"), ("name", "ascending")])
    assert sorted_helper.equals(sorted_arrow)


def test_nested_row_permutation_invariance_list_struct_map() -> None:
    """Invariant 2: permuting equal nested multisets never changes the verdict."""
    # list
    list_a = _list_table([2, 1, 3], [[3, 1], [1, 2], None])
    list_b = _list_table([1, 3, 2], [[1, 2], None, [3, 1]])
    assert_frames_equal(list_a, list_b)

    # struct
    struct_a = _struct_table(
        [2, 1],
        [{"x": 9, "y": "b"}, {"x": 1, "y": "a"}],
    )
    struct_b = _struct_table(
        [1, 2],
        [{"x": 1, "y": "a"}, {"x": 9, "y": "b"}],
    )
    assert_frames_equal(struct_a, struct_b)

    # map (including different key storage order — normalized before compare)
    map_a = _map_table(
        [2, 1],
        [[("b", 2), ("a", 1)], [("k", 0)]],
    )
    map_b = _map_table(
        [1, 2],
        [[("k", 0)], [("a", 1), ("b", 2)]],
    )
    assert_frames_equal(map_a, map_b)


def test_nested_multiset_sensitivity_list_mutation() -> None:
    """Invariant 3a: a changed list element fails the multiset compare."""
    left = _list_table([1, 2], [[1, 2], [3]])
    right = _list_table([1, 2], [[1, 9], [3]])  # mutated nested value
    with pytest.raises(FrameMismatchError, match="value mismatch"):
        assert_frames_equal(left, right)
    dropped = _list_table([1], [[1, 2]])  # dropped row
    with pytest.raises(FrameMismatchError, match="row count mismatch"):
        assert_frames_equal(left, dropped)


def test_nested_multiset_sensitivity_struct_mutation() -> None:
    """Invariant 3b: a changed struct field fails the multiset compare."""
    left = _struct_table([1, 2], [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}])
    right = _struct_table([1, 2], [{"x": 1, "y": "a"}, {"x": 2, "y": "Z"}])
    with pytest.raises(FrameMismatchError, match="value mismatch"):
        assert_frames_equal(left, right)


def test_nested_multiset_sensitivity_map_mutation() -> None:
    """Invariant 3c: a changed map entry fails the multiset compare."""
    left = _map_table([1, 2], [[("a", 1)], [("b", 2)]])
    right = _map_table([1, 2], [[("a", 1)], [("b", 99)]])
    with pytest.raises(FrameMismatchError, match="value mismatch"):
        assert_frames_equal(left, right)
    # Missing key is also a multiset difference.
    missing_key = _map_table([1, 2], [[("a", 1)], []])
    with pytest.raises(FrameMismatchError, match="value mismatch"):
        assert_frames_equal(left, missing_key)


def test_order_sensitive_nested_untouched() -> None:
    """Invariant 4: order_sensitive=True does not reorder nested rows either."""
    left = _list_table([1, 2], [[1], [2]])
    right = _list_table([2, 1], [[2], [1]])
    # Default path: permutation ignored.
    assert_frames_equal(left, right)
    # order_sensitive: same multiset, different row order → fail (path not rewritten).
    with pytest.raises(FrameMismatchError, match="value mismatch"):
        assert_frames_equal(left, right, order_sensitive=True)
    # Identical order still passes under order_sensitive.
    assert_frames_equal(left, left, order_sensitive=True)


def test_nested_list_element_order_is_significant() -> None:
    """Array element order is part of the value (Spark arrays are ordered)."""
    left = _list_table([1], [[1, 2]])
    right = _list_table([1], [[2, 1]])
    with pytest.raises(FrameMismatchError, match="value mismatch"):
        assert_frames_equal(left, right)


def test_nested_only_columns_no_flat_key() -> None:
    """Tables whose every column is nested still get a total order-insensitive compare."""
    list_type = pa.list_(pa.int64())
    left = pa.table({"items": pa.array([[2], [1], [2]], list_type)})
    right = pa.table({"items": pa.array([[1], [2], [2]], list_type)})
    assert_frames_equal(left, right)
    mutated = pa.table({"items": pa.array([[1], [2], [3]], list_type)})
    with pytest.raises(FrameMismatchError, match="value mismatch"):
        assert_frames_equal(left, mutated)
