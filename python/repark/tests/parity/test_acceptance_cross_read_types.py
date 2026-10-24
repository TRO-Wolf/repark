"""Pure pins for the nanosecond check over nested Iceberg type nodes; no Spark, no AWS.

pins: aws-ephemeral-runs-1/C-031
"""

from __future__ import annotations

from typing import Any

import pytest
from _acceptance_cross_read import iceberg_type_has_ns


def _nested(leaf: str) -> list[dict[str, Any]]:
    """A struct field holding a list of structs whose last field has the type ``leaf``."""
    inner = {"type": "struct", "fields": [{"id": 3, "name": "t", "type": leaf}]}
    listing = {"type": "list", "element-id": 2, "element": inner, "element-required": False}
    outer = {"type": "struct", "fields": [{"id": 1, "name": "xs", "type": listing}]}
    return [{"id": 0, "name": "root", "type": outer}]


@pytest.mark.parametrize("leaf", ["timestamp_ns", "timestamptz_ns"])
def test_a_nanosecond_leaf_inside_a_list_inside_a_struct_is_found(leaf: str) -> None:
    assert iceberg_type_has_ns(_nested(leaf)) is True


@pytest.mark.parametrize("leaf", ["timestamp", "timestamptz", "long", "string"])
def test_a_microsecond_or_plain_leaf_inside_the_same_nesting_is_not_flagged(leaf: str) -> None:
    assert iceberg_type_has_ns(_nested(leaf)) is False


def test_a_bare_dict_type_node_does_not_raise_and_is_searched() -> None:
    assert iceberg_type_has_ns({"type": {"type": "list", "element": "timestamp_ns"}}) is True
    assert iceberg_type_has_ns({"type": {"type": "list", "element": "int"}}) is False
    assert iceberg_type_has_ns({"type": {"type": "map", "key": "int", "value": "long"}}) is False


def test_a_flat_nanosecond_field_and_a_bare_string_are_found() -> None:
    assert iceberg_type_has_ns([{"id": 1, "name": "t", "type": "timestamp_ns"}]) is True
    assert iceberg_type_has_ns("timestamptz_ns") is True
    assert iceberg_type_has_ns("timestamp") is False
