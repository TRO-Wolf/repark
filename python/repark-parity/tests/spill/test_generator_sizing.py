"""Pins for the in-engine generator sizing math and the engine capacity-string contract."""

from __future__ import annotations

from matrix_cells import CI_LIMIT_BYTES
from matrix_generators import (
    GENERATOR_ROWS,
    input_arrow_bytes,
    input_select_expr,
    memory_limit_string,
    payload_extra,
)


def test_generator_sizes_input_to_the_target_multiple() -> None:
    """Twice the CI limit sizes a 134-byte row so one million rows land on the target bytes."""
    extra = payload_extra(CI_LIMIT_BYTES * 2)
    assert extra == 89
    assert input_arrow_bytes(extra) == GENERATOR_ROWS * 134
    assert abs(input_arrow_bytes(extra) - CI_LIMIT_BYTES * 2) <= GENERATOR_ROWS
    assert input_select_expr(extra) == (
        "id",
        "concat(md5(cast(id as string)), repeat('x', 89)) AS payload",
    )


def test_memory_limit_string_matches_the_engine_capacity_parser() -> None:
    """The engine's capacity parser refuses bare byte counts, so the limit renders with a unit."""
    assert memory_limit_string(CI_LIMIT_BYTES) == "64M"
    assert memory_limit_string(1024 * 1024 * 1024) == "1024M"
