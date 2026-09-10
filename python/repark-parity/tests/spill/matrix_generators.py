"""In-engine input generators: range rows plus a string payload sized to the limit multiple."""

from __future__ import annotations

from typing import Final

GENERATOR_ROWS: Final[int] = 1_000_000
ID_BYTES: Final[int] = 8
UTF8_OFFSET_BYTES: Final[int] = 4
UTF8_VALIDITY_BYTES: Final[int] = 1
MD5_LENGTH: Final[int] = 32

_ROW_FIXED_BYTES: Final[int] = ID_BYTES + UTF8_OFFSET_BYTES + UTF8_VALIDITY_BYTES + MD5_LENGTH


def payload_extra(target_bytes: int, rows: int = GENERATOR_ROWS) -> int:
    """Return the repeat width that lands `rows` Arrow rows on `target_bytes`."""
    if rows <= 0 or target_bytes <= _ROW_FIXED_BYTES * rows:
        raise ValueError(
            f"target {target_bytes} too small for {rows} fixed rows of {_ROW_FIXED_BYTES} bytes"
        )
    return target_bytes // rows - _ROW_FIXED_BYTES


def input_arrow_bytes(payload_extra_value: int, rows: int = GENERATOR_ROWS) -> int:
    """Return the Arrow bytes one input view occupies: id, validity, offsets and payload data."""
    return rows * (_ROW_FIXED_BYTES + payload_extra_value)


def input_select_expr(payload_extra_value: int) -> tuple[str, str]:
    """Return the selectExpr projection: the range id and the fixed-width payload string."""
    return (
        "id",
        f"concat(md5(cast(id as string)), repeat('x', {payload_extra_value})) AS payload",
    )


def memory_limit_string(limit_bytes: int) -> str:
    """Render a byte limit in the unit-suffixed form the engine's capacity parser requires."""
    if limit_bytes <= 0 or limit_bytes % (1024 * 1024) != 0:
        raise ValueError(f"limit {limit_bytes} must be a positive whole number of MiB")
    return f"{limit_bytes // (1024 * 1024)}M"
