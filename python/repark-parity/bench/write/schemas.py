"""Synthetic narrow/wide parquet generators for MERGE + OVERWRITE benches.

Measurement-only fixtures (no TPC-H dependency). Never AWS. Polars -> parquet on disk
so seed is not VALUES-replan bound.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final, Literal

Width = Literal["narrow", "wide"]

# Wide: id + 32 float64 payload columns (~264 B/row raw) - stresses materialize/RSS.
WIDE_FLOAT_COLS: Final[int] = 32
# Source fraction for MERGE upserts (matched + a tail of inserts).
DEFAULT_SOURCE_FRACTION: Final[float] = 0.10


def narrow_column_names() -> list[str]:
    """id (i64) + v (f64)."""
    return ["id", "v"]


def wide_column_names() -> list[str]:
    """id (i64) + f0..f{N-1} (f64)."""
    return ["id"] + [f"f{index}" for index in range(WIDE_FLOAT_COLS)]


def column_names(width: Width) -> list[str]:
    if width == "narrow":
        return narrow_column_names()
    if width == "wide":
        return wide_column_names()
    msg = f"unknown width {width!r}; expected 'narrow' or 'wide'"
    raise ValueError(msg)


def bytes_per_row_estimate(width: Width) -> int:
    """Rough raw Arrow bytes/row (id i64 + payload f64s) for disclosures."""
    if width == "narrow":
        return 8 + 8
    return 8 + WIDE_FLOAT_COLS * 8


def _validate_width(width: str) -> Width:
    """Reject unknown width before optional heavy imports (polars)."""
    if width == "narrow" or width == "wide":
        return width
    msg = f"unknown width {width!r}; expected 'narrow' or 'wide'"
    raise ValueError(msg)


def write_synthetic_parquet(
    path: Path,
    *,
    rows: int,
    width: Width,
    id_start: int = 0,
    value_offset: float = 0.0,
) -> Path:
    """Write ``rows`` synthetic rows starting at ``id_start`` to ``path``.

    Uses Polars column expressions (no Python per-row lists) so 10M-wide seeds
    stay tractable. Requires optional ``polars`` (bench night / ``repark[polars]``);
    width/rows are validated **before** the import so unit pins reject bad inputs
    without the extra.

    Raises:
        ValueError: non-positive rows or unknown width.
        ModuleNotFoundError: polars missing when a valid write is attempted.
    """
    if rows < 1:
        msg = f"rows must be >= 1; got {rows}"
        raise ValueError(msg)
    width = _validate_width(width)
    import polars as pl

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # id_start .. id_start+rows-1 as int64; payload from modular arithmetic.
    base = pl.int_range(id_start, id_start + rows, dtype=pl.Int64, eager=True).alias("id")
    if width == "narrow":
        frame = pl.DataFrame({"id": base}).with_columns(
            ((pl.col("id") % 100).cast(pl.Float64) + value_offset).alias("v")
        )
    else:
        frame = pl.DataFrame({"id": base})
        frame = frame.with_columns(
            [
                ((pl.col("id") + col_index).mod(1000).cast(pl.Float64) + value_offset).alias(
                    f"f{col_index}"
                )
                for col_index in range(WIDE_FLOAT_COLS)
            ]
        )
    frame.write_parquet(path)
    return path


def expected_rows_after_merge(*, target_rows: int, source_rows: int, id_start_source: int) -> int:
    """MERGE WHEN MATCHED UPDATE + WHEN NOT MATCHED INSERT row count.

    Target ids are ``0..target_rows-1``. Source ids are
    ``id_start_source .. id_start_source+source_rows-1``. Matched = overlap;
    final rows = target U source.
    """
    if target_rows < 0 or source_rows < 0:
        msg = f"row counts must be >= 0; target={target_rows} source={source_rows}"
        raise ValueError(msg)
    target_ids = set(range(target_rows))
    source_ids = set(range(id_start_source, id_start_source + source_rows))
    return len(target_ids | source_ids)


def merge_source_plan(
    target_rows: int,
    *,
    source_fraction: float = DEFAULT_SOURCE_FRACTION,
) -> tuple[int, int]:
    """Return ``(source_rows, id_start)`` for a mixed upsert plan.

    First half of source overlaps the target (UPDATE); second half is new ids
    past ``target_rows`` (INSERT). Source size ~ ``source_fraction * target_rows``.
    """
    if target_rows < 1:
        msg = f"target_rows must be >= 1; got {target_rows}"
        raise ValueError(msg)
    if not 0.0 < source_fraction <= 1.0:
        msg = f"source_fraction must be in (0, 1]; got {source_fraction}"
        raise ValueError(msg)
    source_rows = max(1, int(target_rows * source_fraction))
    # Overlap half (or all if tiny): start so roughly half the source keys match.
    overlap = source_rows // 2
    id_start = max(0, target_rows - overlap)
    return source_rows, id_start
