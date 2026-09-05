"""Seed-42 seven-column parquet fixture for the facade-boundary cells."""

from __future__ import annotations

from pathlib import Path

SEED = 42
STRING_WIDTH = 16
CAT_CARDINALITY = 100
VI_CARDINALITY = 1000
PART_CARDINALITY = 8
ROW_GROUP_SIZE = 100_000
COLUMNS = ("id", "ts", "v", "vi", "s", "cat", "part")


def fixture_path(bed: Path, rows: int) -> Path:
    """Path of the parquet fixture for ``rows`` under ``bed``."""
    return bed / f"facade_{rows}.parquet"


def build_table(rows: int) -> object:
    """Build the seven-column Arrow table deterministically from ``SEED``."""
    import numpy as np
    import pyarrow as pa

    generator = np.random.default_rng(SEED)
    ids = np.arange(rows, dtype=np.int64)
    letters = np.array(list("abcdefghijklmnopqrstuvwxyz"))
    codes = generator.integers(0, 26, size=(rows, STRING_WIDTH))
    strings = ["".join(row) for row in letters[codes]]
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "ts": pa.array(ids * 7 + generator.integers(0, 7, size=rows), type=pa.int64()),
            "v": pa.array(generator.random(rows), type=pa.float64()),
            "vi": pa.array((ids % VI_CARDINALITY).astype(np.int32), type=pa.int32()),
            "s": pa.array(strings, type=pa.string()),
            "cat": pa.array([f"c{value}" for value in (ids % CAT_CARDINALITY)], type=pa.string()),
            "part": pa.array((ids % PART_CARDINALITY).astype(np.int32), type=pa.int32()),
        }
    )


def ensure_fixture(bed: Path, rows: int) -> Path:
    """Write the fixture for ``rows`` if it is absent and return its path."""
    import pyarrow.parquet as pq

    bed.mkdir(parents=True, exist_ok=True)
    target = fixture_path(bed, rows)
    if target.is_file():
        return target
    pq.write_table(build_table(rows), target, row_group_size=ROW_GROUP_SIZE, compression="zstd")
    return target
