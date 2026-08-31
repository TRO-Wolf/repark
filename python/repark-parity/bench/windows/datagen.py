"""Seeded window-bench frames. No wall clock in the seed path."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .roster import DEFAULT_SEED

MIX_A = 1_103_515_245
MIX_B = 12_345
TS_MODULUS = 1 << 30
VALUE_MODULUS = 100
PARTITION_COUNT = 8


def seed_table(n_rows: int, *, seed: int = DEFAULT_SEED) -> pa.Table:
    """Build a deterministic ``n_rows`` Arrow table.

    Columns: ``id`` int64, ``ts`` int64 (not monotonic in ``id``), ``v`` float64,
    ``vi`` int64, ``v2`` float64, ``part`` int32. ``seed`` folds into ``ts`` so
    two seeds cannot collide.

    Args:
        n_rows: row count, non-negative.
        seed: additive mix for ``ts``.

    Returns:
        An Arrow table with six columns.

    Raises:
        ValueError: ``n_rows`` is negative.
    """
    if n_rows < 0:
        raise ValueError(f"n_rows must be >= 0, got {n_rows}")
    ids = pa.array(range(n_rows), type=pa.int64())
    mixed = pc.add(pc.multiply(ids, MIX_A), MIX_B + seed)
    ts = pc.bit_wise_and(mixed, TS_MODULUS - 1)
    vi = pa.array((index % VALUE_MODULUS for index in range(n_rows)), type=pa.int64())
    v = pc.divide(pc.cast(vi, pa.float64()), float(VALUE_MODULUS))
    v2 = pc.add(v, 1.0)
    part = pc.cast(pc.bit_wise_and(ids, PARTITION_COUNT - 1), pa.int32())
    return pa.table(
        {
            "id": ids,
            "ts": ts,
            "v": v,
            "vi": vi,
            "v2": v2,
            "part": part,
        }
    )


def write_seed_parquet(path: Path, n_rows: int, *, seed: int = DEFAULT_SEED) -> int:
    """Write :func:`seed_table` to ``path`` and return the file size in bytes.

    Args:
        path: destination parquet path.
        n_rows: row count.
        seed: generator seed.

    Returns:
        Size of the written file in bytes.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    table = seed_table(n_rows, seed=seed)
    pq.write_table(table, path)
    return path.stat().st_size


def cleanup_scratch(root: Path, *, keep: bool) -> bool:
    """Delete ``root`` unless ``keep`` is true.

    Args:
        root: scratch directory.
        keep: when true, leave the tree in place.

    Returns:
        True when the directory is gone (or never existed) after the call.
    """
    import shutil

    if keep:
        return not root.exists()
    shutil.rmtree(root, ignore_errors=True)
    return not root.exists()


def directory_bytes(root: Path) -> int:
    """Total size of every regular file under ``root``.

    Args:
        root: directory to walk. Missing roots count as zero.

    Returns:
        Sum of file sizes in bytes.
    """
    if not root.exists():
        return 0
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
