"""TPC-H parquet source for the write-path bench.

Reuses ``bench/tpch/datagen.py`` so SF caches stay shared. Default fact table is
``lineitem`` (largest SF1 file). Never commits data; never touches AWS.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Final

# Peer package: bench/tpch (not an installed distribution).
_TPCH_DIR: Final[Path] = Path(__file__).resolve().parent.parent / "tpch"
_PACKAGE: Final[str] = "repark_tpch_bench_for_write"


def _load_tpch_datagen() -> types.ModuleType:
    """Import tpch.datagen without requiring bench/ to be on PYTHONPATH."""
    import importlib

    if _PACKAGE not in sys.modules:
        package = types.ModuleType(_PACKAGE)
        package.__path__ = [str(_TPCH_DIR)]  # type: ignore[attr-defined]
        sys.modules[_PACKAGE] = package
    return importlib.import_module(f"{_PACKAGE}.datagen")


def ensure_source_parquet(
    scale_factor: float,
    *,
    data_root: Path | None = None,
    table: str = "lineitem",
) -> Path:
    """Ensure TPC-H ``table`` parquet exists; return the file path.

    Raises:
        ValueError: invalid scale factor (delegated) or unknown table name.
        RuntimeError: dbgen export incomplete.
    """
    datagen = _load_tpch_datagen()
    known: tuple[str, ...] = datagen.TABLES
    if table not in known:
        msg = f"unknown TPC-H table {table!r}; known={known}"
        raise ValueError(msg)
    data_dir: Path = datagen.ensure_parquet_sf(scale_factor, data_root=data_root)
    path = data_dir / f"{table}.parquet"
    if not path.is_file() or path.stat().st_size <= 0:
        msg = f"source parquet missing or empty: {path}"
        raise RuntimeError(msg)
    return path


def default_data_root() -> Path:
    """Same private cache root as the TPC-H harness."""
    return _load_tpch_datagen().default_data_root()


DEFAULT_SOURCE_TABLE: Final[str] = "lineitem"
"""Default fact table for the write matrix (largest SF1 parquet)."""
