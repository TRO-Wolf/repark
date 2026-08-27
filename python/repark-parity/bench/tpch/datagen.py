"""DuckDB ``dbgen`` → parquet cache under a private root (not sticky ``/tmp``).

Default: ``$XDG_CACHE_HOME/repark-tpch`` or ``~/.cache/repark-tpch``. Never commits
data files. Regenerates when any of the eight tables is missing/unusable.
"""

from __future__ import annotations

import logging
import math
import os
from pathlib import Path
from typing import Final

from repark_parity.sql import escape_sql_single_quotes

LOGGER = logging.getLogger(__name__)

# Shared with CLI — library API enforces the same bound (octo C2-SEC-003).
MAX_SCALE_FACTOR: Final[float] = 100.0

TABLES: Final[tuple[str, ...]] = (
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
)


def default_data_root() -> Path:
    """Private per-user cache root (E7-SEC-001 — not sticky world-writable /tmp)."""
    xdg = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".cache"
    return base / "repark-tpch"


# Back-compat name for imports; call default_data_root() for live env.
DEFAULT_DATA_ROOT: Final[Path] = default_data_root()


def ensure_parquet_sf(
    scale_factor: float,
    *,
    data_root: Path | None = None,
) -> Path:
    """Ensure all eight TPC-H tables exist as parquet for ``scale_factor``.

    Returns the directory containing ``{table}.parquet`` files.
    """
    if not math.isfinite(scale_factor) or scale_factor <= 0 or scale_factor > MAX_SCALE_FACTOR:
        msg = f"scale_factor must be finite and in (0, {MAX_SCALE_FACTOR}]; got {scale_factor!r}"
        raise ValueError(msg)
    root = (data_root if data_root is not None else default_data_root()).expanduser()
    # Format SF path: 0.01 → sf0.01, 1 → sf1, 10 → sf10
    sf_label = _sf_label(scale_factor)
    out_dir = root / sf_label
    _assert_safe_cache_path(root, out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    _assert_safe_cache_path(root, out_dir)

    if _cache_is_usable(out_dir):
        LOGGER.info("TPC-H SF%s parquet cache hit at %s", scale_factor, out_dir)
        return out_dir

    # Drop zero-size / incomplete leftovers before regenerate (E1-SEC-001).
    for table_name in TABLES:
        target = out_dir / f"{table_name}.parquet"
        if target.is_symlink() or (target.is_file() and target.stat().st_size == 0):
            target.unlink(missing_ok=True)

    LOGGER.info(
        "TPC-H SF%s generating parquet → %s",
        scale_factor,
        out_dir,
    )
    import duckdb

    connection = duckdb.connect(database=":memory:")
    try:
        _load_tpch_extension(connection)
        connection.execute(f"CALL dbgen(sf={scale_factor})")
        for table_name in TABLES:
            target = out_dir / f"{table_name}.parquet"
            if target.is_symlink():
                target.unlink()
            # Escape path for SQL single-quoted string (DuckDB: backslash-literal, quotes only).
            path_sql = escape_sql_single_quotes(str(target))
            connection.execute(f"COPY {table_name} TO '{path_sql}' (FORMAT PARQUET)")
    finally:
        connection.close()

    if not _cache_is_usable(out_dir):
        msg = f"dbgen export incomplete or zero-size under {out_dir}"
        raise RuntimeError(msg)
    return out_dir


def _assert_safe_cache_path(root: Path, out_dir: Path) -> None:
    """Refuse directory-level symlinks under the cache root (E7-SEC-001)."""
    for path in (root, out_dir, *root.parents):
        if path.exists() and path.is_symlink():
            msg = f"refusing symlink cache path {path} (use a real private directory)"
            raise ValueError(msg)
    # out_dir may not exist yet before mkdir; after mkdir re-check.
    if out_dir.exists() and not out_dir.is_dir():
        msg = f"cache out_dir is not a directory: {out_dir}"
        raise ValueError(msg)
    if out_dir.exists() and out_dir.is_symlink():
        msg = f"refusing symlink cache out_dir {out_dir}"
        raise ValueError(msg)


def _cache_is_usable(out_dir: Path) -> bool:
    """True only when all eight tables exist as regular non-empty files (not symlinks)."""
    if out_dir.is_symlink() or not out_dir.is_dir():
        return False
    for table_name in TABLES:
        target = out_dir / f"{table_name}.parquet"
        if target.is_symlink():
            return False
        if not target.is_file() or target.stat().st_size <= 0:
            return False
    return True


def _sf_label(scale_factor: float) -> str:
    """Canonical cache directory name for a scale factor."""
    if scale_factor == int(scale_factor):
        return f"sf{int(scale_factor)}"
    # trim trailing zeros: 0.010 → 0.01
    text = f"{scale_factor:g}"
    return f"sf{text}"


def _load_tpch_extension(connection: object) -> None:
    """INSTALL + LOAD the DuckDB ``tpch`` extension (network once; caches under ~/.duckdb)."""
    # Typed as object to avoid hard duckdb import at module load for importorskip paths.
    connection.execute("INSTALL tpch")  # type: ignore[attr-defined]
    connection.execute("LOAD tpch")  # type: ignore[attr-defined]
