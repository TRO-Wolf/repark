"""DuckDB ``dsdgen`` → parquet cache under a private root (not sticky ``/tmp``).

Default: ``$XDG_CACHE_HOME/repark-tpcds`` or ``~/.cache/repark-tpcds``. Never commits
data files. Regenerates when any of the twenty-four tables is missing/unusable.
"""

from __future__ import annotations

import logging
import math
import os
from pathlib import Path
from typing import Final

LOGGER = logging.getLogger(__name__)

# Shared with CLI — library API enforces the same bound.
MAX_SCALE_FACTOR: Final[float] = 100.0

# DuckDB tpcds / TPC-DS standard 24 base tables (dsdgen materializes these).
TABLES: Final[tuple[str, ...]] = (
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
)


def default_data_root() -> Path:
    """Private per-user cache root (not sticky world-writable /tmp)."""
    xdg = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".cache"
    return base / "repark-tpcds"


# Back-compat name for imports; call default_data_root() for live env.
DEFAULT_DATA_ROOT: Final[Path] = default_data_root()


def ensure_parquet_sf(
    scale_factor: float,
    *,
    data_root: Path | None = None,
) -> Path:
    """Ensure all twenty-four TPC-DS tables exist as parquet for ``scale_factor``.

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
        LOGGER.info("TPC-DS SF%s parquet cache hit at %s", scale_factor, out_dir)
        return out_dir

    # Drop zero-size / incomplete leftovers before regenerate.
    for table_name in TABLES:
        target = out_dir / f"{table_name}.parquet"
        if target.is_symlink() or (target.is_file() and target.stat().st_size == 0):
            target.unlink(missing_ok=True)

    LOGGER.info(
        "TPC-DS SF%s generating parquet → %s",
        scale_factor,
        out_dir,
    )
    import duckdb

    connection = duckdb.connect(database=":memory:")
    try:
        _load_tpcds_extension(connection)
        connection.execute(f"CALL dsdgen(sf={scale_factor})")
        for table_name in TABLES:
            target = out_dir / f"{table_name}.parquet"
            if target.is_symlink():
                target.unlink()
            # Escape path for SQL single-quoted string
            path_sql = str(target).replace("'", "''")
            connection.execute(f"COPY {table_name} TO '{path_sql}' (FORMAT PARQUET)")
    finally:
        connection.close()

    if not _cache_is_usable(out_dir):
        msg = f"dsdgen export incomplete or zero-size under {out_dir}"
        raise RuntimeError(msg)
    return out_dir


def _assert_safe_cache_path(root: Path, out_dir: Path) -> None:
    """Refuse directory-level symlinks under the cache root."""
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
    """True only when all 24 tables exist as regular non-empty files (not symlinks)."""
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


def _load_tpcds_extension(connection: object) -> None:
    """INSTALL + LOAD the DuckDB ``tpcds`` extension (network once; caches under ~/.duckdb)."""
    # Typed as object to avoid hard duckdb import at module load for importorskip paths.
    connection.execute("INSTALL tpcds")  # type: ignore[attr-defined]
    connection.execute("LOAD tpcds")  # type: ignore[attr-defined]
