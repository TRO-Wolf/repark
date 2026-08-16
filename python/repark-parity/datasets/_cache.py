"""Shared cache-root helpers for torture-dataset generators.

Default root: ``$XDG_CACHE_HOME/repark-datasets`` or ``~/.cache/repark-datasets``.
Each family writes under ``<root>/<family>/``. Data is never committed; callers must
not point ``--out`` at the repository tree.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

CACHE_DIR_NAME: Final[str] = "repark-datasets"

KNOWN_FAMILIES: Final[tuple[str, ...]] = (
    "nested",
    "schema_inference",
    "extreme_types",
    "secrets",
    "smartcsv",
)


def default_datasets_root() -> Path:
    """Private per-user cache root (not sticky world-writable ``/tmp``)."""
    xdg = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".cache"
    return base / CACHE_DIR_NAME


def family_cache_dir(family: str, *, data_root: Path | None = None) -> Path:
    """Return ``<root>/<family>`` after validating the family slug."""
    if family not in KNOWN_FAMILIES:
        known = ", ".join(KNOWN_FAMILIES)
        msg = f"unknown dataset family {family!r}; expected one of: {known}"
        raise ValueError(msg)
    root = (data_root if data_root is not None else default_datasets_root()).expanduser()
    return root / family


def assert_safe_cache_path(root: Path, out_dir: Path) -> None:
    """Refuse directory-level symlinks under the cache root."""
    for path in (root, out_dir, *root.parents):
        if path.exists() and path.is_symlink():
            msg = f"refusing symlink cache path {path} (use a real private directory)"
            raise ValueError(msg)
    if out_dir.exists() and not out_dir.is_dir():
        msg = f"cache out_dir is not a directory: {out_dir}"
        raise ValueError(msg)
    if out_dir.exists() and out_dir.is_symlink():
        msg = f"refusing symlink cache out_dir {out_dir}"
        raise ValueError(msg)


def refuse_repository_output(out_dir: Path) -> None:
    """Refuse to write generated data inside a repark checkout."""
    resolved = out_dir.expanduser().resolve()
    for candidate in (resolved, *resolved.parents):
        if (candidate / "AGENTS.md").is_file() and (candidate / "Cargo.toml").is_file():
            msg = (
                f"refusing to write dataset files inside the repository ({resolved}); "
                "use the cache root or an --out path outside the checkout"
            )
            raise ValueError(msg)


def prepare_output_dir(out_dir: Path, *, root: Path | None = None) -> Path:
    """Validate, create, and re-check ``out_dir``. Returns the expanded path."""
    expanded = out_dir.expanduser()
    refuse_repository_output(expanded)
    check_root = root if root is not None else expanded
    assert_safe_cache_path(check_root.expanduser(), expanded)
    expanded.mkdir(parents=True, exist_ok=True)
    assert_safe_cache_path(check_root.expanduser(), expanded)
    refuse_repository_output(expanded)
    return expanded


def refuse_symlink_file(path: Path) -> None:
    """Refuse to overwrite or follow a symlink at a data-file path.

    ``is_symlink()`` alone (no ``exists()`` pre-check): ``exists()`` follows the
    link, so a DANGLING symlink would slip through and ``open("wb")`` would write
    through it to the link target.
    """
    if path.is_symlink():
        msg = f"refusing symlink data file {path}"
        raise ValueError(msg)
