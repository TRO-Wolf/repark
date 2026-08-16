"""Torture-dataset generators (checked in; data files stay in the cache root).

Import this tree as ``repark_datasets`` via the bench sys.modules loader — it is
not part of the hatch ``repark_parity`` package. See ``datasets/map.md``.
"""

from __future__ import annotations

from ._cache import KNOWN_FAMILIES, default_datasets_root, family_cache_dir

__all__ = [
    "KNOWN_FAMILIES",
    "default_datasets_root",
    "family_cache_dir",
]
