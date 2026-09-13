"""Refcounted ownership of ``__repark_cache_*`` MemTable registrations (EAGER-OWN-1).

The frame whose materialize registered a cache view owns one :class:`CacheViewHandle`;
every frame whose plan scans the view carries it in its ``_handles`` tuple. The
registration dies with the last holder (the ``weakref.finalize`` below) or by an
explicit ``release()`` from ``unpersist`` / ``clearCache`` / checkpoint truncation,
whichever comes first. ``__repark_ckpt_*`` views are a different family and stay
outside this ownership model.
"""

from __future__ import annotations

import logging
import warnings
import weakref
from typing import Any

_LOGGER = logging.getLogger(__name__)


def _drop_cache_view_registration(
    session: Any,
    alive_token: dict[str, Any],
    view_name: str,
) -> None:
    """Finalizer callback: drop the registration unless the session already stopped."""
    if not alive_token.get("alive", True):
        return
    try:
        session.drop_temp_view(view_name)
    except Exception:
        _LOGGER.debug("cache-view finalizer could not drop %s", view_name, exc_info=True)


class CacheViewHandle:
    """Shared ownership of one ``__repark_cache_*`` view registration.

    Frames hold this handle in ``_handles``; the registering frame also keeps it in
    ``_cache_view_owned_handle`` so ``unpersist`` can tell owner from sharing
    wrapper. The finalizer never touches a stopped session and never raises out of
    a GC callback — explicit ``release()`` errors still propagate.
    """

    __slots__ = (
        "__weakref__",
        "_alive_token",
        "_finalizer",
        "_released",
        "_session",
        "_view_name",
    )

    def __init__(
        self,
        session: Any,
        alive_token: dict[str, Any],
        view_name: str,
    ) -> None:
        """Create the handle and register it for ``Catalog.clearCache`` enumeration."""
        self._session = session
        self._alive_token = alive_token
        self._view_name = view_name
        self._released = False
        self._finalizer = weakref.finalize(
            self, _drop_cache_view_registration, session, alive_token, view_name
        )
        self._finalizer.atexit = False
        registry = alive_token.get("cache_view_handles")
        if not isinstance(registry, weakref.WeakSet):
            registry = weakref.WeakSet()
            alive_token["cache_view_handles"] = registry
        registry.add(self)

    @property
    def view_name(self) -> str:
        """The home-qualified name of the owned cache view."""
        return self._view_name

    @property
    def released(self) -> bool:
        """Whether the registration was already dropped explicitly."""
        return self._released

    def release(self) -> None:
        """Drop the registration and disarm the finalizer. Idempotent; errors propagate."""
        if self._released:
            return
        self._finalizer.detach()
        if self._alive_token.get("alive", True):
            self._session.drop_temp_view(self._view_name)
        self._released = True


def union_handles(own: tuple[Any, ...], other: tuple[Any, ...]) -> tuple[Any, ...]:
    """Union two handle tuples, deduplicated by identity, order-stable."""
    merged = list(own)
    for handle in other:
        if handle not in merged:
            merged.append(handle)
    return tuple(merged)


def find_live_handle(frame: Any, view_name: str) -> CacheViewHandle | None:
    """Return the unreleased handle backing ``view_name`` on ``frame``, if any."""
    for handle in frame._handles:
        if handle.view_name == view_name and not handle.released:
            return handle
    return None


def bind_registered_view(frame: Any, view_name: str, lineage: Any) -> None:
    """Point ``frame`` at a freshly registered cache view and adopt its handle.

    A failed ``SELECT *`` over the new view drops the registration, so a
    post-registration failure leaves no orphan view and no live handle.
    """
    try:
        frame._inner = frame._session.sql(f"SELECT * FROM {view_name}")
    except Exception:
        frame._session.drop_temp_view(view_name)
        raise
    frame._lineage_inner = lineage
    frame._cache_view = view_name
    handle = CacheViewHandle(frame._session, frame._alive_token, view_name)
    frame._cache_view_owned_handle = handle
    frame._handles = (*frame._handles, handle)


def release_view_hold(frame: Any, view_name: str) -> None:
    """Release ``frame``'s hold on ``view_name``; the owner also drops the registration."""
    owned = frame._cache_view_owned_handle
    if owned is not None and owned.view_name == view_name:
        frame._cache_view_owned_handle = None
        owned.release()
    frame._handles = tuple(handle for handle in frame._handles if handle.view_name != view_name)


def _warn_storage_level_cosmetic_once(
    alive_token: dict[str, Any],
    level: Any,
    *,
    stacklevel: int = 3,
) -> None:
    """Warn once per session when StorageLevel flags claim disk/off-heap/replication.

    repark always pins to an in-process MemTable; those flags are signature parity only
    ``MEMORY_ONLY`` with replication 1 is honest and does not warn.
    """
    if alive_token.get("storage_level_cosmetic_warned"):
        return
    use_disk = bool(getattr(level, "useDisk", False))
    use_off_heap = bool(getattr(level, "useOffHeap", False))
    try:
        replication = int(getattr(level, "replication", 1))
    except (TypeError, ValueError):
        replication = 1
    if not (use_disk or use_off_heap or replication != 1):
        return
    warnings.warn(
        "repark StorageLevel disk / off-heap / replication flags are accepted for PySpark "
        "signature parity but ignored — cache/persist always materializes to a single-node "
        "in-process MemTable (no disk spill, no off-heap, no replication). "
        "Set repark.cache.max_bytes to refuse oversized materialize (OTH-005/014).",
        UserWarning,
        stacklevel=stacklevel,
    )
    alive_token["storage_level_cosmetic_warned"] = True
