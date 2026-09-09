"""Eager materialization entry points behind the DataFrame facade."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.errors import IllegalArgumentException

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame

_CACHE_MAX_BYTES_KEY = "repark.cache.max_bytes"


def _cache_conf_lookup(alive_token: dict[str, Any], key: str) -> str | None:
    """Runtime conf then builder snapshot for a cache-related conf key."""
    tomb = alive_token.get("runtime_conf_unset")
    if isinstance(tomb, set) and key in tomb:
        return None
    store = alive_token.get("runtime_conf")
    if isinstance(store, dict):
        raw = store.get(key)
        if raw is not None and str(raw) != "":
            return str(raw)
    builder = alive_token.get("builder_config") or {}
    if isinstance(builder, dict):
        raw = builder.get(key)
        if raw is not None and str(raw) != "":
            return str(raw)
    return None


def _resolve_cache_max_bytes(alive_token: dict[str, Any]) -> int | None:
    """Parse ``repark.cache.max_bytes``; ``None`` when unset or zero (no size guard)."""
    raw = _cache_conf_lookup(alive_token, _CACHE_MAX_BYTES_KEY)
    if raw is None:
        return None
    try:
        value = int(str(raw).strip())
    except ValueError as error:
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {raw!r} in the config "
            f"{_CACHE_MAX_BYTES_KEY!r} is invalid. Expected a non-negative integer byte budget "
            f"(0 = no size guard)."
        ) from error
    if value < 0:
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {raw!r} in the config "
            f"{_CACHE_MAX_BYTES_KEY!r} is invalid. Expected a non-negative integer byte budget "
            f"(0 = no size guard)."
        )
    if value == 0:
        return None
    if value > 0xFFFF_FFFF_FFFF_FFFF:
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {raw!r} in the config "
            f"{_CACHE_MAX_BYTES_KEY!r} is invalid. Expected a non-negative integer byte budget "
            f"fitting u64 (0 = no size guard)."
        )
    return value


def _eager_materialize(frame: DataFrame) -> DataFrame:
    """Materialize the frame plan through the cache view and return a new eager frame."""
    frame._ensure_alive()
    sibling = frame._identity_child()
    sibling._persist_requested = True
    try:
        sibling._materialize_cache_if_needed()
    except IllegalArgumentException as error:
        raise IllegalArgumentException(f".eager() cannot materialize this plan: {error}") from error
    table = sibling.to_arrow()
    sibling._eager_shape = (table.num_rows, table.num_columns)
    return sibling


def _to_lazy(frame: DataFrame) -> DataFrame:
    """Return self when lazy; a shape-less copy over the same view when eager."""
    frame._ensure_alive()
    if frame._eager_shape is None:
        return frame
    child = frame._spawn_preserving_identity(frame._inner)
    child._inner = frame._session.sql(f"SELECT * FROM {frame._cache_view}")
    return child


def _count_rows(frame: DataFrame) -> int:
    """Return the row count, reusing a known eager shape instead of querying."""
    frame._ensure_alive()
    shape = frame._eager_shape
    if shape is not None:
        return shape[0]
    return frame._action_inner().count()
