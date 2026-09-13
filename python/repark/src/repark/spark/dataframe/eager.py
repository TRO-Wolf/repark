"""Eager materialization entry points behind the DataFrame facade."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.errors import IllegalArgumentException

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame

_CACHE_MAX_BYTES_KEY = "repark.cache.max_bytes"
_CACHE_MAX_TOTAL_BYTES_KEY = "repark.cache.max_total_bytes"


def _cache_conf_lookup(alive_token: dict[str, Any], key: str) -> str | None:
    """Runtime conf then builder snapshot for a cache-related conf key.

    Matching is case-insensitive like the ``repark.cache.retained_bytes``
    intercept: any spelling in the unset tomb disables the key, and when two
    spellings coexist the last one set wins (runtime layer over builder).
    """
    lowered = key.lower()
    tomb = alive_token.get("runtime_conf_unset")
    if isinstance(tomb, set) and any(entry.lower() == lowered for entry in tomb):
        return None
    for layer in (
        alive_token.get("runtime_conf"),
        alive_token.get("builder_config") or {},
    ):
        if not isinstance(layer, dict):
            continue
        for stored_key in reversed(list(layer)):
            if stored_key.lower() == lowered:
                raw = layer[stored_key]
                if raw is not None and str(raw) != "":
                    return str(raw)
                break
    return None


def _resolve_cache_byte_budget(alive_token: dict[str, Any], key: str) -> int | None:
    """Parse a cache byte-budget conf key; ``None`` when unset or zero (no guard)."""
    raw = _cache_conf_lookup(alive_token, key)
    if raw is None:
        return None
    try:
        value = int(str(raw).strip())
    except ValueError as error:
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {raw!r} in the config "
            f"{key!r} is invalid. Expected a non-negative integer byte budget "
            f"(0 = no size guard)."
        ) from error
    if value < 0:
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {raw!r} in the config "
            f"{key!r} is invalid. Expected a non-negative integer byte budget "
            f"(0 = no size guard)."
        )
    if value == 0:
        return None
    if value > 0xFFFF_FFFF_FFFF_FFFF:
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {raw!r} in the config "
            f"{key!r} is invalid. Expected a non-negative integer byte budget "
            f"fitting u64 (0 = no size guard)."
        )
    return value


def _resolve_cache_max_bytes(alive_token: dict[str, Any]) -> int | None:
    """Parse ``repark.cache.max_bytes``; ``None`` when unset or zero (no size guard)."""
    return _resolve_cache_byte_budget(alive_token, _CACHE_MAX_BYTES_KEY)


def _resolve_cache_max_total_bytes(alive_token: dict[str, Any]) -> int | None:
    """Parse ``repark.cache.max_total_bytes``; ``None`` when unset or zero (no budget)."""
    return _resolve_cache_byte_budget(alive_token, _CACHE_MAX_TOTAL_BYTES_KEY)


def _resolve_cache_budgets(alive_token: dict[str, Any]) -> tuple[int | None, int | None]:
    """``(max_bytes, max_total_bytes)`` limits for one cache materialization."""
    return (
        _resolve_cache_max_bytes(alive_token),
        _resolve_cache_max_total_bytes(alive_token),
    )


def _eager_materialize(frame: DataFrame) -> DataFrame:
    """Materialize the frame plan through the cache view and return a new eager frame."""
    from repark.spark.dataframe import cache_handle

    frame._ensure_alive()
    if (
        frame._eager_shape is not None
        and frame._cache_view is not None
        and cache_handle.find_live_handle(frame, frame._cache_view) is not None
    ):
        sibling = frame._identity_child()
        sibling._persist_requested = True
        sibling._lineage_inner = sibling._inner
        sibling._cache_view = frame._cache_view
        sibling._eager_shape = frame._eager_shape
        from repark.spark.dataframe.core import _register_cache_frame

        _register_cache_frame(frame._alive_token, sibling)
        return sibling
    sibling = frame._identity_child()
    sibling._persist_requested = True
    try:
        sibling._materialize_cache_if_needed()
    except IllegalArgumentException as error:
        raise IllegalArgumentException(f".eager() cannot materialize this plan: {error}") from error
    sibling._eager_shape = (sibling._action_inner().count(), len(sibling.columns))
    return sibling


def _to_lazy(frame: DataFrame) -> DataFrame:
    """Return self when lazy; a shape-less copy over the same view when eager."""
    frame._ensure_alive()
    if frame._eager_shape is None:
        return frame
    return frame._spawn_preserving_identity(frame._inner)


def _count_rows(frame: DataFrame) -> int:
    """Return the row count, reusing a known eager shape instead of querying."""
    frame._ensure_alive()
    frame._materialize_cache_if_needed()
    shape = frame._eager_shape
    if shape is not None:
        return shape[0]
    return frame._action_inner().count()
