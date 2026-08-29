"""Active-session state and drop-in warnings."""

from __future__ import annotations

import types
import sys
import warnings

from typing import Any


_active_session: ReparkSession | None = None


_master_warned = False


_unbounded_batch_warned = False

_MUTABLE_STATE_NAMES = frozenset({"_active_session", "_master_warned", "_unbounded_batch_warned"})
_MUTABLE_OWNER_MODULES = {
    "_ARRAY_TYPECODES_SUPPORTED": "repark.spark.session.create_dataframe_values",
}


class _SessionStateProxyModule(types.ModuleType):
    def __getattribute__(self, name: str) -> Any:
        """Read mutable compatibility state from its owning module."""
        if name in _MUTABLE_STATE_NAMES:
            return globals()[name]
        if owner_name := _MUTABLE_OWNER_MODULES.get(name):
            return getattr(sys.modules[owner_name], name)
        return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Write mutable compatibility state to its owning module."""
        if name in _MUTABLE_STATE_NAMES:
            globals()[name] = value
            return
        if owner_name := _MUTABLE_OWNER_MODULES.get(name):
            setattr(sys.modules[owner_name], name, value)
            return
        super().__setattr__(name, value)


def _install_state_proxy(module: types.ModuleType) -> None:
    """Route legacy module attributes to the extracted state owner."""
    module.__class__ = _SessionStateProxyModule


def _config_value_error(key: str, value: int, requirement: str) -> str:
    """Render an out-of-range config refusal in live Spark 4.1.2's message shape (SAF-006).

    Spark 4.1.2 raises ``IllegalArgumentException`` for a rejected ``SQLConf`` value using the
    ``INVALID_CONF_VALUE.REQUIREMENT`` error class. Captured live (zulu-17, pyspark 4.1.2) for
    ``spark.conf.set("spark.sql.shuffle.partitions", "0")``::

        [INVALID_CONF_VALUE.REQUIREMENT] The value '0' in the config

        "spark.sql.shuffle.partitions" is invalid. The value of spark.sql.shuffle.partitions

        must be positive SQLSTATE: 22022

    repark emits that verbatim with **two recorded deltas**:

    1. the trailing ``SQLSTATE: 22022`` is omitted — no repark error carries SQLSTATE
       (cf. ``[INVALID_SAVE_MODE]`` / ``[AMBIGUOUS_REFERENCE]``);
    2. the repark-native key spellings (``repark.target.partitions`` /
       ``repark.memory.limit.gb``) have no Spark counterpart at all, so for those the same
       shape is emitted with the repark key substituted — Spark would silently ignore them.

    ``requirement`` is the ``<confRequirement>`` sub-class payload and is quoted verbatim from
    Spark where one exists (``The value of spark.sql.shuffle.partitions must be positive`` —
    byte-checked in ``SQLConf$.class``; note Spark has **no** trailing period there).
    """

    return (
        f"[INVALID_CONF_VALUE.REQUIREMENT] The value '{value}' in the config "
        f'"{key}" is invalid. {requirement}'
    )


def _to_str(value: Any) -> str | None:
    """Spark ``pyspark.sql.utils.to_str`` (4.1.2): bool → lowercase, ``None`` stays ``None``.

    Live Spark does **not** use bare ``str(...)`` for ``Builder.config`` map/kv values: bools
    become ``"true"`` / ``"false"`` (not ``"True"`` / ``"False"``), and ``None`` is stored
    as ``None`` rather than the string ``"None"`` (so an engine-knob lookup treats it as unset
    instead of failing ``int("None")``). Integers and other types still go through ``str(...)``.
    """

    if isinstance(value, bool):
        return str(value).lower()

    if value is None:
        return None

    return str(value)


def _reset_dropin_warnings_for_tests() -> None:
    """Test helper: re-arm the process-once drop-in disclosure warnings (OTH-010 / SAF-006)."""

    global _master_warned, _unbounded_batch_warned

    _master_warned = False

    _unbounded_batch_warned = False


def _warn_unbounded_batch_once(key: str, value: int, *, stacklevel: int = 2) -> None:
    """Emit the SAF-006 "no limit" batch-size disclosure at most once per process."""

    global _unbounded_batch_warned

    if _unbounded_batch_warned:
        return

    warnings.warn(
        f"config {key!r} = {value} means 'no limit' in Spark; repark accepts it but cannot honor "
        "it (DataFusion always emits bounded Arrow batches), so the engine default batch size is "
        "used instead. Set a positive value to control batching (SAF-006).",
        UserWarning,
        stacklevel=stacklevel,
    )

    _unbounded_batch_warned = True


def _late_catalog_names(keys: set[str]) -> set[str]:
    """Catalog names appearing in ``spark.sql.catalog.*`` keys (the reuse-path warning helper)."""

    prefix = "spark.sql.catalog."

    return {k[len(prefix) :].split(".", 1)[0] for k in keys if k.lower().startswith(prefix)}


def _warn_master_once(*, stacklevel: int = 2) -> None:
    """Emit the OTH-010 single-node master warning at most once per process."""

    global _master_warned

    if _master_warned:
        return

    warnings.warn(
        "Spark master URL is accepted for source compatibility but ignored; "
        "repark runs single-node (distribution is deferred, OTH-010).",
        UserWarning,
        stacklevel=stacklevel,
    )

    _master_warned = True


_STOPPED_MESSAGE = "Cannot call methods on a stopped ReparkSession"


def _reset_active_session_for_tests() -> None:
    """Test helper: clear the process-wide active session without requiring a live handle."""

    global _active_session

    if _active_session is not None:
        # Mirror production stop so held SC/DF tokens die.

        _active_session._spark_context._mark_stopped()  # type: ignore[attr-defined]

        _active_session._alive_token["alive"] = False  # type: ignore[attr-defined]

        _active_session._inner = None  # type: ignore[attr-defined]

        _active_session = None
