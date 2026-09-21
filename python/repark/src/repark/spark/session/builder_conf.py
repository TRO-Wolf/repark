"""SparkContext, RuntimeConfig."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

from typing import Any

from repark import _native
from repark.spark.session import _funcs as _session_funcs
from repark.spark.session.session_configuration import (
    PARTITION_OVERWRITE_MODE_KEY,
    SPARK_SQL_ANSI_ENABLED_KEY,
    SPARK_SQL_CASE_SENSITIVE_KEY,
    MERGE_SCHEMA_KEY,
    WAP_SESSION_KEYS,
    _DISPLAY_INT_DEFAULTS,
    _RETAINED_CACHE_BYTES_KEY,
    _SQLCONF_DEFAULTS,
    _builder_display_int,
    _display_token_key,
    _normalize_display_int,
    _refuse_read_only_conf_key,
    _retained_cache_bytes_value,
    _sync_display_int_into_builder_config,
    is_iceberg_session_write_key,
)
from repark.spark.session.session_time_zone import (
    SESSION_TIME_ZONE_KEY,
    refresh_session_zone_canonical,
)
from repark.spark.session.timestamp_type import TIMESTAMP_TYPE_KEY, parse_timestamp_type

for _name in dir(_session_funcs):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_session_funcs, _name)
del _name, _session_funcs

_CACHE_BYTE_BUDGET_KEYS_LOWER: frozenset[str] = frozenset(
    {"repark.cache.max_bytes", "repark.cache.max_total_bytes"}
)

_CATALOG_KEY_PREFIXES: tuple[str, str] = ("spark.sql.catalog.", "repark.sql.catalog.")


def _late_catalog_block_name(key: str) -> str | None:
    """The catalog name a ``spark.sql.catalog.*`` key belongs to, else ``None``."""
    for prefix in _CATALOG_KEY_PREFIXES:
        if not key.startswith(prefix):
            continue
        rest = key[len(prefix) :]
        name = rest.split(".", 1)[0]
        return name or None
    return None


def _late_catalog_block_keys(store: dict[str, str], name: str) -> dict[str, str]:
    """The accumulated runtime block for one catalog name (both key spellings)."""
    block: dict[str, str] = {}
    for key, value in store.items():
        for prefix in _CATALOG_KEY_PREFIXES:
            if key == f"{prefix}{name}" or key.startswith(f"{prefix}{name}."):
                block[key] = value
    return block


class SparkContext:
    """Minimal ``spark.sparkContext`` surface for near-drop-in jobs.

    Production scripts touch this for logging and identity: :meth:`setLogLevel` is a
    silent accepted no-op (OTH-010; engine logging is ``tracing``), :attr:`applicationId`
    is a stable per-session id, :attr:`master` echoes the builder's ``spark.master``
    (default ``local[repark]``). After :meth:`ReparkSession.stop`, every member raises
    :class:`RuntimeError`; any other attribute raises :class:`AttributeError` naming
    the gap (full SparkContext is out of scope).
    """

    __slots__ = ("_alive", "_application_id", "_master")

    def __init__(self, *, application_id: str, master: str) -> None:
        """Bind identity fields for this session's context handle."""
        self._application_id = application_id
        self._master = master
        self._alive = True

    def _ensure_alive(self) -> None:
        """Raise if the owning session has been stopped."""
        if not self._alive:
            raise RuntimeError(_STOPPED_MESSAGE)

    def _mark_stopped(self) -> None:
        """Invalidate this handle (called from :meth:`ReparkSession.stop`)."""
        self._alive = False

    def setLogLevel(self, level: str) -> None:  # noqa: N802 — PySpark camelCase
        """Accept ``setLogLevel`` for source compatibility; silent no-op (OTH-010).

        repark does not wire JVM log4j levels; see ``docs/spark-sql-iceberg-parity.md`` §8.
        """
        self._ensure_alive()
        _ = level  # accepted, ignored

    @property
    def applicationId(self) -> str:  # noqa: N802 — PySpark camelCase
        """Stable per-session application id (PySpark ``spark.sparkContext.applicationId``)."""
        self._ensure_alive()
        return self._application_id

    @property
    def master(self) -> str:
        """Master URL recorded on the builder (single-node; default ``local[repark]``)."""
        self._ensure_alive()
        return self._master

    def __getattr__(self, name: str) -> Any:
        """Fail loud on any SparkContext surface beyond the three implemented members."""
        # Prefer stopped-session errors over gap AttributeError when the handle is dead.
        self._ensure_alive()
        # Deliberately a bare AttributeError: PySpark's SparkContext HAS these attributes, so
        # there is no PySpark raise to mirror — a repark scope gap, not the user-misuse class
        # PySparkAttributeError models.
        raise AttributeError(
            f"repark SparkContext has no attribute {name!r} "
            f"(only setLogLevel / applicationId / master are implemented; "
            f"full SparkContext is out of scope)"
        )


class RuntimeConfig:
    """Facade runtime configuration (PySpark ``SparkSession.conf`` / ``RuntimeConfig``).

    Stores string values on the session's alive-token conf map; not a full SQLConf.

    ``datafusion.*`` keys are forwarded to the live DataFusion session via ``SET``
    (refuse-loud on rejection). Keys must be canonical lowercase paths; mixed-case /
    padded lookalikes refuse-loud so the facade never keeps a silent store-only twin.

    The memory pool has one truth: build-time ``repark.memory.limit.gb`` (or
    ``builder.config``) installs the FairSpillPool, fixed at ``getOrCreate`` — a runtime
    set refuses loud (the live pool would not move). Runtime
    ``datafusion.runtime.memory_limit`` swaps in a new FairSpillPool of that size.
    Setting both on the same builder refuses loud (ambiguous initial size).
    ``datafusion.runtime.temp_directory`` is build-time only; a runtime set refuses
    loud and names ``TMPDIR`` (the DiskManager is fixed after ``build()``).

    ``spark.sql.ansi.enabled``, ``spark.sql.session.timeZone``, and
    ``spark.sql.caseSensitive`` apply to the live session immediately: the value is validated
    in Rust and written to the running engine, so fresh queries answer it while frames built
    before the set keep the snapshot they were analysed under. An invalid value refuses
    before anything is stored.
    """

    __slots__ = ("_session",)

    def __init__(self, session: ReparkSession) -> None:
        self._session = session

    def _store(self) -> dict[str, str]:
        token = self._session._alive_token
        store = token.get("runtime_conf")
        if not isinstance(store, dict):
            store = {}
            token["runtime_conf"] = store
        return store

    def _unset_keys(self) -> set[str]:
        """Keys explicitly :meth:`unset` (tombstones over builder snapshot fallback)."""
        token = self._session._alive_token
        tomb = token.get("runtime_conf_unset")
        if not isinstance(tomb, set):
            tomb = set()
            token["runtime_conf_unset"] = tomb
        return tomb

    def set(self, key: str, value: str | int | bool) -> None:
        """Set a configuration property (coerced to ``str``).

        Bool → ``"true"`` / ``"false"`` (Spark parity). ``None`` raises
        :class:`~repark.errors.IllegalArgumentException`; other non-str/int/bool types
        raise :class:`Exception` (Apache ``test_conf_with_python_objects``).
        ``datafusion.`` keys are forwarded to the engine; unknown DF keys and
        non-canonical lookalikes raise. Runtime ``repark.memory.limit.gb`` refuses
        (use ``datafusion.runtime.memory_limit`` to re-size the pool). Setting
        ``repark.display.style`` drives the live session's display style.
        """
        inner = self._session._ensure_alive()
        if not isinstance(key, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={"arg_name": "key", "arg_type": type(key).__name__},
            )
        if value is None:
            raise IllegalArgumentException(f"value cannot be None for config key {key!r}")
        if key in _SQLCONF_STATIC_KEYS:
            raise Exception(f"Cannot modify the value of static config: {key}")
        if key.lower() == _RETAINED_CACHE_BYTES_KEY:
            _refuse_read_only_conf_key(key)
        # A collation SQLConf key would otherwise be stored and ignored.
        from repark.spark.types import refuse_collation_session_key

        refuse_collation_session_key(key)
        if key == TIMESTAMP_TYPE_KEY:
            if isinstance(value, bool):
                text = parse_timestamp_type("true" if value else "false")
            else:
                text = parse_timestamp_type(str(value))
            self._unset_keys().discard(key)
            self._store()[key] = text
            return
        if isinstance(value, bool):
            text = "true" if value else "false"
        elif isinstance(value, (str, int)):
            text = str(value)
        else:
            # Decimal / arbitrary objects: Spark refuses; keep class broad for Apache assert.
            raise Exception(
                f"value type {type(value).__name__} is not supported for config key {key!r}"
            )
        # Build-time FairSpillPool size is not runtime-mutable via conf (one truth).
        _refuse_runtime_memory_limit_gb(key)
        if key in (
            SESSION_TIME_ZONE_KEY,
            SPARK_SQL_ANSI_ENABLED_KEY,
            SPARK_SQL_CASE_SENSITIVE_KEY,
        ):
            _native.set_runtime_config(inner, key, text)
            if key == SESSION_TIME_ZONE_KEY:
                refresh_session_zone_canonical(self._session)
        if key in (PARTITION_OVERWRITE_MODE_KEY, MERGE_SCHEMA_KEY) or key in WAP_SESSION_KEYS:
            _native.set_runtime_config(inner, key, text)
        if is_iceberg_session_write_key(key):
            _native.set_runtime_config(inner, key, text)
        if _looks_like_datafusion_conf_key(key):
            _forward_datafusion_conf(self._session, key, text)
        # conf.set("repark.display.style", …) must drive show() — not only the conf map.
        if key.lower() == _DISPLAY_STYLE_KEY:
            style = normalize_display_style(text)
            self._unset_keys().discard(_DISPLAY_STYLE_KEY)
            store = self._store()
            for existing in list(store):
                if existing.lower() == _DISPLAY_STYLE_KEY:
                    del store[existing]
            store[_DISPLAY_STYLE_KEY] = style
            self._session._alive_token["display_style"] = style
            _sync_display_style_into_builder_config(self._session._builder_config, style)
            return
        for canonical in _DISPLAY_INT_DEFAULTS:
            if key.lower() != canonical:
                continue
            parsed = _normalize_display_int(canonical, text)
            self._unset_keys().discard(canonical)
            store = self._store()
            for existing in list(store):
                if existing.lower() == canonical:
                    del store[existing]
            store[canonical] = str(parsed)
            self._session._alive_token[_display_token_key(canonical)] = parsed
            _sync_display_int_into_builder_config(self._session._builder_config, canonical, parsed)
            return
        lowered = key.lower()
        if lowered in _CACHE_BYTE_BUDGET_KEYS_LOWER:
            tombs = self._unset_keys()
            for existing in list(tombs):
                if existing.lower() == lowered:
                    tombs.discard(existing)
            store = self._store()
            for existing in list(store):
                if existing.lower() == lowered:
                    del store[existing]
            store[key] = text
            return
        self._unset_keys().discard(key)
        self._store()[key] = text
        catalog_name = _late_catalog_block_name(key)
        if catalog_name is not None:
            self._register_late_catalog_block(catalog_name)

    def _register_late_catalog_block(self, name: str) -> None:
        """Offer one name's accumulated block to the live session (H-02).

        Incomplete blocks stay silent; a first complete parse registers and later
        sets only update the catalog side map. A complete block that cannot build
        raises from the engine.
        """
        store = {
            key: value for key, value in self._store().items() if key not in self._unset_keys()
        }
        if _native.register_late_catalog_block(
            self._session._ensure_alive(), _late_catalog_block_keys(store, name)
        ):
            self._session._note_registered_catalog(name)

    def get(
        self,
        key: str,
        default: str | None | object = _CONF_GET_UNSET,
    ) -> str | None:
        """Get a configuration property.

        When ``default`` is omitted and the key is unset in both the runtime store and
        the builder snapshot, raises :class:`Exception` naming the key (Apache
        ``test_conf``). Explicit ``default=None`` returns ``None`` for an unset key.
        Keys previously :meth:`unset` stay unset even if the builder snapshot still
        carries them. ``repark.display.style`` reads the live session display style.
        """
        self._session._ensure_alive()
        if not isinstance(key, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={"arg_name": "key", "arg_type": type(key).__name__},
            )
        if key.lower() == _RETAINED_CACHE_BYTES_KEY:
            return _retained_cache_bytes_value(self._session)
        # Honor the unset tomb for display style before any store read, so get and getAll agree.
        if key.lower() == _DISPLAY_STYLE_KEY:
            if self._display_style_is_unset() and default is not _CONF_GET_UNSET:
                return default  # type: ignore[return-value]
            return str(self._session._alive_token.get("display_style", _DEFAULT_DISPLAY_STYLE))
        for canonical, fallback in _DISPLAY_INT_DEFAULTS.items():
            if key.lower() != canonical:
                continue
            if self._display_int_is_unset(canonical):
                if default is not _CONF_GET_UNSET:
                    return default  # type: ignore[return-value]
                return str(fallback)
            token_value = self._session._alive_token.get(_display_token_key(canonical))
            if isinstance(token_value, bool) or not isinstance(token_value, int):
                return str(_builder_display_int(self._session._builder_config, canonical, fallback))
            return str(token_value)
        if key in self._unset_keys():
            if default is not _CONF_GET_UNSET:
                return default  # type: ignore[return-value]
            if key in _SQLCONF_DEFAULTS:
                return _SQLCONF_DEFAULTS[key]
            raise Exception(f"Configuration property {key} is not set.")
        store = self._store()
        if key in store:
            return store[key]
        # Fall back to builder config snapshot (immutable build-time values).
        builder = self._session._builder_config
        if key in builder and builder[key] is not None:
            return builder[key]
        # Explicit default (including None) wins over SQLConf static defaults —
        # matches Spark: get(key, None) is None even when SQLConf has a default.
        if default is not _CONF_GET_UNSET:
            return default  # type: ignore[return-value]
        if key in _SQLCONF_DEFAULTS:
            return _SQLCONF_DEFAULTS[key]
        raise Exception(f"Configuration property {key} is not set.")

    def _display_style_is_unset(self) -> bool:
        """True when ``repark.display.style`` was :meth:`unset` (case-insensitive tomb)."""
        return any(tomb.lower() == _DISPLAY_STYLE_KEY for tomb in self._unset_keys())

    def _display_int_is_unset(self, canonical: str) -> bool:
        """True when a ``repark.display.*`` int key was :meth:`unset` (case-insensitive tomb)."""
        return any(tomb.lower() == canonical for tomb in self._unset_keys())

    def unset(self, key: str) -> None:
        """Remove a configuration property (runtime store + builder-fallback tombstone).

        ``repark.display.style`` also resets the live session style to the default
        ``polars`` so ``conf.get`` / ``session.display_style`` / ``show()`` stay lockstep.
        """
        inner = self._session._ensure_alive()
        if isinstance(key, str) and key.lower() == _RETAINED_CACHE_BYTES_KEY:
            _refuse_read_only_conf_key(key)
        if key in WAP_SESSION_KEYS:
            self._store().pop(key, None)
            self._unset_keys().add(key)
            _native.set_runtime_config(inner, key, "")
            return
        if key == MERGE_SCHEMA_KEY:
            self._store().pop(key, None)
            self._unset_keys().add(key)
            _native.set_runtime_config(inner, key, "false")
            return
        if key in (
            SESSION_TIME_ZONE_KEY,
            SPARK_SQL_ANSI_ENABLED_KEY,
            SPARK_SQL_CASE_SENSITIVE_KEY,
            PARTITION_OVERWRITE_MODE_KEY,
        ):
            self._store().pop(key, None)
            self._unset_keys().add(key)
            _native.set_runtime_config(inner, key, _SQLCONF_DEFAULTS[key])
            if key == SESSION_TIME_ZONE_KEY:
                refresh_session_zone_canonical(self._session)
            return
        if key.lower() == _DISPLAY_STYLE_KEY:
            store = self._store()
            for existing in list(store):
                if existing.lower() == _DISPLAY_STYLE_KEY:
                    del store[existing]
            # Canonical tomb (case-insensitive get/getAll honor via _display_style_is_unset).
            tombs = self._unset_keys()
            for existing in list(tombs):
                if existing.lower() == _DISPLAY_STYLE_KEY:
                    tombs.discard(existing)
            tombs.add(_DISPLAY_STYLE_KEY)
            self._session._alive_token["display_style"] = default_display_style()
            # Snapshot matches default so a later reuse without explicit style cannot
            # re-absorb a prior non-default from the builder map.
            _sync_display_style_into_builder_config(
                self._session._builder_config, default_display_style()
            )
            return
        for canonical, fallback in _DISPLAY_INT_DEFAULTS.items():
            if key.lower() != canonical:
                continue
            store = self._store()
            for existing in list(store):
                if existing.lower() == canonical:
                    del store[existing]
            tombs = self._unset_keys()
            for existing in list(tombs):
                if existing.lower() == canonical:
                    tombs.discard(existing)
            tombs.add(canonical)
            self._session._alive_token[_display_token_key(canonical)] = fallback
            _sync_display_int_into_builder_config(
                self._session._builder_config, canonical, fallback
            )
            return
        if is_iceberg_session_write_key(key):
            _native.unset_runtime_config(inner, key)
        self._store().pop(key, None)
        # Tombstone so get/getAll do not resurrect the builder snapshot value
        # (Spark SQLConf unset removes the entry entirely).
        self._unset_keys().add(key)

    @property
    def getAll(self) -> dict[str, str]:  # noqa: N802 — PySpark camelCase property
        """All known configuration entries (defaults + builder + runtime).

        Runtime values win on key collision. Explicitly :meth:`unset` keys are omitted
        even when still present on the builder snapshot. Always non-empty via
        ``_SQLCONF_DEFAULTS``. Secret-shaped keys have their **values** replaced with
        ``***`` (keys remain visible); explicit :meth:`get` of a named secret key
        returns the real value so intentional lookups still work.
        """
        self._session._ensure_alive()
        tomb = self._unset_keys()
        merged: dict[str, str] = dict(_SQLCONF_DEFAULTS)
        for key, value in self._session._builder_config.items():
            if value is not None and key not in tomb:
                merged[key] = value
        for key, value in self._store().items():
            if key not in tomb:
                merged[key] = value
        merged[_RETAINED_CACHE_BYTES_KEY] = _retained_cache_bytes_value(self._session)
        return {
            key: ("***" if _prop_key_is_secret(key) else value) for key, value in merged.items()
        }

    def isModifiable(self, key: str) -> bool:  # noqa: N802 — PySpark camelCase
        """Return whether ``key`` can be set at runtime (Spark static-conf subset)."""
        self._session._ensure_alive()
        if not isinstance(key, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={"arg_name": "key", "arg_type": type(key).__name__},
            )
        return key not in _SQLCONF_STATIC_KEYS and key.lower() != _RETAINED_CACHE_BYTES_KEY
