"""SparkContext, RuntimeConfig."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

from typing import Any

from repark.spark.session import _funcs as _session_funcs
from repark.spark.session.session_time_zone import warn_runtime_session_time_zone_not_applied
from repark.spark.session.timestamp_type import TIMESTAMP_TYPE_KEY, parse_timestamp_type

for _name in dir(_session_funcs):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_session_funcs, _name)
del _name, _session_funcs


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

    The session timezone is build-time too: a runtime set/unset is accepted, warned
    once, and not applied (PySpark's ``sql_conf`` context manager sets this key, so a
    raise would break a drop-in script). The value is deliberately NOT stored, so
    ``conf.get`` reports the zone the live engine session really has (default ``UTC``).
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
        self._session._ensure_alive()
        if not isinstance(key, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={"arg_name": "key", "arg_type": type(key).__name__},
            )
        if value is None:
            raise IllegalArgumentException(f"value cannot be None for config key {key!r}")
        if key in _SQLCONF_STATIC_KEYS:
            raise Exception(f"Cannot modify the value of static config: {key}")
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
        # The zone is resolved once at session build. PySpark scripts (and Apache's `sql_conf`
        # context manager) set this key at runtime, so the call is accepted but NOT stored —
        # `conf.get` keeps reporting the zone the live engine session actually has. Warns once.
        if warn_runtime_session_time_zone_not_applied(key, stacklevel=3):
            return
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
        self._unset_keys().discard(key)
        self._store()[key] = text

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
        # Honor the unset tomb for display style before any store read, so get and getAll agree.
        if key.lower() == _DISPLAY_STYLE_KEY:
            if self._display_style_is_unset():
                if default is not _CONF_GET_UNSET:
                    return default  # type: ignore[return-value]
                return _DEFAULT_DISPLAY_STYLE
            return str(self._session._alive_token.get("display_style", _DEFAULT_DISPLAY_STYLE))
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

    def unset(self, key: str) -> None:
        """Remove a configuration property (runtime store + builder-fallback tombstone).

        ``repark.display.style`` also resets the live session style to the default
        ``spark`` so ``conf.get`` / ``session.display_style`` / ``show()`` stay lockstep.
        """
        self._session._ensure_alive()
        # The zone always has a value (resolved at build), so there is nothing to unset;
        # tombstoning would make conf.get report a zone the live session does not have.
        # Accepted, warned once, no state change — same as `set`.
        if warn_runtime_session_time_zone_not_applied(key, stacklevel=3):
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
            self._session._alive_token["display_style"] = _DEFAULT_DISPLAY_STYLE
            # Snapshot matches default so a later reuse without explicit style cannot
            # re-absorb a prior non-default from the builder map.
            _sync_display_style_into_builder_config(
                self._session._builder_config, _DEFAULT_DISPLAY_STYLE
            )
            return
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
        return key not in _SQLCONF_STATIC_KEYS
