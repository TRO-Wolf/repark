"""Session configuration validation and forwarding."""

from __future__ import annotations

import logging
import os

import re
from typing import Any

from typing import TYPE_CHECKING

from repark.errors import IllegalArgumentException
from repark.spark._idents import sql_string_literal

from repark.spark.session.session_state import _config_value_error, _warn_unbounded_batch_once
from repark.spark.session.session_time_zone import DEFAULT_SESSION_TIME_ZONE, SESSION_TIME_ZONE_KEY

from repark.spark.session.timestamp_type import DEFAULT_TIMESTAMP_TYPE, TIMESTAMP_TYPE_KEY


if TYPE_CHECKING:
    from repark.spark.session.session_core import Builder


_CONF_GET_UNSET: object = object()


_SQLCONF_DEFAULTS: dict[str, str] = {
    "spark.sql.sources.partitionOverwriteMode": "STATIC",
    # Default app name where we control the default (Spark has no default appName).
    "spark.app.name": "repark",
    # Conf true infers StructType for dict-valued *cells* (any nesting depth); false keeps
    # MapType inference (byte-identical to PySpark's default); row-dicts are unaffected.
    # repark defaults TRUE — a DECLARED divergence from PySpark's false (registry row in
    # docs/spark-sql-iceberg-parity.md): nested dict rows flatten without an explicit
    # schema. Set "false" to restore byte-identical PySpark behavior.
    "spark.sql.pyspark.inferNestedDictAsStruct.enabled": "true",
    # Readable back before anything sets it. UTC, not the host zone — a DECLARED divergence
    # from Spark's JVM-local default (reproducibility; no host-environment read).
    SESSION_TIME_ZONE_KEY: DEFAULT_SESSION_TIME_ZONE,
    # Default TIMESTAMP_LTZ (current LTZ behavior).
    TIMESTAMP_TYPE_KEY: DEFAULT_TIMESTAMP_TYPE,
}


_SQLCONF_STATIC_KEYS: frozenset[str] = frozenset(
    {
        "spark.sql.warehouse.dir",
    }
)


logger = logging.getLogger(__name__)


_MEMORY_LIMIT_KEYS: tuple[str, ...] = (
    "repark.memory.limit.gb",
    "spark.repark.memory.limit.gb",
)


_BATCH_SIZE_KEYS: tuple[str, ...] = (
    "repark.batch.size",
    "spark.sql.execution.arrow.maxRecordsPerBatch",
)


_TARGET_PARTITIONS_KEYS: tuple[str, ...] = (
    "repark.target.partitions",
    "spark.sql.shuffle.partitions",
)


_DATAFUSION_CONF_PREFIX = "datafusion."


_DATAFUSION_CONF_KEY_RE = re.compile(r"^datafusion\.[A-Za-z_][A-Za-z0-9_.]*\Z")


_DATAFUSION_RUNTIME_MEMORY_LIMIT_KEY = "datafusion.runtime.memory_limit"


_MEMORY_LIMIT_KEY_LOWER: frozenset[str] = frozenset(key.lower() for key in _MEMORY_LIMIT_KEYS)


def _is_datafusion_conf_key(key: str) -> bool:
    """Return whether ``key`` is a well-formed canonical ``datafusion.*`` identifier path."""

    return bool(_DATAFUSION_CONF_KEY_RE.match(key))


def _looks_like_datafusion_conf_key(key: str) -> bool:
    """True when ``key`` is intended as a datafusion.* conf (strip + case-insensitive prefix)."""

    return key.strip().lower().startswith(_DATAFUSION_CONF_PREFIX)


def _format_datafusion_set_sql(key: str, value: str) -> str:
    """Build a DataFusion ``SET key = 'value'`` statement (value always single-quoted).



    Always quoting accepts both pure integers (``batch_size``) and unit suffixes

    (``memory_limit = '2G'``). Single quotes inside ``value`` are doubled (SQL escape).

    ``key`` must already be a canonical identifier path (caller validates) — never interpolate

    an unvalidated key (injection surface).

    """

    return f"SET {key} = {sql_string_literal(value)}"


def _forward_datafusion_conf(session: ReparkSession, key: str, value: str) -> None:
    """Forward one ``datafusion.*`` key to the live engine via SQL ``SET`` (r21 T2).



    Raises :class:`~repark.errors.IllegalArgumentException` for a malformed / non-canonical

    key or when DataFusion rejects the key/value (unknown option, bad capacity string, …).

    """

    if not _is_datafusion_conf_key(key):
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {value!r} in the config "
            f"{key!r} is invalid. datafusion.* keys must be canonical lowercase "
            f"'datafusion.<identifier path>' (letters, digits, underscore, dots; "
            f"no surrounding whitespace)."
        )

    sql = _format_datafusion_set_sql(key, value)

    try:
        session.sql(sql)

    except Exception as engine_error:
        # Engine already classifies most SET failures as PySparkException; re-surface as
        # IllegalArgumentException so conf typos match the rest of the facade conf surface.

        message = str(engine_error).strip() or repr(engine_error)

        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {value!r} in the config "
            f"{key!r} is invalid. {message}"
        ) from engine_error


def _builder_has_memory_limit_key(config: dict[str, str | None]) -> bool:
    """True when the builder map carries any ``repark.memory.limit.gb`` spelling."""

    lower_keys = {key.lower() for key in config}

    return any(key.lower() in lower_keys for key in _MEMORY_LIMIT_KEYS)


def _refuse_dual_memory_pool_knobs(config: dict[str, str | None]) -> None:
    """Refuse builder maps that set both spellings of the FairSpillPool size (one truth)."""

    has_repark = _builder_has_memory_limit_key(config)

    has_datafusion = any(key.lower() == _DATAFUSION_RUNTIME_MEMORY_LIMIT_KEY for key in config)

    if has_repark and has_datafusion:
        raise IllegalArgumentException(
            "[INVALID_CONF_VALUE.REQUIREMENT] both "
            f"{_MEMORY_LIMIT_KEYS[0]!r} and {_DATAFUSION_RUNTIME_MEMORY_LIMIT_KEY!r} "
            "are set. They configure the same FairSpillPool — use exactly one: "
            f"{_MEMORY_LIMIT_KEYS[0]} at builder/getOrCreate (RAM-relative default, cap 8 GiB, "
            "0 = unbounded), or datafusion.runtime.memory_limit via spark.conf.set / "
            "SQL SET (runtime; e.g. '16G')."
        )


def _refuse_runtime_memory_limit_gb(key: str) -> None:
    """Refuse runtime ``conf.set`` of build-time FairSpillPool size keys (one truth, octo T2 C3).



    ``repark.memory.limit.gb`` / ``spark.repark.memory.limit.gb`` size the pool at

    ``getOrCreate`` only. Live resize is ``datafusion.runtime.memory_limit`` — a facade-only

    write would leave the pool unchanged while ``conf.get`` lies.

    """

    if key.lower() not in _MEMORY_LIMIT_KEY_LOWER:
        return

    raise IllegalArgumentException(
        f"[INVALID_CONF_VALUE.REQUIREMENT] config {key!r} is build-time only "
        f"(FairSpillPool size at getOrCreate; RAM-relative default, cap 8 GiB; 0 = unbounded). "
        f"To re-size the live pool use spark.conf.set("
        f"{_DATAFUSION_RUNTIME_MEMORY_LIMIT_KEY!r}, 'NG') or SQL SET "
        f"{_DATAFUSION_RUNTIME_MEMORY_LIMIT_KEY} = 'NG' — same pool, one truth."
    )


def _apply_builder_datafusion_conf(session: ReparkSession, config: dict[str, str | None]) -> None:
    """Apply ``datafusion.*`` keys from the builder map onto a freshly built session.



    Runs after the native session exists so SQL ``SET`` can reach the live DataFusion

    context. Insertion order is preserved (last alias wins for duplicate keys).

    Non-canonical / mixed-case keys refuse-loud via :meth:`RuntimeConfig.set`.
    ``datafusion.runtime.temp_directory`` is skipped (already applied at Rust build;
    a runtime SET of it refuses loud and names TMPDIR).

    """

    runtime = RuntimeConfig(session)

    for key, value in config.items():
        if value is None:
            continue

        if not _looks_like_datafusion_conf_key(key):
            continue

        # Build-time only: Rust already applied with_temp_file_path. A runtime SET refuses.
        if key.lower() == "datafusion.runtime.temp_directory":
            continue

        runtime.set(key, value)


_DISPLAY_STYLE_KEY = "repark.display.style"


_DISPLAY_STYLE_VALUES: frozenset[str] = frozenset({"spark", "polars", "duckdb"})


_DEFAULT_DISPLAY_STYLE = "polars"


def normalize_display_style(value: str | object) -> str:
    """Normalize and validate a ``repark.display.style`` value (``spark``/``polars``/``duckdb``).



    Case-insensitive. Raises :class:`~repark.errors.IllegalArgumentException` for anything else

    so a typo fails loud at the builder/setter rather than silently falling back to spark.

    """

    if not isinstance(value, str):
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {value!r} in the config "
            f'"{_DISPLAY_STYLE_KEY}" is invalid. '
            f"The value of {_DISPLAY_STYLE_KEY} must be one of "
            f"{sorted(_DISPLAY_STYLE_VALUES)}"
        )

    normalized = value.strip().lower()

    if normalized not in _DISPLAY_STYLE_VALUES:
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value '{value}' in the config "
            f'"{_DISPLAY_STYLE_KEY}" is invalid. '
            f"The value of {_DISPLAY_STYLE_KEY} must be one of "
            f"{sorted(_DISPLAY_STYLE_VALUES)}"
        )

    return normalized


def default_display_style() -> str:
    """Session default display style: env ``REPARK_DISPLAY_STYLE`` first, else ``polars``."""
    override = os.environ.get("REPARK_DISPLAY_STYLE")
    if override is None:
        return _DEFAULT_DISPLAY_STYLE
    return normalize_display_style(override)


def fold_config_file_into_builder(builder: Builder) -> None:
    """Fold forced-or-discovered repark.toml pairs into the builder through .config."""
    from repark import _native

    pairs: dict[str, str] = _native.PyReparkSession.config_file_pairs(builder._config_file)
    for key in sorted(pairs):
        if key in builder._config:
            continue
        if key.lower() == _DISPLAY_STYLE_KEY and any(
            existing.lower() == _DISPLAY_STYLE_KEY for existing in builder._config
        ):
            continue
        builder.config(key, pairs[key])


def lookup_int_entry(
    config: dict[str, str | None], keys: tuple[str, ...]
) -> tuple[str, int] | None:
    """Return the winning ``(key, integer)`` among ``keys``, or ``None`` if none are set.

    The key is returned alongside the value because range validation is **per key family**
    (SAF-006) and the error messages name the spelling the user actually set.

    Keys are tried in order (repark-native first). If several spellings are set:
    identical values collapse; different values raise
    :class:`~repark.errors.IllegalArgumentException` naming both keys. Non-integer values
    raise naming the key (never warn-and-default).

    This is the FACADE twin of the engine's ``repark_core::Error::Config``: both raise the
    SAME class live PySpark raises for an invalid ``SQLConf`` value
    (``IllegalArgumentException``) — a deliberate break from repark's former
    ``ValueError``, which ``except ValueError`` never caught either.

    **TIMING divergence (deliberate):** the CLASS matches PySpark but the
    MOMENT does not — repark validates eagerly inside ``getOrCreate()`` where a fresh
    PySpark process validates at the first ``sessionState`` touch. The user-readable copy
    of this disclosure lives on :meth:`Builder.config` (the ``help()`` surface); keep the
    two in sync.
    """
    found: list[tuple[str, int]] = []
    for key in keys:
        value = config.get(key)
        if value is None:
            continue
        try:
            parsed = int(value)
        except ValueError as error:
            raise IllegalArgumentException(
                f"config key {key!r} must be an integer, got {value!r}"
            ) from error
        found.append((key, parsed))
    if not found:
        return None
    first_key, first_value = found[0]
    for key, value in found[1:]:
        if value != first_value:
            raise IllegalArgumentException(
                f"conflicting config: {first_key!r} and {key!r} set different values"
            )
    return first_key, first_value


def resolve_memory_limit_gb(config: dict[str, str | None]) -> int | None:
    """Resolve ``repark.memory.limit.gb`` (repark-only knob; no Spark counterpart).

    ``0`` is meaningful — it opts the session out of the bounded memory pool entirely —
    so only a NEGATIVE budget is a config error. Raising here (rather than letting a
    negative reach the native ``Option<usize>`` argument) keeps the class the facade
    contracts: :class:`~repark.errors.IllegalArgumentException`, not PyO3's
    ``OverflowError``.
    """
    entry = lookup_int_entry(config, _MEMORY_LIMIT_KEYS)
    if entry is None:
        return None
    key, value = entry
    if value < 0:
        raise IllegalArgumentException(
            _config_value_error(
                key,
                value,
                f"The value of {key} must not be negative (0 opts out of the bounded memory pool)",
            )
        )
    return value


def resolve_batch_size(config: dict[str, str | None]) -> int | None:
    """Resolve the Arrow batch-size knob, honoring Spark's "no limit" sentinel (SAF-006).

    ``spark.sql.execution.arrow.maxRecordsPerBatch`` carries no ``checkValue`` in Spark's
    ``SQLConf`` and is documented "If set to zero or negative there is no limit" — a legal,
    commonly used PySpark value. repark therefore **accepts** ``<= 0`` (returning ``None``,
    i.e. the knob is left unset) rather than refusing it, which is what
    ``spark.sql.shuffle.partitions`` gets — the two keys are validated per Spark's own
    per-key rules, never by one blanket rule.

    **Disclosed divergence:** Spark's sentinel asks for *unbounded* Arrow batches, and
    DataFusion has no unbounded-batch mode, so the default batch size stays in force.
    Values are unaffected (batching is not observable in results, only in batch
    boundaries), so this is an accepted-but-not-honored knob, warned once per process like
    ``.master(...)`` (OTH-010). The repark-native spelling ``repark.batch.size`` shares the
    sentinel so both spellings of one knob cannot diverge.
    """
    entry = lookup_int_entry(config, _BATCH_SIZE_KEYS)
    if entry is None:
        return None
    key, value = entry
    if value <= 0:
        _warn_unbounded_batch_once(key, value, stacklevel=4)
        return None
    return value


def resolve_shuffle_partitions(config: dict[str, str | None]) -> int | None:
    """Resolve the partition-count knob with Spark's positive-only rule (SAF-006).

    ``spark.sql.shuffle.partitions`` is declared in Spark's ``SQLConf`` with
    ``checkValue(_ > 0, …)``, so ``0`` / negative raise ``IllegalArgumentException`` in
    real PySpark. repark mirrors the class AND Spark 4.1.2's
    ``[INVALID_CONF_VALUE.REQUIREMENT]`` message verbatim — see :func:`_config_value_error`
    for the live capture and the two recorded deltas (no ``SQLSTATE`` suffix; the
    repark-native spelling has no Spark counterpart). Contrast
    :meth:`_resolve_batch_size`, whose key is documented to accept ``0``.
    """
    entry = lookup_int_entry(config, _TARGET_PARTITIONS_KEYS)
    if entry is None:
        return None
    key, value = entry
    if value <= 0:
        raise IllegalArgumentException(
            _config_value_error(key, value, f"The value of {key} must be positive")
        )
    return value


_DEFAULT_DISPLAY_MAX_ROWS = 10


_DEFAULT_DISPLAY_MAX_COLS = 8


_DEFAULT_DISPLAY_STR_LEN = 30


_DISPLAY_INT_DEFAULTS: dict[str, int] = {
    "repark.display.max_rows": _DEFAULT_DISPLAY_MAX_ROWS,
    "repark.display.max_cols": _DEFAULT_DISPLAY_MAX_COLS,
    "repark.display.str_len": _DEFAULT_DISPLAY_STR_LEN,
}


def _display_token_key(canonical: str) -> str:
    """Map a ``repark.display.*`` conf key to its alive-token field."""
    return "display_" + canonical.rsplit(".", 1)[-1]


def _sync_display_int_into_builder_config(
    builder_config: dict[str, str | None],
    canonical: str,
    value: int,
) -> None:
    """Collapse case-variant aliases onto the canonical int display key."""
    for existing in list(builder_config):
        if existing.lower() == canonical:
            del builder_config[existing]
    builder_config[canonical] = str(value)


def _builder_display_int(
    builder_config: dict[str, str | None],
    canonical: str,
    fallback: int,
) -> int:
    """Resolve one int display key from a builder map (last case-insensitive hit wins)."""
    raw: str | None = None
    for key, value in builder_config.items():
        if key.lower() == canonical:
            raw = value
    if raw is None:
        return fallback
    return _normalize_display_int(canonical, raw)


def _canonicalize_display_key(
    config: dict[str, str | None],
    key: str,
    value: str | None,
) -> bool:
    """Collapse a display key alias onto its canonical spelling, validating int values."""
    lowered = key.lower()
    if lowered == _DISPLAY_STYLE_KEY:
        for existing in list(config):
            if existing.lower() == _DISPLAY_STYLE_KEY:
                del config[existing]
        config[_DISPLAY_STYLE_KEY] = value
        return True
    for canonical in _DISPLAY_INT_DEFAULTS:
        if lowered != canonical:
            continue
        parsed: str | None = (
            None if value is None else str(_normalize_display_int(canonical, value))
        )
        for existing in list(config):
            if existing.lower() == canonical:
                del config[existing]
        config[canonical] = parsed
        return True
    return False


def _reuse_display_ints(
    config: dict[str, str | None],
    live: Any,
    unset_keys: set[str],
) -> None:
    """Validate and apply present int display keys to a live session's token and maps."""
    token = live._alive_token
    for canonical, fallback in _DISPLAY_INT_DEFAULTS.items():
        if not any(key.lower() == canonical for key in config):
            continue
        parsed = _builder_display_int(config, canonical, fallback)
        token[_display_token_key(canonical)] = parsed
        _sync_display_int_into_builder_config(live._builder_config, canonical, parsed)
        store = token.get("runtime_conf")
        if isinstance(store, dict):
            for existing in list(store):
                if existing.lower() == canonical:
                    del store[existing]
            store[canonical] = str(parsed)
        unset_keys.discard(canonical)


def _normalize_display_int(key: str, value: str | int | object) -> int:
    """Validate a ``repark.display.max_rows|max_cols|str_len`` value as a positive int."""
    if isinstance(value, bool):
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {value!r} in the config "
            f'"{key}" is invalid. '
            f"The value of {key} must be a positive integer."
        )
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.strip().isascii() and value.strip().isdigit():
        parsed = int(value.strip())
    else:
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {value!r} in the config "
            f'"{key}" is invalid. '
            f"The value of {key} must be a positive integer."
        )
    if parsed < 1:
        raise IllegalArgumentException(
            f"[INVALID_CONF_VALUE.REQUIREMENT] The value {value!r} in the config "
            f'"{key}" is invalid. '
            f"The value of {key} must be a positive integer."
        )
    return parsed
