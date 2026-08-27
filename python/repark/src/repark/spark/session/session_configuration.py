"""Session configuration validation and forwarding."""

from __future__ import annotations

import logging

import re

from repark.errors import IllegalArgumentException

from repark.spark.session.session_time_zone import DEFAULT_SESSION_TIME_ZONE, SESSION_TIME_ZONE_KEY

from repark.spark.session.timestamp_type import DEFAULT_TIMESTAMP_TYPE, TIMESTAMP_TYPE_KEY


_CONF_GET_UNSET: object = object()


_SQLCONF_DEFAULTS: dict[str, str] = {
    "spark.sql.sources.partitionOverwriteMode": "STATIC",
    # === r21 T3: ux-polish ===
    # Default app name where we control the default (Spark has no default appName).
    "spark.app.name": "repark",
    # === r23b N1: nested dict-cell → StructType (Spark SPARK-35929) ===
    # Conf true infers StructType for dict-valued *cells* (any nesting depth); false keeps
    # MapType inference (byte-identical to PySpark's default); row-dicts (r22 key-union)
    # are unaffected either way. repark defaults TRUE — a DECLARED divergence from
    # PySpark's false (owner decision, 2026-08-16; registry row in
    # docs/spark-sql-iceberg-parity.md): nested dict rows should flatten without an
    # explicit schema. Set "false" to restore byte-identical PySpark behavior.
    "spark.sql.pyspark.inferNestedDictAsStruct.enabled": "true",
    # === H-1a: session timezone (gap G1) ===
    # Readable back before anything sets it. UTC, not the host zone — a DECLARED divergence
    # from Spark's JVM-local default (reproducibility; no host-environment read).
    SESSION_TIME_ZONE_KEY: DEFAULT_SESSION_TIME_ZONE,
    # === Q10: spark.sql.timestampType (default TIMESTAMP_LTZ, current LTZ behavior) ===
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

    escaped = value.replace("'", "''")

    return f"SET {key} = '{escaped}'"


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


_DEFAULT_DISPLAY_STYLE = "spark"


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
