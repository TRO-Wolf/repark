"""The :class:`ReparkSession` facade — the near-drop-in entry point.



Migrating a PySpark script is a one-line import change::



    from repark import ReparkSession   # was: from pyspark.sql import SparkSession



    spark = ReparkSession.builder.appName("etl").getOrCreate()

    df = spark.sql("SELECT 1 AS a")

    df.show()



For source-compatible drop-in, ``SparkSession`` is kept as an alias of :class:`ReparkSession`, so

``from repark import SparkSession`` also works and the rest of an existing PySpark script stays

byte-identical. The builder mirrors PySpark's ``SparkSession.builder…getOrCreate()`` chain. Compute

runs in Rust behind the native ``repark._native.PyReparkSession`` — a thin, typed Python shell.

"""

from __future__ import annotations


import contextlib

import contextvars

import logging

import re

import uuid

import warnings

from pathlib import Path

from typing import TYPE_CHECKING, Any


from repark import _native

from repark._secrets import prop_key_is_secret as _prop_key_is_secret


# === r23 QI1: idents ===

from repark._idents import is_plain_ident as _is_plain_ident

from repark._idents import quote_ident as _quote_ident

from repark._idents import quote_ident_if_needed as _quote_ident_if_needed

from repark._idents import reject_path_escape_segment as _reject_path_escape_segment


# === r24 A3: SEC-04 conf redaction ===

from repark.catalog import (
    DEFAULT_CATALOG_NAME,
    DEFAULT_DATABASE_NAME,
    Catalog,
)

from repark.dataframe import DataFrame

from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkException,
    PySparkRuntimeError,
    PySparkTypeError,
    PySparkValueError,
)

# === H-1a: the session-timezone conf key (ONE spelling; engine-validated at build) ===

from repark.session.session_time_zone import (
    DEFAULT_SESSION_TIME_ZONE,
    SESSION_TIME_ZONE_KEY,
)


if TYPE_CHECKING:
    from typing import Self


# Sentinel for RuntimeConfig.get: distinguish "default not passed" from default=None

# (Apache test_conf: get(unset) raises; get(unset, None) returns None).

_CONF_GET_UNSET: object = object()


# Spark SQLConf defaults consulted only when get() is called *without* a default

# argument (Apache test_conf: partitionOverwriteMode → "STATIC" vs get(..., None) → None).

# Not stored in the runtime map — getAll still surfaces them so the dump is non-empty.

_SQLCONF_DEFAULTS: dict[str, str] = {
    "spark.sql.sources.partitionOverwriteMode": "STATIC",
    # === r21 T3: ux-polish ===
    # Default app name where we control the default (Spark has no default appName).
    "spark.app.name": "repark",
    # === r23b N1: nested dict-cell → StructType (Spark SPARK-35929) ===
    # Conf false/unset keeps MapType inference (byte-identical to pre-N1). Conf true
    # infers StructType for dict-valued *cells* (any nesting depth); row-dicts (r22
    # key-union) are unaffected.
    "spark.sql.pyspark.inferNestedDictAsStruct.enabled": "false",
    # === H-1a: session timezone (gap G1) ===
    # Readable back before anything sets it. UTC, not the host zone — a DECLARED divergence
    # from Spark's JVM-local default (reproducibility; no host-environment read).
    SESSION_TIME_ZONE_KEY: DEFAULT_SESSION_TIME_ZONE,
}


# Keys Spark treats as non-modifiable static conf (isModifiable → False).

_SQLCONF_STATIC_KEYS: frozenset[str] = frozenset(
    {
        "spark.sql.warehouse.dir",
    }
)


logger = logging.getLogger(__name__)


# Multipart catalog table identifiers only (catalog.db.table). Rejects SQL fragments so

# spark.table / read.table stay identifier APIs, not free-form FROM clauses (octo C1-SEC-001).


# Free-SQL statement-prefix expanders (E2 + F1). Whole-statement only — no scripts/CTE inject.

# Matches a whole-statement DROP only — does not rewrite DROP inside scripts/CTEs.

_DROP_TABLE_SQL_RE = re.compile(r"(?is)^\s*DROP\s+TABLE\s+(IF\s+EXISTS\s+)?(.+?)\s*;?\s*$")


# INSERT [OVERWRITE [TABLE]] [INTO [TABLE]] <prefix only> — target name scanned as identifier

# (not `.+?` until VALUES/SELECT — avoids eating trailing `select`/`values` in table names).

# Optional TABLE after INTO as well as OVERWRITE (Spark ``INSERT INTO TABLE t``).

# DIRECTORY / LOCAL DIRECTORY are *not* table targets — handled in ``_try_expand_insert_sql``.

_INSERT_PREFIX_RE = re.compile(
    r"(?is)^\s*(INSERT\s+(?:OVERWRITE\s+(?:TABLE\s+)?|INTO\s+(?:TABLE\s+)?))"
)

# Path-insert dialect: INSERT OVERWRITE [LOCAL] DIRECTORY … — not a catalog table.

_INSERT_DIRECTORY_HEAD_RE = re.compile(r"(?is)^(?:LOCAL\s+)?DIRECTORY\b")


# CREATE [OR REPLACE] TABLE [IF NOT EXISTS] <prefix only> — table name scanned as identifier

# (not `.+?` until AS — that swallows trailing `as` in names like `bare_ctas`).

_CREATE_TABLE_PREFIX_RE = re.compile(
    r"(?is)^\s*(CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)"
)

# Skip CREATE VIEW / CREATE TEMPORARY TABLE — not durable Iceberg three-part targets.

_CREATE_VIEW_SQL_RE = re.compile(
    r"(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+|TEMP\s+)?VIEW\b"
)

_CREATE_TEMP_TABLE_SQL_RE = re.compile(
    r"(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY|TEMP)\s+TABLE\b"
)


# MERGE INTO <target> … USING <source> … ON …  — whole-statement shape (alias optional).

_MERGE_INTO_SQL_RE = re.compile(r"(?is)^\s*MERGE\s+INTO\s+(.+?)\s+USING\s+(.+?)\s+ON\b(.*)\s*$")


# UPDATE <prefix only> — target scanned as identifier; optional alias + SET body + WHERE

# stay in the rest (G1). Never regex the SET/WHERE body.

_UPDATE_PREFIX_RE = re.compile(r"(?is)^\s*(UPDATE\s+)")


# DELETE FROM <prefix only> — target scanned as identifier; optional alias + WHERE stay in rest

# (G1). Multi-table DELETE without FROM is not a Spark free-SQL form we expand.

_DELETE_FROM_PREFIX_RE = re.compile(r"(?is)^\s*(DELETE\s+FROM\s+)")


# SELECT / WITH free-SQL: structural FROM/JOIN table-ref expansion (F1 Path A).

_SELECT_OR_WITH_HEAD_RE = re.compile(r"(?is)^\s*(?:WITH\b|SELECT\b)")

# Keywords that cannot start a table identifier after FROM / JOIN.

_FROM_JOIN_NON_TABLE = frozenset(
    {
        "LATERAL",
        "ONLY",
        "UNNEST",
        "VALUES",
        "SELECT",
        "WITH",
    }
)

# Tokens that end a relation (not a table alias) after FROM/JOIN table-ref.

_RELATION_FOLLOW_KEYWORDS = frozenset(
    {
        "WHERE",
        "GROUP",
        "HAVING",
        "ORDER",
        "LIMIT",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "JOIN",
        "ON",
        "USING",
        "LEFT",
        "RIGHT",
        "FULL",
        "INNER",
        "CROSS",
        "NATURAL",
        "OUTER",
        "SEMI",
        "ANTI",
        "WINDOW",
        "QUALIFY",
        "LATERAL",
        "AS",
        "TABLESAMPLE",
        "PIVOT",
        "UNPIVOT",
        "FOR",
        "CLUSTER",
        "DISTRIBUTE",
        "SORT",
        "TABLE",
        "FETCH",
        "OFFSET",
        "SETTINGS",  # engine-specific; never an alias
    }
)


# Reader options that would change *which* rows/files load in Spark; repark does not honor them

# yet and must fail loud rather than silently ignore (octo C1-SAF-001 / C2-SAF-*).

# I1 / R-TIME-TRAVEL: snapshot-id / as-of-timestamp / branch / tag are supported on the Iceberg

# read path (removed from this denylist). Incremental-read bounds stay denylisted.

# === r20 R1: read-formats ===

# `compression` is wired for csv/json (removed from the global denylist); parquet still rejects it

# via the format-aware gate in `_reject_unsupported_semantic_options`.

_UNSUPPORTED_SEMANTIC_READER_OPTIONS: frozenset[str] = frozenset(
    {
        "pathglobfilter",
        "recursivefilelookup",
        "mergeschema",
        "basepath",
        "modifiedbefore",
        "modifiedafter",
        "ignorecorruptfiles",
        "ignoremissingfiles",
        "encryption",
        # Parquet value-semantic read options (C2-Q-003).
        "datetimerebasemode",
        "datetimerebasemodeinread",
        "int96rebasemode",
        "int96rebasemodeinread",
        # Iceberg incremental-read window (future seed) — not time-travel pins.
        "start-snapshot-id",
        "end-snapshot-id",
    }
)


# === r20 R1: read-formats ===

# CSV/JSON parse options that would change values if silently ignored (loud when set unsupported).

_CSV_UNSUPPORTED_PARSE_OPTIONS: frozenset[str] = frozenset(
    {
        "encoding",
        "dateformat",
        "timestampformat",
        "timestampntzformat",
        "locale",
        "chartoescapequoteescaping",
        "samplingratio",
        "enforceschema",
        "columnnameofcorruptrecord",
        "nanvalue",
        "positiveinf",
        "negativeinf",
        "maxcolumns",
        "maxcharspercolumn",
        "preferdate",
        "ignoreleadingwhitespace",
        "ignoretrailingwhitespace",
        "linesep",
        "unescapedquotehandling",
    }
)

_JSON_UNSUPPORTED_PARSE_OPTIONS: frozenset[str] = frozenset(
    {
        "primitivesasstring",
        "prefersdecimal",
        "allowcomments",
        "allowunquotedfieldnames",
        "allowsinglequotes",
        "allownumericleadingzeros",
        "allowbackslashescapinganycharacter",
        "allowunquotedcontrolchars",
        "columnnameofcorruptrecord",
        "dateformat",
        "timestampformat",
        "timestampntzformat",
        "encoding",
        "linesep",
        "samplingratio",
        "dropfieldifallnull",
        "locale",
        "timezone",
        "timeZone",
    }
)

# Keys applied by the native CSV/JSON readers (passed through; others may be denylisted).

_CSV_NATIVE_OPTION_KEYS: frozenset[str] = frozenset(
    {
        "header",
        "sep",
        "delimiter",
        "quote",
        "escape",
        "comment",
        "nullvalue",
        "multiline",
        "compression",
    }
)

_JSON_NATIVE_OPTION_KEYS: frozenset[str] = frozenset(
    {
        "multiline",
        "compression",
    }
)


# === r25 T5: excel ===

_EXCEL_NATIVE_OPTION_KEYS: frozenset[str] = frozenset(
    {
        "sheet_name",
        "sheetname",
        "header",
        "skip_rows",
        "skiprows",
        "schema",
        "column_names",
        "names",
    }
)


# Iceberg time-travel reader options supported on format("iceberg") / table() (I1).

_ICEBERG_TIME_TRAVEL_OPTIONS: frozenset[str] = frozenset(
    {
        "snapshot-id",
        "as-of-timestamp",
        "branch",
        "tag",
    }
)


# Signed i64 domain for PyO3 `i64` snapshot / timestamp pins (octo C7-Q-001).

_I64_MIN = -(2**63)

_I64_MAX = 2**63 - 1


# Engine-config keys the native builder understands. Ordered tuples (repark-native first, then

# Spark drop-in spelling): lookup is deterministic, dual spellings with different values raise,

# unparsable ints raise (aligned with catalog dual-prefix NR-1 policy). Unknown keys are

# accepted and ignored (PySpark also tolerates unknown ``.config`` keys).

#

# **Out-of-range policy is PER KEY FAMILY — Spark itself validates these keys differently**

# (audit SAF-006; verified against the `SQLConf` shipped in pyspark 4.1.2's

# `spark-catalyst_2.13-4.1.2.jar` AND a live pyspark 4.1.2 raise under zulu-17, not from memory):

#

# * ``spark.sql.execution.arrow.maxRecordsPerBatch`` declares **no** ``checkValue`` and is

#   documented "If set to zero or negative there is no limit." Zero is a legal, commonly used

#   PySpark value (live: builds, ``conf.get`` → ``'0'``, queries run), so repark must NOT refuse

#   it — see ``Builder._resolve_batch_size`` for how the sentinel is honored (and the one

#   disclosed divergence).

# * ``spark.sql.shuffle.partitions`` declares ``checkValue(_ > 0, "The value of

#   spark.sql.shuffle.partitions must be positive")``, so ``<= 0`` raises

#   ``IllegalArgumentException`` in Spark — and raises here too. See

#   ``_config_value_error`` for the message shape (Spark 4.1.2's

#   ``[INVALID_CONF_VALUE.REQUIREMENT]`` error class, verbatim minus SQLSTATE) and the deltas.

# * ``repark.memory.limit.gb`` is a repark-only knob with no Spark counterpart: ``0`` opts out of

#   the bounded memory pool (documented), negative is a config error.

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

# === r21 T2: sort-memory ===

# DataFusion session / runtime config namespace forwarded by RuntimeConfig.set via SQL SET.

# Key shape is validated before interpolation (identifier path only — no SQL injection via key).

# Canonical form is lowercase ``datafusion.<id path>`` with no surrounding whitespace; mixed

# case / padded keys refuse-loud so the facade never keeps a silent store-only twin (octo T2 C2).

# Anchor with ``\Z`` (not ``$``): Python ``$`` also matches before a final newline, which would

# accept ``datafusion.foo\n`` as "canonical", store a twin key, and still SET the live option

# (newline-as-whitespace) — extra-octo T2 E1-1.

_DATAFUSION_CONF_PREFIX = "datafusion."

_DATAFUSION_CONF_KEY_RE = re.compile(r"^datafusion\.[A-Za-z_][A-Za-z0-9_.]*\Z")

# Live FairSpillPool size (DataFusion runtime). Same pool as builder repark.memory.limit.gb —

# one truth, not two independent knobs (see RuntimeConfig / Builder docs).

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
            f"{_MEMORY_LIMIT_KEYS[0]} at builder/getOrCreate (build-time; default 8 GiB, "
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
        f"(FairSpillPool size at getOrCreate; default 8 GiB; 0 = unbounded). "
        f"To re-size the live pool use spark.conf.set("
        f"{_DATAFUSION_RUNTIME_MEMORY_LIMIT_KEY!r}, 'NG') or SQL SET "
        f"{_DATAFUSION_RUNTIME_MEMORY_LIMIT_KEY} = 'NG' — same pool, one truth."
    )


def _apply_builder_datafusion_conf(session: ReparkSession, config: dict[str, str | None]) -> None:
    """Apply ``datafusion.*`` keys from the builder map onto a freshly built session.



    Runs after the native session exists so SQL ``SET`` can reach the live DataFusion

    context. Insertion order is preserved (last alias wins for duplicate keys).

    Non-canonical / mixed-case keys refuse-loud via :meth:`RuntimeConfig.set`.

    """

    runtime = RuntimeConfig(session)

    for key, value in config.items():
        if value is None:
            continue

        if not _looks_like_datafusion_conf_key(key):
            continue

        runtime.set(key, value)


# Facade-only display style for DataFrame.show() (R-DISPLAY). Kept under the ``repark.`` prefix

# so it never collides with a PySpark ``spark.*`` config key. Runtime-mutable via

# :attr:`ReparkSession.display_style`; default ``spark`` keeps ``df.show()`` byte-identical.

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


def _catalog_names_from_builder_config(builder_config: dict[str, str | None]) -> set[str]:
    """Catalog names declared via ``spark.sql.catalog.<name>`` / ``repark.sql.catalog.<name>``."""

    names: set[str] = set()

    for key in builder_config:
        lower = key.lower()

        for prefix in ("spark.sql.catalog.", "repark.sql.catalog."):
            if lower.startswith(prefix):
                rest = key[len(prefix) :]

                name = rest.split(".", 1)[0]

                if name:
                    names.add(name)

                break

    return names


def _default_catalog_from_builder_config(builder_config: dict[str, str | None]) -> str | None:
    """``spark.sql.defaultCatalog`` from the builder map (case-insensitive), if set."""

    for key, value in builder_config.items():
        if key.lower() == "spark.sql.defaultcatalog" and value is not None and value != "":
            return value

    return None


# Opt-out knob for the auto-registered session-scoped memory catalog (default on).

_AUTO_MEMORY_CATALOG_KEY = "repark.sql.automemorycatalog"


def _auto_memory_catalog_wanted(builder_config: dict[str, str | None]) -> bool:
    """Whether a bare session should auto-register ``spark_catalog`` (R-AUTO-MEMCAT).



    True only when ALL hold: the knob (``repark.sql.autoMemoryCatalog``) is not ``false``;

    no ``spark.sql.catalog.*`` / ``repark.sql.catalog.*`` blocks are configured (a user who

    configured catalogs gets exactly those); and ``spark.sql.defaultCatalog`` is unset or

    already ``spark_catalog`` (an explicit different default names a catalog the user must

    provide — auto-seeding a catalog they did not name would mask their misconfiguration).

    """

    for key, value in builder_config.items():
        if (
            key.lower() == _AUTO_MEMORY_CATALOG_KEY
            and value is not None
            and str(value).strip().lower() in ("false", "0", "no")
        ):
            return False

    if _catalog_names_from_builder_config(builder_config):
        return False

    explicit_default = _default_catalog_from_builder_config(builder_config)

    return explicit_default is None or explicit_default == DEFAULT_CATALOG_NAME


def _default_namespace_from_builder_config(builder_config: dict[str, str | None]) -> str | None:
    """``spark.sql.defaultNamespace`` from the builder map (case-insensitive), if set.



    Spark's key is the session default database/namespace. Accepted and seeded into

    facade ``currentDatabase`` at build (E2 bare-name resolution). Hardcoded

    :data:`~repark.catalog.DEFAULT_DATABASE_NAME` when unset (v1).

    """

    for key, value in builder_config.items():
        if key.lower() == "spark.sql.defaultnamespace" and value is not None and value != "":
            return value

    return None


def _alias_catalog_name(
    catalog: str,
    *,
    current_catalog: str,
    known_catalogs: set[str],
    default_catalog_is_auto: bool = False,
) -> str:
    """Resolve ``spark_catalog`` as an alias of the session's registered catalog (E2).



    When ``spark_catalog`` is not itself registered — or is only the AUTO-registered

    fallback (R-AUTO-MEMCAT), which never blocks user-intent resolution — map it to

    ``current_catalog`` if that name is known, else the sole known catalog when exactly one

    is registered. Fully-qualified three-part names and real catalog names pass through

    unchanged. (In a bare session the auto case is identity anyway: current IS

    ``spark_catalog``. After a user registration flips current, ``spark_catalog.…`` refs

    alias to the user catalog — tables written to the auto catalog before such a flip are

    then reachable only by bare name resolution against it, a documented edge.)

    """

    if catalog != DEFAULT_CATALOG_NAME:
        return catalog

    if DEFAULT_CATALOG_NAME in known_catalogs and not default_catalog_is_auto:
        return catalog

    if current_catalog in known_catalogs:
        return current_catalog

    if len(known_catalogs) == 1:
        return next(iter(known_catalogs))

    return catalog


def _join_table_identifier_segments(segments: list[str]) -> str:
    """Rejoin identifier segments so dotted / special segments stay one part (E2 / C2-SEC-001).



    Plain ``[A-Za-z_][A-Za-z0-9_]*`` segments stay unquoted (stable string form for tests and

    native three-part probes). Any other segment is double-quoted so a later

    :func:`_sql_table_ref` / :func:`_parse_table_identifier_segments` pass cannot re-split

    embedded dots into extra multipart identity pieces (silent wrong-object).

    """

    # Quote-if-needed SSOT (octo C1-Q-001) — plain bare unquoted; else always-quote.

    return ".".join(_quote_ident_if_needed(segment) for segment in segments)


def resolve_table_name(
    name: str,
    *,
    current_catalog: str,
    current_database: str,
    known_catalogs: set[str] | None = None,
    prefer_temp_view: bool = False,
    temp_view_exists: Any | None = None,
    default_catalog_is_auto: bool = False,
) -> str:
    """Qualify a bare / two-part table identifier under the session default catalog + NS (E2).



    Shared name-resolution layer for free-SQL entry points and the DataFrame API

    (``table`` / ``saveAsTable`` / ``writeTo`` / ``insertInto`` / MERGE /

    :meth:`ReparkSession.read_iceberg_table`). Returns a multipart identifier string that

    preserves segment boundaries (quote-aware rejoin — C2-SEC-001); callers pass it through

    :func:`_sql_table_ref` for full quoting when embedding in SQL.



    * **one-part** ``t`` → temp view (when ``prefer_temp_view`` + probe hits), else

      ``currentCatalog.currentDatabase.t``

    * **two-part** ``ns.t`` → ``currentCatalog.ns.t``

    * **three-part** ``cat.ns.t`` → as-is, with ``spark_catalog`` alias expansion

    """

    known = known_catalogs if known_catalogs is not None else set()

    stripped = name.strip()

    try:
        segments = _parse_table_identifier_segments(stripped)

    except ValueError as error:
        # Match `_sql_table_ref` surface: invalid / SQL-fragment identifiers raise

        # AnalysisException (writer / table injection gate — C1-SEC-001).

        from repark.errors import AnalysisException

        raise AnalysisException(
            f"invalid table identifier {stripped[:128]!r}: {error} "
            "(expected multipart name like catalog.db.table; SQL fragments are not allowed)"
        ) from error

    if len(segments) == 1:
        bare = segments[0]

        if prefer_temp_view and temp_view_exists is not None:
            try:
                if bool(temp_view_exists(bare)):
                    return _join_table_identifier_segments([bare])

            except Exception:
                # Soften probe failures: fall through to catalog qualification.

                pass

        catalog = _alias_catalog_name(
            current_catalog,
            current_catalog=current_catalog,
            known_catalogs=known,
            default_catalog_is_auto=default_catalog_is_auto,
        )

        return _join_table_identifier_segments([catalog, current_database, bare])

    if len(segments) == 2:
        catalog = _alias_catalog_name(
            current_catalog,
            current_catalog=current_catalog,
            known_catalogs=known,
            default_catalog_is_auto=default_catalog_is_auto,
        )

        return _join_table_identifier_segments([catalog, segments[0], segments[1]])

    if len(segments) == 3:
        catalog = _alias_catalog_name(
            segments[0],
            current_catalog=current_catalog,
            known_catalogs=known,
            default_catalog_is_auto=default_catalog_is_auto,
        )

        return _join_table_identifier_segments([catalog, segments[1], segments[2]])

    # Four+ parts: leave as-is (quote-aware); downstream engine refuses with a clear plan error.

    return _join_table_identifier_segments(segments)


def _sync_display_style_into_builder_config(builder_config: dict[str, str], style: str) -> None:
    """Record the applied display style on the session builder snapshot (canonical key).



    Drops any prior case-variant of the key so the snapshot stays a single entry and

    repeated pure-style reuse stays silent after the style is applied (C6-Q-001).

    """

    for key in list(builder_config):
        if key.lower() == _DISPLAY_STYLE_KEY:
            del builder_config[key]

    builder_config[_DISPLAY_STYLE_KEY] = style


# Process-wide active session (PySpark getOrCreate semantics). Facade-level only — no Rust global.

_active_session: ReparkSession | None = None


# Drop-in disclosure (OTH-010): `.master(...)` / config key ``spark.master`` is accepted for

# source compatibility but ignored (repark is single-node). Warn ONCE per process the first time

# either path records a master URL, so a script pointed at a real cluster URL is told it runs

# single-node — without warning on every builder chain. Reset by `_reset_dropin_warnings_for_tests`.

_master_warned = False


# Drop-in disclosure (SAF-006): Spark's documented "no limit" batch sentinel

# (``spark.sql.execution.arrow.maxRecordsPerBatch <= 0``) is ACCEPTED — refusing it would break a

# legal PySpark program — but it cannot be honoured: DataFusion has no unbounded-batch mode, so the

# engine's default batch size stays in force. Warned ONCE per process, like the master disclosure.

_unbounded_batch_warned = False


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
        # Mirror production stop so held SC/DF tokens die (octo r3 C1-Q-005).

        _active_session._spark_context._mark_stopped()  # type: ignore[attr-defined]

        _active_session._alive_token["alive"] = False  # type: ignore[attr-defined]

        _active_session._inner = None  # type: ignore[attr-defined]

        _active_session = None


#: ``SparkSession`` is kept as a source-compatible alias of :class:`ReparkSession`, so an existing

#: PySpark script migrates with only its import line changed (``from repark import SparkSession``).


#: ``ReParkSession`` (the pre-2026-07-12 capital-P casing) is kept as a back-compat alias so any

#: early notebook/script written against the original brand casing keeps importing.


def _parse_jdbc_int_option(name: str, raw: str | None) -> int | None:
    """Parse a JDBC partition integer option; map bad text to IllegalArgumentException."""

    if raw is None:
        return None

    try:
        return int(raw)

    except (TypeError, ValueError) as exc:
        from repark.errors import IllegalArgumentException

        raise IllegalArgumentException(
            f"jdbc option {name} must be an integer, got {raw!r}"
        ) from exc


# === r20 R1: read-formats ===


def _reader_path_to_str(path: str | Path | list[str]) -> str:
    """Normalize a reader path argument to a single filesystem path string.



    Multi-path lists: Spark unions them; repark v1 accepts a single path or a one-element list

    and fails loud on multi-path (no silent partial read).

    """

    if isinstance(path, list):
        if len(path) == 0:
            from repark.errors import AnalysisException

            raise AnalysisException("CSV/JSON load requires a non-empty path")

        if len(path) > 1:
            from repark.errors import AnalysisException

            raise AnalysisException(
                "reading multiple paths in one load() is not supported by repark yet "
                f"(got {len(path)} paths)"
            )

        return str(path[0])

    return str(path)


def _json_input_nonempty(path_str: str) -> bool:
    """True when a local JSON path (file or dir) has non-zero content to parse."""

    path = Path(path_str)

    if path.is_file():
        try:
            return path.stat().st_size > 0

        except OSError:
            return False

    if path.is_dir():
        for child in path.rglob("*"):
            if child.is_file():
                try:
                    if child.stat().st_size > 0:
                        return True

                except OSError:
                    continue

    return False


def _json_multiline_empty_schema_is_mismatch(path_str: str) -> bool:
    """True when multiLine empty schema likely means wrong shape (not empty ``[]``).



    Empty array files are valid zero-row inputs (octo R1-C5-001). Pretty single objects and

    NDJSON under multiLine still fail loud (R1-C1-005).

    """

    if not _json_input_nonempty(path_str):
        return False

    path = Path(path_str)

    sample = b""

    try:
        if path.is_file():
            with path.open("rb") as handle:
                sample = handle.read(4096)

        elif path.is_dir():
            for child in sorted(path.rglob("*")):
                if child.is_file() and child.stat().st_size > 0:
                    with child.open("rb") as handle:
                        sample = handle.read(4096)

                    break

    except OSError:
        return True

    stripped = sample.lstrip()

    if not stripped:
        return False

    # Top-level array (including ``[]``) is the multiLine contract — empty schema is OK.

    return not stripped.startswith(b"[")


def _promote_csv_string_types(frame: DataFrame) -> DataFrame:
    """Spark-like type promotion on an all-string CSV frame after nullValue application.



    Tries bigint → double → boolean per column via engine CAST; keeps string on failure.

    Validates each trial by materializing so a late bad value rejects the type.

    """

    from repark import functions as F  # noqa: N812

    columns = list(frame.columns)

    if not columns:
        return frame

    candidates = ("bigint", "double", "boolean")

    selects: list[Any] = []

    for name in columns:
        promoted: Any | None = None

        for type_name in candidates:
            trial = frame.select(F.col(name).cast(type_name).alias(name))

            try:
                trial.to_arrow()

                promoted = type_name

                break

            except (AnalysisException, PySparkException, RuntimeError, ValueError, TypeError):
                continue

        if promoted is not None:
            selects.append(F.col(name).cast(promoted).alias(name))

        else:
            selects.append(F.col(name))

    return frame.select(*selects)


def _schema_fields(schema: Any) -> list[dict[str, Any]]:
    """Normalize StructType / DDL / field-list into ``[{name, dataType}, …]`` for reader casts."""

    from repark.types import DataType, StructField, StructType

    if isinstance(schema, StructType):
        return [{"name": field.name, "dataType": field.dataType} for field in schema.fields]

    if isinstance(schema, str):
        parsed = DataType.fromDDL(schema)

        if isinstance(parsed, StructType):
            return [{"name": field.name, "dataType": field.dataType} for field in parsed.fields]

        return [{"name": "value", "dataType": parsed}]

    if isinstance(schema, (list, tuple)) and schema and isinstance(schema[0], StructField):
        return [{"name": field.name, "dataType": field.dataType} for field in schema]

    if isinstance(schema, DataType) and not isinstance(schema, StructType):
        return [{"name": "value", "dataType": schema}]

    from repark.errors import AnalysisException

    raise AnalysisException(
        f"DataFrameReader.schema expects StructType, DDL string, or StructField list; "
        f"got {type(schema).__name__}"
    )


# Untyped / all-null columns on the VALUES path have no StructType anchor. Emit a stable

# string null so Arrow types are not bare Null (C2-L-003). Name-only empty frames use the same.

_TYPED_NULL_SQL = "CAST(NULL AS VARCHAR)"


# Spark-inferred DecimalType for Python Decimal on createDataFrame (INT-002 oracle).

_DECIMAL_PRECISION = 38

_DECIMAL_SCALE = 18

_DECIMAL_MAX_ABS = 10 ** (_DECIMAL_PRECISION - _DECIMAL_SCALE)  # 10**20


def _sql_literal(value: Any) -> str:
    """Render a Python scalar as a SQL literal for VALUES-based createDataFrame."""

    import datetime as dt

    from decimal import Decimal

    value = _normalize_create_dataframe_cell(value)

    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)

    if isinstance(value, float):
        # NaN already normalized to None; reject inf so we never emit a bare `inf` token.

        if value == float("inf") or value == float("-inf"):
            raise PySparkTypeError("createDataFrame does not support infinite float values")

        return repr(value)

    if isinstance(value, str):
        escaped = value.replace("'", "''")

        return f"'{escaped}'"

    if isinstance(value, dt.datetime):
        # TIMESTAMP literal. Session is UTC; tz-aware values convert to UTC first so absolute

        # time is preserved (wall-clock strip via replace(tzinfo=None) alone is wrong for non-UTC).

        wall = value.astimezone(dt.UTC).replace(tzinfo=None) if value.tzinfo is not None else value

        if wall.microsecond:
            rendered = wall.strftime("%Y-%m-%d %H:%M:%S.%f")

        else:
            rendered = wall.strftime("%Y-%m-%d %H:%M:%S")

        return f"TIMESTAMP '{rendered}'"

    if isinstance(value, dt.date):
        return f"DATE '{value.isoformat()}'"

    if isinstance(value, Decimal):
        # Spark's inferred DecimalType for Python Decimal is DECIMAL(38, 18); pin that width so

        # createDataFrame → to_arrow type matches the live oracle (INT-002). Emit fixed-point

        # text via format(..., 'f') — str(Decimal) can be scientific (1E-10) which SQL parsers

        # mis-handle inside CAST. Refuse values outside the envelope rather than silent zero

        # (1E-19 → 0) or round (C2-L-002).

        _validate_decimal_envelope(value)

        return f"CAST({format(value, 'f')} AS DECIMAL({_DECIMAL_PRECISION}, {_DECIMAL_SCALE}))"

    raise PySparkTypeError(
        f"createDataFrame does not support values of type {type(value).__name__} yet"
    )


def _numpy_datetime64_unit(value: Any) -> str | None:
    """Return the numpy ``datetime64`` unit (``'D'``, ``'ns'``, …) or ``None`` if unknown.



    Used so all-null ``NaT`` witnesses pick DATE vs TIMESTAMP the same way non-null cells do

    after ``.item()`` (calendar units ``D``/``W``/``M``/``Y`` → ``datetime.date`` — C3-Q-001).

    """

    dtype = getattr(value, "dtype", None)

    if dtype is None:
        return None

    text = str(dtype)

    if "[" in text and text.endswith("]"):
        return text[text.rindex("[") + 1 : -1]

    return None


# Calendar units: non-null ``numpy.datetime64.item()`` returns ``datetime.date`` (not datetime).

_NUMPY_DATETIME64_DATE_UNITS = frozenset({"D", "W", "M", "Y"})


# Python array.array typecodes Spark accepts (pyspark.sql.types._array_type_mappings shape).

# Platform-dependent via ctypes sizes — unsigned needs a signed JVM type at least 1 bit larger.


def _supported_array_typecodes() -> frozenset[str]:
    """Spark-supported ``array.array`` typecodes on this platform (F1 / test_array_types)."""

    import ctypes

    import sys

    def int_size_to_ok(bit_width: int) -> bool:

        return bit_width <= 64

    supported: set[str] = {"f", "d"}

    for typecode, ctype in (
        ("b", ctypes.c_byte),
        ("h", ctypes.c_short),
        ("i", ctypes.c_int),
        ("l", ctypes.c_long),
    ):
        if int_size_to_ok(ctypes.sizeof(ctype) * 8):
            supported.add(typecode)

    for typecode, ctype in (
        ("B", ctypes.c_ubyte),
        ("H", ctypes.c_ushort),
        ("I", ctypes.c_uint),
        ("L", ctypes.c_ulong),
    ):
        # JVM has no unsigned — need signed slot at least 1 bit larger.

        if int_size_to_ok(ctypes.sizeof(ctype) * 8 + 1):
            supported.add(typecode)

    if sys.version_info[0] < 4:
        supported.add("u")

    return frozenset(supported)


_ARRAY_TYPECODES_SUPPORTED: frozenset[str] | None = None


def _array_typecodes_supported() -> frozenset[str]:
    """Cached :func:`_supported_array_typecodes`."""

    global _ARRAY_TYPECODES_SUPPORTED

    if _ARRAY_TYPECODES_SUPPORTED is None:
        _ARRAY_TYPECODES_SUPPORTED = _supported_array_typecodes()

    return _ARRAY_TYPECODES_SUPPORTED


def _normalize_create_dataframe_cell(value: Any, *, field_name: str | None = None) -> Any:
    """Coerce pandas / numpy / Row-adjacent scalars to plain Python for SQL literals.



    Missing markers (``None``, ``NaN``, pandas ``NA`` / ``NaT``) become ``None``. Numpy scalar

    wrappers unwrap via ``.item()`` — except ``numpy.datetime64[ns]`` (and finer), where

    ``.item()`` returns an epoch int; those cast to ``datetime64[us]`` first so VALUES emits

    TIMESTAMP, not a silent integer (prior C3-L-002). ``numpy.timedelta64`` refuses (``.item()``

    can return a bare int for unit ``ns`` — silent duration→count — C3-L-001). pandas

    ``Timestamp`` becomes ``datetime.datetime`` (tz-aware kept; UTC conversion happens in

    :func:`_sql_literal`).



    **ML vectors (R-ML-SKELETON):** :class:`~repark.ml.linalg.DenseVector` → dense float list;

    :class:`~repark.ml.linalg.SparseVector` → sparse struct dict. Mixed dense widths are

    rejected later in :func:`_arrow_table_from_tuples` (v1 fixed-width only).



    **array.array (F1 / test_array_types):** supported typecodes → ``list``; unsupported

    raise :class:`~repark.errors.PySparkTypeError` with ``CANNOT_INFER_TYPE_FOR_FIELD`` when

    ``field_name`` is known (Apache check_error keys).

    """

    if value is None:
        return None

    type_name = type(value).__name__

    module_name = type(value).__module__

    # Nested Row kept as Row for struct vs map inference (dict → map, Row → struct).

    # Children still need recursive normalize when building Arrow (see _prepare_nested_cell).

    # array.array → list for Spark-supported typecodes (Apache test_array_types).

    if module_name == "array" and type_name == "array":
        typecode = getattr(value, "typecode", None)

        if typecode is None or typecode not in _array_typecodes_supported():
            if field_name is not None:
                raise PySparkTypeError(
                    errorClass="CANNOT_INFER_TYPE_FOR_FIELD",
                    messageParameters={"field_name": str(field_name)},
                )

            raise PySparkTypeError(
                errorClass="UNSUPPORTED_DATA_TYPE",
                messageParameters={"data_type": f"array({typecode})"},
            )

        return list(value)

    # repark.ml vectors (lazy import — avoid hard cycle on session import).

    if module_name.startswith("repark.ml") or module_name.startswith("pyspark.ml"):
        if type_name == "DenseVector" or (hasattr(value, "toArray") and type_name == "DenseVector"):
            return list(value.toArray())

        if type_name == "SparseVector":
            if hasattr(value, "as_struct_dict"):
                return value.as_struct_dict()

            # pyspark SparseVector: size + indices + values attributes

            size = int(value.size) if not callable(value.size) else int(value.size())

            indices = list(getattr(value, "indices", []))

            values = [float(item) for item in getattr(value, "values", [])]

            return {"size": size, "indices": indices, "values": values}

    # Infinite floats refuse on every path (Arrow CDF + VALUES) — C2-Q-002.

    if isinstance(value, float) and (value == float("inf") or value == float("-inf")):
        raise PySparkTypeError("createDataFrame does not support infinite float values")

    # Nested list — normalize children (array.array / DenseVector inside lists).

    if isinstance(value, list):
        return [_normalize_create_dataframe_cell(item) for item in value]

    # pandas missing markers (NA / NaT) and float NaN — avoid importing pandas unless present.

    module_name = type(value).__module__

    type_name = type(value).__name__

    if module_name.startswith("pandas"):
        if type_name in {"NAType", "NaTType"}:
            return None

        if type_name == "Timestamp":
            return value.to_pydatetime()

        if type_name == "Timedelta":
            raise PySparkTypeError("createDataFrame does not support values of type Timedelta yet")

        if type_name == "Interval":
            raise PySparkTypeError("createDataFrame does not support values of type Interval yet")

        if type_name == "Period":
            # Period is not a SQL scalar; refuse so all-null PeriodDtype cannot soft-succeed

            # as VARCHAR while non-null Period fails (C4-Q-002 / C4-L-002).

            raise PySparkTypeError("createDataFrame does not support values of type Period yet")

    if module_name.startswith("numpy"):
        # numpy.nan is a float; other scalars unwrap.

        if type_name == "float64" or type_name == "float32":
            as_float = float(value)

            if as_float != as_float:
                return None

            return as_float

        if type_name in {"complex64", "complex128", "complexfloating"}:
            # Refuse before .item() → Python complex soft-path (C5-Q-002).

            raise PySparkTypeError(
                f"createDataFrame does not support values of type {type_name} yet"
            )

        if type_name == "datetime64":
            # NaT → None. Unit 'ns' (and finer) .item() returns int epoch ns — recover wall-clock

            # via us cast so we never emit a bare integer SQL literal for a timestamp cell.

            # Calendar units D/W/M/Y .item() → datetime.date (DATE SQL); finer → datetime.

            if str(value) == "NaT":
                return None

            item = value.item()

            if isinstance(item, (int, float)):
                casted = value.astype("datetime64[us]")

                if str(casted) == "NaT":
                    return None

                return casted.item()

            return item  # datetime.datetime or datetime.date

        if type_name == "timedelta64":
            # Unit ns .item() returns int — silent duration→count if unwrapped (C3-L-001).

            raise PySparkTypeError(
                "createDataFrame does not support values of type numpy.timedelta64 yet"
            )

        if hasattr(value, "item"):
            return _normalize_create_dataframe_cell(value.item())

    if isinstance(value, complex):
        # Python complex (incl. unwrapped numpy complex) — not a SQL scalar (C5-Q-002).

        raise PySparkTypeError("createDataFrame does not support values of type complex yet")

    if isinstance(value, float) and value != value:
        return None

    return value


def _is_pandas_dataframe(data: Any) -> bool:
    """Duck-type a pandas DataFrame without importing pandas at module load."""

    return type(data).__module__.startswith("pandas") and type(data).__name__ == "DataFrame"


def _is_polars_dataframe(data: Any) -> bool:
    """Duck-type a polars DataFrame without importing polars at module load."""

    return type(data).__module__.startswith("polars") and type(data).__name__ == "DataFrame"


def _coerce_schema_names(schema: Any) -> list[str] | None:
    """Validate name-only ``schema=`` (list/tuple of str).



    See :func:`_parse_create_dataframe_schema`.

    """

    names, _engine_types = _parse_create_dataframe_schema(schema)

    return names


def _parse_create_dataframe_schema(
    schema: Any,
) -> tuple[list[str] | None, list[str] | None]:
    """Parse ``createDataFrame(..., schema=)`` into ``(names, engine_type_strings|None)``.



    Forms (R-PARITY3, live PySpark 4.1.2):



    * ``None`` → ``(None, None)``

    * ``list``/``tuple`` of ``str`` → names only (types inferred)

    * :class:`~repark.types.StructType` → names + engine type strings per field

    * DDL string ``"a INT, b STRING"`` → same as StructType



    A bare ``str`` that is **not** a DDL schema would character-iterate into per-character

    column names — we only accept DDL when it parses as ``name TYPE`` pairs.

    """

    if schema is None:
        return None, None

    # Late import avoids a hard cycle (types imports nothing from session).

    from repark.types import (
        DataType,
        StructType,
    )

    if isinstance(schema, StructType):
        names = [field.name for field in schema.fields]

        engine_types = [_data_type_to_sql_type(field.dataType) for field in schema.fields]

        return names, engine_types

    if isinstance(schema, str):
        parsed = _parse_schema_ddl(schema)

        if parsed is not None:
            return parsed

        raise PySparkTypeError(
            "createDataFrame schema string must be a DDL field list like "
            f"'a INT, b STRING' (got {schema!r}; a bare non-DDL string would be "
            "character-iterated into column names)"
        )

    if isinstance(schema, (list, tuple)):
        names = list(schema)

        for index, name in enumerate(names):
            if not isinstance(name, str):
                raise PySparkTypeError(
                    "createDataFrame schema names must be str; "
                    f"got {type(name).__name__} at index {index}"
                )

        return names, None

    if isinstance(schema, DataType):
        # Spark wraps a bare atomic/complex type as a single-column StructType named

        # ``value`` (Apache ``test_reciprocal_trig_functions`` / ``createDataFrame(lst,

        # DoubleType())`` — F2).

        from repark.types import StructField

        wrapped = StructType([StructField("value", schema, True)])

        names = [field.name for field in wrapped.fields]

        engine_types = [_data_type_to_sql_type(field.dataType) for field in wrapped.fields]

        return names, engine_types

    raise PySparkTypeError(
        "createDataFrame schema must be a list/tuple of column name strings, "
        f"a StructType, a DDL string, or a scalar DataType; got {type(schema).__name__}"
    )


def _data_type_to_sql_type(data_type: Any) -> str:
    """Map a repark :class:`~repark.types.DataType` to a SQL cast target for VALUES cells."""

    from repark.types import (
        ArrayType,
        BinaryType,
        BooleanType,
        ByteType,
        CharType,
        DateType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        MapType,
        NullType,
        ShortType,
        StringType,
        StructType,
        TimestampNTZType,
        TimestampType,
        VarcharType,
    )

    if isinstance(data_type, IntegerType):
        return "INT"

    if isinstance(data_type, LongType):
        return "BIGINT"

    if isinstance(data_type, ShortType):
        return "SMALLINT"

    if isinstance(data_type, ByteType):
        return "TINYINT"

    if isinstance(data_type, DoubleType):
        return "DOUBLE"

    if isinstance(data_type, FloatType):
        return "FLOAT"

    if isinstance(data_type, BooleanType):
        return "BOOLEAN"

    if isinstance(data_type, (StringType, CharType, VarcharType)):
        # STRING (not VARCHAR): nested ARRAY/MAP/STRUCT engine markers are re-parsed by

        # DataType.fromDDL in _sql_type_to_arrow; fromDDL does not treat bare VARCHAR as

        # string (only string / str / varchar(n)). Using VARCHAR made every nested type

        # that contained a string field silently fall back to pa.string() (octo X2 C1).

        return "STRING"

    if isinstance(data_type, BinaryType):
        return "BINARY"

    if isinstance(data_type, DateType):
        return "DATE"

    if isinstance(data_type, (TimestampType, TimestampNTZType)):
        return "TIMESTAMP"

    if isinstance(data_type, DecimalType):
        return f"DECIMAL({data_type.precision},{data_type.scale})"

    if isinstance(data_type, NullType):
        return "VARCHAR"

    if isinstance(data_type, ArrayType):
        # Nested complex types are applied via Arrow schema (engine_types marker below).

        return f"ARRAY<{_data_type_to_sql_type(data_type.elementType)}>"

    if isinstance(data_type, MapType):
        return (
            f"MAP<{_data_type_to_sql_type(data_type.keyType)},"
            f"{_data_type_to_sql_type(data_type.valueType)}>"
        )

    if isinstance(data_type, StructType):
        inner = ",".join(
            f"{field.name}:{_data_type_to_sql_type(field.dataType)}" for field in data_type.fields
        )

        return f"STRUCT<{inner}>"

    # Fallback: engine string if present.

    engine = getattr(data_type, "_engine_type", None)

    if callable(engine):
        return str(engine())

    raise PySparkTypeError(
        f"createDataFrame schema field type {type(data_type).__name__} is not supported"
    )


def _parse_schema_ddl(ddl: str) -> tuple[list[str], list[str]] | None:
    """Parse ``'a INT, b STRING'`` / nested ``'a ARRAY<INT>'`` → names + SQL types.



    Returns None if not a field-list DDL (bare type tokens / character-iteration trap).

    Nested array/map/struct field types use :meth:`DataType.fromDDL` so Apache-style

    nested DDL schemas work the same as StructType (octo X2 C1).

    """

    stripped = ddl.strip()

    # Single token without a type is not DDL (would be the old character-iteration trap).

    if not stripped or (" " not in stripped and ":" not in stripped and "<" not in stripped):
        return None

    from repark.types import DataType, StructType

    try:
        parsed = DataType.fromDDL(stripped)

    except (ValueError, TypeError):
        return None

    if not isinstance(parsed, StructType) or not parsed.fields:
        return None

    names = [field.name for field in parsed.fields]

    engine_types = [_data_type_to_sql_type(field.dataType) for field in parsed.fields]

    return names, engine_types


def _datetime64_unit_from_dtype(dtype: Any) -> str | None:
    """Extract a numpy/pandas ``datetime64[unit]`` unit with case preserved (C5-Q-001 / C5-L-001).



    Numpy units are case-sensitive: ``M`` = month (calendar → DATE), ``m`` = minute (TIMESTAMP).

    Callers must not lowercase the dtype text before extracting the unit. Returns ``None`` when

    the spelling is not ``datetime64[…]`` (ArrowDtype ``timestamp[…]`` / bare timestamp).

    """

    raw = str(dtype)

    marker = "datetime64["

    start = raw.find(marker)

    if start < 0:
        return None

    unit_start = start + len(marker)

    unit_end = raw.find("]", unit_start)

    if unit_end < 0:
        return None

    unit = raw[unit_start:unit_end]

    # DatetimeTZDtype: ``datetime64[us, UTC]`` — unit is the token before the comma.

    if "," in unit:
        unit = unit.split(",", 1)[0].strip()

    return unit or None


def _null_sql_for_pandas_dtype(dtype: Any) -> str:
    """Map a pandas dtype to ``CAST(NULL AS …)`` for all-null columns (C3-Q-001 / C4-* / C5-*).



    Integer dtypes always map to ``BIGINT`` so all-null vs non-null occupancy cannot change

    Arrow width (VALUES emits bare Python ``int`` → int64 for every non-null integer cell —

    C4-Q-001). ``ArrowDtype`` spellings (``timestamp[ns][pyarrow]``, ``date32[day][pyarrow]``,

    ``double[pyarrow]``, …) are recognized (C4-L-002). Timedelta / duration refuse loud so

    all-null cannot soft-succeed as VARCHAR while non-null Timedelta raises (C4-Q-002).

    ``IntervalDtype`` refuses before the ``startswith("int")`` arm (``"interval…".startswith(

    "int")`` is true — silent BIGINT fail-open — C3-L-002). ``PeriodDtype`` refuses before the

    date arm (``period[D]`` ends with ``[d]`` → silent DATE; ``period[M]`` was VARCHAR while

    non-null Period raised — C4-Q-002 / C4-L-002). Categorical maps via ``categories.dtype`` so

    int categories cannot flip VARCHAR↔int64 by null occupancy (C4-Q-003). Unsupported

    ArrowDtype time/binary/nested refuse rather than VARCHAR (parity with polars — C4-Q-004 /

    C4-L-003). Calendar ``datetime64[D|W|M|Y]`` → DATE so all-null occupancy matches non-null

    unit-D cells (C3-Q-001); unit match is **case-sensitive** so minute ``m`` is never month

    ``M`` (C5-Q-001 / C5-L-001). ``complex*`` refuses (C5-Q-002). ``SparseDtype`` unwraps to

    ``subtype`` so Sparse[int64]/Sparse[bool] cannot flip VARCHAR↔payload (C5-Q-003 /

    C5-SAF-002). Sparse[object] all-null is re-typed from cell witnesses in

    ``_rows_from_pandas`` (NaN→DOUBLE, NaT→TIMESTAMP — C6-Q-001), not via this map alone.

    """

    raw_text = str(dtype)

    text = raw_text.lower()

    type_name = type(dtype).__name__

    # Period before date/[d]: "period[d]" ends with "[d]" and would soft-map to DATE.

    if "period" in text or type_name == "PeriodDtype":
        raise PySparkTypeError(
            f"createDataFrame does not support pandas Period dtypes yet (got dtype {dtype!s})"
        )

    # Interval before int/float: "interval…".startswith("int") and "float" in "interval[float64,…]".

    if "interval" in text:
        raise PySparkTypeError(
            f"createDataFrame does not support pandas Interval dtypes yet (got dtype {dtype!s})"
        )

    # complex before float/VARCHAR: all-null must not soft-succeed as VARCHAR (C5-Q-002).

    if "complex" in text or type_name.startswith("Complex"):
        raise PySparkTypeError(
            f"createDataFrame does not support pandas complex dtypes yet (got dtype {dtype!s})"
        )

    # SparseDtype before int/bool/float arms: ``Sparse[int64, nan]`` does not startswith("int")

    # and would fall through to VARCHAR while non-null cells type as int64 (C5-Q-003 / C5-SAF-002).

    # Sparse[object]: subtype unwrap alone → VARCHAR; cell witnesses run in ``_rows_from_pandas``

    # (C6-Q-001). Keep unwrap for typed subtypes (int/bool/float/…).

    if "sparse" in text or type_name == "SparseDtype":
        subtype = getattr(dtype, "subtype", None)

        if subtype is not None:
            return _null_sql_for_pandas_dtype(subtype)

        return _TYPED_NULL_SQL

    # Timestamp / datetime before bare "date" / "time" substrings (incl. ArrowDtype).

    if "datetime64" in text or text.startswith("datetime") or "timestamp" in text:
        # Calendar units → DATE (null-occupancy stable with non-null numpy/pandas unit-D → date).

        # Unit is extracted case-sensitively from the raw dtype string: numpy ``M`` = month

        # (DATE), ``m`` = minute (TIMESTAMP). Lowercasing the whole text would map both to

        # ``datetime64[m]`` and flip all-null DATE vs non-null TIMESTAMP (C5-Q-001 / C5-L-001).

        # Closed-bracket form still keeps ``datetime64[ms]`` off the calendar arm (C4-Q-001).

        unit = _datetime64_unit_from_dtype(dtype)

        if unit is not None and unit in _NUMPY_DATETIME64_DATE_UNITS:
            return "CAST(NULL AS DATE)"

        return "CAST(NULL AS TIMESTAMP)"

    if "timedelta" in text or "duration" in text:
        raise PySparkTypeError(
            "createDataFrame does not support pandas timedelta/duration dtypes yet "
            f"(got dtype {dtype!s})"
        )

    # Categorical: non-null cells are the underlying category values (int → int64, …). Map

    # all-null via categories.dtype so occupancy cannot flip VARCHAR↔payload type (C4-Q-003).

    if "category" in text or type_name == "CategoricalDtype":
        categories = getattr(dtype, "categories", None)

        categories_dtype = getattr(categories, "dtype", None) if categories is not None else None

        if categories_dtype is not None:
            return _null_sql_for_pandas_dtype(categories_dtype)

        return _TYPED_NULL_SQL

    # Unsupported ArrowDtype shapes — refuse before VARCHAR (C4-Q-004 / C4-L-003).

    # time32/time64 + binary/large_binary still refuse. Nested list/struct/map land via

    # pa.Table.from_pandas (r21 T1). dictionary stays refuse (category unwrap is separate).

    if (text.endswith("[pyarrow]") or "[pyarrow]" in text or type_name == "ArrowDtype") and (
        text.startswith("time")
        or "time32" in text
        or "time64" in text
        or "binary" in text
        or "large_binary" in text
        or "dictionary" in text
    ):
        raise PySparkTypeError(
            "createDataFrame does not support pandas Arrow time/binary/dictionary dtypes yet "
            f"(got dtype {dtype!s})"
        )

    if text in {"bool", "boolean"} or text.startswith("bool"):
        return "CAST(NULL AS BOOLEAN)"

    # float* + ArrowDtype double[pyarrow] / float[pyarrow] (C4-L-002 sibling).

    if (
        "float" in text
        or text.startswith("float")
        or text.startswith("double")
        or "double[" in text
        or text in {"float32", "float64", "float16", "double"}
    ):
        return "CAST(NULL AS DOUBLE)"

    if (
        text.startswith("int")
        or text.startswith("uint")
        or "int[" in text
        or "uint[" in text
        or text
        in {
            "int8",
            "int16",
            "int32",
            "int64",
            "uint8",
            "uint16",
            "uint32",
            "uint64",
        }
    ):
        # VALUES path always widens non-null Python int → int64; keep all-null stable (C4-Q-001).

        return "CAST(NULL AS BIGINT)"

    # date32 / date64 / pure date — after datetime/timestamp so "datetime" is not misread.

    # Period already refused above so period[D] cannot land here via endswith("[d]").

    if (
        text == "date"
        or text.startswith("date32")
        or text.startswith("date64")
        or "date32" in text
        or "date64" in text
        or (text.endswith("[d]") and "datetime" not in text and "timestamp" not in text)
    ):
        return "CAST(NULL AS DATE)"

    if "decimal" in text:
        return f"CAST(NULL AS DECIMAL({_DECIMAL_PRECISION}, {_DECIMAL_SCALE}))"

    # string / object / unknown — stable VARCHAR (C2-L-003 fallback).

    # Object-dtype all-null columns are re-typed from cell witnesses in ``_rows_from_pandas``

    # (NaN→DOUBLE, NaT→TIMESTAMP — C5-SAF-001); pure None stays VARCHAR here.

    return _TYPED_NULL_SQL


def _null_sql_for_polars_dtype(dtype: Any) -> str:
    """Map a polars dtype to ``CAST(NULL AS …)`` for all-null columns (C3-Q-001 / C4-*).



    All integer widths → ``BIGINT`` (null-occupancy-stable with VALUES int literals — C4-Q-001).

    ``Duration`` refuses (parity with non-null Timedelta / duration refuse — C4-Q-002).

    Binary / Time / Object refuse so all-null cannot soft-succeed as VARCHAR while non-null

    cells raise at the SQL literal boundary (C3-L-003). Nested ``List`` / ``Struct`` /

    ``Array`` are **accepted** via the polars ``.to_arrow()`` path (r21 T1) — they never

    hit VALUES literals.

    """

    text = str(dtype)

    text_lower = text.lower()

    if text_lower.startswith("datetime") or "datetime(" in text_lower:
        return "CAST(NULL AS TIMESTAMP)"

    if text_lower.startswith("duration") or "duration(" in text_lower:
        raise PySparkTypeError(
            f"createDataFrame does not support polars Duration dtypes yet (got dtype {dtype!s})"
        )

    # Binary / Time / Object still refuse (engine cannot represent / no VALUES path).

    # Nested List/Struct/Array pass through (r21 T1 — Arrow C-stream path).

    if text in {"Binary", "Time", "Object"} or text_lower in {"binary", "time", "object"}:
        raise PySparkTypeError(
            "createDataFrame does not support polars binary/time/object dtypes yet "
            f"(got dtype {dtype!s})"
        )

    if text == "Date" or text_lower == "date":
        return "CAST(NULL AS DATE)"

    if text in {"Boolean", "Bool"} or text_lower in {"boolean", "bool"}:
        return "CAST(NULL AS BOOLEAN)"

    if text in {"Float64", "Float32"} or text_lower.startswith("float"):
        return "CAST(NULL AS DOUBLE)"

    if (
        text in {"Int64", "Int32", "Int16", "Int8", "UInt64", "UInt32", "UInt16", "UInt8"}
        or text_lower.startswith("int")
        or text_lower.startswith("uint")
    ):
        # Match VALUES bare-int → int64; no data-dependent int32/int64 flip (C4-Q-001).

        return "CAST(NULL AS BIGINT)"

    if text_lower.startswith("decimal"):
        return f"CAST(NULL AS DECIMAL({_DECIMAL_PRECISION}, {_DECIMAL_SCALE}))"

    return _TYPED_NULL_SQL


def _pandas_dtype_needs_object_null_witness(dtype: Any) -> bool:
    """True when all-null typing must scan cells (object / Sparse[object] — C5-SAF-001 / C6-Q-001).



    Top-level object is untyped. Sparse[object] unwraps to object in the dtype map and would

    soft-map VARCHAR while non-null Sparse[object] cells type as DOUBLE/TIMESTAMP from values —

    a null-occupancy flip unless the object NaN/NaT witness runs (C6-Q-001).

    """

    text = str(dtype).lower()

    type_name = type(dtype).__name__

    if text == "object" or type_name in {"ObjectDType", "object"}:
        return True

    if "sparse" in text or type_name == "SparseDtype":
        subtype = getattr(dtype, "subtype", None)

        if subtype is None:
            return False

        subtype_text = str(subtype).lower()

        subtype_type_name = type(subtype).__name__

        return subtype_text == "object" or subtype_type_name in {"ObjectDType", "object"}

    return False


def _infer_null_sql_from_raw_cells(cells: list[Any]) -> str:
    """Infer ``CAST(NULL AS …)`` for an all-null column from pre-normalize witnesses (C4-L-001).



    Normalize erases ``float('nan')`` / ``NaT`` / ``numpy.datetime64('NaT')`` to ``None``. On

    list/dict/Row/tuple paths there is no frame dtype, so without this witness scan the VALUES

    emitter would emit VARCHAR for all-NaN (Spark double) and all-NaT (Spark timestamp).

    Pure ``None`` columns stay VARCHAR (C2-L-003).

    """

    import datetime as dt

    from decimal import Decimal

    saw_timestamp = False

    saw_date = False

    saw_decimal = False

    saw_float = False

    saw_bool = False

    saw_int = False

    for value in cells:
        if value is None:
            continue

        module_name = type(value).__module__

        type_name = type(value).__name__

        if module_name.startswith("pandas"):
            if type_name == "NAType":
                # Untyped pandas missing — no dtype witness.

                continue

            if type_name == "NaTType":
                saw_timestamp = True

                continue

            if type_name == "Timestamp":
                saw_timestamp = True

                continue

            if type_name == "Timedelta":
                raise PySparkTypeError(
                    "createDataFrame does not support values of type Timedelta yet"
                )

        if module_name.startswith("numpy"):
            if type_name == "datetime64":
                # Calendar units D/W/M/Y → DATE; finer (and ns) → TIMESTAMP (C3-Q-001).

                # Must not force TIMESTAMP for every datetime64: non-null unit-D becomes DATE,

                # so all-null NaT[D] would otherwise flip Arrow type by null occupancy.

                unit = _numpy_datetime64_unit(value)

                if unit is not None and unit in _NUMPY_DATETIME64_DATE_UNITS:
                    saw_date = True

                else:
                    saw_timestamp = True

                continue

            if type_name == "timedelta64":
                raise PySparkTypeError(
                    "createDataFrame does not support values of type numpy.timedelta64 yet"
                )

            if type_name in {"float64", "float32", "float16"}:
                saw_float = True

                continue

            if type_name in {
                "int64",
                "int32",
                "int16",
                "int8",
                "uint64",
                "uint32",
                "uint16",
                "uint8",
            }:
                saw_int = True

                continue

            if type_name in {"bool_", "bool"}:
                saw_bool = True

                continue

            if hasattr(value, "item"):
                # Unwrap other numpy scalars and re-inspect.

                try:
                    unwrapped = value.item()

                except (ValueError, AttributeError):
                    continue

                if unwrapped is value:
                    continue

                # Classify the unwrapped Python scalar (one level).

                if isinstance(unwrapped, float):
                    saw_float = True

                elif isinstance(unwrapped, bool):
                    saw_bool = True

                elif isinstance(unwrapped, int):
                    saw_int = True

                elif isinstance(unwrapped, dt.datetime):
                    saw_timestamp = True

                elif isinstance(unwrapped, dt.date):
                    saw_date = True

                elif isinstance(unwrapped, Decimal):
                    saw_decimal = True

                continue

        if isinstance(value, float):
            saw_float = True

            continue

        if isinstance(value, bool):
            saw_bool = True

            continue

        if isinstance(value, int):
            saw_int = True

            continue

        if isinstance(value, dt.datetime):
            saw_timestamp = True

            continue

        if isinstance(value, dt.date):
            saw_date = True

            continue

        if isinstance(value, Decimal):
            saw_decimal = True

            continue

        # str / unknown: do not force a type from a non-null witness of an unsupported shape;

        # all-null after normalize with only opaque witnesses falls through to VARCHAR.

    if saw_timestamp:
        return "CAST(NULL AS TIMESTAMP)"

    if saw_date:
        return "CAST(NULL AS DATE)"

    if saw_decimal:
        return f"CAST(NULL AS DECIMAL({_DECIMAL_PRECISION}, {_DECIMAL_SCALE}))"

    if saw_float:
        return "CAST(NULL AS DOUBLE)"

    if saw_bool:
        return "CAST(NULL AS BOOLEAN)"

    if saw_int:
        return "CAST(NULL AS BIGINT)"

    return _TYPED_NULL_SQL


def _column_null_sql_from_raw_tuples(
    tuples: list[tuple[Any, ...]],
    width: int,
    names: list[str] | None = None,
) -> list[str]:
    """Per-column all-null CAST for non-frame paths from raw (pre-normalize) cells (C4-L-001).



    When ``names`` is provided, unsupported ``array.array`` typecodes raise

    ``CANNOT_INFER_TYPE_FOR_FIELD`` with the column name (F1 / test_array_types).

    """

    column_null_sql: list[str] = []

    for column_index in range(width):
        cells = [row[column_index] for row in tuples]

        field_name: str | None = None

        if names is not None and column_index < len(names):
            field_name = names[column_index]

        if all(
            _normalize_create_dataframe_cell(cell, field_name=field_name) is None for cell in cells
        ):
            column_null_sql.append(_infer_null_sql_from_raw_cells(cells))

        else:
            # Non-all-null: entry unused by ``_values_sql_with_typed_nulls``; stable default.

            column_null_sql.append(_TYPED_NULL_SQL)

    return column_null_sql


def _schema_names_and_permutation(
    source_names: list[str],
    schema: list[str] | None,
    *,
    kind: str,
) -> tuple[list[str], list[int]]:
    """Resolve ``schema=[names]`` against ordered source column names (C2-L-001).



    Returns ``(output_names, permutation)`` where ``permutation[i]`` is the source index that

    feeds output column ``i``.



    * ``schema is None`` → identity (keep source names and order).

    * same name multiset as ``source_names`` → **by-name reorder** (values follow names).

    * same length, no shared names → **positional rename**.

    * length mismatch or partial name overlap → fail loud (no silent swap / project / drop).

    """

    if schema is None:
        return list(source_names), list(range(len(source_names)))

    if len(schema) != len(source_names):
        raise PySparkValueError(
            f"schema length {len(schema)} does not match {kind} column count {len(source_names)}"
        )

    if len(set(source_names)) != len(source_names):
        raise PySparkValueError(
            f"createDataFrame {kind} has duplicate column names; "
            "ambiguous schema bind is not supported"
        )

    if len(set(schema)) != len(schema):
        raise PySparkValueError(
            "createDataFrame schema has duplicate names; ambiguous schema bind is not supported"
        )

    source_set = set(source_names)

    schema_set = set(schema)

    if source_set == schema_set:
        index_by_name = {name: index for index, name in enumerate(source_names)}

        permutation = [index_by_name[name] for name in schema]

        return list(schema), permutation

    overlap = source_set & schema_set

    if overlap:
        raise PySparkValueError(
            f"createDataFrame schema partially overlaps {kind} column names "
            f"{sorted(overlap)!r}; pass a pure rename (disjoint names) or a pure "
            "reorder (same names in a different order) — mixed bind is not supported"
        )

    # Pure rename: positional cells under the new names.

    return list(schema), list(range(len(source_names)))


def _apply_permutation(row: tuple[Any, ...], permutation: list[int]) -> tuple[Any, ...]:
    """Reorder a source-order row tuple by ``permutation`` (output index → source index)."""

    return tuple(row[source_index] for source_index in permutation)


# === r21 T1: cdf-ingest ===


def _spark_dict_key_union_order(mappings: list[dict[str, Any]]) -> list[str]:
    """Spark createDataFrame dict key-union column order (live 4.1.2 oracle).



    PySpark ``_infer_schema`` sorts each dict's items alphabetically; ``_merge_type`` keeps

    the first schema's field order and **appends** newly seen keys from later rows (still in

    that later row's sorted-key order). Result for

    ``[{"c":1,"a":2},{"b":3,"a":4},{"d":5,"c":6}]`` → ``["a","c","b","d"]``.

    """

    if not mappings:
        return []

    names = sorted(mappings[0].keys())

    seen = set(names)

    for mapping in mappings[1:]:
        for key in sorted(mapping.keys()):
            if key not in seen:
                names.append(key)

                seen.add(key)

    return names


def _bind_named_row(
    mapping: dict[str, Any],
    names: list[str],
    *,
    kind: str,
    allow_extra: bool = False,
    allow_missing: bool = False,
) -> tuple[Any, ...]:
    """Bind a name→value mapping to ``names``.



    * Default (Row path / strict name lists): missing keys and extra keys fail loud

      (BUG-007 / C1-L-001 / C2-L-004 — a typo must not become an all-null column).

    * Dict key-union / StructType null-fill (r21 T1): ``allow_missing=True`` yields

      ``None`` for absent keys (Spark null fill); ``allow_extra=True`` ignores keys not in

      ``names`` (Spark drops extras under an explicit StructType schema).



    Explicit ``None`` values are always kept (SQL NULL).

    """

    if not allow_missing:
        missing = [name for name in names if name not in mapping]

        if missing:
            raise PySparkValueError(
                f"createDataFrame {kind} row is missing field(s) {missing!r} "
                f"(expected keys {list(names)!r}; silent NULL-fill is not supported)"
            )

    if not allow_extra:
        extra = [key for key in mapping if key not in names]

        if extra:
            raise PySparkValueError(
                f"createDataFrame {kind} row has unexpected field(s) {extra!r} "
                f"(expected keys {list(names)!r}; silent drop is not supported)"
            )

    # Raw cells — normalize later so all-null NaN/NaT can still witness DOUBLE/TIMESTAMP (C4-L-001).

    if allow_missing:
        return tuple(mapping.get(name) for name in names)

    return tuple(mapping[name] for name in names)


def _rows_from_mapping_list(
    data: list[Any],
    schema: list[str] | None,
    *,
    kind: str,
    as_mapping: Any,
    null_fill: bool = False,
    key_union: bool = False,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Convert homogeneous dict/Row lists to (names, row tuples) with unified schema bind.



    Cells are left un-normalized so the caller can infer all-null CAST types from NaN/NaT

    witnesses before erasure (C4-L-001).



    * ``kind="dict"`` + ``key_union=True`` (schema=None): Spark key-union across rows with

      null fill for missing fields (r21 T1 / live 4.1.2 oracle).

    * ``kind="dict"`` + ``null_fill=True`` (StructType/DDL schema): bind to schema field

      names; missing → None; extras dropped.

    * ``kind="Row"`` / strict name lists: exact key-set match — refuse missing and extra.

    """

    mappings: list[dict[str, Any]] = []

    for row_index, row in enumerate(data):
        if kind == "dict" and not isinstance(row, dict):
            raise PySparkTypeError(
                "createDataFrame dict lists must be homogeneous; "
                f"got element type {type(row).__name__} at index {row_index}"
            )

        if kind == "Row":
            from repark.row import Row

            if not isinstance(row, Row):
                raise PySparkTypeError(
                    "createDataFrame Row lists must be homogeneous; "
                    f"got element type {type(row).__name__} at index {row_index}"
                )

        mappings.append(as_mapping(row))

    if key_union and kind == "dict" and schema is None:
        # Spark-parity: sorted first-row keys, then append newly seen keys (sorted per row).

        source_names = _spark_dict_key_union_order(mappings)

        names, permutation = list(source_names), list(range(len(source_names)))

        allow_missing = True

        allow_extra = True

    elif null_fill and kind == "dict" and schema is not None:
        # Explicit StructType / DDL: schema field order is authoritative; null-fill + drop extras.

        source_names = list(schema)

        names, permutation = list(schema), list(range(len(schema)))

        allow_missing = True

        allow_extra = True

    else:
        source_names = list(mappings[0].keys())

        names, permutation = _schema_names_and_permutation(source_names, schema, kind=kind)

        allow_missing = False

        allow_extra = False

    tuples: list[tuple[Any, ...]] = []

    for mapping in mappings:
        source_row = _bind_named_row(
            mapping,
            source_names,
            kind=kind,
            allow_extra=allow_extra,
            allow_missing=allow_missing,
        )

        tuples.append(_apply_permutation(source_row, permutation))

    return names, tuples


def _refuse_duplicate_pandas_columns(data: Any) -> None:
    """Fail loud on duplicate pandas column names (critic-octo C2).



    ``data[name].dtype`` returns a DataFrame when names collide (AttributeError on ``.dtype``),

    and ``pa.Table.from_pandas`` raises a bare ValueError. Surface a stable PySparkValueError

    before either path.

    """

    source_columns = [str(column) for column in data.columns]

    if len(source_columns) != len(set(source_columns)):
        raise PySparkValueError(
            f"createDataFrame pandas DataFrame has duplicate column names: {source_columns}"
        )


def _rows_from_pandas(
    data: Any, schema: list[str] | None
) -> tuple[list[str], list[tuple[Any, ...]], list[str]]:
    """Convert a pandas DataFrame to (column names, row tuples, per-col null SQL) for VALUES.



    Empty frames raise :class:`PySparkValueError` (Spark ``CANNOT_INFER_EMPTY_SCHEMA``) — the

    VALUES path has no StructType schema, so types cannot be inferred from zero rows.



    Schema bind: pure reorder by name, pure rename positionally (C2-L-001); length and partial

    overlap fail loud. Per-column null SQL preserves source dtypes for all-null columns

    (C3-Q-001) so Arrow types are not silently forced to string.

    """

    _refuse_duplicate_pandas_columns(data)

    source_columns = [str(column) for column in data.columns]

    names, permutation = _schema_names_and_permutation(source_columns, schema, kind="pandas")

    if len(data) == 0:
        raise PySparkValueError(
            "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema for empty pandas DataFrame; "
            "pass a non-empty frame or a typed StructType schema "
            "(repark createDataFrame is VALUES-only and has no StructType path yet)"
        )

    # Positional series (iloc) — name lookup is wrong under duplicate labels (octo C2).

    column_series = [data.iloc[:, source_index] for source_index in range(data.shape[1])]

    # Per-column null SQL: dtype map for typed columns; object / Sparse[object] is untyped so

    # all-null columns witness raw cells (NaN→DOUBLE, NaT→TIMESTAMP) like the list path

    # (C5-SAF-001 / C6-Q-001). Sparse[int64]/Sparse[bool] stay on the dtype-map unwrap path.

    source_null_sql: list[str] = []

    for series in column_series:
        dtype = series.dtype

        if _pandas_dtype_needs_object_null_witness(dtype):
            raw_cells = [series.iloc[row_index] for row_index in range(len(series))]

            if all(_normalize_create_dataframe_cell(cell) is None for cell in raw_cells):
                source_null_sql.append(_infer_null_sql_from_raw_cells(raw_cells))

            else:
                # Non-all-null: entry unused by ``_values_sql_with_typed_nulls``.

                source_null_sql.append(_TYPED_NULL_SQL)

        else:
            source_null_sql.append(_null_sql_for_pandas_dtype(dtype))

    column_null_sql = [source_null_sql[source_index] for source_index in permutation]

    tuples: list[tuple[Any, ...]] = []

    for row_index in range(len(data)):
        source_row = tuple(
            _normalize_create_dataframe_cell(series.iloc[row_index]) for series in column_series
        )

        tuples.append(_apply_permutation(source_row, permutation))

    return names, tuples, column_null_sql


def _rows_from_polars(
    data: Any, schema: list[str] | None
) -> tuple[list[str], list[tuple[Any, ...]], list[str]]:
    """Convert a polars DataFrame to (column names, row tuples, per-col null SQL) for VALUES.



    Empty frames raise (same CANNOT_INFER_EMPTY_SCHEMA class as pandas). Schema bind matches

    pandas (name reorder / positional rename / fail-loud partial). All-null typed columns keep

    dtype-matched CAST nulls (C3-Q-001).

    """

    source_columns = list(data.columns)

    names, permutation = _schema_names_and_permutation(source_columns, schema, kind="polars")

    if data.height == 0:
        raise PySparkValueError(
            "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema for empty polars DataFrame; "
            "pass a non-empty frame or a typed StructType schema "
            "(repark createDataFrame is VALUES-only and has no StructType path yet)"
        )

    source_null_sql = [_null_sql_for_polars_dtype(dtype) for dtype in data.dtypes]

    column_null_sql = [source_null_sql[source_index] for source_index in permutation]

    tuples = []

    for row_index in range(data.height):
        source_row = tuple(
            _normalize_create_dataframe_cell(cell) for cell in data.row(row_index, named=False)
        )

        tuples.append(_apply_permutation(source_row, permutation))

    return names, tuples, column_null_sql


def _empty_frame_sql(names: list[str]) -> str:
    """Build a zero-row SELECT with typed null columns (stable Arrow string types)."""

    nulls = ", ".join(f"{_TYPED_NULL_SQL} AS {_quote_ident(name)}" for name in names)

    return f"SELECT {nulls} WHERE 1 = 0"


def _empty_typed_arrow_frame(
    session: ReparkSession,
    names: list[str],
    engine_types: list[str],
) -> DataFrame:
    """Zero-row createDataFrame keeping StructType/DDL/scalar DataType types (octo C2-Q-001)."""

    if len(engine_types) != len(names):
        raise PySparkValueError(
            f"schema type count {len(engine_types)} does not match name count {len(names)}"
        )

    column_null_sql = [f"CAST(NULL AS {sql_type})" for sql_type in engine_types]

    arrow_table = _arrow_table_from_tuples(
        names, [], column_null_sql=column_null_sql, engine_types=engine_types
    )

    return _materialize_arrow_as_memtable_frame(session, arrow_table)


def _values_sql_with_typed_nulls(
    names: list[str],
    tuples: list[tuple[Any, ...]],
    *,
    column_null_sql: list[str] | None = None,
) -> str:
    """Emit VALUES SQL; all-null columns use a typed CAST (default VARCHAR — C2-L-003 / C3-Q-001).



    When ``column_null_sql`` is provided (pandas/polars source dtypes), all-null columns use that

    CAST so Arrow types match the source dtype rather than silent string.

    """

    width = len(names)

    if column_null_sql is not None and len(column_null_sql) != width:
        raise PySparkValueError(
            f"column_null_sql length {len(column_null_sql)} does not match schema width {width}"
        )

    all_null_columns = {
        column_index
        for column_index in range(width)
        if all(row[column_index] is None for row in tuples)
    }

    value_rows: list[str] = []

    for row in tuples:
        if len(row) != width:
            raise PySparkValueError("ragged rows are not supported by createDataFrame")

        cells: list[str] = []

        for column_index, cell in enumerate(row):
            if cell is None and column_index in all_null_columns:
                if column_null_sql is not None:
                    cells.append(column_null_sql[column_index])

                else:
                    cells.append(_TYPED_NULL_SQL)

            else:
                cells.append(_sql_literal(cell))

        value_rows.append("(" + ", ".join(cells) + ")")

    values_sql = ", ".join(value_rows)

    alias_cols = ", ".join(_quote_ident(name) for name in names)

    return f"SELECT * FROM (VALUES {values_sql}) AS t({alias_cols})"


def _create_dataframe_from_rows(
    session: ReparkSession,
    data: Any,
    schema: Any,
) -> DataFrame:
    """Materialize row data as a DataFrame via Arrow MemTable (C-stream; IPC skew fallback).



    Non-empty inputs and typed empty frames build a ``pyarrow.Table`` then register via

    :func:`_materialize_arrow_as_memtable_frame`. Untyped empty frames still use a

    ``WHERE 1 = 0`` VALUES seed via :func:`_materialize_values_as_memtable_frame`.

    """

    # === r21 T1 (combine rider): legacy first-element coerce follows the session conf.

    legacy_first = str(
        session.conf.get("spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled", "false")
    ).lower() in {"true", "1"}

    # === r23b N1: nested dict-cell → StructType (Spark SPARK-35929) ===

    # strip() matches other bool conf parsers in this module (octo C2-Q-001).

    infer_dict_as_struct = str(
        session.conf.get("spark.sql.pyspark.inferNestedDictAsStruct.enabled", "false")
    ).strip().lower() in {"true", "1"}

    token_legacy = _LEGACY_FIRST_ELEMENT_COERCE.set(legacy_first)

    token_struct = _INFER_NESTED_DICT_AS_STRUCT.set(infer_dict_as_struct)

    try:
        return _create_dataframe_from_rows_inner(session, data, schema)

    finally:
        _LEGACY_FIRST_ELEMENT_COERCE.reset(token_legacy)

        _INFER_NESTED_DICT_AS_STRUCT.reset(token_struct)


def _create_dataframe_from_rows_inner(
    session: ReparkSession,
    data: Any,
    schema: Any,
) -> DataFrame:
    """Body of :func:`_create_dataframe_from_rows` under the legacy-coerce contextvar."""

    from repark.row import Row

    schema_names, engine_types = _parse_create_dataframe_schema(schema)

    schema = schema_names

    column_null_sql: list[str] | None = None

    # Frame-shaped inputs first — never `if not data` on a DataFrame (pandas raises

    # "truth value is ambiguous"; that was the G-INT bug that forced this expansion).

    # === r20 P2a: cdf-extractor ===

    # P2a: pandas/polars → native Arrow (from_pandas / .to_arrow) + cast/null/refuse rules,

    # then P1a C-stream materialize. No per-row Python tuple explode on the hot path.

    if _is_pandas_dataframe(data):
        import repark.session as _session_pkg

        arrow_table = _session_pkg._arrow_table_from_pandas(data, schema, engine_types=engine_types)

        return _materialize_arrow_as_memtable_frame(session, arrow_table)

    if _is_polars_dataframe(data):
        import repark.session as _session_pkg

        arrow_table = _session_pkg._arrow_table_from_polars(data, schema, engine_types=engine_types)

        return _materialize_arrow_as_memtable_frame(session, arrow_table)

    if isinstance(data, list):
        if not data:
            if not schema:
                raise PySparkValueError(
                    "createDataFrame on empty data requires a schema (column name list)"
                )

            # Typed schema (StructType / DDL / bare DataType wrap) must keep declared

            # types on a 0-row frame — string default was silent wrong (octo C2-Q-001).

            if engine_types is not None:
                return _empty_typed_arrow_frame(session, list(schema), engine_types)

            return _materialize_values_as_memtable_frame(session, _empty_frame_sql(list(schema)))

        first = data[0]

        if isinstance(first, dict):
            # r21 T1: schema=None → Spark key-union; StructType/DDL → null-fill field names.

            names, tuples = _rows_from_mapping_list(
                data,
                schema,
                kind="dict",
                as_mapping=lambda row: row,
                null_fill=engine_types is not None,
                key_union=schema is None,
            )

        elif isinstance(first, Row):
            # Row stays fail-loud on key mismatch (Spark STRUCT_ARRAY_LENGTH_MISMATCH class).

            names, tuples = _rows_from_mapping_list(
                data, schema, kind="Row", as_mapping=lambda row: row.asDict()
            )

        elif isinstance(first, (list, tuple)):
            width = len(first)

            fields = getattr(first, "_fields", None)

            if fields is not None:
                # collections.namedtuple / typing.NamedTuple — source names are _fields

                # (C3-Q-002). schema= uses the same by-name reorder / positional rename /

                # fail-loud partial as dict/Row (C6-L-001); never positional-only when names

                # are known (that swapped values vs dict/Row/pandas/polars under reorder).

                names, permutation = _schema_names_and_permutation(
                    list(fields), schema, kind="namedtuple"
                )

            elif schema is not None:
                names = list(schema)

                permutation = list(range(width))

            else:
                names = [f"_{index + 1}" for index in range(width)]

                permutation = list(range(width))

            # Spark pads a short name list with ``_2``, ``_3``, … (1-based position of the

            # missing columns — Apache ``test_infer_schema_not_enough_names``). Too many names

            # still fails loud (width mismatch).

            if len(names) < width:
                names = list(names) + [f"_{index + 1}" for index in range(len(names), width)]

            if len(names) != width:
                raise PySparkValueError(
                    f"schema length {len(names)} does not match row width {width}"
                )

            tuples = []

            for row_index, row in enumerate(data):
                # Refuse str / other iterables — character-iterating a string yields wrong rows

                # (C1-Q-002). Only list/tuple are row shapes on this path.

                if not isinstance(row, (list, tuple)):
                    raise PySparkTypeError(
                        "createDataFrame tuple/list rows must be homogeneous list/tuple rows; "
                        f"got element type {type(row).__name__} at index {row_index}"
                    )

                if len(row) != width:
                    raise PySparkValueError(
                        "ragged rows are not supported by createDataFrame "
                        f"(row 0 width {width}, row {row_index} width {len(row)})"
                    )

                # Raw cells — normalize after all-null type inference (C4-L-001).

                tuples.append(_apply_permutation(tuple(row), permutation))

        elif schema is not None and len(schema) == 1:
            # Scalar cells + single-column schema (typically from a bare DataType wrap →

            # ``StructField("value", …)``). Spark accepts ``createDataFrame([0.0, 1.0],

            # DoubleType())`` (F2 / Apache test_reciprocal_trig_functions).

            names = list(schema)

            tuples = []

            for row_index, cell in enumerate(data):
                if isinstance(cell, (list, tuple, dict)) or (
                    type(cell).__name__ == "Row"
                    and type(cell).__module__.startswith(("repark", "pyspark"))
                ):
                    raise PySparkTypeError(
                        "createDataFrame scalar-schema path expects scalar cells; "
                        f"got element type {type(cell).__name__} at index {row_index}"
                    )

                tuples.append((cell,))

        else:
            raise PySparkTypeError(
                "createDataFrame expects a list of tuples/lists, dicts, or Row, "
                f"got element type {type(first).__name__}"
            )

    else:
        raise PySparkTypeError(
            "createDataFrame expects a list of rows, a pandas DataFrame, or a polars "
            f"DataFrame, got {type(data).__name__}"
        )

    if not tuples:
        if not names:
            raise PySparkValueError(
                "createDataFrame on empty data requires a schema (column name list)"
            )

        if engine_types is not None:
            return _empty_typed_arrow_frame(session, names, engine_types)

        return _materialize_values_as_memtable_frame(session, _empty_frame_sql(names))

    # Non-frame paths: infer CAST from pre-normalize NaN/NaT/… witnesses, then erase missing

    # markers. Frame paths already provide dtype-matched column_null_sql and normalized cells.

    if column_null_sql is None:
        width = len(names)

        column_null_sql = _column_null_sql_from_raw_tuples(tuples, width, names=names)

        tuples = [
            tuple(
                _normalize_create_dataframe_cell(cell, field_name=names[column_index])
                for column_index, cell in enumerate(row)
            )
            for row in tuples
        ]

    # Explicit StructType / DDL types override null-witness casts so IntegerType stays INT

    # (int32) rather than the VALUES-path Python-int → BIGINT widening (R-PARITY3 / G-INT).

    if engine_types is not None:
        if len(engine_types) != len(names):
            raise PySparkValueError(
                f"schema type count {len(engine_types)} does not match name count {len(names)}"
            )

        column_null_sql = [f"CAST(NULL AS {sql_type})" for sql_type in engine_types]

    # R-PERF-ARROW-CDF + P1a C-stream: build a pyarrow.Table with the inferred/declared types

    # and register a MemTable via Arrow C Stream (no IPC encode/to_vec; no VALUES SQL plan).

    arrow_table = _arrow_table_from_tuples(
        names, tuples, column_null_sql=column_null_sql, engine_types=engine_types
    )

    return _materialize_arrow_as_memtable_frame(session, arrow_table)


def _register_cdf_view_cleanup(session: ReparkSession, frame: DataFrame, view_name: str) -> None:
    """Drop ``__repark_cdf_*`` when the owning DataFrame is GC'd (R-FACADE-HYGIENE W7).



    Uses :func:`weakref.finalize` — no new public close API. Pin is bounded-growth after

    ``gc.collect()`` x2, not exact-zero (greylight B7).

    """

    import weakref

    def _drop_view(
        session_ref: ReparkSession = session,
        name: str = view_name,
    ) -> None:

        with contextlib.suppress(Exception):
            # Session may already be stopped; best-effort cleanup only.

            session_ref._ensure_alive().drop_temp_view(name)

    weakref.finalize(frame, _drop_view)


def _materialize_values_as_memtable_frame(session: ReparkSession, values_sql: str) -> DataFrame:
    """Plan VALUES once, collect into a MemTable temp view, return a scan of that view.



    Retained for untyped empty-frame SQL paths (`WHERE 1 = 0`). Non-empty createDataFrame

    and typed empty frames use :func:`_materialize_arrow_as_memtable_frame` (R-PERF-ARROW-CDF /

    P1a C-stream).

    """

    ephemeral = session.sql(values_sql)

    view_name = f"__repark_cdf_{uuid.uuid4().hex}"

    native = session._ensure_alive()

    registered = False

    try:
        native.materialize_as_temp_view(view_name, ephemeral._inner)

        registered = True

        frame = session.sql(f"SELECT * FROM {view_name}")

    except BaseException:
        # Drop orphan MemTable if sql() fails after register (parity mapInArrow C3-SAF-001).

        # BaseException so KeyboardInterrupt/SystemExit also release the view (octo C3).

        if registered:
            with contextlib.suppress(Exception):
                native.drop_temp_view(view_name)

        raise

    _register_cdf_view_cleanup(session, frame, view_name)

    return frame


def _materialize_arrow_as_memtable_frame(session: ReparkSession, table: Any) -> DataFrame:
    """Register a ``pyarrow.Table`` as a MemTable temp view; return a scan of that view.



    Prefers the Arrow **C Stream** seam (``register_arrow_stream_as_temp_view``) so the

    table rides ``__arrow_c_stream__`` into the engine with **no** IPC encode /

    ``ipc_bytes.to_vec()`` intermediate (P1a / scout #4). When the native C-stream symbol

    is absent (version-skew), falls back to the R-PERF-ARROW-CDF IPC path.



    If registration succeeds and the follow-up ``SELECT * FROM`` view scan fails (or a

    ``BaseException`` such as ``KeyboardInterrupt`` is raised after register), the MemTable

    is dropped immediately so the session does not retain an untracked ``__repark_cdf_*``

    view (octo P1a C1 SAF-001 / C3; same discipline as mapInArrow C3-SAF-001).

    """

    import pyarrow as pa

    if not isinstance(table, pa.Table):
        raise TypeError(f"expected pyarrow.Table, got {type(table).__name__}")

    view_name = f"__repark_cdf_{uuid.uuid4().hex}"

    native = session._ensure_alive()

    register_stream = getattr(native, "register_arrow_stream_as_temp_view", None)

    registered = False

    try:
        if callable(register_stream):
            # pa.Table is an Arrow C Stream exporter — same path mapInArrow uses post-I4.

            register_stream(view_name, table)

        else:
            # Version-skew fallback: IPC encode + register_ipc_stream_as_temp_view.

            import io

            import pyarrow.ipc as pa_ipc

            sink = io.BytesIO()

            with pa_ipc.new_stream(sink, table.schema) as writer:
                for batch in table.to_batches():
                    writer.write_batch(batch)

            native.register_ipc_stream_as_temp_view(view_name, sink.getvalue())

        registered = True

        frame = session.sql(f"SELECT * FROM {view_name}")

    except BaseException:
        # BaseException: also drop on KeyboardInterrupt/SystemExit after register (octo C3).

        if registered:
            with contextlib.suppress(Exception):
                native.drop_temp_view(view_name)

        raise

    _register_cdf_view_cleanup(session, frame, view_name)

    return frame


def _validate_decimal_envelope(value: Any) -> None:
    """Refuse Decimal values outside Spark's inferred DECIMAL(38, 18) envelope (C2-L-002)."""

    from decimal import ROUND_DOWN, Decimal

    if not value.is_finite():
        raise PySparkTypeError(
            f"createDataFrame does not support non-finite Decimal values (got {value!s})"
        )

    if abs(value) >= _DECIMAL_MAX_ABS:
        raise PySparkValueError(
            f"createDataFrame Decimal value {value!s} exceeds DECIMAL("
            f"{_DECIMAL_PRECISION}, {_DECIMAL_SCALE}) magnitude "
            f"(|value| must be < 10**{_DECIMAL_PRECISION - _DECIMAL_SCALE})"
        )

    quantum = Decimal(1).scaleb(-_DECIMAL_SCALE)

    quantized = value.quantize(quantum, rounding=ROUND_DOWN)

    if quantized != value:
        raise PySparkValueError(
            f"createDataFrame Decimal value {value!s} is outside DECIMAL("
            f"{_DECIMAL_PRECISION}, {_DECIMAL_SCALE}) scale "
            "(fractional digits beyond 18 are not representable without rounding; "
            "refuse rather than silent zero/round)"
        )


def _infer_arrow_type_from_python_sample(sample: Any) -> Any:
    """Best-effort pyarrow type from a single Python sample cell (nested createDataFrame)."""

    import datetime as _dt

    from decimal import Decimal as _Decimal

    import pyarrow as pa

    if sample is None:
        return pa.string()

    if isinstance(sample, bool):
        return pa.bool_()

    if isinstance(sample, int) and not isinstance(sample, bool):
        return pa.int64()

    if isinstance(sample, float):
        return pa.float64()

    if isinstance(sample, str):
        return pa.string()

    if isinstance(sample, (bytes, bytearray, memoryview)):
        return pa.binary()

    if isinstance(sample, _dt.datetime):
        return pa.timestamp("us", tz="UTC" if sample.tzinfo else None)

    if isinstance(sample, _dt.date):
        return pa.date32()

    if isinstance(sample, _Decimal):
        return pa.decimal128(38, 18)

    if isinstance(sample, list):
        non_null = [item for item in sample if item is not None]

        # === r23b N1: list element merge under inferNestedDictAsStruct ===

        # Live Spark merges ALL element types (ArrayType _merge_type) unless legacy

        # first-element conf is on. List-of-dict → struct field union; nested

        # list<list<dict>> must also merge sibling element schemas (octo C2-L-001).

        # Empty list under conf true → list<null> so multi-row / multi-sample merge

        # can still adopt a concrete element type (octo C3-L-001; empty→string was

        # swallowing later struct elements via string-wins-all).

        if _INFER_NESTED_DICT_AS_STRUCT.get() and not non_null:
            return pa.list_(pa.null())

        if _INFER_NESTED_DICT_AS_STRUCT.get() and non_null:
            if all(isinstance(item, dict) for item in non_null):
                if _LEGACY_FIRST_ELEMENT_COERCE.get():
                    return pa.list_(_infer_struct_arrow_from_dict_samples([non_null[0]]))

                return pa.list_(_infer_struct_arrow_from_dict_samples(non_null))

            if _LEGACY_FIRST_ELEMENT_COERCE.get():
                return pa.list_(_infer_arrow_type_from_python_sample(non_null[0]))

            merged_element = _infer_arrow_type_from_python_sample(non_null[0])

            for item in non_null[1:]:
                merged_element = _merge_inferred_arrow_types(
                    merged_element, _infer_arrow_type_from_python_sample(item)
                )

            return pa.list_(merged_element)

        element = next((item for item in sample if item is not None), None)

        return pa.list_(_infer_arrow_type_from_python_sample(element))

    if type(sample).__name__ == "Row" and type(sample).__module__.startswith("repark"):
        return pa.struct(
            [
                (name, _infer_arrow_type_from_python_sample(value))
                for name, value in zip(sample.__fields__, list(sample), strict=True)
            ]
        )

    # Spark createDataFrame: bare tuple → struct with positional ``_1``, ``_2``, … fields

    # (1-based; Apache ``test_print_schema`` / nested ``(2, 2)`` — F2). Namedtuples are handled

    # as row shapes earlier; here the sample is a nested cell.

    if isinstance(sample, tuple):
        if not sample:
            return pa.struct([])

        return pa.struct(
            [
                (f"_{index + 1}", _infer_arrow_type_from_python_sample(value))
                for index, value in enumerate(sample)
            ]
        )

    if isinstance(sample, dict):
        # === r23b N1: conf true → StructType for dict-valued cells (SPARK-35929) ===

        # Row-dicts never reach this helper (they go through key-union / mapping bind).

        if _INFER_NESTED_DICT_AS_STRUCT.get():
            return _infer_struct_arrow_from_dict_samples([sample])

        # Spark schema inference: Python dict → map (key type from samples). Non-str keys

        # must not force map<string,…> then fail at array build (octo X2 C2).

        # Mixed value types (e.g. Legs [{"LegId":1,"Side":"Buy"}]) → map value string

        # (Spark 4.1.2 stringifies map values under key-union / mixed inference — r21 T1).

        if not sample:
            return pa.map_(pa.string(), pa.string())

        key_sample = next(iter(sample.keys()))

        value_types: list[Any] = []

        for value in sample.values():
            if value is None:
                continue

            value_types.append(_infer_arrow_type_from_python_sample(value))

        if not value_types:
            value_arrow = pa.string()

        else:
            first_value_type = value_types[0]

            if all(value_type.equals(first_value_type) for value_type in value_types[1:]):
                value_arrow = first_value_type

            else:
                value_arrow = pa.string()

        return pa.map_(
            _infer_arrow_type_from_python_sample(key_sample),
            value_arrow,
        )

    return pa.string()


# === r21 T1 (combine rider): legacy first-element inference coerce mode ===

# repark's sample inference is first-element-only by construction, which matches Spark's

# LEGACY spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled=true mode. In that

# mode Spark COERCES later mismatched numerics toward the first element's type (float -> long

# truncate; Apache test_infer_nested_array_element_type_with_struct); with the conf off Spark

# raises CANNOT_MERGE_TYPE at merge — which the T1 refuses model. The contextvar carries the

# session conf into the recursive cell walk without threading a parameter through every site.

_LEGACY_FIRST_ELEMENT_COERCE: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "repark_legacy_first_element_coerce", default=False
)


# === r23b N1: inferNestedDictAsStruct conf (SPARK-35929) ===

# When true, dict-valued *cells* (column values, any nesting depth) infer as StructType with

# field union across samples (null-fill missing keys). Default false keeps MapType. Sparse

# vector exact-key struct path stays conf-invariant (checked before this flag). Row-dicts

# (createDataFrame list-of-dicts key-union) never consult this contextvar.

_INFER_NESTED_DICT_AS_STRUCT: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "repark_infer_nested_dict_as_struct", default=False
)


def _arrow_type_merge_label(arrow_type: Any) -> str:
    """Spark-ish type name for CANNOT_MERGE_TYPE messages from Arrow types."""

    import pyarrow as pa

    if pa.types.is_boolean(arrow_type):
        return "BooleanType"

    if pa.types.is_integer(arrow_type):
        return "LongType"

    if pa.types.is_floating(arrow_type):
        return "DoubleType"

    if pa.types.is_decimal(arrow_type):
        return "DecimalType"

    if pa.types.is_timestamp(arrow_type):
        return "TimestampType"

    if pa.types.is_date(arrow_type):
        return "DateType"

    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "StringType"

    if pa.types.is_struct(arrow_type):
        return "StructType"

    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return "ArrayType"

    if pa.types.is_map(arrow_type):
        return "MapType"

    return str(arrow_type)


def _arrow_type_is_nested(arrow_type: Any) -> bool:
    """True for list/struct/map (string must not silently win over these — octo C3)."""

    import pyarrow as pa

    return bool(
        pa.types.is_struct(arrow_type)
        or pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
        or pa.types.is_map(arrow_type)
    )


def _merge_inferred_arrow_types(left: Any, right: Any) -> Any:
    """Merge two inferred Arrow types (Spark ``_merge_type`` subset for dict-as-struct).



    NullType is soft (merges as the other side). String wins over **atomic** only

    (live Spark long+string → string) — never over nested list/struct/map (that

    stringified dict cells — octo C3-L-001). Long+Double / other incompatible scalar

    pairs refuse ``CANNOT_MERGE_TYPE``. Nested list/struct/map recurse.

    """

    import pyarrow as pa

    if left.equals(right):
        return left

    # NullType (empty-list element under conf true) absorbs into the concrete side.

    if pa.types.is_null(left):
        return right

    if pa.types.is_null(right):
        return left

    if (pa.types.is_list(left) or pa.types.is_large_list(left)) and (
        pa.types.is_list(right) or pa.types.is_large_list(right)
    ):
        return pa.list_(_merge_inferred_arrow_types(left.value_type, right.value_type))

    if pa.types.is_struct(left) and pa.types.is_struct(right):
        return _merge_struct_arrow_types(left, right)

    if pa.types.is_map(left) and pa.types.is_map(right):
        return pa.map_(
            _merge_inferred_arrow_types(left.key_type, right.key_type),
            _merge_inferred_arrow_types(left.item_type, right.item_type),
        )

    # Atomic + String → String (Spark promotes; Apache long+str field pin).

    # Nested + String refuses (do not stringify struct/list cells).

    if pa.types.is_string(left) or pa.types.is_large_string(left):
        if _arrow_type_is_nested(right):
            left_label = _arrow_type_merge_label(left)

            right_label = _arrow_type_merge_label(right)

            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type {left_label} and {right_label}"
            )

        return left

    if pa.types.is_string(right) or pa.types.is_large_string(right):
        if _arrow_type_is_nested(left):
            left_label = _arrow_type_merge_label(left)

            right_label = _arrow_type_merge_label(right)

            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type {left_label} and {right_label}"
            )

        return right

    left_label = _arrow_type_merge_label(left)

    right_label = _arrow_type_merge_label(right)

    raise PySparkTypeError(f"[CANNOT_MERGE_TYPE] Can not merge type {left_label} and {right_label}")


def _merge_struct_arrow_types(left: Any, right: Any) -> Any:
    """Union two struct types: keep left field order, append new fields from right.



    Live Spark ``_merge_type`` for StructType (field-union order pin).

    """

    import pyarrow as pa

    right_by_name = {field.name: field.type for field in right}

    fields: list[tuple[str, Any]] = []

    seen: set[str] = set()

    for field in left:
        name = field.name

        seen.add(name)

        if name in right_by_name:
            fields.append((name, _merge_inferred_arrow_types(field.type, right_by_name[name])))

        else:
            fields.append((name, field.type))

    for field in right:
        if field.name not in seen:
            fields.append((field.name, field.type))

    return pa.struct(fields)


def _infer_struct_arrow_from_dict_samples(samples: list[dict[str, Any]]) -> Any:
    """Build a struct Arrow type by unioning keys across dict *cell* samples.



    Field order: insertion order of the first sample that contributes each key

    (Spark dict-as-struct uses ``dict.items()`` order, not sorted row-key-union order).

    Null values do not contribute a field type (live: ``{"a": None, "b": 1}`` → only ``b``).

    Non-string keys refuse (Spark ``field name … should be a string``).

    """

    import pyarrow as pa

    field_order: list[str] = []

    field_types: dict[str, Any] = {}

    for sample in samples:
        if not isinstance(sample, dict):
            continue

        for key, value in sample.items():
            # Null *values* do not contribute a field type (live Spark). Null *keys*

            # are not valid struct field names — refuse (octo C1-L-002); do not

            # silently skip and drop the cell's association.

            if key is None:
                raise PySparkTypeError("field name None should be a string")

            if value is None:
                continue

            if not isinstance(key, str):
                raise PySparkTypeError(f"field name {key!r} should be a string")

            inferred = _infer_arrow_type_from_python_sample(value)

            if key not in field_types:
                field_order.append(key)

                field_types[key] = inferred

            else:
                field_types[key] = _merge_inferred_arrow_types(field_types[key], inferred)

    return pa.struct([(name, field_types[name]) for name in field_order])


def _prepare_nested_cell(cell: Any, arrow_type: Any) -> Any:
    """Convert Row / dict / list cells into shapes ``pa.array`` accepts for ``arrow_type``.



    Also coerces Python values toward the declared Arrow type (Spark createDataFrame

    stringifies non-strings into StringType columns — Apache ``test_convert_list_to_str``).

    """

    import pyarrow as pa

    if cell is None:
        return None

    # Declared string column: Spark ``to_str`` for non-string Python values.

    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        if isinstance(cell, str):
            return cell

        return str(cell)

    if pa.types.is_integer(arrow_type):
        # Never ``int(float)`` / Arrow Decimal→int truncate on list/scalar/map cells

        # (octo C2-L1; EXTRA XC1-L1). Spark refuses Long+Double/Decimal/Boolean.

        if isinstance(cell, bool):
            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type LongType and BooleanType "
                f"(got bool {cell!r} for integer field)"
            )

        if isinstance(cell, float):
            if _LEGACY_FIRST_ELEMENT_COERCE.get():
                # Legacy first-element mode: Spark truncates toward the inferred Long.

                return int(cell)

            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type LongType and DoubleType "
                f"(got float {cell!r} for integer field)"
            )

        from decimal import Decimal as _Decimal

        if isinstance(cell, _Decimal):
            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type LongType and DecimalType "
                f"(got Decimal {cell!r} for integer field)"
            )

        if isinstance(cell, int):
            return int(cell)

    if pa.types.is_floating(arrow_type):
        # Double + Boolean → 1.0 via pa.array was silent wrong (EXTRA XC1-L3).

        if isinstance(cell, bool):
            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type DoubleType and BooleanType "
                f"(got bool {cell!r} for floating field)"
            )

        from decimal import Decimal as _Decimal

        if isinstance(cell, _Decimal):
            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type DoubleType and DecimalType "
                f"(got Decimal {cell!r} for floating field)"
            )

        if isinstance(cell, (int, float)):
            return float(cell)

    if pa.types.is_decimal(arrow_type):
        # Inferred Decimal + Double/Boolean refuse; int is allowed under explicit Decimal schema.

        if isinstance(cell, bool):
            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type DecimalType and BooleanType "
                f"(got bool {cell!r} for decimal field)"
            )

        if isinstance(cell, float):
            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type DecimalType and DoubleType "
                f"(got float {cell!r} for decimal field)"
            )

    if pa.types.is_timestamp(arrow_type):
        # Never accept int/float as epoch under timestamp (extra XC2-L1 silent 1970-01-01).

        import datetime as _dt

        from decimal import Decimal as _Decimal

        if isinstance(cell, _dt.datetime):
            return cell

        # ``date`` is listed after the datetime return (datetime is a date subclass).

        if isinstance(cell, (bool, int, float, _Decimal, _dt.date)):
            kind = _python_scalar_merge_kind(cell) or type(cell).__name__

            label = _SPARK_SCALAR_MERGE_LABELS.get(kind, kind)

            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type TimestampType and {label} "
                f"(got {cell!r} for timestamp field)"
            )

    if pa.types.is_date(arrow_type):
        # Never accept int as day-epoch under date32 (extra XC2-L2 silent 1970-01-02).

        import datetime as _dt

        from decimal import Decimal as _Decimal

        if isinstance(cell, _dt.datetime):
            raise PySparkTypeError(
                "[CANNOT_MERGE_TYPE] Can not merge type DateType and TimestampType "
                f"(got {cell!r} for date field)"
            )

        if isinstance(cell, _dt.date):
            return cell

        if isinstance(cell, (bool, int, float, _Decimal)):
            kind = _python_scalar_merge_kind(cell) or type(cell).__name__

            label = _SPARK_SCALAR_MERGE_LABELS.get(kind, kind)

            raise PySparkTypeError(
                f"[CANNOT_MERGE_TYPE] Can not merge type DateType and {label} "
                f"(got {cell!r} for date field)"
            )

    if type(cell).__name__ == "Row" and type(cell).__module__.startswith("repark"):
        # Struct from Row: dict of field → prepared value (schema field order, name match).

        if pa.types.is_struct(arrow_type):
            prepared: dict[str, Any] = {}

            for field in arrow_type:
                name = field.name

                try:
                    value = cell[name]

                except Exception:
                    value = None

                prepared[name] = _prepare_nested_cell(value, field.type)

            return prepared

        return dict(zip(cell.__fields__, list(cell), strict=True))

    if isinstance(cell, dict) and pa.types.is_map(arrow_type):
        # Arrow map cells: list of (key, value) pairs; prepare keys for non-string map keys.

        key_type = arrow_type.key_type

        item_type = arrow_type.item_type

        return [
            (
                _prepare_nested_cell(key, key_type),
                _prepare_nested_cell(value, item_type),
            )
            for key, value in cell.items()
        ]

    if isinstance(cell, dict) and pa.types.is_struct(arrow_type):
        return {
            field.name: _prepare_nested_cell(cell.get(field.name), field.type)
            for field in arrow_type
        }

    if isinstance(cell, (list, tuple)) and pa.types.is_struct(arrow_type):
        # Positional tuple/list → struct by field order (Spark createDataFrame).

        fields = list(arrow_type)

        if len(cell) != len(fields):
            raise PySparkTypeError(
                f"createDataFrame struct expects {len(fields)} field(s), got {len(cell)}"
            )

        return {
            field.name: _prepare_nested_cell(cell[index], field.type)
            for index, field in enumerate(fields)
        }

    if isinstance(cell, list) and (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        value_type = arrow_type.value_type

        return [_prepare_nested_cell(item, value_type) for item in cell]

    return cell


def _normalize_nested_sql_type_aliases(sql_type: str) -> str:
    """Rewrite SQL aliases inside nested type markers so :meth:`DataType.fromDDL` accepts them.



    ``_data_type_to_sql_type`` historically emitted ``VARCHAR`` for strings; bare VARCHAR is

    not an atomic fromDDL token (only ``string`` / ``str`` / ``varchar(n)``). Nested markers

    must use STRING (or be rewritten here) or the whole column silently became string.

    """

    import re as _re

    return _re.sub(r"\bVARCHAR\b", "STRING", sql_type, flags=_re.IGNORECASE)


def _sql_type_to_arrow(sql_type: str) -> Any:
    """Map engine CAST type strings to pyarrow types (createDataFrame schema path)."""

    import pyarrow as pa

    from repark.types import DataType, repark_type_to_arrow

    stripped = sql_type.strip()

    upper = stripped.upper()

    # Nested ARRAY<> / MAP<> / STRUCT<> from _data_type_to_sql_type — parse via types.

    # Fail loud on parse errors (never silent pa.string() — that stringified nested cells

    # and looked like a successful createDataFrame with wrong schema; octo X2 C1 S0).

    if upper.startswith(("ARRAY<", "MAP<", "STRUCT<")):
        normalized = _normalize_nested_sql_type_aliases(stripped)

        try:
            return repark_type_to_arrow(DataType.fromDDL(normalized))

        except Exception as error:
            raise PySparkTypeError(
                f"createDataFrame cannot map nested schema type {sql_type!r} to Arrow: {error}"
            ) from error

    # Strip DECIMAL(p,s) precision for matching.

    base = upper.split("(", 1)[0].strip()

    mapping = {
        "BOOLEAN": pa.bool_(),
        "BOOL": pa.bool_(),
        "TINYINT": pa.int8(),
        "SMALLINT": pa.int16(),
        "INT": pa.int32(),
        "INTEGER": pa.int32(),
        "BIGINT": pa.int64(),
        "LONG": pa.int64(),
        "FLOAT": pa.float32(),
        "REAL": pa.float32(),
        "DOUBLE": pa.float64(),
        "FLOAT8": pa.float64(),
        "VARCHAR": pa.string(),
        "STRING": pa.string(),
        "TEXT": pa.string(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us"),
        "BINARY": pa.binary(),
        "BYTEA": pa.binary(),
    }

    if base in mapping:
        return mapping[base]

    if base == "DECIMAL" or base == "NUMERIC":
        # DECIMAL(p,s) — default 38,18 when unparsed; try extract.

        precision, scale = 38, 18

        if "(" in upper:
            inside = upper[upper.index("(") + 1 : upper.rindex(")")]

            parts = [part.strip() for part in inside.split(",")]

            if len(parts) == 2:
                precision, scale = int(parts[0]), int(parts[1])

        return pa.decimal128(precision, scale)

    # Fallback matches prior VALUES-path default for unknown (string).

    return pa.string()


def _arrow_null_sql_to_type(null_sql: str) -> Any:
    """Map ``CAST(NULL AS TYPE)`` (or bare TYPE) to a pyarrow type for object witnesses."""

    import pyarrow as pa

    upper = null_sql.upper().strip()

    if " AS " in upper:
        type_sql = upper.rsplit(" AS ", 1)[-1].strip()

        if type_sql.endswith(")") and type_sql.count("(") < type_sql.count(")"):
            type_sql = type_sql[:-1].strip()

    else:
        type_sql = upper

    if type_sql in {"VARCHAR", "STRING"}:
        return pa.string()

    return _sql_type_to_arrow(type_sql)


def _normalize_frame_arrow_column(column: Any, *, engine_type: str | None) -> Any:
    """Spark-parity Arrow column normalize after native pandas/polars export (P2a).



    * dictionary (category) → decoded values (ChunkedArray-safe)

    * refuse non-finite floats (inf) **before** typed cast — critic-octo C1: an early

      return on ``engine_type`` previously skipped the is_inf gate so StructType/DDL

      DoubleType/FloatType frames silently accepted ±inf while the untyped path refused

    * decimal envelope validate **before** rescale cast — native path must raise the same

      ``PySparkValueError`` as the list path (C2-L-002), not bare ArrowInvalid on rescale

    * integer widths → int64 when no declared engine type (VALUES parity — C4-Q-001)

    * float32 → float64 when no declared engine type

    * decimal* → decimal128(38, 18) when no declared engine type

    * large_string / string_view → utf8 string (tuple-path / interchange pins)

    """

    import pyarrow as pa

    import pyarrow.compute as pc

    # ChunkedArray has no dictionary_decode; cast to value type instead.

    if pa.types.is_dictionary(column.type):
        value_type = column.type.value_type

        column = column.cast(value_type)

    # Inf refuse must run for BOTH untyped and engine_type paths (octo C1).

    if pa.types.is_floating(column.type) and len(column) > 0:
        # is_inf is null-safe; any true → refuse (tuple path refuses bare inf).

        inf_mask = pc.is_inf(column)

        if pc.any(inf_mask).as_py():
            raise PySparkTypeError("createDataFrame does not support infinite float values")

    # Decimal envelope before any rescale cast (list path validates Python Decimal first).

    if pa.types.is_decimal(column.type):
        _validate_decimal_column_envelope(column)

    if engine_type is not None:
        target = _sql_type_to_arrow(engine_type)

        if not column.type.equals(target):
            column = column.cast(target)

        return column

    if pa.types.is_integer(column.type) and not pa.types.is_int64(column.type):
        column = column.cast(pa.int64())

    elif pa.types.is_float32(column.type):
        column = column.cast(pa.float64())

    elif pa.types.is_decimal(column.type):
        if column.type.precision != _DECIMAL_PRECISION or column.type.scale != _DECIMAL_SCALE:
            column = column.cast(pa.decimal128(_DECIMAL_PRECISION, _DECIMAL_SCALE))

    elif pa.types.is_large_string(column.type) or pa.types.is_string_view(column.type):
        column = column.cast(pa.string())

    elif pa.types.is_large_binary(column.type):
        column = column.cast(pa.binary())

    return column


def _validate_decimal_column_envelope(column: Any) -> None:
    """Refuse Decimal values outside DECIMAL(38,18) on a native Arrow column (C2-L-002)."""

    import pyarrow as pa

    if not pa.types.is_decimal(column.type):
        return

    for value in column.to_pylist():
        if value is not None:
            _validate_decimal_envelope(value)


def _arrow_table_from_pandas(
    data: Any,
    schema: list[str] | None,
    *,
    engine_types: list[str] | None,
) -> Any:
    """Native pandas → Arrow (no full-frame row loop). Schema bind + refuse + cast rules.



    # === r20 P2a: cdf-extractor ===

    Uses ``pa.Table.from_pandas`` for the bulk conversion. Refuse classes fire via the same

    dtype map as the legacy extractor (Period/Interval/timedelta/complex/nested). Object /

    Sparse[object] all-null columns still run the NaN→DOUBLE / NaT→TIMESTAMP witness

    (C5-SAF-001 / C6-Q-001). Integer widths widen to int64 (C4-Q-001).

    """

    import pyarrow as pa

    _refuse_duplicate_pandas_columns(data)

    source_columns = [str(column) for column in data.columns]

    names, permutation = _schema_names_and_permutation(source_columns, schema, kind="pandas")

    if len(data) == 0:
        # Typed StructType/DDL empty frames keep declared types (list-path parity — octo C4).

        # Name-only schema still cannot infer payload types → CANNOT_INFER_EMPTY_SCHEMA.

        if engine_types is not None:
            column_null_sql = [f"CAST(NULL AS {sql_type})" for sql_type in engine_types]

            return _arrow_table_from_tuples(
                names,
                [],
                column_null_sql=column_null_sql,
                engine_types=engine_types,
            )

        raise PySparkValueError(
            "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema for empty pandas DataFrame; "
            "pass a non-empty frame or a typed StructType schema "
            "(repark createDataFrame is VALUES-only and has no StructType path yet)"
        )

    # SparseDtype: ``pa.Table.from_pandas`` refuses sparse blocks, and densify-with-fill

    # corrupts nulls (int fill becomes a sentinel). Keep the proven cell extractor for any

    # frame that carries Sparse columns (rare; not the ingest hot path).

    for source_index in range(data.shape[1]):
        dtype = data.iloc[:, source_index].dtype

        dtype_text = str(dtype).lower()

        if "sparse" in dtype_text or type(dtype).__name__ == "SparseDtype":
            names_s, tuples_s, column_null_sql_s = _rows_from_pandas(data, schema)

            return _arrow_table_from_tuples(
                names_s,
                tuples_s,
                column_null_sql=column_null_sql_s,
                engine_types=engine_types,
            )

    # Per-source-column: refuse dtypes + collect object-column all-null type overrides.

    object_null_types: dict[int, Any] = {}

    for source_index, column_name in enumerate(data.columns):
        series = data.iloc[:, source_index]

        dtype = series.dtype

        if _pandas_dtype_needs_object_null_witness(dtype):
            raw_cells = [series.iloc[row_index] for row_index in range(len(series))]

            # Refuse bad cells (inf / Period cell / Decimal envelope) even when mixed.

            normalized = [
                _normalize_create_dataframe_cell(cell, field_name=str(column_name))
                for cell in raw_cells
            ]

            if all(cell is None for cell in normalized):
                null_sql = _infer_null_sql_from_raw_cells(raw_cells)

                object_null_types[source_index] = _arrow_null_sql_to_type(null_sql)

        else:
            # Typed columns: refuse at dtype map (side effect); discard null-SQL.

            _null_sql_for_pandas_dtype(dtype)

    table = pa.Table.from_pandas(data, preserve_index=False)

    # Drop pandas metadata so engine consumers see a plain schema.

    table = table.replace_schema_metadata(None)

    out_arrays: list[Any] = []

    for out_index, source_index in enumerate(permutation):
        engine_type = None if engine_types is None else engine_types[out_index]

        if source_index in object_null_types:
            # All-null object: force Spark-parity type (from_pandas yields null type).

            column = pa.nulls(table.num_rows, type=object_null_types[source_index])

        else:
            column = _normalize_frame_arrow_column(
                table.column(source_index), engine_type=engine_type
            )

            _validate_decimal_column_envelope(column)

        if engine_type is not None and source_index in object_null_types:
            target = _sql_type_to_arrow(engine_type)

            if not column.type.equals(target):
                try:
                    column = column.cast(target)

                except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as error:
                    # Witness type (NaT→timestamp, NaN→double) vs StructType mismatch —

                    # fail as PySparkTypeError, not a raw Arrow stack (octo C2).

                    raise PySparkTypeError(
                        f"createDataFrame cannot cast inferred null type {column.type} to "
                        f"schema type {engine_type}: {error}"
                    ) from error

        out_arrays.append(column)

    return pa.Table.from_arrays(out_arrays, names=names)


def _arrow_table_from_polars(
    data: Any,
    schema: list[str] | None,
    *,
    engine_types: list[str] | None,
) -> Any:
    """Native polars → Arrow (no per-row ``.row()`` loop). Schema bind + refuse + cast.



    # === r20 P2a: cdf-extractor ===

    # === r21 T1: cdf-ingest ===

    Uses polars ``.to_arrow()``. Duration / binary / time refuse via the dtype map; nested

    ``List``/``Struct``/``Array`` pass through Arrow (r21 T1). Integer widths widen to

    int64 (C4-Q-001).

    """

    import pyarrow as pa

    source_columns = list(data.columns)

    names, permutation = _schema_names_and_permutation(source_columns, schema, kind="polars")

    if data.height == 0:
        # Typed StructType/DDL empty frames keep declared types (list-path parity — octo C4).

        if engine_types is not None:
            column_null_sql = [f"CAST(NULL AS {sql_type})" for sql_type in engine_types]

            return _arrow_table_from_tuples(
                names,
                [],
                column_null_sql=column_null_sql,
                engine_types=engine_types,
            )

        raise PySparkValueError(
            "[CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema for empty polars DataFrame; "
            "pass a non-empty frame or a typed StructType schema "
            "(repark createDataFrame is VALUES-only and has no StructType path yet)"
        )

    for dtype in data.dtypes:
        _null_sql_for_polars_dtype(dtype)

    table = data.to_arrow()

    out_arrays: list[Any] = []

    for out_index, source_index in enumerate(permutation):
        engine_type = None if engine_types is None else engine_types[out_index]

        column = _normalize_frame_arrow_column(table.column(source_index), engine_type=engine_type)

        _validate_decimal_column_envelope(column)

        out_arrays.append(column)

    return pa.Table.from_arrays(out_arrays, names=names)


_SPARK_SCALAR_MERGE_LABELS: dict[str, str] = {
    "boolean": "BooleanType",
    "long": "LongType",
    "double": "DoubleType",
    "decimal": "DecimalType",
    "timestamp": "TimestampType",
    "date": "DateType",
}


# Preference order for CANNOT_MERGE_TYPE messages (stable, Spark-ish).

_SPARK_SCALAR_MERGE_KIND_ORDER: tuple[str, ...] = (
    "boolean",
    "long",
    "double",
    "decimal",
    "date",
    "timestamp",
)


def _python_scalar_merge_kind(cell: Any) -> str | None:
    """Spark-merge kind for a scalar cell, or ``None`` if not in the merge-checked set.



    ``bool`` is checked before ``int`` (``isinstance(True, int)`` is true in Python).

    ``datetime`` is checked before ``date`` (datetime is a date subclass).

    Infinite floats refuse immediately (createDataFrame does not support them).

    """

    import datetime as _dt

    from decimal import Decimal as _Decimal

    if cell is None:
        return None

    if isinstance(cell, bool):
        return "boolean"

    if isinstance(cell, int):
        return "long"

    if isinstance(cell, float):
        if cell == float("inf") or cell == float("-inf"):
            raise PySparkTypeError("createDataFrame does not support infinite float values")

        return "double"

    if isinstance(cell, _Decimal):
        return "decimal"

    if isinstance(cell, _dt.datetime):
        return "timestamp"

    if isinstance(cell, _dt.date):
        return "date"

    return None


def _refuse_incompatible_scalar_merge_kinds(kinds: set[str], *, column_name: str) -> None:
    """Spark ``CANNOT_MERGE_TYPE`` when two merge-checked scalar kinds co-occur."""

    present = [kind for kind in _SPARK_SCALAR_MERGE_KIND_ORDER if kind in kinds]

    if len(present) < 2:
        return

    left = _SPARK_SCALAR_MERGE_LABELS[present[0]]

    right = _SPARK_SCALAR_MERGE_LABELS[present[1]]

    raise PySparkTypeError(
        f"[CANNOT_MERGE_TYPE] Can not merge type {left} and {right} (column {column_name!r})"
    )


def _refuse_long_double_merge(
    tuples: list[tuple[Any, ...]],
    column_index: int,
    column_name: str,
) -> None:
    """Refuse Spark ``CANNOT_MERGE_TYPE`` on inferred scalar columns (r21 T1 / extra octo).



    Live Spark 4.1.2 rejects mixed Boolean/Long/Double/Decimal/Date/Timestamp on the same

    inferred field rather than truncating or silently promoting. Covers:



    * Long + Double (``int(2.5)`` silent truncate — critic-octo C1-L1)

    * Long + Decimal (``Decimal("2.5")`` → 2 via ``pa.array`` — EXTRA XC1-L1)

    * Decimal + Long / Decimal + Double / Double + Decimal (EXTRA XC1-L2)

    * Double + Boolean (``True`` → 1.0 via Arrow — EXTRA XC1-L3)

    * Long + Boolean / Boolean + Long (EXTRA XC1-L4)

    * Timestamp/Date + Long/Double (epoch coercion via ``pa.array`` — EXTRA XC2-L1/L2)

    * Date + Timestamp (EXTRA XC2-L3)



    Nested list/map element conflicts are enforced in :func:`_prepare_nested_cell` and, for

    list-of-scalar columns, :func:`_refuse_list_element_type_merge`.

    """

    kinds: set[str] = set()

    for row in tuples:
        kind = _python_scalar_merge_kind(row[column_index])

        if kind is None:
            continue

        kinds.add(kind)

        _refuse_incompatible_scalar_merge_kinds(kinds, column_name=column_name)


def _refuse_list_element_type_merge(
    tuples: list[tuple[Any, ...]],
    column_index: int,
    column_name: str,
) -> None:
    """Refuse Boolean/Long/Double/Decimal mix among list-of-scalar elements (Spark merge)."""

    kinds: set[str] = set()

    for row in tuples:
        cell = row[column_index]

        if not isinstance(cell, list):
            continue

        for item in cell:
            kind = _python_scalar_merge_kind(item)

            if kind is None:
                continue

            kinds.add(kind)

            _refuse_incompatible_scalar_merge_kinds(kinds, column_name=column_name)


def _arrow_table_from_tuples(
    names: list[str],
    tuples: list[tuple[Any, ...]],
    *,
    column_null_sql: list[str] | None,
    engine_types: list[str] | None,
) -> Any:
    """Build a ``pyarrow.Table`` from row tuples with declared/inferred Arrow types."""

    import pyarrow as pa

    width = len(names)

    if engine_types is not None:
        arrow_types = [_sql_type_to_arrow(sql_type) for sql_type in engine_types]

    else:
        # Infer from first non-null cell per column (Python types → Arrow). All-null

        # columns fall back to column_null_sql CAST type when provided, else string

        # (matches VALUES-path default VARCHAR for untyped nulls).

        arrow_types = []

        for column_index in range(width):
            sample = next(
                (row[column_index] for row in tuples if row[column_index] is not None),
                None,
            )

            if sample is None:
                if column_null_sql is not None and column_index < len(column_null_sql):
                    upper = column_null_sql[column_index].upper()

                    # CAST(NULL AS TYPE) → TYPE (keep DECIMAL(p,s) parens intact).

                    if " AS " in upper:
                        type_sql = upper.rsplit(" AS ", 1)[-1].strip()

                        if type_sql.endswith(")") and type_sql.count("(") < type_sql.count(")"):
                            type_sql = type_sql[:-1].strip()

                    else:
                        type_sql = upper

                    arrow_types.append(_sql_type_to_arrow(type_sql))

                else:
                    arrow_types.append(pa.string())

            elif isinstance(sample, bool):
                # Boolean + Long/Double/Decimal → CANNOT_MERGE_TYPE (extra octo XC1-L3/L4).

                _refuse_long_double_merge(tuples, column_index, names[column_index])

                arrow_types.append(pa.bool_())

            elif isinstance(sample, int) and not isinstance(sample, bool):
                # Spark 4.1.2: LongType + Double/Decimal/Boolean cannot merge (CANNOT_MERGE_TYPE).

                # First-int-then-float used to infer int64 then int(2.5) → silent 2 (octo C1-L1);

                # first-int-then-Decimal truncated via pa.array (extra XC1-L1).

                _refuse_long_double_merge(tuples, column_index, names[column_index])

                arrow_types.append(pa.int64())

            elif isinstance(sample, float):
                if sample == float("inf") or sample == float("-inf"):
                    raise PySparkTypeError("createDataFrame does not support infinite float values")

                # Symmetric: first-float-then-int/Decimal/bool also refuses on Spark.

                _refuse_long_double_merge(tuples, column_index, names[column_index])

                arrow_types.append(pa.float64())

            elif isinstance(sample, list):
                # Dense ML vector: non-empty float-only list → FixedSizeList[n] (v1).

                # General arrays (int lists, empty-first, nested) → variable list_ (X2 census).

                # List-of-scalar Long+Double/Decimal/Boolean refuse before element infer (XC1).

                _refuse_list_element_type_merge(tuples, column_index, names[column_index])

                if sample and all(isinstance(item, float) for item in sample):
                    arrow_types.append(pa.list_(pa.float64(), len(sample)))

                else:
                    # === r23b N1: multi-row list element merge under conf true ===

                    # Pure list-of-dict → struct field union; nested list elements

                    # (list<list<dict>>) merge via _merge_inferred_arrow_types (C2-L-001).

                    if _INFER_NESTED_DICT_AS_STRUCT.get():
                        dict_elements: list[dict[str, Any]] = []

                        saw_non_dict = False

                        for row in tuples:
                            cell = row[column_index]

                            if not isinstance(cell, list):
                                continue

                            for item in cell:
                                if item is None:
                                    continue

                                if isinstance(item, dict):
                                    dict_elements.append(item)

                                    if _LEGACY_FIRST_ELEMENT_COERCE.get():
                                        break

                                else:
                                    saw_non_dict = True

                            if _LEGACY_FIRST_ELEMENT_COERCE.get() and dict_elements:
                                break

                        if dict_elements and not saw_non_dict:
                            arrow_types.append(
                                pa.list_(_infer_struct_arrow_from_dict_samples(dict_elements))
                            )

                            continue

                        if saw_non_dict:
                            element_types: list[Any] = []

                            for row in tuples:
                                cell = row[column_index]

                                if not isinstance(cell, list):
                                    continue

                                for item in cell:
                                    if item is None:
                                        continue

                                    element_types.append(_infer_arrow_type_from_python_sample(item))

                                    if _LEGACY_FIRST_ELEMENT_COERCE.get():
                                        break

                                if _LEGACY_FIRST_ELEMENT_COERCE.get() and element_types:
                                    break

                            if element_types:
                                merged_element = element_types[0]

                                for element_type in element_types[1:]:
                                    merged_element = _merge_inferred_arrow_types(
                                        merged_element, element_type
                                    )

                                arrow_types.append(pa.list_(merged_element))

                                continue

                    element_sample = next(
                        (item for item in sample if item is not None),
                        None,
                    )

                    if element_sample is None:
                        # All-empty / all-null elements — scan other rows for a witness.

                        for row in tuples:
                            cell = row[column_index]

                            if isinstance(cell, list):
                                element_sample = next(
                                    (item for item in cell if item is not None),
                                    None,
                                )

                                if element_sample is not None:
                                    break

                    arrow_types.append(
                        pa.list_(_infer_arrow_type_from_python_sample(element_sample))
                    )

            elif type(sample).__name__ == "Row" and type(sample).__module__.startswith("repark"):
                # Nested Row → struct (Spark createDataFrame inference).

                arrow_types.append(_infer_arrow_type_from_python_sample(sample))

            elif isinstance(sample, tuple):
                # Nested bare tuple → struct<_1,_2,…> (Spark createDataFrame; F2 /

                # Apache test_print_schema). Must not fall through to str(tuple).

                arrow_types.append(_infer_arrow_type_from_python_sample(sample))

            elif (
                isinstance(sample, dict)
                and set(sample.keys()) == {"size", "indices", "values"}
                and isinstance(sample.get("size"), int)
                and not isinstance(sample.get("size"), bool)
                and isinstance(sample.get("indices"), (list, tuple))
                and isinstance(sample.get("values"), (list, tuple))
            ):
                # Sparse ML vector struct (design decision 1). Exact key set + value shapes

                # only — a plain map that happens to contain a "size" key must stay map

                # (octo X2 C5; prior `keys() >= {…}` over-matched).

                arrow_types.append(
                    pa.struct(
                        [
                            ("size", pa.int32()),
                            ("indices", pa.list_(pa.int32())),
                            ("values", pa.list_(pa.float64())),
                        ]
                    )
                )

            elif isinstance(sample, dict):
                # === r23b N1: conf true → multi-row struct field union (null-fill missing) ===

                # Sparse-vector exact-key branch above is conf-invariant (Q8).

                if _INFER_NESTED_DICT_AS_STRUCT.get():
                    dict_samples: list[dict[str, Any]] = [
                        cell for row in tuples if isinstance((cell := row[column_index]), dict)
                    ]

                    arrow_types.append(
                        _infer_struct_arrow_from_dict_samples(dict_samples or [sample])
                    )

                    continue

                # Plain dict → map<key, value>. Empty / null-only first sample: scan later

                # rows for a witness with a concrete non-null value so

                # ``[{}, {"a": None}, {"a": 1}]`` becomes map<string,bigint> not

                # map<string,string> with ``"1"`` (Apache test_infer_map_pair_type_empty

                # order — F2 / octo C1-Q-001).

                map_sample = sample

                needs_value_witness = (not map_sample) or all(
                    value is None for value in map_sample.values()
                )

                if needs_value_witness:
                    for row in tuples:
                        cell = row[column_index]

                        if (
                            isinstance(cell, dict)
                            and cell
                            and any(value is not None for value in cell.values())
                        ):
                            map_sample = cell

                            break

                    # No concrete value in any row: fall back to first non-empty (null-only

                    # values) so empty→null still builds map<string,string>.

                    if not map_sample:
                        for row in tuples:
                            cell = row[column_index]

                            if isinstance(cell, dict) and cell:
                                map_sample = cell

                                break

                arrow_types.append(_infer_arrow_type_from_python_sample(map_sample))

            else:
                import datetime as _dt

                from decimal import Decimal as _Decimal

                if isinstance(sample, _dt.datetime):
                    # Timestamp + Long/Double/Date refuse — no epoch coercion (extra XC2-L1).

                    _refuse_long_double_merge(tuples, column_index, names[column_index])

                    arrow_types.append(pa.timestamp("us", tz="UTC" if sample.tzinfo else None))

                elif isinstance(sample, _dt.date):
                    # Date + Long/Timestamp refuse — no day-epoch coercion (extra XC2-L2).

                    _refuse_long_double_merge(tuples, column_index, names[column_index])

                    arrow_types.append(pa.date32())

                elif isinstance(sample, _dt.time):
                    # time-of-day → string until engine TIME type is wired end-to-end.

                    arrow_types.append(pa.string())

                elif isinstance(sample, _Decimal):
                    # Decimal + Long/Double/Boolean refuse (extra XC1-L2); not silent promote.

                    _refuse_long_double_merge(tuples, column_index, names[column_index])

                    arrow_types.append(pa.decimal128(38, 18))

                elif isinstance(sample, (bytes, bytearray, memoryview)):
                    arrow_types.append(pa.binary())

                else:
                    arrow_types.append(pa.string())

    # Exact-duplicate names were rejected by the VALUES planner; keep fail-loud.

    if len(names) != len(set(names)):
        from repark.errors import AnalysisException

        raise AnalysisException(
            "unique expression names required; createDataFrame schema has duplicate column names"
        )

    # Decimal envelope (was enforced only on VALUES SQL literals) — validate before Arrow build.

    from decimal import Decimal as _Decimal

    for row in tuples:
        for cell in row:
            if isinstance(cell, _Decimal):
                _validate_decimal_envelope(cell)

    columns: list[Any] = []

    for column_index in range(width):
        values = [row[column_index] for row in tuples]

        arrow_type = arrow_types[column_index]

        # v1 dense vector: refuse mixed FixedSizeList widths (greylight Q3).

        if pa.types.is_fixed_size_list(arrow_type):
            expected_width = arrow_type.list_size

            for row_index, cell in enumerate(values):
                if cell is None:
                    continue

                if not isinstance(cell, list):
                    raise PySparkTypeError(
                        f"createDataFrame column {names[column_index]!r}: expected dense "
                        f"float list of width {expected_width}, got {type(cell).__name__}"
                    )

                if len(cell) != expected_width:
                    from repark.errors import AnalysisException

                    raise AnalysisException(
                        f"repark.ml v1 vector columns are fixed-width only; column "
                        f"{names[column_index]!r} has mixed widths "
                        f"(expected {expected_width}, row {row_index} has {len(cell)}). "
                        f"Do not fall back to List<Float64> — that silently loses the "
                        f"width guarantee (dense FixedSizeList only; see repark.ml.linalg)."
                    )

                values[row_index] = [float(item) for item in cell]

        # Sparse ML vector reshape (design decision 1): only exact three-field sparse

        # layout ``{size,indices,values}`` — never any struct that merely contains an

        # ``indices`` field (r23b N1 conf-true dict→struct + explicit StructType schemas

        # used to silent-drop extra fields / KeyError on missing size — octo C1-L-001).

        if pa.types.is_struct(arrow_type) and {field.name for field in arrow_type} == {
            "size",
            "indices",
            "values",
        }:
            for row_index, cell in enumerate(values):
                if cell is None:
                    continue

                if not isinstance(cell, dict):
                    raise PySparkTypeError(
                        f"createDataFrame column {names[column_index]!r}: expected sparse "
                        f"vector struct dict, got {type(cell).__name__}"
                    )

                values[row_index] = {
                    "size": int(cell["size"]),
                    "indices": [int(item) for item in cell["indices"]],
                    "values": [float(item) for item in cell["values"]],
                }

        # Coerce / reshape cells to the target Arrow type (nested + stringified schema).

        values = [_prepare_nested_cell(cell, arrow_type) for cell in values]

        try:
            columns.append(pa.array(values, type=arrow_type))

        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as error:
            # Match VALUES-path loud refusals (decimal scale, unsupported casts, …).

            raise PySparkTypeError(
                f"createDataFrame cannot build Arrow column {names[column_index]!r}: {error}"
            ) from error

    return pa.Table.from_arrays(columns, names=names)


def _values_sql_with_explicit_casts(
    names: list[str],
    tuples: list[tuple[Any, ...]],
    *,
    engine_types: list[str],
) -> str:
    """VALUES list with per-cell ``CAST(… AS type)`` so explicit schema types stick (int32)."""

    value_rows: list[str] = []

    for row in tuples:
        cells: list[str] = []

        for cell, sql_type in zip(row, engine_types, strict=True):
            if cell is None:
                cells.append(f"CAST(NULL AS {sql_type})")

            else:
                cells.append(f"CAST({_sql_literal(cell)} AS {sql_type})")

        value_rows.append("(" + ", ".join(cells) + ")")

    values_sql = ", ".join(value_rows)

    alias_cols = ", ".join(_quote_ident(name) for name in names)

    return f"SELECT * FROM (VALUES {values_sql}) AS t({alias_cols})"


# =============================================================================

# U8 — SQL-embedded registered Python UDF helpers (session.sql rewrite support)

# =============================================================================


def _sql_top_level_keyword_index(query: str, keyword: str) -> int | None:
    """Index of top-level ``keyword`` (paren depth 0), or ``None``."""

    upper = query.upper()

    target = keyword.upper()

    depth = 0

    index = 0

    length = len(query)

    in_single = False

    in_double = False

    while index < length:
        char = query[index]

        if in_single:
            if char == "'":
                if index + 1 < length and query[index + 1] == "'":
                    index += 2

                    continue

                in_single = False

            index += 1

            continue

        if in_double:
            if char == '"':
                if index + 1 < length and query[index + 1] == '"':
                    index += 2

                    continue

                in_double = False

            index += 1

            continue

        if char == "'":
            in_single = True

            index += 1

            continue

        if char == '"':
            in_double = True

            index += 1

            continue

        if char == "(":
            depth += 1

            index += 1

            continue

        if char == ")":
            depth = max(0, depth - 1)

            index += 1

            continue

        if depth == 0 and upper.startswith(target, index):
            # Word boundary: prev not alnum/_, next not alnum/_

            prev_ok = index == 0 or not (query[index - 1].isalnum() or query[index - 1] == "_")

            end = index + len(target)

            next_ok = end >= length or not (query[end].isalnum() or query[end] == "_")

            if prev_ok and next_ok:
                return index

        index += 1

    return None


def _sql_udf_in_nested_subquery(query: str, udf_index: int) -> bool:
    """True when ``udf_index`` sits inside a parenthesized ``(SELECT|WITH …)`` subquery.



    U9: expression parens (``CAST(…)``, ``abs(…)``, ``f(g(x))``) are **not** subqueries —

    only regions whose open-paren is followed by SELECT/WITH (after whitespace).

    """

    # === r20 U9: sql-udf-rewrite ===

    stack: list[bool] = []

    index = 0

    length = len(query)

    in_single = False

    in_double = False

    while index < udf_index and index < length:
        char = query[index]

        if in_single:
            if char == "'":
                if index + 1 < length and query[index + 1] == "'":
                    index += 2

                    continue

                in_single = False

            index += 1

            continue

        if in_double:
            if char == '"':
                if index + 1 < length and query[index + 1] == '"':
                    index += 2

                    continue

                in_double = False

            index += 1

            continue

        if char == "'":
            in_single = True

            index += 1

            continue

        if char == '"':
            in_double = True

            index += 1

            continue

        if char == "(":
            peek = index + 1

            while peek < length and query[peek].isspace():
                peek += 1

            is_sub = False

            if query[peek : peek + 6].upper() == "SELECT":
                end = peek + 6

                is_sub = end >= length or not (query[end].isalnum() or query[end] == "_")

            elif query[peek : peek + 4].upper() == "WITH":
                end = peek + 4

                is_sub = end >= length or not (query[end].isalnum() or query[end] == "_")

            stack.append(is_sub)

            index += 1

            continue

        if char == ")":
            if stack:
                stack.pop()

            index += 1

            continue

        index += 1

    return any(stack)


def _split_sql_select_list(select_list: str) -> list[str]:
    """Split a SELECT list on top-level commas (respecting parens/quotes)."""

    items: list[str] = []

    depth = 0

    start = 0

    index = 0

    length = len(select_list)

    in_single = False

    in_double = False

    while index < length:
        char = select_list[index]

        if in_single:
            if char == "'":
                if index + 1 < length and select_list[index + 1] == "'":
                    index += 2

                    continue

                in_single = False

            index += 1

            continue

        if in_double:
            if char == '"':
                if index + 1 < length and select_list[index + 1] == '"':
                    index += 2

                    continue

                in_double = False

            index += 1

            continue

        if char == "'":
            in_single = True

            index += 1

            continue

        if char == '"':
            in_double = True

            index += 1

            continue

        if char == "(":
            depth += 1

        elif char == ")":
            depth = max(0, depth - 1)

        elif char == "," and depth == 0:
            items.append(select_list[start:index].strip())

            start = index + 1

        index += 1

    tail = select_list[start:].strip()

    if tail:
        items.append(tail)

    return items


def _sql_strip_comments_preserve_strings(query: str) -> str:
    """Remove ``--`` / ``/* */`` comments; keep string/ident quotes intact (octo C4-L-001).



    Unlike :func:`_sql_mask_strings_and_comments`, strings are preserved so UDF arg

    literals remain parseable. Used only for SELECT-list item structure matching.

    """

    if not query:
        return query

    out: list[str] = []

    length = len(query)

    index = 0

    while index < length:
        char = query[index]

        if char == "-" and index + 1 < length and query[index + 1] == "-":
            end = query.find("\n", index)

            if end < 0:
                break

            out.append("\n")

            index = end + 1

            continue

        if char == "/" and index + 1 < length and query[index + 1] == "*":
            end = query.find("*/", index + 2)

            if end < 0:
                break

            out.append(" ")

            index = end + 2

            continue

        if char in {"'", '"', "`"}:
            quote = char

            out.append(char)

            index += 1

            while index < length:
                current = query[index]

                out.append(current)

                index += 1

                if current == quote:
                    if index < length and query[index] == quote:
                        out.append(query[index])

                        index += 1

                        continue

                    break

            continue

        out.append(char)

        index += 1

    return "".join(out)


def _parse_simple_sql_udf_call(
    item: str,
    registry: dict[str, dict[str, Any]],
) -> tuple[str, list[str], str | None] | None:
    """Parse ``name(arg[, …]) [AS alias]`` with simple args, or return ``None``.



    Args must be bare identifiers, double-quoted idents, or simple numeric/string/NULL

    literals. Nested calls / expressions refuse by returning ``None``.

    """

    # Strip comments so ``udf /*c*/ (a)`` still parses (hit scan already masks them).

    text = _sql_strip_comments_preserve_strings(item).strip()

    # Optional trailing AS alias

    alias: str | None = None

    as_alias_pattern = r"(?i)\s+AS\s+((?:\"[^\"]+\")|(?:`[^`]+`)|(?:[A-Za-z_][A-Za-z0-9_]*))\s*$"

    as_match = re.search(as_alias_pattern, text)

    if as_match:
        alias_raw = as_match.group(1)

        if alias_raw.startswith('"') and alias_raw.endswith('"'):
            alias = alias_raw[1:-1].replace('""', '"')

        elif alias_raw.startswith("`") and alias_raw.endswith("`"):
            alias = alias_raw[1:-1].replace("``", "`")

        else:
            alias = alias_raw

        text = text[: as_match.start()].strip()

    call_match = re.match(
        r"^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)\s*$",
        text,
        re.DOTALL,
    )

    if not call_match:
        return None

    func_name = call_match.group(1)

    # Resolve against registry (case-insensitive key match → canonical registered name).

    registered_name: str | None = None

    for key in registry:
        if key.lower() == func_name.lower():
            registered_name = key

            break

    if registered_name is None:
        return None

    args_blob = call_match.group(2).strip()

    if not args_blob:
        return None  # zero-arg SQL UDF unsupported (same as DF path)

    arg_parts = _split_sql_select_list(args_blob)

    simple_args: list[str] = []

    for part in arg_parts:
        arg = part.strip()

        if not arg:
            return None

        # Bare ident / qualified col (t.a / cat.db.col) / quoted ident / numeric /

        # string / NULL / boolean — simple form only (octo C2-L-001 qualified cols).

        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", arg):
            simple_args.append(arg)

            continue

        if re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+",
            arg,
        ):
            simple_args.append(arg)

            continue

        if re.fullmatch(r'"([^"]|"")*"', arg) or re.fullmatch(r"`([^`]|``)*`", arg):
            simple_args.append(arg)

            continue

        # Quoted multi-part: "t"."a" or `t`.`a`

        if re.fullmatch(
            r'("([^"]|"")*"|`([^`]|``)*`|[A-Za-z_][A-Za-z0-9_]*)'
            r'(\.("([^"]|"")*"|`([^`]|``)*`|[A-Za-z_][A-Za-z0-9_]*))+',
            arg,
        ):
            simple_args.append(arg)

            continue

        if re.fullmatch(r"-?\d+(\.\d+)?([eE][+-]?\d+)?", arg):
            simple_args.append(arg)

            continue

        if re.fullmatch(r"'(?:[^']|'')*'", arg):
            simple_args.append(arg)

            continue

        if arg.upper() in {"NULL", "TRUE", "FALSE"}:
            simple_args.append(arg)

            continue

        return None  # expression arg — not simple form

    return registered_name, simple_args, alias


def _try_rewrite_select_list_python_udfs(
    body: str,
    *,
    registry: dict[str, dict[str, Any]],
    hits: list[tuple[str, str, int]],
) -> tuple[str, dict[str, Any]] | None:
    """Rewrite SELECT-list (+ WHERE/GROUP BY/HAVING) UDF calls (U9/U10).



    Returns ``(base_sql, materialize_plan)`` or ``None`` when the shape is out of bounds.

    ``materialize_plan`` keys: ``stages``, ``final_exprs``, ``distinct``, ``order_by``,

    ``limit``, ``where_sql``, ``group_by_keys``, ``having_sql``, ``user_out_names``.

    """

    # === r20 U9: sql-udf-rewrite ===

    # === r21 T7: census-r6 ===

    _ = hits

    stripped = body.strip()

    if re.match(r"(?is)^WITH\b", stripped):
        return None

    select_match = re.match(r"(?is)^SELECT\s+(DISTINCT\s+)?", stripped)

    if not select_match:
        return None

    is_distinct = bool(select_match.group(1))

    select_list_start = select_match.end()

    from_index = _sql_top_level_keyword_index(stripped, "FROM")

    if from_index is None:
        # No FROM — Spark allows ``SELECT expr`` (U9-C7-001). Select list runs to

        # trailing ORDER BY / LIMIT / GROUP / HAVING (peeled below).

        select_list = stripped[select_list_start:].strip()

        rest = ""

        # If trailing clauses appear without FROM, peel them from the select-list blob.

        for keyword in ("WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"):
            kw_index = _sql_top_level_keyword_index(select_list, keyword)

            if kw_index is not None:
                rest = select_list[kw_index:]

                select_list = select_list[:kw_index].strip()

                break

    else:
        select_list = stripped[select_list_start:from_index].strip()

        rest = stripped[from_index:]  # FROM …

    items = _split_sql_select_list(select_list)

    if not items:
        return None

    # Peel WHERE / GROUP BY / HAVING / ORDER BY / LIMIT from rest so base SQL never

    # references user aliases that only exist after UDF materialization (U9 Q13;

    # U10 peels WHERE when it holds UDF residuals).

    if rest:
        core_rest, peeled = _sql_peel_select_trailing_clauses(rest)

    else:
        core_rest, peeled = (
            "",
            {
                "where": None,
                "group_by": None,
                "having": None,
                "order_by": None,
                "limit": None,
            },
        )

    # U10: aggregates + GROUP BY are out of bounds for the keys-only path.

    if peeled.get("group_by") or peeled.get("having"):
        agg_pattern = re.compile(
            r"(?is)\b(count|sum|avg|mean|min|max|first|last|collect_list|"
            r"collect_set|percentile|stddev|variance|var_pop|var_samp|"
            r"skewness|kurtosis|approx_count_distinct)\s*\("
        )

        if agg_pattern.search(select_list):
            from repark.errors import UnsupportedOperationException

            raise UnsupportedOperationException(
                "registered Python UDF with GROUP BY / HAVING and aggregate SELECT "
                "expressions is not supported in repark v1 (keys-only GROUP BY after "
                "UDF materialization). Materialize via DataFrame.select / withColumn, "
                "then groupBy / agg."
            )

    base_select_parts: list[str] = []

    temp_counter = 0

    any_udf = False

    # UDF calls ordered innermost-first with dependency depths for staging.

    udf_nodes: list[dict[str, Any]] = []  # each: registered_name, input_names, out_name, depth

    final_exprs: list[str] = []

    user_out_names: list[str] = []

    # Map from original call span (start,end) within an item → hidden out name (per item).

    for item in items:
        item_clean = _sql_strip_comments_preserve_strings(item).strip()

        if not item_clean:
            return None

        # Optional trailing AS alias for the whole item (and Spark optional-AS form

        # ``expr alias`` without the AS keyword — U9-C5-001).

        alias: str | None = None

        expr_text = item_clean

        as_match = re.search(
            r"(?i)\s+AS\s+((?:\"[^\"]+\")|(?:`[^`]+`)|(?:[A-Za-z_][A-Za-z0-9_]*))\s*$",
            item_clean,
        )

        if as_match:
            alias_raw = as_match.group(1)

            if alias_raw.startswith('"') and alias_raw.endswith('"'):
                alias = alias_raw[1:-1].replace('""', '"')

            elif alias_raw.startswith("`") and alias_raw.endswith("`"):
                alias = alias_raw[1:-1].replace("``", "`")

            else:
                alias = alias_raw

            expr_text = item_clean[: as_match.start()].strip()

        else:
            # Spark allows ``SELECT expr alias`` without AS. Only peel when the

            # trailing token is a bare/quoted ident and the left side does not end

            # with an operator (so ``udf(x) + 1`` is not misread).

            opt_alias = re.search(
                r"\s+((?:\"[^\"]+\")|(?:`[^`]+`)|(?:[A-Za-z_][A-Za-z0-9_]*))\s*$",
                item_clean,
            )

            if opt_alias:
                maybe_expr = item_clean[: opt_alias.start()].strip()

                alias_raw = opt_alias.group(1)

                if maybe_expr and not re.search(
                    r"[,+\-*/%&|<>=!.]$",
                    maybe_expr.rstrip(),
                ):
                    # Reject reserved trailing tokens that are not aliases.

                    reserved = {
                        "from",
                        "where",
                        "group",
                        "having",
                        "order",
                        "limit",
                        "union",
                        "intersect",
                        "except",
                        "and",
                        "or",
                        "as",
                    }

                    bare_for_check = alias_raw

                    if (bare_for_check.startswith('"') and bare_for_check.endswith('"')) or (
                        bare_for_check.startswith("`") and bare_for_check.endswith("`")
                    ):
                        bare_for_check = bare_for_check[1:-1]

                    if bare_for_check.lower() not in reserved:
                        if alias_raw.startswith('"') and alias_raw.endswith('"'):
                            alias = alias_raw[1:-1].replace('""', '"')

                        elif alias_raw.startswith("`") and alias_raw.endswith("`"):
                            alias = alias_raw[1:-1].replace("``", "`")

                        else:
                            alias = alias_raw

                        expr_text = maybe_expr

        # Star expansion cannot be aliased into a hidden base column (U9-C1-003).

        if expr_text == "*" or re.fullmatch(
            r'(?:"[^"]+"|`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*\*',
            expr_text,
        ):
            from repark.errors import UnsupportedOperationException

            raise UnsupportedOperationException(
                "registered Python UDF SELECT with star (*) expansion is not supported "
                "in repark v1 (SELECT-list rewrite cannot project '*' into a hidden "
                "base column). List columns explicitly or apply the UDF via DataFrame."
            )

        calls = _sql_find_registry_udf_calls(expr_text, registry)

        if not calls:
            # Pure pass-through (no UDF).

            if alias is not None:
                out_name = alias

                pass_expr = expr_text

            else:
                bare = expr_text

                if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", bare):
                    out_name = bare

                    pass_expr = bare

                elif re.fullmatch(r'"([^"]|"")*"', bare):
                    out_name = bare[1:-1].replace('""', '"')

                    pass_expr = bare

                elif re.fullmatch(r"`([^`]|``)*`", bare):
                    out_name = bare[1:-1].replace("``", "`")

                    pass_expr = bare

                else:
                    out_name = bare

                    pass_expr = bare

            # Defense: never let pass-through out names surface internal temps.

            if "__repark_sql_udf" in out_name:
                out_name = f"col{temp_counter}"

            temp_name = f"__repark_sql_udf_pass_{temp_counter}"

            temp_counter += 1

            base_select_parts.append(f"{pass_expr} AS {_quote_ident(temp_name)}")

            final_exprs.append(f"{_quote_ident(temp_name)} AS {_quote_ident(out_name)}")

            user_out_names.append(out_name)

            continue

        any_udf = True

        # Process calls innermost-first (larger start index first among nested).

        calls_sorted = sorted(calls, key=lambda call: (-call["start"], -call["end"]))

        # span → out temp name for this item's UDF calls

        span_to_out: dict[tuple[int, int], str] = {}

        call_records: list[dict[str, Any]] = []

        for call in calls_sorted:
            registered_name = call["registered_name"]

            arg_texts: list[str] = call["args"]

            if not arg_texts:
                return None  # zero-arg SQL UDF unsupported (same as DF path)

            input_names: list[str] = []

            max_dep_depth = -1

            for arg in arg_texts:
                arg_stripped = arg.strip()

                if not arg_stripped:
                    return None

                # If the entire arg is a nested UDF call we already assigned, use that out.

                nested_out: str | None = None

                for span, out_temp in span_to_out.items():
                    nested_call_text = expr_text[span[0] : span[1]]

                    if arg_stripped == nested_call_text.strip():
                        nested_out = out_temp

                        # depth of that node

                        for record in call_records:
                            if record["out_name"] == out_temp:
                                max_dep_depth = max(max_dep_depth, record["depth"])

                                break

                        break

                if nested_out is not None:
                    input_names.append(nested_out)

                    continue

                # Simple arg → project from base.

                if not _sql_udf_arg_is_simple(arg_stripped):
                    return None

                temp_name = f"__repark_sql_udf_in_{temp_counter}"

                temp_counter += 1

                base_select_parts.append(f"{arg_stripped} AS {_quote_ident(temp_name)}")

                input_names.append(temp_name)

            out_temp = f"__repark_sql_udf_out_{temp_counter}"

            temp_counter += 1

            depth = max_dep_depth + 1

            record = {
                "kind": "udf",
                "registered_name": registered_name,
                "input_names": input_names,
                "out_name": out_temp,
                "depth": depth,
                "start": call["start"],
                "end": call["end"],
            }

            call_records.append(record)

            span_to_out[(call["start"], call["end"])] = out_temp

            udf_nodes.append(record)

        # Build residual expression: replace only **outermost** UDF call spans with

        # their out temps (nested calls are already baked into the outer stage).

        # Replacing nested spans first would shift indices and break outer spans.

        outermost_calls = [
            call
            for call in calls
            if not any(
                other["start"] < call["start"] and call["end"] < other["end"]
                for other in calls
                if other is not call
            )
        ]

        residual_chars = list(expr_text)

        for call in sorted(outermost_calls, key=lambda item_call: -item_call["start"]):
            span_key = (call["start"], call["end"])

            out_temp = span_to_out[span_key]

            replacement = _quote_ident(out_temp)

            residual_chars[call["start"] : call["end"]] = list(replacement)

        residual_expr = "".join(residual_chars).strip()

        # User-visible output name (U9-C1-001 / Q13: never surface __repark_sql_udf_*).

        if alias is not None:
            out_name = alias

        else:
            # When residual is solely one outermost UDF out temp → Spark-style function name

            # (covers simple ``udf(x)`` and nested ``f(g(x))`` without AS).

            single_outer_name: str | None = None

            if len(outermost_calls) == 1:
                only = outermost_calls[0]

                out_temp = span_to_out[(only["start"], only["end"])]

                if residual_expr == _quote_ident(out_temp):
                    single_outer_name = only["registered_name"]

            # Expression wrap without AS: original user SQL fragment as display name

            # (residual_expr keeps temps for selectExpr evaluation).

            out_name = single_outer_name if single_outer_name is not None else expr_text

            # Hard guard — never emit internal materialization names to the user schema.

            if "__repark_sql_udf" in out_name:
                out_name = (
                    outermost_calls[0]["registered_name"]
                    if outermost_calls
                    else f"col{temp_counter}"
                )

        final_exprs.append(f"{residual_expr} AS {_quote_ident(out_name)}")

        user_out_names.append(out_name)

    # U10: materialize UDF calls in WHERE / GROUP BY / HAVING residuals.

    where_sql: str | None = None

    having_sql: str | None = None

    group_by_keys: list[str] | None = None

    # Map UDF call text in SELECT residual → user out name so GROUP BY my_udf(v)

    # binds to the SELECT alias. Keys are case+whitespace normalized (U10 C2).

    select_udf_call_to_out: dict[str, str] = {}

    for item_index, item in enumerate(items):
        item_clean = _sql_strip_comments_preserve_strings(item).strip()

        calls = _sql_find_registry_udf_calls(item_clean, registry)

        if not calls:
            continue

        # Use outermost call text → corresponding user out name when residual is pure UDF.

        outermost = [
            call
            for call in calls
            if not any(
                other["start"] < call["start"] and call["end"] < other["end"]
                for other in calls
                if other is not call
            )
        ]

        if len(outermost) == 1 and item_index < len(user_out_names):
            call_text = item_clean[outermost[0]["start"] : outermost[0]["end"]].strip()

            # Also strip trailing AS alias from item for keying pure call.

            as_match = re.search(
                r"(?i)\s+AS\s+((?:\"[^\"]+\")|(?:`[^`]+`)|(?:[A-Za-z_][A-Za-z0-9_]*))\s*$",
                item_clean,
            )

            pure = item_clean[: as_match.start()].strip() if as_match else item_clean

            out_alias = user_out_names[item_index]

            select_udf_call_to_out[_sql_udf_call_match_key(call_text)] = out_alias

            select_udf_call_to_out[_sql_udf_call_match_key(pure)] = out_alias

    if peeled.get("where"):
        where_fragment = peeled["where"] or ""

        where_body = re.sub(r"(?is)^\s*WHERE\s+", "", where_fragment, count=1).strip()

        where_calls = _sql_find_registry_udf_calls(where_body, registry)

        if where_calls:
            residual, new_nodes, new_parts, temp_counter = _sql_materialize_expr_udfs(
                where_body,
                registry=registry,
                temp_counter=temp_counter,
            )

            if residual is None:
                from repark.errors import UnsupportedOperationException

                raise UnsupportedOperationException(
                    "registered Python UDF in WHERE could not be rewritten in repark v1 "
                    "(simple col/lit UDF args only). Use DataFrame.filter after withColumn."
                )

            any_udf = True

            udf_nodes.extend(new_nodes)

            base_select_parts.extend(new_parts)

            # Compound WHERE may still reference base columns outside UDF calls

            # (``my_double(a) > 2 AND s = 'z'``). Residual filter runs on the

            # materialization frame (temp names only) — identity-project residual

            # base idents so the residual resolves without leaking temps (U10 C1).

            residual, residual_base_parts, temp_counter = _sql_where_residual_base_projections(
                residual,
                base_select_parts=base_select_parts,
                temp_counter=temp_counter,
            )

            base_select_parts.extend(residual_base_parts)

            # Subqueries in residual cannot run on the DataFrame.filter SQL path

            # (U10 C3 — was engine ParseException; refuse-loud with accurate shape).

            if _sql_residual_has_subquery(residual):
                from repark.errors import UnsupportedOperationException

                raise UnsupportedOperationException(
                    "registered Python UDF in WHERE with a nested subquery / EXISTS is "
                    "not supported in repark v1 (residual filter cannot host SELECT "
                    "subqueries after UDF materialization). Flatten the predicate or use "
                    "DataFrame.filter after withColumn."
                )

            where_sql = residual

        else:
            # Non-UDF WHERE stays in engine base SQL.

            core_rest = (core_rest + " " + where_fragment).strip() if core_rest else where_fragment

    if peeled.get("group_by"):
        group_fragment = peeled["group_by"] or ""

        group_body = re.sub(r"(?is)^\s*GROUP\s+BY\s+", "", group_fragment, count=1).strip()

        if not group_body:
            from repark.errors import UnsupportedOperationException

            raise UnsupportedOperationException(
                "registered Python UDF with empty GROUP BY is not supported in repark v1"
            )

        planned_keys: list[str] = []

        for part in _split_sql_select_list(group_body):
            piece = part.strip()

            if not piece:
                from repark.errors import UnsupportedOperationException

                raise UnsupportedOperationException(
                    "registered Python UDF GROUP BY has an empty key in repark v1"
                )

            # Alias / ordinal of SELECT outputs.

            out_lower = {name.lower(): name for name in user_out_names}

            if re.fullmatch(r"\d+", piece):
                ordinal = int(piece)

                if ordinal < 1 or ordinal > len(user_out_names):
                    from repark.errors import UnsupportedOperationException

                    raise UnsupportedOperationException(
                        "registered Python UDF GROUP BY ordinal is out of range in repark v1"
                    )

                planned_keys.append(user_out_names[ordinal - 1])

                continue

            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", piece):
                canonical = out_lower.get(piece.lower())

                if canonical is not None:
                    planned_keys.append(canonical)

                    continue

                # Bare base column not in SELECT outs — refuse (would need pass-through).

                from repark.errors import UnsupportedOperationException

                raise UnsupportedOperationException(
                    f"registered Python UDF GROUP BY key {piece!r} must be a SELECT-list "
                    "output alias (or matching UDF expression) in repark v1. Materialize "
                    "via DataFrame then groupBy."
                )

            if re.fullmatch(r'"([^"]|"")*"', piece) or re.fullmatch(r"`([^`]|``)*`", piece):
                name = piece[1:-1].replace('""', '"').replace("``", "`")

                canonical = out_lower.get(name.lower(), name if name in user_out_names else None)

                if canonical is None:
                    from repark.errors import UnsupportedOperationException

                    raise UnsupportedOperationException(
                        f"registered Python UDF GROUP BY key {piece!r} must be a "
                        "SELECT-list output alias in repark v1"
                    )

                planned_keys.append(canonical)

                continue

            # UDF expression matching a SELECT UDF call.

            piece_calls = _sql_find_registry_udf_calls(piece, registry)

            if piece_calls:
                # Prefer mapping to SELECT out when the call text matches (ws/case).

                mapped = select_udf_call_to_out.get(_sql_udf_call_match_key(piece))

                if mapped is not None:
                    planned_keys.append(mapped)

                    continue

                # Materialize a standalone GROUP BY UDF and use its temp as key after

                # folding into final projection under a stable name — refuse for v1 if

                # not already a SELECT out (keep scope tight).

                from repark.errors import UnsupportedOperationException

                raise UnsupportedOperationException(
                    "registered Python UDF in GROUP BY must match a SELECT-list UDF "
                    "output (same expression or its alias) in repark v1. Project the UDF "
                    "in the SELECT list and GROUP BY the alias."
                )

            from repark.errors import UnsupportedOperationException

            raise UnsupportedOperationException(
                f"registered Python UDF GROUP BY key shape {piece!r} is not supported "
                "in repark v1 (SELECT aliases, ordinals, or matching UDF expressions only)."
            )

        # Every SELECT out must be a group key (keys-only, no aggregates).

        key_set = {key.lower() for key in planned_keys}

        for out_name in user_out_names:
            if out_name.lower() not in key_set:
                from repark.errors import UnsupportedOperationException

                raise UnsupportedOperationException(
                    f"registered Python UDF SELECT output {out_name!r} is not a GROUP BY "
                    "key and aggregates are not supported in the UDF rewrite path (repark "
                    "v1 keys-only). Include it in GROUP BY or use DataFrame.groupBy.agg."
                )

        group_by_keys = planned_keys

        any_udf = True  # force rewrite path even if SELECT had no UDF (GB-only is rare)

    if peeled.get("having"):
        from repark.errors import UnsupportedOperationException

        if group_by_keys is None:
            raise UnsupportedOperationException(
                "registered Python UDF HAVING without GROUP BY is not supported in repark v1"
            )

        having_fragment = peeled["having"] or ""

        having_body = re.sub(r"(?is)^\s*HAVING\s+", "", having_fragment, count=1).strip()

        # Keys-only path: refuse aggregate HAVING (count/sum/…) before engine plan

        # garbage (U10 C1 — was "Physical plan does not support logical expression").

        having_agg_pattern = re.compile(
            r"(?is)\b(count|sum|avg|mean|min|max|first|last|collect_list|"
            r"collect_set|percentile|stddev|variance|var_pop|var_samp|"
            r"skewness|kurtosis|approx_count_distinct)\s*\("
        )

        if having_agg_pattern.search(having_body):
            raise UnsupportedOperationException(
                "registered Python UDF with aggregate HAVING is not supported in repark v1 "
                "(keys-only GROUP BY after UDF materialization; HAVING may filter on "
                "SELECT-list keys / matching UDF expressions only). Use DataFrame.groupBy.agg."
            )

        having_calls = _sql_find_registry_udf_calls(having_body, registry)

        if not having_calls:
            # Non-UDF HAVING: post-group filter on user-visible names / keys.

            having_sql = having_body

        else:
            # Map each HAVING UDF call span to a SELECT-list output alias (U10).

            residual_chars = list(having_body)

            for call in sorted(having_calls, key=lambda item_call: -item_call["start"]):
                call_text = having_body[call["start"] : call["end"]].strip()

                out_name = select_udf_call_to_out.get(_sql_udf_call_match_key(call_text))

                if out_name is None:
                    raise UnsupportedOperationException(
                        "registered Python UDF in HAVING must match a SELECT-list UDF "
                        "output (same expression or its alias) in repark v1. Project the "
                        "UDF in the SELECT list and filter on the alias."
                    )

                replacement = _quote_ident(out_name)

                residual_chars[call["start"] : call["end"]] = list(replacement)

            having_sql = "".join(residual_chars).strip()

            if "__repark_sql_udf" in having_sql:
                raise UnsupportedOperationException(
                    "registered Python UDF in HAVING produced an internal name leak "
                    "guard in repark v1; use SELECT-list aliases only."
                )

        any_udf = True

    if not any_udf:
        return None

    if not base_select_parts:
        return None

    # Stage UDF nodes by depth (independent same-depth calls share a stage).

    if not udf_nodes:
        return None

    max_depth = max(node["depth"] for node in udf_nodes)

    stages: list[list[dict[str, Any]]] = []

    for depth in range(max_depth + 1):
        stage = [
            {
                "kind": "udf",
                "registered_name": node["registered_name"],
                "input_names": node["input_names"],
                "out_name": node["out_name"],
            }
            for node in udf_nodes
            if node["depth"] == depth
        ]

        if stage:
            stages.append(stage)

    # ORDER BY: only simple aliases / ordinals of the SELECT list (post-materialization).

    order_by: list[tuple[str, bool]] | None = None

    if peeled.get("order_by"):
        order_by = _sql_plan_order_by_aliases(peeled["order_by"], user_out_names)

        if order_by is None:
            from repark.errors import UnsupportedOperationException

            raise UnsupportedOperationException(
                "registered Python UDF SELECT with this ORDER BY shape is not supported "
                "in repark v1 (order by SELECT-list output aliases or 1-based ordinals only "
                "after UDF materialization). Use DataFrame.orderBy after select."
            )

    limit_n: int | None = None

    if peeled.get("limit"):
        limit_match = re.match(r"(?is)^\s*LIMIT\s+(\d+)\s*$", peeled["limit"].strip())

        if limit_match is None:
            from repark.errors import UnsupportedOperationException

            # OFFSET / FETCH / non-integer LIMIT are all out of bounds for the peel path.

            raise UnsupportedOperationException(
                "registered Python UDF SELECT with this LIMIT shape is not supported in "
                "repark v1 (integer LIMIT n only after UDF materialization; LIMIT/OFFSET "
                "and non-integer LIMIT are refused). Use DataFrame.limit after select."
            )

        limit_n = int(limit_match.group(1))

    if core_rest:
        base_sql = "SELECT " + ", ".join(base_select_parts) + " " + core_rest

    else:
        # No-FROM SELECT (U9-C7-001): project UDF inputs/literals only.

        base_sql = "SELECT " + ", ".join(base_select_parts)

    materialize_plan: dict[str, Any] = {
        "stages": stages,
        "final_exprs": final_exprs,
        "distinct": is_distinct,
        "order_by": order_by,
        "limit": limit_n,
        "where_sql": where_sql,
        "group_by_keys": group_by_keys,
        "having_sql": having_sql,
        "user_out_names": user_out_names,
    }

    return base_sql, materialize_plan


# =============================================================================

# U9 — SQL UDF rewrite helpers (expression wrap / CTE / ORDER BY peel / leak guard)

# =============================================================================


def _sql_collect_registry_udf_hits(
    body: str,
    registry: dict[str, dict[str, Any]],
) -> list[tuple[str, str, int]]:
    """Collect ``(canonical_name, matched_text, index)`` for registry UDF calls in ``body``."""

    # === r20 U9: sql-udf-rewrite ===

    body_code = _sql_mask_strings_and_comments(body)

    hits: list[tuple[str, str, int]] = []

    seen_spans: set[tuple[int, int]] = set()

    for registered_name in registry:
        pattern = re.compile(
            rf"(?<![\w.])({re.escape(registered_name)})\s*\(",
            re.IGNORECASE,
        )

        for match in pattern.finditer(body_code):
            span = (match.start(), match.end())

            if span in seen_spans:
                continue

            # Star-only call ``name(*)`` is an engine aggregate form, not a Python UDF

            # invocation (U9-C2-001 — registering ``count`` must not break ``count(*)``).

            paren_open = match.end() - 1

            close = _find_matching_paren(body, paren_open)

            if close is not None:
                args_blob = body[paren_open + 1 : close].strip()

                if args_blob == "*":
                    continue

            seen_spans.add(span)

            matched_raw = body[match.start() : match.end() - 1].rstrip()

            canonical = registered_name

            for key in registry:
                if key.lower() == matched_raw.lower():
                    canonical = key

                    break

            hits.append((canonical, matched_raw, match.start()))

    hits.sort(key=lambda item: item[2])

    return hits


def _sql_find_registry_udf_calls(
    expr_text: str,
    registry: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Find registry UDF calls in a SELECT-list expression (paren-matched args)."""

    # === r20 U9: sql-udf-rewrite ===

    masked = _sql_mask_strings_and_comments(expr_text)

    calls: list[dict[str, Any]] = []

    seen: set[tuple[int, int]] = set()

    for registered_name in registry:
        pattern = re.compile(
            rf"(?<![\w.])({re.escape(registered_name)})\s*\(",
            re.IGNORECASE,
        )

        for match in pattern.finditer(masked):
            name_start = match.start(1)

            paren_open = match.end() - 1  # index of '('

            # Skip whitespace/comments between name and '(' already consumed by \s*.

            close = _find_matching_paren(expr_text, paren_open)

            if close is None:
                continue

            span = (name_start, close + 1)

            if span in seen:
                continue

            args_blob = expr_text[paren_open + 1 : close]

            # Engine aggregate ``name(*)`` is not a Python UDF call (U9-C2-001).

            if args_blob.strip() == "*":
                continue

            seen.add(span)

            args = _split_sql_select_list(args_blob) if args_blob.strip() else []

            # Resolve canonical registry name.

            raw_name = expr_text[name_start:paren_open].strip()

            # Strip trailing comments between name and paren.

            raw_name = _sql_strip_comments_preserve_strings(raw_name).strip()

            canonical = registered_name

            for key in registry:
                if key.lower() == raw_name.lower():
                    canonical = key

                    break

            calls.append(
                {
                    "registered_name": canonical,
                    "start": name_start,
                    "end": close + 1,
                    "args": args,
                }
            )

    return calls


def _sql_udf_arg_is_simple(arg: str) -> bool:
    """True when ``arg`` is a simple col/lit suitable as a UDF input projection."""

    # === r20 U9: sql-udf-rewrite ===

    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", arg):
        return True

    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+", arg):
        return True

    if re.fullmatch(r'"([^"]|"")*"', arg) or re.fullmatch(r"`([^`]|``)*`", arg):
        return True

    if re.fullmatch(
        r'("([^"]|"")*"|`([^`]|``)*`|[A-Za-z_][A-Za-z0-9_]*)'
        r'(\.("([^"]|"")*"|`([^`]|``)*`|[A-Za-z_][A-Za-z0-9_]*))+',
        arg,
    ):
        return True

    if re.fullmatch(r"-?\d+(\.\d+)?([eE][+-]?\d+)?", arg):
        return True

    if re.fullmatch(r"'(?:[^']|'')*'", arg):
        return True

    return arg.upper() in {"NULL", "TRUE", "FALSE"}


def _sql_peel_select_trailing_clauses(rest: str) -> tuple[str, dict[str, str | None]]:
    """Split ``FROM … [WHERE …] [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT …]``.



    Returns ``(core_rest, peeled)`` where ``core_rest`` keeps FROM + JOIN chain (U10

    peels WHERE too so UDF residuals can be applied post-materialization) and

    ``peeled`` holds optional trailing clause SQL fragments.

    """

    # === r20 U9: sql-udf-rewrite ===

    # === r21 T7: census-r6 ===

    peeled: dict[str, str | None] = {
        "where": None,
        "group_by": None,
        "having": None,
        "order_by": None,
        "limit": None,
    }

    # Find top-level clause starts (paren depth 0).

    markers: list[tuple[str, int]] = []

    for keyword in ("WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"):
        index = _sql_top_level_keyword_index(rest, keyword)

        if index is not None:
            markers.append((keyword, index))

    if not markers:
        return rest, peeled

    markers.sort(key=lambda item: item[1])

    core_end = markers[0][1]

    core_rest = rest[:core_end].rstrip()

    # Slice each clause until the next marker (or end).

    for position, (keyword, start) in enumerate(markers):
        end = markers[position + 1][1] if position + 1 < len(markers) else len(rest)

        fragment = rest[start:end].strip()

        key = {
            "WHERE": "where",
            "GROUP BY": "group_by",
            "HAVING": "having",
            "ORDER BY": "order_by",
            "LIMIT": "limit",
        }[keyword]

        peeled[key] = fragment

    return core_rest, peeled


def _sql_udf_call_match_key(call_text: str) -> str:
    """Case- and whitespace-normalized key for SELECT↔GROUP BY/HAVING UDF match (U10 C2)."""

    # === r21 T7: census-r6 ===

    return re.sub(r"\s+", "", call_text.strip()).lower()


def _sql_residual_has_subquery(residual: str) -> bool:
    """True when a WHERE/HAVING residual still embeds a SELECT/EXISTS subquery (U10 C3)."""

    # === r21 T7: census-r6 ===

    masked = _sql_mask_strings_and_comments(residual)

    return bool(
        re.search(r"(?is)\(\s*SELECT\b", masked) or re.search(r"(?is)\bEXISTS\s*\(", masked)
    )


def _sql_where_residual_base_projections(
    residual: str,
    *,
    base_select_parts: list[str],
    temp_counter: int,
) -> tuple[str, list[str], int]:
    """Identity-project residual base columns needed by compound WHERE (U10 C1).



    After UDF call spans are replaced with ``__repark_sql_udf_out_*`` temps, residual

    predicates may still reference table columns (``AND a < 10``, ``AND s = 'z'``).

    Those names are not on the materialization frame unless projected. Returns

    ``(residual, new_base_parts, temp_counter)`` — bare idents project as

    ``col AS col``; qualified ``t.col`` projects under a stable temp and the

    residual span is rewritten so the filter resolves (alias ``t.col`` is not a

    valid multi-part field name on the post-scan frame).



    Syntax keywords (``FROM`` / ``BOTH`` / ``FOR`` / …) are never identity-projected so

    engine forms like ``IS [NOT] DISTINCT FROM``, ``trim(BOTH … FROM …)``,

    ``substring(… FROM … FOR …)``, ``extract(YEAR FROM …)`` stay intact (F-E1-1).

    SQL type tokens are only skipped after ``AS`` (CAST) so legitimate columns named

    ``date`` / ``double`` / ``string`` still project (F-E1-2); U10 C6 AS-skip retained.

    ``END`` is CASE-terminator only when nested under unmatched ``CASE`` (column ``end``

    still projects). Ambiguous names are quoted on both the base projection and residual.



    **r22 U11 residual poles (F-E1 class):**



    * ``INTERVAL '1' DAY`` — unit tokens after an INTERVAL literal must not be

      identity-projected / quote-rewritten (would break unit syntax). Multi-unit

      ``DAY TO SECOND`` trailing units after ``TO`` are also syntax (octo C1).

    * Typed literals ``DATE '…'`` / ``TIMESTAMP '…'`` / ``TIME '…'`` — constructor

      keywords are syntax when followed by a string literal (octo C2).

    * Columns named ``and`` / ``or`` / ``not`` — ``DataFrame.filter``'s SQL-string

      identifier rewriter case-steals boolean keywords when those columns sit on the

      materialization frame; project them under ``__repark_sql_udf_wcol_*`` temps and

      rewrite residual spans (never leak temps to the user projection).

    """

    # === r21 T7: census-r6 ===

    # === r22 U11: residual-keyword-poles ===

    # Pure syntax / boolean / clause keywords — never bare column projections.

    # Type tokens (date/double/string/…) are intentionally absent: they skip only after

    # AS (CAST). Bare ``from``/``both``/``for`` cannot be unquoted columns in Spark SQL.

    reserved = {
        "and",
        "or",
        "not",
        "in",
        "is",
        "null",
        "true",
        "false",
        "like",
        "ilike",
        "rlike",
        "regexp",
        "between",
        "case",
        "when",
        "then",
        "else",
        # "end" — CASE nesting heuristic below (legal column name — F-E1-2).
        "as",
        "distinct",
        "cast",
        "try_cast",
        "exists",
        "any",
        "all",
        "some",
        "escape",
        "div",
        "mod",
        "over",
        "filter",
        "within",
        "group",
        "order",
        "by",
        "asc",
        "desc",
        "interval",
        "current_date",
        "current_timestamp",
        "current_time",
        "localtimestamp",
        "localtime",
        # Multi-word SQL syntax (F-E1-1 under-reserve class).
        "from",
        "both",
        "for",
        "leading",
        "trailing",
        "similar",
        "to",
        "placing",
        "using",
        "symmetric",
        "asymmetric",
        "only",
        "nulls",
        "first",
        "last",
        "unknown",
        "extract",
        "trim",
        "substring",
        "position",
        "overlay",
        "zone",
        "at",
        "time",
        "with",
        "without",
        "select",
        "where",
        "having",
        "limit",
        "offset",
        "join",
        "on",
        "left",
        "right",
        "inner",
        "outer",
        "cross",
        "full",
        "natural",
        "union",
        "except",
        "intersect",
        "lateral",
        "recursive",
        "values",
        "window",
        "qualify",
        "cube",
        "rollup",
        "sets",
        "partition",
        "range",
        "rows",
        "unbounded",
        "preceding",
        "following",
        "current",
        "row",
    }

    # extract(YEAR FROM …) / date_part field names — syntax when followed by FROM.

    extract_fields = {
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "millisecond",
        "microsecond",
        "nanosecond",
        "week",
        "quarter",
        "dow",
        "doy",
        "epoch",
        "date",
        "time",
    }

    # INTERVAL <lit|n> <unit> — unit is syntax, not a column (U11 residual pole).

    interval_units = {
        "year",
        "years",
        "month",
        "months",
        "week",
        "weeks",
        "day",
        "days",
        "hour",
        "hours",
        "minute",
        "minutes",
        "second",
        "seconds",
        "millisecond",
        "milliseconds",
        "microsecond",
        "microseconds",
        "nanosecond",
        "nanoseconds",
    }

    # DataFrame.filter SQL-string rewriter case-steals these boolean keywords when a

    # same-named column is on the frame (``a > 0 AND "and" = 5`` → ParseException).

    # Always project under a temp and rewrite residual (U11 residual pole).

    filter_boolean_steal_names = {"and", "or", "not"}

    # Legal column names that collide with CAST types / CASE END — quote on project + residual.

    quote_project_names = {
        "bigint",
        "long",
        "int",
        "integer",
        "smallint",
        "tinyint",
        "byte",
        "short",
        "double",
        "float",
        "real",
        "boolean",
        "bool",
        "string",
        "varchar",
        "char",
        "binary",
        "date",
        "timestamp",
        "timestamp_ntz",
        "decimal",
        "numeric",
        "void",
        "end",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "week",
        "quarter",
    }

    # Aliases already present on the base projection (quoted or bare AS tail).

    existing_aliases: set[str] = set()

    for part in base_select_parts:
        as_match = re.search(
            r'(?i)\s+AS\s+("([^"]|"")*"|`([^`]|``)*`|[A-Za-z_][A-Za-z0-9_]*)\s*$',
            part.strip(),
        )

        if as_match is None:
            continue

        raw = as_match.group(1)

        if raw.startswith('"') and raw.endswith('"'):
            existing_aliases.add(raw[1:-1].replace('""', '"').lower())

        elif raw.startswith("`") and raw.endswith("`"):
            existing_aliases.add(raw[1:-1].replace("``", "`").lower())

        else:
            existing_aliases.add(raw.lower())

    masked = _sql_mask_strings_and_comments(residual)

    new_parts: list[str] = []

    residual_chars = list(residual)

    counter = temp_counter

    # Qualified table.col — rewrite residual to a single-part temp (right-to-left).

    qual_matches = list(
        re.finditer(
            r"(?<![\w.])([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)",
            masked,
        )
    )

    qual_alias_by_span: dict[tuple[int, int], str] = {}

    for match in qual_matches:
        if re.match(r"\s*\(", masked[match.end() :]):
            continue

        table_name = match.group(1)

        column_name = match.group(2)

        if table_name.lower() in reserved:
            continue

        # Column side may be a type-token name (t.date) — still project; only skip

        # pure syntax keywords that cannot be unquoted columns.

        if column_name.lower() in reserved:
            continue

        expr = f"{table_name}.{column_name}"

        # Reuse bare column alias when free and already projected.

        if column_name.lower() in existing_aliases:
            alias = column_name

        else:
            alias = f"__repark_sql_udf_wcol_{counter}"

            counter += 1

            new_parts.append(f"{expr} AS {_quote_ident(alias)}")

            existing_aliases.add(alias.lower())

        qual_alias_by_span[(match.start(), match.end())] = alias

    for (start, end), alias in sorted(qual_alias_by_span.items(), key=lambda item: -item[0][0]):
        residual_chars[start:end] = list(_quote_ident(alias))

    residual_after_qual = "".join(residual_chars)

    masked = _sql_mask_strings_and_comments(residual_after_qual)

    # Bare identifiers (not function names, not internal temps, not syntax keywords).

    # Type tokens after AS and extract fields before FROM are context-skipped (C6 / F-E1-1).

    seen_bare: set[str] = set()

    # Spans of bare idents rewritten to quoted form so filter SQL accepts keyword-ish names.

    # Third element is the replacement text (quoted ident or temp alias).

    quote_residual_spans: list[tuple[int, int, str]] = []

    # filter-boolean-steal names already assigned a temp on this residual.

    steal_temp_by_key: dict[str, str] = {}

    for match in re.finditer(r"(?<![\w.])([A-Za-z_][A-Za-z0-9_]*)", masked):
        ident = match.group(1)

        if ident.startswith("__repark_sql_udf"):
            continue

        if ident.lower() in reserved:
            continue

        # Skip CAST/TRY_CAST type names even when not in the static reserved set

        # (``AS decimal(10,2)`` leaves ``decimal``; also bare ``AS customtype``).

        prefix = masked[: match.start()]

        if re.search(r"(?is)\bAS\s+$", prefix):
            continue

        # extract(YEAR FROM x) / date_part — field then FROM is syntax, not a column (F-E1-1).

        key = ident.lower()

        if key in extract_fields and re.match(r"(?is)\s+FROM\b", masked[match.end() :]):
            continue

        if re.search(r"(?is)\b(EXTRACT|DATE_PART|DATEPART)\s*\(\s*$", prefix):
            continue

        # INTERVAL '1' DAY / INTERVAL 1 DAY — unit token is syntax (U11 residual pole).

        # String literals are length-masked to spaces by ``_sql_mask_strings_and_comments``,

        # so ``INTERVAL '1'`` becomes ``INTERVAL     `` (no quotes remain on the masked prefix).

        # Multi-unit qualifiers ``DAY TO SECOND`` / ``YEAR TO MONTH``: the trailing unit sits

        # after ``TO``, not immediately after INTERVAL — still syntax, never a column

        # (octo U11 C1 — was identity-projected / quote-rewritten → Schema error).

        if key in interval_units and (
            re.search(r"(?is)\bINTERVAL\s+(?:\d+\s+)?$", prefix)
            or re.search(r"(?is)\bINTERVAL\b[\s\S]*\bTO\s+$", prefix)
        ):
            continue

        # Typed SQL literals ``DATE '…'`` / ``TIMESTAMP '…'`` / ``TIME '…'`` — constructor

        # keyword is syntax, not a column (octo U11 C2). Mask turns the string into spaces,

        # so detect the quote on the unmasked residual (length-preserving mask).

        if key in {"date", "timestamp", "timestamp_ntz", "time"} and re.match(
            r"\s*'",
            residual_after_qual[match.end() :],
        ):
            continue

        # CASE … END terminator only when nested under unmatched CASE (F-E1-2 column end).

        if key == "end":
            cases = len(re.findall(r"(?is)\bCASE\b", prefix))

            ends = len(re.findall(r"(?is)\bEND\b", prefix))

            if cases > ends:
                continue

        if re.match(r"\s*\.", masked[match.end() :]):
            continue  # table qualifier of a qualified ref

        if re.match(r"\s*\(", masked[match.end() :]):
            continue  # function name

        needs_quote = key in quote_project_names

        needs_steal_temp = key in filter_boolean_steal_names

        if key in seen_bare or key in existing_aliases:
            # Already on frame — still rewrite residual keyword-ish / steal-name refs.

            if needs_steal_temp:
                temp_alias = steal_temp_by_key.get(key)

                if temp_alias is None:
                    # Frame already has the column under its real name (e.g. SELECT list

                    # pass-through) — still need a steal-safe temp for residual filter.

                    temp_alias = f"__repark_sql_udf_wcol_{counter}"

                    counter += 1

                    steal_temp_by_key[key] = temp_alias

                    new_parts.append(f"{_quote_ident(ident)} AS {_quote_ident(temp_alias)}")

                quote_residual_spans.append((match.start(), match.end(), _quote_ident(temp_alias)))

            elif needs_quote:
                quote_residual_spans.append((match.start(), match.end(), _quote_ident(ident)))

            continue

        seen_bare.add(key)

        if needs_steal_temp:
            temp_alias = f"__repark_sql_udf_wcol_{counter}"

            counter += 1

            steal_temp_by_key[key] = temp_alias

            existing_aliases.add(temp_alias.lower())

            new_parts.append(f"{_quote_ident(ident)} AS {_quote_ident(temp_alias)}")

            quote_residual_spans.append((match.start(), match.end(), _quote_ident(temp_alias)))

        else:
            existing_aliases.add(key)

            if needs_quote:
                new_parts.append(f"{_quote_ident(ident)} AS {_quote_ident(ident)}")

                quote_residual_spans.append((match.start(), match.end(), _quote_ident(ident)))

            else:
                new_parts.append(f"{ident} AS {_quote_ident(ident)}")

    # Quote residual keyword-ish bare columns right-to-left (filter cannot parse bare END/DATE).

    residual_chars = list(residual_after_qual)

    for start, end, replacement in sorted(quote_residual_spans, key=lambda item: -item[0]):
        residual_chars[start:end] = list(replacement)

    residual_after_qual = "".join(residual_chars)

    # Quoted residual identifiers (``"from"`` / `` `date` ``) — project when not already on frame.

    # Strings use single quotes and are already excluded by the quote-style scan.

    # filter-boolean-steal names always get a temp even when already quoted in residual.

    quoted_rewrite_spans: list[tuple[int, int, str]] = []

    for match in re.finditer(
        r'"((?:[^"]|"")*)"|`((?:[^`]|``)*)`',
        residual_after_qual,
    ):
        if match.group(1) is not None:
            name = match.group(1).replace('""', '"')

            expr = match.group(0)

        else:
            name = match.group(2).replace("``", "`")

            expr = match.group(0)

        if not name or name.startswith("__repark_sql_udf"):
            continue

        key = name.lower()

        if key in filter_boolean_steal_names:
            temp_alias = steal_temp_by_key.get(key)

            if temp_alias is None:
                temp_alias = f"__repark_sql_udf_wcol_{counter}"

                counter += 1

                steal_temp_by_key[key] = temp_alias

                existing_aliases.add(temp_alias.lower())

                new_parts.append(f"{expr} AS {_quote_ident(temp_alias)}")

                seen_bare.add(key)

            quoted_rewrite_spans.append((match.start(), match.end(), _quote_ident(temp_alias)))

            continue

        # Skip if this span is only an alias we already emitted (quoted temp rewrite).

        if key in seen_bare or key in existing_aliases:
            continue

        # Quoted form IS the column (including syntax-keyword column names like "from").

        seen_bare.add(key)

        existing_aliases.add(key)

        new_parts.append(f"{expr} AS {_quote_ident(name)}")

    if quoted_rewrite_spans:
        residual_chars = list(residual_after_qual)

        for start, end, replacement in sorted(quoted_rewrite_spans, key=lambda item: -item[0]):
            residual_chars[start:end] = list(replacement)

        residual_after_qual = "".join(residual_chars)

    return residual_after_qual, new_parts, counter


def _sql_materialize_expr_udfs(
    expr_text: str,
    *,
    registry: dict[str, dict[str, Any]],
    temp_counter: int,
) -> tuple[str | None, list[dict[str, Any]], list[str], int]:
    """Materialize registry UDF calls inside a WHERE/HAVING expression (U10).



    Returns ``(residual_expr, udf_nodes, base_select_parts, new_temp_counter)``.

    ``residual_expr`` is ``None`` when the shape is out of bounds (caller refuses loud).

    """

    # === r21 T7: census-r6 ===

    calls = _sql_find_registry_udf_calls(expr_text, registry)

    if not calls:
        return expr_text, [], [], temp_counter

    calls_sorted = sorted(calls, key=lambda call: (-call["start"], -call["end"]))

    span_to_out: dict[tuple[int, int], str] = {}

    call_records: list[dict[str, Any]] = []

    base_parts: list[str] = []

    counter = temp_counter

    for call in calls_sorted:
        registered_name = call["registered_name"]

        arg_texts: list[str] = call["args"]

        if not arg_texts:
            return None, [], [], temp_counter

        input_names: list[str] = []

        max_dep_depth = -1

        for arg in arg_texts:
            arg_stripped = arg.strip()

            if not arg_stripped:
                return None, [], [], temp_counter

            nested_out: str | None = None

            for span, out_temp in span_to_out.items():
                nested_call_text = expr_text[span[0] : span[1]]

                if arg_stripped == nested_call_text.strip():
                    nested_out = out_temp

                    for record in call_records:
                        if record["out_name"] == out_temp:
                            max_dep_depth = max(max_dep_depth, record["depth"])

                            break

                    break

            if nested_out is not None:
                input_names.append(nested_out)

                continue

            if not _sql_udf_arg_is_simple(arg_stripped):
                return None, [], [], temp_counter

            temp_name = f"__repark_sql_udf_in_{counter}"

            counter += 1

            base_parts.append(f"{arg_stripped} AS {_quote_ident(temp_name)}")

            input_names.append(temp_name)

        out_temp = f"__repark_sql_udf_out_{counter}"

        counter += 1

        depth = max_dep_depth + 1

        record = {
            "kind": "udf",
            "registered_name": registered_name,
            "input_names": input_names,
            "out_name": out_temp,
            "depth": depth,
            "start": call["start"],
            "end": call["end"],
        }

        call_records.append(record)

        span_to_out[(call["start"], call["end"])] = out_temp

    outermost_calls = [
        call
        for call in calls
        if not any(
            other["start"] < call["start"] and call["end"] < other["end"]
            for other in calls
            if other is not call
        )
    ]

    residual_chars = list(expr_text)

    for call in sorted(outermost_calls, key=lambda item_call: -item_call["start"]):
        span_key = (call["start"], call["end"])

        out_temp = span_to_out[span_key]

        replacement = _quote_ident(out_temp)

        residual_chars[call["start"] : call["end"]] = list(replacement)

    residual_expr = "".join(residual_chars).strip()

    return residual_expr, call_records, base_parts, counter


def _sql_plan_order_by_aliases(
    order_by_sql: str,
    user_out_names: list[str],
) -> list[tuple[str, bool]] | None:
    """Parse ``ORDER BY`` into ``(out_name, ascending)`` when only aliases/ordinals.



    Explicit ``NULLS FIRST`` / ``NULLS LAST`` is refused (returns ``None`` → loud UOE):

    DataFrame.orderBy does not yet wire Column nulls markers end-to-end (U9-C4-001;

    H1 owns dataframe sort). Bare ASC/DESC use Column.asc/desc defaults.



    Returns ``None`` when the ORDER BY shape is out of bounds (caller refuses loud).

    """

    # === r20 U9: sql-udf-rewrite ===

    text = order_by_sql.strip()

    if not re.match(r"(?is)^ORDER\s+BY\b", text):
        return None

    items_blob = re.sub(r"(?is)^ORDER\s+BY\s+", "", text, count=1).strip()

    if not items_blob:
        return None

    parts = _split_sql_select_list(items_blob)

    out_lower = {name.lower(): name for name in user_out_names}

    planned: list[tuple[str, bool]] = []

    for part in parts:
        piece = part.strip()

        if not piece:
            return None

        ascending = True

        # Explicit NULLS FIRST/LAST: refuse loud (do not silently ignore — U9-C4-001).

        if re.search(r"(?is)\bNULLS\s+(FIRST|LAST)\b", piece):
            return None

        direction = re.search(r"(?is)\s+(ASC|DESC)\s*$", piece)

        if direction:
            ascending = direction.group(1).upper() == "ASC"

            piece = piece[: direction.start()].strip()

        # Ordinal 1-based.

        if re.fullmatch(r"\d+", piece):
            ordinal = int(piece)

            if ordinal < 1 or ordinal > len(user_out_names):
                return None

            planned.append((user_out_names[ordinal - 1], ascending))

            continue

        # Bare / quoted alias matching a SELECT out name.

        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", piece):
            canonical = out_lower.get(piece.lower())

            if canonical is None:
                return None

            planned.append((canonical, ascending))

            continue

        if re.fullmatch(r'"([^"]|"")*"', piece):
            name = piece[1:-1].replace('""', '"')

            if name not in user_out_names and name.lower() not in out_lower:
                return None

            planned.append((out_lower.get(name.lower(), name), ascending))

            continue

        if re.fullmatch(r"`([^`]|``)*`", piece):
            name = piece[1:-1].replace("``", "`")

            if name not in user_out_names and name.lower() not in out_lower:
                return None

            planned.append((out_lower.get(name.lower(), name), ascending))

            continue

        return None

    return planned


def _sql_udf_public_error_text(error: BaseException) -> str:
    """Strip internal ``__repark_sql_udf_*`` names from error text (U9 Q13)."""

    # === r20 U9: sql-udf-rewrite ===

    text = str(error)

    if "__repark_sql_udf" not in text:
        return text

    return (
        "UDF SELECT rewrite could not complete for this statement shape "
        "(internal materialization columns are not user-visible). "
        "Use SELECT-list UDF forms with ORDER BY on output aliases, or the "
        "DataFrame udf path."
    )


def _sql_udf_clean_exception(error: BaseException) -> BaseException:
    """Map engine errors that leak internal UDF temp names to a loud clean UOE.



    Preserves :class:`~repark.errors.PySparkException` taxonomy (user UDF raises,

    Analysis/Parse, …) so runtime failures are not re-framed as rewrite-shape UOEs

    (U9-C3-001). Only internal-name leaks and unexpected non-PySpark errors are wrapped.

    """

    # === r20 U9: sql-udf-rewrite ===

    from repark.errors import PySparkException, UnsupportedOperationException

    text = str(error)

    if "__repark_sql_udf" in text:
        return UnsupportedOperationException(
            "registered Python UDF in SQL could not be applied with the surrounding "
            "statement shape in repark v1 (SELECT-list rewrite materializes UDF outputs "
            "after the engine FROM scan; internal column names are never user-visible). "
            "Use DataFrame.select / withColumn + orderBy, or ORDER BY the UDF output "
            "alias only for supported SELECT-list forms."
        )

    if isinstance(error, UnsupportedOperationException):
        return error

    # Surface user UDF / analysis / parse errors without rewrite framing (U9-C3-001).

    if isinstance(error, PySparkException):
        return error

    return UnsupportedOperationException(
        "registered Python UDF in SQL could not be rewritten in repark v1 "
        f"({type(error).__name__}: {_sql_udf_public_error_text(error)}). "
        "Use DataFrame F.udf / spark.udf.register + select/withColumn."
    )


def _sql_table_ref(table_name: str) -> str:
    """Validate a multipart table identifier and return a quoted SQL table reference.



    Accepts ``ident`` or ``ident.ident…`` with unquoted segments matching

    ``[A-Za-z_][A-Za-z0-9_]*``, double-quoted segments (``""`` escapes a quote; dots allowed

    inside quotes), or Spark-style backtick-quoted segments. Rejects SQL fragments so

    ``spark.table`` cannot be used as a FROM-clause injection surface.



    Does **not** apply default-catalog / default-namespace qualification — callers that need

    bare-name expansion must resolve first via :meth:`ReparkSession.resolve_table_name` /

    :meth:`ReparkSession._sql_table_ref_resolved` (E2).

    """

    from repark.errors import AnalysisException

    name = table_name.strip()

    if not name:
        raise AnalysisException("table name must not be empty")

    try:
        segments = _parse_table_identifier_segments(name)

    except ValueError as error:
        raise AnalysisException(
            f"invalid table identifier {name[:128]!r}: {error} "
            "(expected multipart name like catalog.db.table; SQL fragments are not allowed)"
        ) from error

    return ".".join(_quote_ident(segment) for segment in segments)


def _sql_mask_strings_and_comments(query: str) -> str:
    """Return ``query`` with string literals and comments replaced by spaces.



    **Length and indices are preserved** so hit positions from a masked scan remain

    valid against the original body (U8 registry-name scan — octo C1-L-001). Handles

    single quotes (``''`` escape), double quotes (``""`` escape), backticks, ``--``

    line comments, and ``/* … */`` block comments. Does not interpret nested block

    comments (SQL standard single-level).

    """

    if not query:
        return query

    chars = list(query)

    length = len(query)

    index = 0

    while index < length:
        char = query[index]

        # Line comment

        if char == "-" and index + 1 < length and query[index + 1] == "-":
            end = query.find("\n", index)

            if end < 0:
                for pos in range(index, length):
                    chars[pos] = " "

                break

            for pos in range(index, end):
                chars[pos] = " "

            index = end

            continue

        # Block comment

        if char == "/" and index + 1 < length and query[index + 1] == "*":
            end = query.find("*/", index + 2)

            if end < 0:
                for pos in range(index, length):
                    chars[pos] = " "

                break

            for pos in range(index, end + 2):
                chars[pos] = " "

            index = end + 2

            continue

        # Quoted string / identifier

        if char in {"'", '"', "`"}:
            quote = char

            chars[index] = " "

            index += 1

            while index < length:
                current = query[index]

                chars[index] = " "

                index += 1

                if current == quote:
                    # SQL doubled-quote escape inside the same quote style.

                    if index < length and query[index] == quote:
                        chars[index] = " "

                        index += 1

                        continue

                    break

            continue

        index += 1

    return "".join(chars)


def _split_leading_sql_trivia(query: str) -> tuple[str, str]:
    """Split leading whitespace + SQL comments from ``query`` (octo C1-Q-006).



    Returns ``(trivia, body)`` so statement-form classifiers see a clean head while the

    original leading trivia is re-prefixed onto the expanded body.

    """

    index = _skip_sql_ws_and_comments(query, 0)

    return query[:index], query[index:]


def _skip_sql_ws_and_comments(query: str, index: int) -> int:
    """Advance ``index`` past whitespace and ``--`` / ``/* */`` comments (octo C5-Q-001)."""

    length = len(query)

    while index < length:
        char = query[index]

        if char.isspace():
            index += 1

            continue

        if char == "-" and index + 1 < length and query[index + 1] == "-":
            end = query.find("\n", index)

            if end < 0:
                return length

            index = end + 1

            continue

        if char == "/" and index + 1 < length and query[index + 1] == "*":
            end = query.find("*/", index + 2)

            if end < 0:
                return length

            index = end + 2

            continue

        break

    return index


def _find_matching_paren(query: str, open_index: int) -> int | None:
    """Return the index of the ``)`` matching ``query[open_index] == '('``, or None."""

    if open_index >= len(query) or query[open_index] != "(":
        return None

    depth = 0

    index = open_index

    length = len(query)

    while index < length:
        char = query[index]

        if char in {'"', "'", "`"}:
            quote = char

            index += 1

            while index < length:
                current = query[index]

                index += 1

                if current == quote:
                    if quote == '"' and index < length and query[index] == '"':
                        index += 1

                        continue

                    break

            continue

        if char == "(":
            depth += 1

        elif char == ")":
            depth -= 1

            if depth == 0:
                return index

        index += 1

    return None


def _collect_cte_names(query: str) -> set[str]:
    """Collect CTE names from a leading ``WITH name AS (…), …`` list (lowercase).



    Used so ``FROM cte`` is not rewritten to ``catalog.db.cte`` (F1 / time-travel CTE pin).

    """

    match = re.match(r"(?is)^\s*WITH\b", query)

    if match is None:
        return set()

    names: set[str] = set()

    index = match.end()

    length = len(query)

    while index < length:
        while index < length and query[index].isspace():
            index += 1

        if index < length and query[index : index + 9].upper() == "RECURSIVE":
            index += 9

            while index < length and query[index].isspace():
                index += 1

        # Optional RECURSIVE already skipped; read CTE name.

        name_end = _scan_sql_table_identifier_end(query, index)

        if name_end is None or name_end == index:
            break

        raw_name = query[index:name_end]

        # Only one-part CTE names are standard; take last segment lowercased.

        segment = raw_name.split(".")[-1].strip().strip('"').strip("`").lower()

        if segment:
            names.add(segment)

        index = name_end

        while index < length and query[index].isspace():
            index += 1

        # Optional column list (name (a, b) AS …)

        if index < length and query[index] == "(":
            close = _find_matching_paren(query, index)

            if close is None:
                break

            index = close + 1

            while index < length and query[index].isspace():
                index += 1

        if index + 2 <= length and query[index : index + 2].upper() == "AS":
            index += 2

        else:
            break

        while index < length and query[index].isspace():
            index += 1

        if index >= length or query[index] != "(":
            break

        close = _find_matching_paren(query, index)

        if close is None:
            break

        index = close + 1

        while index < length and query[index].isspace():
            index += 1

        if index < length and query[index] == ",":
            index += 1

            continue

        break

    return names


def _split_leading_table_ident(blob: str) -> tuple[str | None, str]:
    """Split ``blob`` into a leading table identifier and the remaining suffix (aliases).



    Returns ``(None, blob)`` when no identifier can be scanned (e.g. subquery ``(SELECT …)``

    — callers treat the whole blob as opaque). Used by MERGE INTO target/source expansion.

    """

    stripped = blob.strip()

    if not stripped:
        return None, blob

    if stripped.startswith("("):
        return stripped, ""

    end = _scan_sql_table_identifier_end(stripped, 0)

    if end is None or end == 0:
        return None, blob

    return stripped[:end], stripped[end:]


def _match_from_or_join_keyword(query: str, index: int) -> str | None:
    """If ``query[index:]`` starts with FROM/JOIN as a whole word, return that keyword.



    Word-boundary only: the char before ``index`` (if any) must be non-identifier, and the

    char after the keyword must be non-identifier (space, end, or punctuation). Case

    insensitive. Used by the F1 free-SQL FROM/JOIN expander.

    """

    if index > 0:
        previous = query[index - 1]

        if previous.isalnum() or previous == "_":
            return None

    remaining = query[index:]

    for keyword in ("FROM", "JOIN"):
        if remaining[: len(keyword)].upper() != keyword:
            continue

        after = index + len(keyword)

        if after < len(query):
            next_char = query[after]

            if next_char.isalnum() or next_char == "_":
                continue

        return query[index:after]  # preserve original case

    return None


def _update_rest_has_set_clause(rest: str) -> bool:
    """True when ``rest`` after an UPDATE target still contains a SET keyword (G1 / octo C1).



    Accepts optional alias forms (``AS a`` / bare ``a``) before SET. Used to refuse expanding

    ``UPDATE SET x = 1`` where the identifier scan ate the SET keyword as a table name.

    """

    stripped = rest.lstrip()

    if not stripped:
        return False

    # Optional alias: AS name | bare name, then SET.

    alias_match = re.match(
        r"(?is)^(?:(?:AS\s+)?(?:\"[^\"]+\"|`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)\s+)?SET\b",
        stripped,
    )

    return alias_match is not None


def _scan_sql_table_identifier_end(query: str, start: int) -> int | None:
    """Return the end index of a multipart table identifier starting at ``start``.



    Accepts unquoted ``[A-Za-z_][A-Za-z0-9_]*`` segments and double/backtick-quoted

    segments, joined by ``.``. Returns ``None`` when no identifier is present.

    """

    length = len(query)

    index = start

    if index >= length:
        return None

    saw_segment = False

    while index < length:
        char = query[index]

        if char in {'"', "`"}:
            quote = char

            index += 1

            closed = False

            while index < length:
                current = query[index]

                if current == quote:
                    if quote == '"' and index + 1 < length and query[index + 1] == '"':
                        index += 2

                        continue

                    index += 1

                    closed = True

                    break

                index += 1

            if not closed:
                return None

            saw_segment = True

        elif char.isalpha() or char == "_":
            index += 1

            while index < length and (query[index].isalnum() or query[index] == "_"):
                index += 1

            saw_segment = True

        else:
            break

        if index < length and query[index] == ".":
            index += 1

            # Require another segment after the dot (trailing '.' is not a table name).

            if index >= length:
                return None

            continue

        break

    if not saw_segment:
        return None

    return index


def _split_sql_table_name_list(names_blob: str) -> list[str]:
    """Split a comma-separated list of table identifiers with quote awareness."""

    parts: list[str] = []

    buf: list[str] = []

    quote: str | None = None

    index = 0

    while index < len(names_blob):
        character = names_blob[index]

        if quote is not None:
            buf.append(character)

            if character == quote:
                if quote == '"' and index + 1 < len(names_blob) and names_blob[index + 1] == '"':
                    buf.append(names_blob[index + 1])

                    index += 2

                    continue

                quote = None

            index += 1

            continue

        if character in {'"', "`"}:
            quote = character

            buf.append(character)

            index += 1

            continue

        if character == ",":
            part = "".join(buf).strip()

            if part:
                parts.append(part)

            buf = []

            index += 1

            continue

        buf.append(character)

        index += 1

    tail = "".join(buf).strip()

    if tail:
        parts.append(tail)

    return parts


def _parse_table_identifier_segments(name: str) -> list[str]:
    """Split a multipart table name with quote-awareness (``"…"`` / ```…```).



    Raises :class:`~repark.errors.PySparkValueError` on empty segments, trailing dots,

    unterminated quotes,

    unquoted non-identifier text (spaces, operators, etc.), or path-escape segments

    (``..`` / ``/`` / ``\\`` — O3-C4-SEC-001).

    """

    segments: list[str] = []

    index = 0

    length = len(name)

    while index < length:
        char = name[index]

        if char in {'"', "`"}:
            quote = char

            index += 1

            buffer: list[str] = []

            closed = False

            while index < length:
                current = name[index]

                if current == quote:
                    if quote == '"' and index + 1 < length and name[index + 1] == '"':
                        buffer.append('"')

                        index += 2

                        continue

                    index += 1

                    closed = True

                    break

                buffer.append(current)

                index += 1

            if not closed:
                raise PySparkValueError("unterminated quoted identifier")

            inner = "".join(buffer)

            if not inner:
                raise PySparkValueError("empty quoted identifier")

            _reject_path_escape_segment(inner)

            segments.append(inner)

        else:
            start = index

            while index < length and name[index] != ".":
                index += 1

            segment = name[start:index]

            if not segment:
                raise PySparkValueError("empty identifier segment")

            if not _is_plain_ident(segment):
                raise PySparkValueError(f"invalid unquoted segment {segment[:64]!r}")

            _reject_path_escape_segment(segment)

            segments.append(segment)

        if index < length:
            if name[index] != ".":
                raise PySparkValueError("expected '.' between identifier segments")

            index += 1

            if index >= length:
                raise PySparkValueError("trailing '.'")

    if not segments:
        raise PySparkValueError("empty identifier")

    return segments
