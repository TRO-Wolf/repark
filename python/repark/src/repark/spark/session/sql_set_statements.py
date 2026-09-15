"""Spark ``SET`` / ``RESET`` statements on the ``spark.sql`` door — SQL-SET-DOOR-1 (B-TZ-5).

``ReparkSession.sql`` hands the recognised D-1 shapes here before the engine sees them:
``SET``, ``SET -v``, ``SET <key>``, ``SET <key> = <value>`` (the value is the raw text
after ``=``, trimmed, quotes kept — Spark refuses ``'Asia/Tokyo'`` with its quotes,
cell BTZ5-4), ``RESET``, ``RESET <key>``, ``SET TIME ZONE '<zone>'`` and
``SET TIME ZONE LOCAL``. Keywords are case-insensitive; surrounding whitespace and one
trailing ``;`` are tolerated. Anything else returns ``None`` so the statement reaches
the engine unchanged — including every ``datafusion.*`` key and every ``spark.wap.*``
assignment/unset. The ``datafusion.*`` exclusion is load-bearing: ``RuntimeConfig.set``
forwards those keys through this same SQL entry point, so intercepting them would loop.
The ``spark.wap.*`` exclusion is fail-closed: repark does not implement WAP, and the
engine's refusal (not a silent conf store) is the pinned answer (REF-3). ``RESET`` of a
collation key refuses through ``refuse_collation_session_key``, mirroring the engine's
G15 valve that this interception would otherwise bypass.

Every read/write goes through the session's ``RuntimeConfig`` (``conf.set``/``get``/
``unset``), so a SQL ``SET`` has exactly the effect ``spark.conf.set`` has today — the
D-4 contract. Two recorded residues ride that contract unchanged: the runtime timezone
key is accepted but neither stored nor applied (registry TZ-3 — the result row echoes
the zone the live session actually has, never a state ``conf.get`` would contradict),
and ``spark.sql.ansi.enabled`` is stored but not applied (registry SET-ANSI-RUNTIME-1).
Errors the conf layer cannot raise are produced here with Spark's classes and message
shapes (minus the recorded no-SQLSTATE delta): ``CANNOT_MODIFY_STATIC_CONFIG`` as
``AnalysisException``, ``INVALID_CONF_VALUE.TIME_ZONE`` and ``INVALID_CONF_VALUE.TYPE_MISMATCH``
as ``IllegalArgumentException``, and ``SET TIME ZONE LOCAL`` as a dated
``UnsupportedOperationException`` refusal (SET-TZ-LOCAL-1 — repark never reads the host
zone).

Result frames are ``pyarrow`` tables materialised through the MemTable seam, which is
the construction that preserves the non-nullable ``key``/``value`` fields and the
zero-column ``RESET`` answer.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    UnsupportedOperationException,
)
from repark.spark._secrets import prop_key_is_secret as _prop_key_is_secret
from repark.spark.session.create_dataframe_rows import _materialize_arrow_as_memtable_frame
from repark.spark.session.session_configuration import (
    _SQLCONF_STATIC_KEYS,
    _looks_like_datafusion_conf_key,
)
from repark.spark.session.session_time_zone import SESSION_TIME_ZONE_KEY
from repark.spark.types import refuse_collation_session_key

if TYPE_CHECKING:
    from repark.spark.dataframe import DataFrame
    from repark.spark.session.session_core import ReparkSession

_UNDEFINED_CONF_VALUE = "<undefined>"

_SECRET_MASK = "***"

_FIXED_OFFSET_ZONE_RE = re.compile(r"[+-](?:\d{2}|\d{4}|\d{2}:\d{2})")

_INT_VALUE_RE = re.compile(r"[+-]?\d+")

_SET_TIME_ZONE_RE = re.compile(
    r"SET\s+TIME\s+ZONE\s+(?P<zone>'(?:[^']|'')*'|LOCAL)\Z", re.IGNORECASE
)
_SET_VERBOSE_RE = re.compile(r"SET\s+-v\Z", re.IGNORECASE)
_SET_ASSIGN_RE = re.compile(
    r"SET\s+(?P<key>[A-Za-z0-9_.][A-Za-z0-9_.\-]*)\s*=\s*(?P<value>.*)\Z",
    re.IGNORECASE | re.DOTALL,
)
_SET_READ_RE = re.compile(r"SET\s+(?P<key>[A-Za-z0-9_.][A-Za-z0-9_.\-]*)\Z", re.IGNORECASE)
_SET_BARE_RE = re.compile(r"SET\Z", re.IGNORECASE)
_RESET_KEY_RE = re.compile(r"RESET\s+(?P<key>[A-Za-z0-9_.][A-Za-z0-9_.\-]*)\Z", re.IGNORECASE)
_RESET_BARE_RE = re.compile(r"RESET\Z", re.IGNORECASE)

_TYPED_VALUE_KINDS: dict[str, str] = {
    "spark.sql.shuffle.partitions": "int",
    "spark.sql.ansi.enabled": "boolean",
}

_BOOLEAN_VALUE_TEXTS: frozenset[str] = frozenset({"true", "false"})

_WAP_SESSION_KEY_PREFIX = "spark.wap."


def try_sql_set_statement(session: ReparkSession, query: str) -> DataFrame | None:
    """Answer a recognised ``SET``/``RESET`` statement, or ``None`` to defer to the engine."""
    text = query.strip()
    if text.endswith(";"):
        text = text[:-1].rstrip()
    words = text.split(None, 1)
    if not words or words[0].upper() not in ("SET", "RESET"):
        return None
    time_zone_match = _SET_TIME_ZONE_RE.fullmatch(text)
    if time_zone_match is not None:
        return _apply_set_time_zone(session, time_zone_match.group("zone"))
    if _SET_VERBOSE_RE.fullmatch(text):
        return _listing_frame(session, verbose=True)
    assign_match = _SET_ASSIGN_RE.fullmatch(text)
    if assign_match is not None:
        key = assign_match.group("key")
        if _looks_like_datafusion_conf_key(key) or _is_wap_session_key(key):
            return None
        return _apply_set(session, key, assign_match.group("value").strip())
    read_match = _SET_READ_RE.fullmatch(text)
    if read_match is not None:
        key = read_match.group("key")
        if _looks_like_datafusion_conf_key(key):
            return None
        return _read_frame(session, key)
    if _SET_BARE_RE.fullmatch(text):
        return _listing_frame(session, verbose=False)
    reset_key_match = _RESET_KEY_RE.fullmatch(text)
    if reset_key_match is not None:
        key = reset_key_match.group("key")
        if _looks_like_datafusion_conf_key(key) or _is_wap_session_key(key):
            return None
        refuse_collation_session_key(key)
        session.conf.unset(key)
        return _empty_frame(session)
    if _RESET_BARE_RE.fullmatch(text):
        return _reset_all(session)
    return None


def _is_wap_session_key(key: str) -> bool:
    """Whether ``key`` is a ``spark.wap.*`` conf the engine must keep refusing."""
    return key.lower().startswith(_WAP_SESSION_KEY_PREFIX)


def _apply_set(session: ReparkSession, key: str, value: str) -> DataFrame:
    """Apply ``SET key = value`` through ``conf.set``; answer the effective conf value."""
    if key in _SQLCONF_STATIC_KEYS:
        raise AnalysisException(
            f"[CANNOT_MODIFY_STATIC_CONFIG] Cannot modify the value of the static Spark "
            f'config: "{key}".'
        )
    _refuse_unless_resolvable_zone(key, value)
    _refuse_unless_typed(key, value)
    session.conf.set(key, value)
    return _pair_frame(session, [(key, session.conf.get(key))])


def _apply_set_time_zone(session: ReparkSession, literal: str) -> DataFrame:
    """Apply ``SET TIME ZONE '<zone>'`` as a ``spark.sql.session.timeZone`` set."""
    if literal.upper() == "LOCAL":
        raise UnsupportedOperationException(
            '"SET TIME ZONE LOCAL" is a DECLARED refusal (registry SET-TZ-LOCAL-1, '
            "2026-09-15): repark never reads the host machine's local timezone; give "
            "the zone explicitly with SET TIME ZONE '<iana-id>' or "
            'ReparkSession.builder.config("spark.sql.session.timeZone", "<iana-id>").'
        )
    zone = literal[1:-1].replace("''", "'")
    if not _resolves_as_session_zone(zone):
        raise IllegalArgumentException(_time_zone_error(zone))
    session.conf.set(SESSION_TIME_ZONE_KEY, zone)
    effective = session.conf.get(SESSION_TIME_ZONE_KEY)
    return _pair_frame(session, [(SESSION_TIME_ZONE_KEY, effective)])


def _read_frame(session: ReparkSession, key: str) -> DataFrame:
    """Answer ``SET <key>`` — the effective conf value, or ``<undefined>`` when unset."""
    try:
        value = session.conf.get(key)
    except Exception:
        value = _UNDEFINED_CONF_VALUE
    return _pair_frame(session, [(key, value)])


def _listing_frame(session: ReparkSession, *, verbose: bool) -> DataFrame:
    """Answer ``SET`` / ``SET -v`` — one row per runtime-set key, sorted by key."""
    rows = sorted(
        (key, _SECRET_MASK if _prop_key_is_secret(key) else value)
        for key, value in session.conf._store().items()
    )
    if verbose:
        return _verbose_frame(session, rows)
    return _pair_frame(session, rows)


def _reset_all(session: ReparkSession) -> DataFrame:
    """Apply ``RESET`` — unset every runtime-set key; answer the empty frame."""
    conf = session.conf
    for key in list(conf._store()):
        conf.unset(key)
    return _empty_frame(session)


def _refuse_unless_resolvable_zone(key: str, value: str) -> None:
    """Raise ``INVALID_CONF_VALUE.TIME_ZONE`` when the zone key carries a bad zone."""
    if key == SESSION_TIME_ZONE_KEY and not _resolves_as_session_zone(value):
        raise IllegalArgumentException(_time_zone_error(value))


def _refuse_unless_typed(key: str, value: str) -> None:
    """Raise ``INVALID_CONF_VALUE.TYPE_MISMATCH`` for the typed keys this door touches."""
    expected = _TYPED_VALUE_KINDS.get(key)
    if expected == "int" and _INT_VALUE_RE.fullmatch(value) is None:
        raise IllegalArgumentException(_type_mismatch_error(key, value, expected))
    if expected == "boolean" and value.lower() not in _BOOLEAN_VALUE_TEXTS:
        raise IllegalArgumentException(_type_mismatch_error(key, value, expected))


def _resolves_as_session_zone(value: str) -> bool:
    """Whether ``value`` resolves as a zone — IANA id or a ``±HH[:MM]`` fixed offset."""
    offset_match = _FIXED_OFFSET_ZONE_RE.fullmatch(value)
    if offset_match is not None:
        digits = value[1:].replace(":", "")
        seconds = int(digits[:2]) * 3600 + int(digits[2:]) * 60
        return seconds < 86400
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError):
        return False
    return True


def _time_zone_error(value: str) -> str:
    """Spark's ``INVALID_CONF_VALUE.TIME_ZONE`` message for one refused value."""
    return (
        f"[INVALID_CONF_VALUE.TIME_ZONE] The value '{value}' in the config "
        f'"{SESSION_TIME_ZONE_KEY}" is invalid. Cannot resolve the given timezone.'
    )


def _type_mismatch_error(key: str, value: str, expected: str) -> str:
    """Spark's ``INVALID_CONF_VALUE.TYPE_MISMATCH`` message for one refused value."""
    return (
        f"[INVALID_CONF_VALUE.TYPE_MISMATCH] The value '{value}' in the config "
        f"\"{key}\" is invalid. It should be a/an '{expected}' value."
    )


def _pair_frame(session: ReparkSession, rows: list[tuple[str, str]]) -> DataFrame:
    """Materialise a ``(key, value)`` result frame — two non-nullable string columns."""
    from repark.spark._pyarrow import require_pyarrow

    pa = require_pyarrow()
    schema = pa.schema(
        [
            pa.field("key", pa.string(), nullable=False),
            pa.field("value", pa.string(), nullable=False),
        ]
    )
    table = pa.Table.from_pydict(
        {"key": [key for key, _ in rows], "value": [value for _, value in rows]},
        schema=schema,
    )
    return _materialize_arrow_as_memtable_frame(session, table)


def _verbose_frame(session: ReparkSession, rows: list[tuple[str, str]]) -> DataFrame:
    """Materialise the ``SET -v`` frame — four non-nullable string columns."""
    from repark.spark._pyarrow import require_pyarrow

    pa = require_pyarrow()
    schema = pa.schema(
        [
            pa.field("key", pa.string(), nullable=False),
            pa.field("value", pa.string(), nullable=False),
            pa.field("meaning", pa.string(), nullable=False),
            pa.field("Since version", pa.string(), nullable=False),
        ]
    )
    table = pa.Table.from_pydict(
        {
            "key": [key for key, _ in rows],
            "value": [value for _, value in rows],
            "meaning": [""] * len(rows),
            "Since version": [""] * len(rows),
        },
        schema=schema,
    )
    return _materialize_arrow_as_memtable_frame(session, table)


def _empty_frame(session: ReparkSession) -> DataFrame:
    """Materialise the ``RESET`` answer — zero columns, zero rows."""
    from repark.spark._pyarrow import require_pyarrow

    pa = require_pyarrow()
    return _materialize_arrow_as_memtable_frame(session, pa.Table.from_pydict({}))
