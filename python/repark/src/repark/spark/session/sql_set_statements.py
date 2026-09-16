"""Spark ``SET`` / ``RESET`` statements on the ``spark.sql`` door — SQL-SET-DOOR-1 (B-TZ-5).

``ReparkSession.sql`` hands the recognised D-1 shapes here before the engine sees them:
``SET``, ``SET -v``, ``SET <key>``, ``SET <key> = <value>`` (the value is the raw text
after ``=``, trimmed, quotes kept — Spark refuses ``'Asia/Tokyo'`` with its quotes,
cell BTZ5-4), backtick-quoted keys, ``RESET``, ``RESET <key>``, ``SET TIME ZONE``
with a single-quoted, double-quoted, or ``INTERVAL '+HH:MM' HOUR TO MINUTE`` zone,
and ``SET TIME ZONE LOCAL``. Keywords are case-insensitive; surrounding whitespace
and one trailing ``;`` are tolerated. A leading-trivia scan returns ``None`` before
any whole-query copy when the first keyword is not ``SET``/``RESET``. ``SET ROLE``
is not intercepted. ``SET key TO value`` raises ``ParseException``
``[INVALID_SET_SYNTAX]``. Offset zones follow Java ``ZoneId.of`` (cells S5-tz-*).

Every read/write goes through the session's ``RuntimeConfig``. ``spark.sql.ansi.enabled``
and ``spark.sql.session.timeZone`` validate in Rust and apply to the live session
(registry SET-ANSI-RUNTIME-1 FIXED); anything they refuse raises before anything is stored.
Residue: SET-TZ-LOCAL-1. ``RESET <key>`` restores a builder-seeded
value when one exists. Redaction on ``SET k`` / ``SET`` / ``SET -v`` uses Spark's
default ``spark.redaction.regex`` against the key or the value; ``SET k = v``
echoes the raw value. Invalid-conf messages carry ``SQLSTATE: 22022``;
``CANNOT_MODIFY_STATIC_CONFIG`` carries ``SQLSTATE: 46110``.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    UnsupportedOperationException,
)
from repark.spark.session.create_dataframe_rows import _materialize_arrow_as_memtable_frame
from repark.spark.session.session_configuration import (
    SPARK_SQL_ANSI_ENABLED_KEY,
    _SQLCONF_STATIC_KEYS,
    _looks_like_datafusion_conf_key,
)
from repark.spark.session.session_time_zone import SESSION_TIME_ZONE_KEY
from repark.spark.session.sql_relations import (
    _split_leading_sql_trivia,
    _sql_mask_strings_and_comments,
)
from repark.spark.types import refuse_collation_session_key

if TYPE_CHECKING:
    from repark.spark.dataframe import DataFrame
    from repark.spark.session.session_core import ReparkSession

_UNDEFINED_CONF_VALUE = "<undefined>"

_REDACTED_VALUE = "*********(redacted)"

_REDACTION_RE = re.compile(r"(?i)secret|password|token|access[.]key")

_INT_VALUE_RE = re.compile(r"[+-]?\d+")

_SQLSTATE_INVALID_CONF = " SQLSTATE: 22022"

_SQLSTATE_STATIC_CONFIG = " SQLSTATE: 46110"

_KEY_TOKEN = r"(?:`(?P<quoted_key>[^`]+)`|(?P<plain_key>[A-Za-z0-9_.][A-Za-z0-9_.\-]*))"

_SET_TIME_ZONE_INTERVAL_RE = re.compile(
    r"SET\s+TIME\s+ZONE\s+INTERVAL\s+'(?P<zone>[^']*)'\s+HOUR\s+TO\s+MINUTE\Z",
    re.IGNORECASE,
)
_SET_TIME_ZONE_RE = re.compile(
    r"SET\s+TIME\s+ZONE\s+(?P<zone>'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|LOCAL)\Z",
    re.IGNORECASE,
)
_SET_VERBOSE_RE = re.compile(r"SET\s+-v\Z", re.IGNORECASE)
_SET_ASSIGN_RE = re.compile(
    rf"SET\s+{_KEY_TOKEN}\s*=\s*(?P<value>.*)\Z",
    re.IGNORECASE | re.DOTALL,
)
_SET_READ_RE = re.compile(rf"SET\s+{_KEY_TOKEN}\Z", re.IGNORECASE)
_SET_BARE_RE = re.compile(r"SET\Z", re.IGNORECASE)
_SET_TO_RE = re.compile(rf"SET\s+{_KEY_TOKEN}\s+TO\b", re.IGNORECASE)
_RESET_KEY_RE = re.compile(rf"RESET\s+{_KEY_TOKEN}\Z", re.IGNORECASE)
_RESET_BARE_RE = re.compile(r"RESET\Z", re.IGNORECASE)

_TYPED_VALUE_KINDS: dict[str, str] = {
    "spark.sql.shuffle.partitions": "int",
}

_WAP_SESSION_KEY_PREFIX = "spark.wap."

_INVALID_SET_SYNTAX = (
    "[INVALID_SET_SYNTAX] Expected format is 'SET', 'SET key', or 'SET key=value'. "
    "If you want to include special characters in key, or include keyword in "
    "unquoted values, please use quotes."
)


def try_sql_set_statement(session: ReparkSession, query: str) -> DataFrame | None:
    """Answer a recognised ``SET``/``RESET`` statement, or ``None`` to defer to the engine."""
    _, body = _split_leading_sql_trivia(query)
    if not body:
        return None
    token_end = 0
    length = len(body)
    while token_end < length and not body[token_end].isspace() and body[token_end] != ";":
        token_end += 1
    if body[:token_end].upper() not in ("SET", "RESET"):
        return None
    text, balanced = _strip_sql_comments(body)
    if not balanced:
        return None
    text = text.strip()
    if text.endswith(";"):
        text = text[:-1].rstrip()
    if ";" in text and ";" in _sql_mask_strings_and_comments(text):
        return None
    interval_match = _SET_TIME_ZONE_INTERVAL_RE.fullmatch(text)
    if interval_match is not None:
        return _apply_set_time_zone(session, interval_match.group("zone"), quoted=False)
    time_zone_match = _SET_TIME_ZONE_RE.fullmatch(text)
    if time_zone_match is not None:
        return _apply_set_time_zone(session, time_zone_match.group("zone"), quoted=True)
    if _SET_VERBOSE_RE.fullmatch(text):
        return _listing_frame(session, verbose=True)
    assign_match = _SET_ASSIGN_RE.fullmatch(text)
    if assign_match is not None:
        key = _matched_key(assign_match)
        if _looks_like_datafusion_conf_key(key) or _is_wap_session_key(key):
            return None
        return _apply_set(session, key, assign_match.group("value").strip())
    read_match = _SET_READ_RE.fullmatch(text)
    if read_match is not None:
        key = _matched_key(read_match)
        if key.upper() == "ROLE":
            return None
        if _looks_like_datafusion_conf_key(key):
            return None
        return _read_frame(session, key)
    if _SET_BARE_RE.fullmatch(text):
        return _listing_frame(session, verbose=False)
    reset_key_match = _RESET_KEY_RE.fullmatch(text)
    if reset_key_match is not None:
        key = _matched_key(reset_key_match)
        if _looks_like_datafusion_conf_key(key) or _is_wap_session_key(key):
            return None
        if key.upper() == "ALL":
            return _reset_all(session)
        _restore_or_unset(session, key)
        return _empty_frame(session)
    if _RESET_BARE_RE.fullmatch(text):
        return _reset_all(session)
    if _SET_TO_RE.search(text):
        raise ParseException(_INVALID_SET_SYNTAX)
    return None


def _matched_key(match: re.Match[str]) -> str:
    """Return the unquoted conf key from a SET/RESET match."""
    quoted = match.group("quoted_key")
    if quoted is not None:
        return quoted
    return match.group("plain_key") or ""


def _is_wap_session_key(key: str) -> bool:
    """Whether ``key`` is a ``spark.wap.*`` conf the engine must keep refusing."""
    return key.lower().startswith(_WAP_SESSION_KEY_PREFIX)


def _strip_sql_comments(text: str) -> tuple[str, bool]:
    """Drop ``--`` / ``/* … */`` comments outside string literals; report quote balance."""
    out: list[str] = []
    index = 0
    quote: str | None = None
    while index < len(text):
        char = text[index]
        if quote is not None:
            out.append(char)
            if char == quote:
                if index + 1 < len(text) and text[index + 1] == quote:
                    out.append(quote)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in "'\"`":
            quote = char
            out.append(char)
            index += 1
            continue
        if char == "-" and index + 1 < len(text) and text[index + 1] == "-":
            end = text.find("\n", index)
            if end < 0:
                break
            index = end
            continue
        if char == "/" and index + 1 < len(text) and text[index + 1] == "*":
            end = text.find("*/", index + 2)
            if end < 0:
                break
            index = end + 2
            continue
        out.append(char)
        index += 1
    return "".join(out), quote is None


def _apply_set(session: ReparkSession, key: str, value: str) -> DataFrame:
    """Apply ``SET key = value`` through ``conf.set``; answer the effective conf value."""
    if key in _SQLCONF_STATIC_KEYS:
        raise AnalysisException(
            f"[CANNOT_MODIFY_STATIC_CONFIG] Cannot modify the value of the static Spark "
            f'config: "{key}".{_SQLSTATE_STATIC_CONFIG}'
        )
    _refuse_unless_typed(key, value)
    session.conf.set(key, value)
    return _pair_frame(session, [(key, session.conf.get(key))])


def _apply_set_time_zone(session: ReparkSession, literal: str, *, quoted: bool) -> DataFrame:
    """Apply ``SET TIME ZONE`` as a ``spark.sql.session.timeZone`` set."""
    if literal.upper() == "LOCAL":
        raise UnsupportedOperationException(
            '"SET TIME ZONE LOCAL" is a DECLARED refusal (registry SET-TZ-LOCAL-1, '
            "2026-09-15): repark never reads the host machine's local timezone; give "
            "the zone explicitly with SET TIME ZONE '<iana-id>' or "
            'ReparkSession.builder.config("spark.sql.session.timeZone", "<iana-id>").'
        )
    zone = _unquote_zone_literal(literal) if quoted else literal
    session.conf.set(SESSION_TIME_ZONE_KEY, zone)
    effective = session.conf.get(SESSION_TIME_ZONE_KEY)
    return _pair_frame(session, [(SESSION_TIME_ZONE_KEY, effective)])


def _unquote_zone_literal(literal: str) -> str:
    """Strip matching quotes from a SET TIME ZONE literal; doubled quotes become one."""
    if len(literal) >= 2 and literal[0] == "'" and literal[-1] == "'":
        return literal[1:-1].replace("''", "'")
    if len(literal) >= 2 and literal[0] == '"' and literal[-1] == '"':
        return literal[1:-1].replace('""', '"')
    return literal


def _read_frame(session: ReparkSession, key: str) -> DataFrame:
    """Answer ``SET <key>`` — the effective conf value, or ``<undefined>`` when unset."""
    value = session.conf.get(key, _UNDEFINED_CONF_VALUE)
    return _pair_frame(session, [(key, _redact_conf_value(key, value))])


def _listing_frame(session: ReparkSession, *, verbose: bool) -> DataFrame:
    """Answer ``SET`` / ``SET -v`` — one row per runtime-set key, sorted by key."""
    rows = sorted(
        (key, _redact_conf_value(key, value)) for key, value in session.conf._store().items()
    )
    if verbose:
        return _verbose_frame(session, rows)
    return _pair_frame(session, rows)


def _redact_conf_value(key: str, value: str) -> str:
    """Redact when Spark's default ``spark.redaction.regex`` matches the key or value."""
    if _REDACTION_RE.search(key) is not None or _REDACTION_RE.search(value) is not None:
        return _REDACTED_VALUE
    return value


def _reset_all(session: ReparkSession) -> DataFrame:
    """Apply ``RESET`` — restore builder values or unset every runtime-set key."""
    conf = session.conf
    for key in list(conf._store()):
        _restore_or_unset(session, key)
    return _empty_frame(session)


def _restore_or_unset(session: ReparkSession, key: str) -> None:
    """RESET one key: restore a builder-seeded value, otherwise ``conf.unset``."""
    refuse_collation_session_key(key)
    builder_value = session._builder_config.get(key)
    if builder_value is not None:
        if key in (SESSION_TIME_ZONE_KEY, SPARK_SQL_ANSI_ENABLED_KEY):
            from repark import _native

            _native.restore_runtime_config(session._ensure_alive(), key, builder_value)
            session.conf._unset_keys().discard(key)
            session.conf._store()[key] = builder_value
            return
        session.conf.set(key, builder_value)
        return
    session.conf.unset(key)


def _refuse_unless_typed(key: str, value: str) -> None:
    """Raise Spark typed-conf errors for the keys this door validates."""
    expected = _TYPED_VALUE_KINDS.get(key)
    if expected == "int":
        if _INT_VALUE_RE.fullmatch(value) is None:
            raise IllegalArgumentException(_type_mismatch_error(key, value, expected))
        if int(value) <= 0:
            raise IllegalArgumentException(_requirement_error(key, value))


def _type_mismatch_error(key: str, value: str, expected: str) -> str:
    """Spark's ``INVALID_CONF_VALUE.TYPE_MISMATCH`` message for one refused value."""
    return (
        f"[INVALID_CONF_VALUE.TYPE_MISMATCH] The value '{value}' in the config "
        f"\"{key}\" is invalid. It should be a/an '{expected}' value."
        f"{_SQLSTATE_INVALID_CONF}"
    )


def _requirement_error(key: str, value: str) -> str:
    """Spark's ``INVALID_CONF_VALUE.REQUIREMENT`` message for a non-positive int."""
    return (
        f"[INVALID_CONF_VALUE.REQUIREMENT] The value '{value}' in the config "
        f'"{key}" is invalid. The value of {key} must be positive'
        f"{_SQLSTATE_INVALID_CONF}"
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
