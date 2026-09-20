"""Spark ``CACHE`` / ``UNCACHE`` / ``REFRESH`` statements on the ``spark.sql`` door.

``ReparkSession.sql`` hands the recognised shapes here before the engine sees them, beside
:func:`sql_set_statements.try_sql_set_statement`: ``CACHE [LAZY] TABLE name``,
``UNCACHE TABLE [IF EXISTS] name``, ``REFRESH [TABLE] name`` (the ``TABLE`` keyword is
optional, probe P-6), and ``REFRESH 'path'``. Every shape routes to the existing
:mod:`repark.spark.catalog_surface` implementation, so ``spark.catalog.isCached`` agrees
with SQL ``CACHE TABLE`` and no second cache exists (H-05).

``CACHE TABLE ... AS SELECT`` and ``OPTIONS`` are not intercepted: they fall through to the
engine's loud refusal (the AS SELECT form stays ``NotImplemented``, probe P-6). ``LAZY`` is
accepted and cached immediately. ``REFRESH ... PARTITION`` is not intercepted. Keywords are
case-insensitive; surrounding whitespace and one trailing ``;`` are tolerated.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from repark.errors import AnalysisException
from repark.spark.session.create_dataframe_rows import _materialize_arrow_as_memtable_frame
from repark.spark.session.sql_relations import (
    _scan_sql_table_identifier_end,
    _split_leading_sql_trivia,
    _sql_mask_strings_and_comments,
)

if TYPE_CHECKING:
    from repark.spark.dataframe import DataFrame
    from repark.spark.session.session_core import ReparkSession

_CACHE_TABLE_PREFIX_RE = re.compile(r"(?is)^\s*(CACHE\s+(?:LAZY\s+)?TABLE\s+)")

_UNCACHE_TABLE_PREFIX_RE = re.compile(r"(?is)^\s*(UNCACHE\s+TABLE\s+(?:IF\s+EXISTS\s+)?)")

_REFRESH_PREFIX_RE = re.compile(r"(?is)^\s*(REFRESH\s+(?:TABLE\s+)?)")

_NOT_FOUND_CODE = "TABLE_OR_VIEW_NOT_FOUND"


def try_sql_cache_statement(session: ReparkSession, query: str) -> DataFrame | None:
    """Answer a recognised ``CACHE`` / ``UNCACHE`` / ``REFRESH`` statement.

    Returns ``None`` when the query is not one of the three heads, when it carries
    more than one statement, or when the tail past the table name is not empty (the
    engine then refuses loud).
    """
    from repark.spark import catalog_surface

    _, body = _split_leading_sql_trivia(query)
    if not body:
        return None
    head = body.split(None, 1)[0].rstrip(";").upper() if body.split(None, 1) else ""
    if head not in ("CACHE", "UNCACHE", "REFRESH"):
        return None
    text = body.strip()
    if text.endswith(";"):
        text = text[:-1].rstrip()
    if ";" in _sql_mask_strings_and_comments(text):
        return None
    if head == "CACHE":
        return _try_cache_table(session, catalog_surface, text)
    if head == "UNCACHE":
        return _try_uncache_table(session, catalog_surface, text)
    return _try_refresh(session, catalog_surface, text)


def _scan_name(text: str, start: int) -> tuple[str, str] | None:
    """Split the multipart identifier at ``start`` from its tail, or ``None``."""
    cursor = start
    while cursor < len(text) and text[cursor].isspace():
        cursor += 1
    end = _scan_sql_table_identifier_end(text, cursor)
    if end is None or end == cursor:
        return None
    return text[cursor:end], text[end:]


def _empty_frame(session: ReparkSession) -> DataFrame:
    """Answer a cache statement: zero columns, zero rows."""
    from repark.spark._pyarrow import require_pyarrow

    pa = require_pyarrow()
    return _materialize_arrow_as_memtable_frame(session, pa.table({}))


def _try_cache_table(session: ReparkSession, catalog_surface: Any, text: str) -> DataFrame | None:
    """Route ``CACHE [LAZY] TABLE name`` to the catalog cache."""
    match = _CACHE_TABLE_PREFIX_RE.match(text)
    if match is None:
        return None
    scanned = _scan_name(text, match.end())
    if scanned is None:
        return None
    name, tail = scanned
    if tail.strip():
        return None
    catalog_surface.cache_table(session.catalog, name)
    return _empty_frame(session)


def _try_uncache_table(session: ReparkSession, catalog_surface: Any, text: str) -> DataFrame | None:
    """Route ``UNCACHE TABLE [IF EXISTS] name`` to the catalog cache."""
    match = _UNCACHE_TABLE_PREFIX_RE.match(text)
    if match is None:
        return None
    scanned = _scan_name(text, match.end())
    if scanned is None:
        return None
    name, tail = scanned
    if tail.strip():
        return None
    tolerant = "IF EXISTS" in match.group(1).upper()
    try:
        catalog_surface.uncache_table(session.catalog, name)
    except AnalysisException as error:
        if not tolerant or _NOT_FOUND_CODE not in str(error):
            raise
    return _empty_frame(session)


def _try_refresh(session: ReparkSession, catalog_surface: Any, text: str) -> DataFrame | None:
    """Route ``REFRESH [TABLE] name`` and ``REFRESH 'path'`` to the catalog."""
    match = _REFRESH_PREFIX_RE.match(text)
    if match is None:
        return None
    cursor = match.end()
    while cursor < len(text) and text[cursor].isspace():
        cursor += 1
    if cursor < len(text) and text[cursor] == "'":
        path, tail = _scan_quoted_path(text, cursor)
        if path is None or (tail is not None and tail.strip()):
            return None
        catalog_surface.refresh_by_path(session.catalog, path)
        return _empty_frame(session)
    scanned = _scan_name(text, match.end())
    if scanned is None:
        return None
    name, tail = scanned
    if tail.strip():
        return None
    catalog_surface.refresh_table(session.catalog, name)
    return _empty_frame(session)


def _scan_quoted_path(text: str, start: int) -> tuple[str | None, str | None]:
    """Split a single-quoted path at ``start`` from its tail (doubled quotes unescape)."""
    cursor = start + 1
    chars: list[str] = []
    while cursor < len(text):
        char = text[cursor]
        if char == "'":
            if cursor + 1 < len(text) and text[cursor + 1] == "'":
                chars.append("'")
                cursor += 2
                continue
            return "".join(chars), text[cursor + 1 :]
        chars.append(char)
        cursor += 1
    return None, None
