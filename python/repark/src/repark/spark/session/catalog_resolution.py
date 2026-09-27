"""Catalog names and table-name resolution."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from repark import _native
from repark.spark._idents import quote_ident_if_needed as _quote_ident_if_needed

from repark.errors import AnalysisException


if TYPE_CHECKING:
    from repark.spark.session.session_configuration import _DISPLAY_STYLE_KEY
    from repark.spark.session.sql_relations import _parse_table_identifier_segments


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


def _join_table_identifier_segments(segments: list[str]) -> str:
    """Rejoin identifier segments so dotted / special segments stay one part (E2 / C2-SEC-001).



    Plain ``[A-Za-z_][A-Za-z0-9_]*`` segments stay unquoted (stable string form for tests and

    native three-part probes). Any other segment is double-quoted so a later

    :func:`_sql_table_ref` / :func:`_parse_table_identifier_segments` pass cannot re-split

    embedded dots into extra multipart identity pieces (silent wrong-object).

    """

    # Quote-if-needed SSOT: plain bare unquoted; else always-quote.

    return ".".join(_quote_ident_if_needed(segment) for segment in segments)


def _temp_view_home_ref(inner: Any, name: str) -> list[str] | None:
    """The temp view's home segments, or ``None`` when it is not a temp view."""
    try:
        return inner.resolve_temp_view_home_ref(name)
    except Exception:
        return None


def _refused_spelling(inner: Any, name: str) -> str | None:
    """Registered spelling of refused catalog ``name`` (case-insensitive), else ``None``."""
    names = _native.session_refused_catalog_names(inner)
    lowered = name.lower()
    for candidate in names:
        if candidate.lower() == lowered:
            return candidate
    return None


def resolve_table_name(
    name: str,
    *,
    current_catalog: str,
    current_database: str,
    prefer_temp_view: bool = False,
    temp_view_home_ref: Any | None = None,
    refused_spelling: Callable[[str], str | None] | None = None,
) -> str:
    """Qualify a bare / two-part table identifier under the session default catalog + NS (E2).



    Shared name-resolution layer for free-SQL entry points and the DataFrame API

    (``table`` / ``saveAsTable`` / ``writeTo`` / ``insertInto`` / MERGE /

    :meth:`ReparkSession.read_iceberg_table`). Returns a multipart identifier string that

    preserves segment boundaries (quote-aware rejoin — C2-SEC-001); callers pass it through

    :func:`_sql_table_ref` for full quoting when embedding in SQL.



    * **one-part** ``t`` → the temp view's HOME-qualified name (when ``prefer_temp_view`` and

      ``temp_view_home_ref`` answers segments), else ``currentCatalog.currentDatabase.t``

    * **two-part** ``ns.t`` → ``currentCatalog.ns.t`` — unless ``ns`` names a refused
      catalog (case-insensitive), which passes through in its registered spelling so
      the engine refusal raises

    * **three-part** ``cat.ns.t`` → as-is (``spark_catalog`` names the session catalog only)

    """

    stripped = name.strip()

    try:
        segments = _parse_table_identifier_segments(stripped)

    except ValueError as error:
        # Match `_sql_table_ref` surface: invalid / SQL-fragment identifiers raise
        # AnalysisException (writer / table injection gate).

        from repark.errors import AnalysisException

        raise AnalysisException(
            f"invalid table identifier {stripped[:128]!r}: {error} "
            "(expected multipart name like catalog.db.table; SQL fragments are not allowed)"
        ) from error

    if len(segments) == 1:
        bare = segments[0]

        if prefer_temp_view and temp_view_home_ref is not None:
            try:
                home = temp_view_home_ref(bare)

            except Exception:
                # Soften probe failures: fall through to catalog qualification.

                home = None

            if home:
                # Emit the HOME-qualified spelling, never the bare name: a bare reference is
                # re-resolved by the engine against the LIVE
                # `datafusion.catalog.default_catalog`, so under a `SET` to another catalog
                # every product read path missed a view `tableExists` reported present.

                return _join_table_identifier_segments(list(home))

        return _join_table_identifier_segments([current_catalog, current_database, bare])

    if len(segments) == 2:
        if refused_spelling is not None:
            canonical = refused_spelling(segments[0])
            if canonical is not None:
                return _join_table_identifier_segments([canonical, segments[1]])
        return _join_table_identifier_segments([current_catalog, segments[0], segments[1]])

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
