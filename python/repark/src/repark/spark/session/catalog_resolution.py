"""Catalog names and table-name resolution."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.spark._idents import quote_ident_if_needed as _quote_ident_if_needed

from repark.spark.catalog import DEFAULT_CATALOG_NAME

from repark.errors import AnalysisException


if TYPE_CHECKING:
    from repark.spark.session.session_configuration import _DISPLAY_STYLE_KEY
    from repark.spark.session.sql_relations import _parse_table_identifier_segments


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


_AUTO_MEMORY_CATALOG_KEY = "repark.sql.automemorycatalog"


def _auto_memory_catalog_wanted(builder_config: dict[str, str | None]) -> bool:
    """Whether a bare session should auto-register ``spark_catalog``.

    True only when ALL hold: the knob (``repark.sql.autoMemoryCatalog``) is not ``false``;
    no ``spark.sql.catalog.*`` / ``repark.sql.catalog.*`` blocks are configured (a user who
    configured catalogs gets exactly those); and ``spark.sql.defaultCatalog`` is unset or
    already ``spark_catalog`` (auto-seeding a catalog the user did not name would mask a
    misconfiguration).
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
    """``spark.sql.defaultNamespace`` from the builder map (case-insensitive), if set."""

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
    """Resolve ``spark_catalog`` as an alias of the session's registered catalog.

    When ``spark_catalog`` is not itself registered — or is only the auto-registered
    fallback, which never blocks user-intent resolution — map it to ``current_catalog``
    if that name is known, else the sole known catalog when exactly one is registered.
    Fully-qualified three-part names and real catalog names pass through unchanged.
    After a user registration flips current, tables written to the auto catalog before
    the flip are reachable only by bare-name resolution (documented edge).
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
    """Rejoin identifier segments so dotted / special segments stay one part.

    Plain ``[A-Za-z_][A-Za-z0-9_]*`` segments stay unquoted; any other segment is
    double-quoted so a later :func:`_sql_table_ref` /
    :func:`_parse_table_identifier_segments` pass cannot re-split embedded dots into
    extra identity pieces (silent wrong-object).
    """

    # Quote-if-needed SSOT: plain bare unquoted; else always-quote.

    return ".".join(_quote_ident_if_needed(segment) for segment in segments)


def resolve_table_name(
    name: str,
    *,
    current_catalog: str,
    current_database: str,
    known_catalogs: set[str] | None = None,
    prefer_temp_view: bool = False,
    temp_view_home_ref: Any | None = None,
    default_catalog_is_auto: bool = False,
) -> str:
    """Qualify a bare / two-part table identifier under the session default catalog + NS.

    Shared name-resolution layer for free-SQL entry points and the DataFrame API
    (``table`` / ``saveAsTable`` / ``writeTo`` / ``insertInto`` / MERGE). Returns a
    multipart identifier string that preserves segment boundaries (quote-aware rejoin);
    callers pass it through :func:`_sql_table_ref` for full quoting when embedding in SQL.

    * one-part ``t`` → the temp view's HOME-qualified name (when ``prefer_temp_view`` and
      ``temp_view_home_ref`` answers segments), else ``currentCatalog.currentDatabase.t``
    * two-part ``ns.t`` → ``currentCatalog.ns.t``
    * three-part ``cat.ns.t`` → as-is, with ``spark_catalog`` alias expansion
    """

    known = known_catalogs if known_catalogs is not None else set()

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
    repeated pure-style reuse stays silent after the style is applied.
    """

    for key in list(builder_config):
        if key.lower() == _DISPLAY_STYLE_KEY:
            del builder_config[key]

    builder_config[_DISPLAY_STYLE_KEY] = style
