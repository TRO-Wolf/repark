"""The :class:`Catalog` facade — ``spark.catalog``, PySpark's metadata surface.

**R-CURCAT-FACADE (2026-07-29):** current-catalog concept is **facade-only** — state lives on the
Python session (dies with ``stop()``), built over existing primitives:

* ``SHOW NAMESPACES IN <catalog>`` → :meth:`listDatabases` / :meth:`databaseExists` /
  :meth:`setCurrentDatabase` validation
* ``DESCRIBE NAMESPACE <catalog>.<db>`` → :meth:`getDatabase` (real ``locationUri`` /
  ``description``; :meth:`listDatabases` still leaves those ``None`` — FA-2)
* Live Iceberg ``list_iceberg_table_names`` + session ``list_temp_view_names`` (default-schema
  directory) → :meth:`listTables` — **not** global ``information_schema.tables`` (that walk
  loads every provider table and hard-fails after OOB drop of a DF-known Iceberg name;
  T6 F-T6-PHANTOM-A). ``SHOW TABLES IN`` refuses — registry row ST-1
  (``docs/spark-sql-iceberg-parity.md`` §2.4)
* :meth:`tableExists` three-part / bare temp-view (native) plus **two-part** and **one-part**
  resolution under the facade current catalog / database

Engine-side ``USE`` / bare ``SHOW NAMESPACES`` remain out of scope (router state is a later unit —
do not sniff/rewrite free SQL strings here); bare / nested forms are registry NS-1 / NS-2.

Return objects match live PySpark 4.1.2 field shapes (namedtuple-oid ``Database`` / ``Table`` /
``CatalogMetadata``). PySpark spells methods camelCase; each is defined snake_case with a
byte-identical camelCase alias so the one-line import swap just works.
"""

from __future__ import annotations

import logging
import re
from collections import namedtuple
from typing import TYPE_CHECKING, Any

from repark.errors import AnalysisException, PySparkTypeError

# === r23 QI1: idents ===
from repark.spark._idents import quote_ident_if_needed as _quote_ident
from repark.spark._idents import quote_multipart as _quote_multipart_ssot
from repark.spark._idents import sql_string_literal

if TYPE_CHECKING:
    from repark.spark.session import ReparkSession

logger = logging.getLogger(__name__)


def _require_str(value: Any, arg_name: str) -> str:
    """Refuse non-str arguments the way live PySpark does (TypeError-shaped)."""
    if not isinstance(value, str):
        raise PySparkTypeError(
            f"[CATALOG_ARG_TYPE] Argument `{arg_name}` must be a str, got {type(value).__name__}."
        )
    return value


# Live PySpark 4.1.2 field shapes (oracle 2026-07-29) — pin names AND order.
# namedtuple (not typing.NamedTuple) matches live pyspark.sql.catalog shapes byte-for-byte.
Database = namedtuple(
    "Database",
    ["name", "catalog", "description", "locationUri"],
)
Table = namedtuple(
    "Table",
    ["name", "catalog", "namespace", "description", "tableType", "isTemporary"],
)
CatalogMetadata = namedtuple(
    "CatalogMetadata",
    ["name", "description"],
)

# Default strings match live Spark when conf is untouched (oracle: currentCatalog /
# currentDatabase / spark.sql.defaultCatalog).
DEFAULT_CATALOG_NAME = "spark_catalog"
DEFAULT_DATABASE_NAME = "default"

# Iceberg metadata tables surface as ``t$snapshots`` etc. in the DF provider name directory; they
# are not Spark Catalog.listTables rows.
_METADATA_TABLE_DOLLAR = "$"


def _is_hidden_list_tables_name(table_name: str) -> bool:
    """Whether ``table_name`` must never appear in :meth:`Catalog.list_tables`.

    Hides Iceberg metadata-table suffixes (``$snapshots`` …) and engine-private registrations
    (CDF / mapInArrow / I1 time-travel static pins — octo C1-Q-002).
    """
    if _METADATA_TABLE_DOLLAR in table_name:
        return True
    return (
        table_name.startswith("__repark_cdf_")
        or table_name.startswith("__repark_mia_")
        or table_name.startswith("__repark_tt_")
    )


def _pattern_matches(name: str, pattern: str | None) -> bool:
    """Spark ``StringUtils.filterPattern`` subset for optional list* filters.

    Whole pattern trimmed once; ``|`` alternatives; ``*`` → ``.*``; case-insensitive full match;
    a syntactically invalid alternative is dropped (matches nothing for that alt).
    """
    if pattern is None:
        return True
    trimmed = pattern.strip()
    if not trimmed:
        return False
    for alternative in trimmed.split("|"):
        body = "".join(
            ".*" if character == "*" else re.escape(character) for character in alternative
        )
        try:
            # Python re: ``\A`` / ``\Z`` (Rust's ``regex`` crate uses ``\z`` — not portable here).
            regex = re.compile(rf"\A{body}\Z", re.IGNORECASE)
        except re.error:
            continue
        if regex.search(name) is not None:
            return True
    return False


def _multipart(parts: list[str]) -> str:
    return _quote_multipart_ssot(parts, always=False)


def _unquote_show_namespace(rendered: str) -> str:
    """Strip Spark-style backtick quoting from a SHOW NAMESPACES row."""
    if len(rendered) >= 2 and rendered.startswith("`") and rendered.endswith("`"):
        return rendered[1:-1].replace("``", "`")
    return rendered


def _split_identifier(name: str) -> list[str]:
    """Split a dotted identifier on unquoted ``.`` (double-quote / backtick aware)."""
    parts: list[str] = []
    buf: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(name):
        character = name[index]
        if quote is not None:
            if character == quote:
                if index + 1 < len(name) and name[index + 1] == quote:
                    buf.append(quote)
                    index += 2
                    continue
                quote = None
                index += 1
                continue
            buf.append(character)
            index += 1
            continue
        if character in ('"', "`"):
            quote = character
            index += 1
            continue
        if character == ".":
            parts.append("".join(buf))
            buf = []
            index += 1
            continue
        buf.append(character)
        index += 1
    parts.append("".join(buf))
    if quote is not None:
        raise RuntimeError(f"invalid table identifier: unmatched quote in {name!r}")
    return parts


class Catalog:
    """Metadata operations on the session (near-drop-in for ``pyspark.sql.catalog.Catalog``).

    Obtained via :attr:`repark.session.ReparkSession.catalog`, never constructed directly.
    Current-catalog / current-database state is **per-session facade state** (not engine router
    state) — see module docstring.
    """

    __slots__ = ("_session",)

    def __init__(self, session: ReparkSession) -> None:
        """Wrap the live :class:`~repark.session.ReparkSession` (facade state + native handle)."""
        self._session = session

    # ===========================================================================================
    # tableExists / dropTempView / clearCache (pre-existing + current-catalog resolution)
    # ===========================================================================================

    def table_exists(self, table_name: str) -> bool:
        """Whether a table exists (PySpark ``spark.catalog.tableExists``).

        * three-part ``catalog.namespace.table`` → Iceberg catalog probe (native), with
          ``spark_catalog`` alias expansion matching :func:`~repark.session.resolve_table_name`
        * two-part ``namespace.table`` → resolved under :meth:`currentCatalog` (R-CURCAT)
        * one-part name → temp view first, else ``currentCatalog.currentDatabase.name``
        """
        from repark.spark.session import _alias_catalog_name

        inner = self._session._ensure_alive()
        table_name = _require_str(table_name, "tableName")
        parts = _split_identifier(table_name)
        state = self._session._catalog_state()
        known_raw = state.get("known_catalogs") or set()
        known: set[str] = known_raw if isinstance(known_raw, set) else set(known_raw)
        current_catalog = str(state["current_catalog"])
        if len(parts) == 3:
            catalog = _alias_catalog_name(
                parts[0],
                current_catalog=current_catalog,
                known_catalogs=known,
                default_catalog_is_auto=bool(state.get("auto_default_catalog")),
            )
            qualified = _multipart([catalog, parts[1], parts[2]])
            return bool(inner.table_exists(qualified))
        if len(parts) == 2:
            catalog = _alias_catalog_name(
                current_catalog,
                current_catalog=current_catalog,
                known_catalogs=known,
                default_catalog_is_auto=bool(state.get("auto_default_catalog")),
            )
            qualified = _multipart([catalog, parts[0], parts[1]])
            return bool(inner.table_exists(qualified))
        if len(parts) == 1:
            # Temp view (native one-part) first — live Spark checks temps before current db.
            if inner.table_exists(parts[0]):
                return True
            catalog = _alias_catalog_name(
                current_catalog,
                current_catalog=current_catalog,
                known_catalogs=known,
                default_catalog_is_auto=bool(state.get("auto_default_catalog")),
            )
            database = self.current_database()
            qualified = _multipart([catalog, database, parts[0]])
            try:
                return bool(inner.table_exists(qualified))
            except RuntimeError:
                # Soften 1-part current-db fallback only; bare absence → False.
                return False
        raise RuntimeError(
            f"tableExists supports `catalog.namespace.table`, `namespace.table` under the "
            f"current catalog, or a bare name; got {table_name!r}"
        )

    tableExists = table_exists  # noqa: N815 — deliberate PySpark-compatible camelCase alias

    def drop_temp_view(self, view_name: str) -> bool:
        """Drop a temp view; returns whether it existed (PySpark ``dropTempView``)."""
        return bool(self._session._ensure_alive().drop_temp_view(view_name))

    dropTempView = drop_temp_view  # noqa: N815

    def clear_cache(self) -> None:
        """Drop all session DataFrame cache MemTables (PySpark ``clearCache``).

        # === r23 CACHE1: cache-honesty ===
        repark has no cluster-side block manager. Cache/persist pins are object-identity
        ``__repark_cache_*`` MemTables (OTH-005/014). This method **really drops** every
        registered cache view and resets each live cached :class:`~repark.dataframe.DataFrame`
        handle (``unpersist``) so the next action re-executes — Spark ``clearCache`` semantics
        for the single-node facade (Q11). Orphan ``__repark_cache_*`` views (GC'd handles) are
        dropped by name. Checkpoint / createDataFrame / mapInArrow temp views are left alone.

        Fail-loud: drop / unpersist errors propagate (no silent partial clear — Q11).
        """
        self._session._ensure_alive()
        import weakref

        # Shared prefix SSOT (lazy import avoids catalog↔dataframe cycle at module load).
        from repark.spark.dataframe import _CACHE_VIEW_PREFIX

        token = self._session._alive_token
        registry = token.get("cache_frames")
        if isinstance(registry, weakref.WeakSet):
            # Snapshot: unpersist mutates marks; WeakSet iteration tolerates removals.
            for frame in list(registry):
                frame.unpersist()
            registry.clear()
        # Orphan cache views (handle GC'd without unpersist) — drop by prefix only.
        # Use Catalog.drop_temp_view (native handle via _ensure_alive); ReparkSession has no
        # drop_temp_view facade method (C1-Q-001: suppress previously hid AttributeError).
        for view_name in self._session.list_temp_view_names():
            if view_name.startswith(_CACHE_VIEW_PREFIX):
                self.drop_temp_view(view_name)

    clearCache = clear_cache  # noqa: N815

    # ===========================================================================================
    # Current catalog / database (facade state)
    # ===========================================================================================

    def current_catalog(self) -> str:
        """The session's current catalog name (PySpark ``currentCatalog``)."""
        self._session._ensure_alive()
        return str(self._session._catalog_state()["current_catalog"])

    currentCatalog = current_catalog  # noqa: N815

    def set_current_catalog(self, catalog_name: str) -> None:
        """Set the current catalog (PySpark ``setCurrentCatalog``).

        Unknown catalog → :class:`~repark.errors.AnalysisException` naming ``CATALOG_NOT_FOUND``
        (live Spark raises ``CatalogNotFoundException``; repark has no separate leaf class).
        """
        self._session._ensure_alive()
        name = _require_str(catalog_name, "catalogName")
        if not self._catalog_is_registered(name):
            raise AnalysisException(
                f"[CATALOG_NOT_FOUND] The catalog `{name}` not found. Consider to set the SQL "
                f'config "spark.sql.catalog.{name}" to a catalog plugin.'
            )
        self._session._catalog_state()["current_catalog"] = name

    setCurrentCatalog = set_current_catalog  # noqa: N815

    def current_database(self) -> str:
        """The session's current database/namespace (PySpark ``currentDatabase``)."""
        self._session._ensure_alive()
        return str(self._session._catalog_state()["current_database"])

    currentDatabase = current_database  # noqa: N815

    def set_current_database(self, db_name: str) -> None:
        """Set the current database (PySpark ``setCurrentDatabase``).

        Missing schema → :class:`~repark.errors.AnalysisException` ``SCHEMA_NOT_FOUND``.
        """
        self._session._ensure_alive()
        name = _require_str(db_name, "dbName")
        catalog = self.current_catalog()
        if not self._namespace_exists(catalog, name):
            raise AnalysisException(
                f"[SCHEMA_NOT_FOUND] The schema `{catalog}`.`{name}` cannot be found. Verify "
                f"the spelling and correctness of the schema and catalog."
            )
        self._session._catalog_state()["current_database"] = name

    setCurrentDatabase = set_current_database  # noqa: N815

    # ===========================================================================================
    # listCatalogs / listDatabases / listTables / databaseExists / getDatabase
    # ===========================================================================================

    def list_catalogs(self, pattern: str | None = None) -> list[Any]:
        """List registered catalogs (PySpark ``listCatalogs``) as :data:`CatalogMetadata` rows."""
        self._session._ensure_alive()
        if pattern is not None:
            pattern = _require_str(pattern, "pattern")
        names = sorted(self._session._catalog_state()["known_catalogs"])
        return [
            CatalogMetadata(name=name, description=None)
            for name in names
            if _pattern_matches(name, pattern)
        ]

    listCatalogs = list_catalogs  # noqa: N815

    def list_databases(self, pattern: str | None = None) -> list[Any]:
        """List namespaces in the current catalog (PySpark ``listDatabases``).

        Built over ``SHOW NAMESPACES IN <currentCatalog>`` (+ optional LIKE). Field shapes match
        live 4.1.2 ``Database``; ``description`` / ``locationUri`` are ``None`` — registry
        row FA-2 (``docs/spark-sql-iceberg-parity.md`` §5).
        """
        self._session._ensure_alive()
        catalog = self.current_catalog()
        sql = f"SHOW NAMESPACES IN {_quote_ident(catalog)}"
        if pattern is not None:
            pattern = _require_str(pattern, "pattern")
            sql = f"{sql} LIKE {sql_string_literal(pattern)}"
        try:
            table = self._session.sql(sql).to_arrow()
        except Exception as exc:
            raise AnalysisException(f"listDatabases failed for catalog `{catalog}`: {exc}") from exc
        out: list[Any] = []
        for row in table.to_pylist():
            rendered = row["namespace"]
            raw = _unquote_show_namespace(rendered)
            out.append(
                Database(
                    name=raw,
                    catalog=catalog,
                    description=None,
                    locationUri=None,
                )
            )
        return out

    listDatabases = list_databases  # noqa: N815

    def database_exists(self, db_name: str) -> bool:
        """Whether a database/namespace exists (PySpark ``databaseExists``).

        Never raises for mere absence (live Spark parity). ``spark_catalog`` in a two-part
        ``catalog.db`` form aliases the same way as :func:`~repark.session.resolve_table_name`
        (E2 / octo C1-L-001).
        """
        from repark.spark.session import _alias_catalog_name

        self._session._ensure_alive()
        name = _require_str(db_name, "dbName")
        state = self._session._catalog_state()
        known_raw = state.get("known_catalogs") or set()
        known: set[str] = known_raw if isinstance(known_raw, set) else set(known_raw)
        current_catalog = str(state["current_catalog"])
        # Accept optional catalog.db two-part form (oracle: spark_catalog.default works).
        parts = _split_identifier(name)
        if len(parts) == 2:
            catalog, name = parts[0], parts[1]
        elif len(parts) == 1:
            catalog = current_catalog
        else:
            return False
        catalog = _alias_catalog_name(
            catalog,
            current_catalog=current_catalog,
            known_catalogs=known,
            default_catalog_is_auto=bool(state.get("auto_default_catalog")),
        )
        try:
            return self._namespace_exists(catalog, name)
        except Exception:
            return False

    databaseExists = database_exists  # noqa: N815

    def get_database(self, db_name: str) -> Any:
        """Get the database with the specified name (PySpark ``spark.catalog.getDatabase``).

        Returns a :data:`Database` namedtuple. ``locationUri`` is the namespace warehouse
        location when the catalog stores one (``location``, else the U2 ``location_uri``
        mirror) — unlike :meth:`listDatabases`, which leaves it ``None`` (registry FA-2).
        Existence and location both come from ``DESCRIBE NAMESPACE`` (the engine already
        checks ``namespace_exists`` and preserves catalog/IO errors). Missing schema →
        :class:`~repark.errors.AnalysisException` ``SCHEMA_NOT_FOUND``. Two-part
        ``catalog.db`` forms expand ``spark_catalog`` like :meth:`database_exists`.
        """
        from repark.spark.session import _alias_catalog_name

        self._session._ensure_alive()
        name = _require_str(db_name, "dbName")
        state = self._session._catalog_state()
        known_raw = state.get("known_catalogs") or set()
        known: set[str] = known_raw if isinstance(known_raw, set) else set(known_raw)
        current_catalog = str(state["current_catalog"])
        parts = _split_identifier(name)
        if len(parts) == 2:
            catalog, name = parts[0], parts[1]
        elif len(parts) == 1:
            catalog = current_catalog
        else:
            raise AnalysisException(
                f"[SCHEMA_NOT_FOUND] The schema `{name}` cannot be found. Verify "
                f"the spelling and correctness of the schema and catalog."
            )
        catalog = _alias_catalog_name(
            catalog,
            current_catalog=current_catalog,
            known_catalogs=known,
            default_catalog_is_auto=bool(state.get("auto_default_catalog")),
        )
        # Existence and location both come from DESCRIBE NAMESPACE (engine
        # namespace_exists + get_namespace + location resolver). Do not SHOW-list
        # first: that walk swallows catalog/IO errors as absence (Q-001 / SEC-001).
        # listDatabases stays on SHOW (FA-2).
        sql = f"DESCRIBE NAMESPACE {_multipart([catalog, name])}"
        table = self._session.sql(sql).to_arrow()
        description: str | None = None
        location_uri: str | None = None
        rows = zip(
            table.column("info_name").to_pylist(),
            table.column("info_value").to_pylist(),
            strict=True,
        )
        for info_name, info_value in rows:
            if info_name == "Comment":
                description = info_value
            elif info_name == "Location":
                location_uri = info_value
        return Database(
            name=name,
            catalog=catalog,
            description=description,
            locationUri=location_uri,
        )

    getDatabase = get_database  # noqa: N815

    def list_tables(
        self,
        db_name: str | None = None,
        pattern: str | None = None,
    ) -> list[Any]:
        """List tables in a database (PySpark ``listTables``).

        Permanent **Iceberg** tables are listed **live** from the catalog handle
        (``Catalog::list_tables`` — list-on-access, T6 / CQ-008 / BUG-007), not from the
        DataFusion provider name snapshot. Non-Iceberg catalogs use the DF provider name
        directory for that catalog/schema only (no full-catalog walk). Temporary views come
        from the session default schema via
        :meth:`~repark.session.ReparkSession.list_temp_view_names` — **not**
        ``information_schema.tables``, which materializes every provider table and hard-fails
        after an out-of-band drop of a DF-known Iceberg name (F-T6-PHANTOM-A).
        Missing schema raises :class:`~repark.errors.AnalysisException` ``SCHEMA_NOT_FOUND``.
        Two-part ``catalog.db`` forms expand ``spark_catalog`` the same way as
        :meth:`table_exists` / :meth:`database_exists` (E2 / octo C2-Q-002).
        """
        from repark.spark.session import _alias_catalog_name

        # === r21 T6: catalog-staleness ========================================================
        self._session._ensure_alive()
        catalog = self.current_catalog()
        database = self.current_database() if db_name is None else _require_str(db_name, "dbName")
        if pattern is not None:
            pattern = _require_str(pattern, "pattern")
        db_parts = _split_identifier(database)
        if len(db_parts) == 2:
            catalog, database = db_parts[0], db_parts[1]
        elif len(db_parts) != 1:
            raise AnalysisException(f"[SCHEMA_NOT_FOUND] The schema `{database}` cannot be found.")
        # spark_catalog two-part alias parity with tableExists/databaseExists (E2 / C2-Q-002).
        state = self._session._catalog_state()
        known_raw = state.get("known_catalogs") or set()
        known: set[str] = known_raw if isinstance(known_raw, set) else set(known_raw)
        current_catalog = str(state["current_catalog"])
        catalog = _alias_catalog_name(
            catalog,
            current_catalog=current_catalog,
            known_catalogs=known,
            default_catalog_is_auto=bool(state.get("auto_default_catalog")),
        )
        # Bare-session parity: PySpark's `default` database always exists, so a NO-ARG
        # listTables() must list temp views, never raise — even when the engine has no
        # `default` schema (fresh session, no catalogs). Fall through: zero base tables for a
        # missing schema and still appends temps. Explicitly-named schemas that do not exist
        # keep raising (W7 oracle pin).
        if db_name is not None and not self._namespace_exists(catalog, database):
            raise AnalysisException(
                f"[SCHEMA_NOT_FOUND] The schema `{catalog}`.`{database}` cannot be found. Verify "
                f"the spelling and correctness of the schema and catalog."
            )
        out: list[Any] = []
        seen_permanent: set[str] = set()
        # Iceberg list-on-access (T6): live Catalog::list_tables, not DF snapshot.
        # Empty list is still a successful live list (must not fall back to a stale snapshot).
        try:
            live_names = self._session.list_iceberg_table_names(catalog, database)
            for table_name in live_names:
                if _is_hidden_list_tables_name(table_name):
                    continue
                if not _pattern_matches(table_name, pattern):
                    continue
                seen_permanent.add(table_name)
                out.append(
                    Table(
                        name=table_name,
                        catalog=catalog,
                        namespace=[database],
                        description=None,
                        tableType="MANAGED",
                        isTemporary=False,
                    )
                )
        except Exception:
            # Non-Iceberg catalog (postgres) or bare session: DF name directory for this
            # catalog/schema only — never information_schema (global walk loads phantom Iceberg).
            try:
                for table_name in self._session.list_df_schema_table_names(catalog, database):
                    if _is_hidden_list_tables_name(table_name):
                        continue
                    if not _pattern_matches(table_name, pattern):
                        continue
                    if table_name in seen_permanent:
                        continue
                    seen_permanent.add(table_name)
                    out.append(
                        Table(
                            name=table_name,
                            catalog=catalog,
                            namespace=[database],
                            description=None,
                            tableType="MANAGED",
                            isTemporary=False,
                        )
                    )
            except Exception:
                # Missing DF catalog/schema → no permanent rows (temps still appended below).
                pass

        # Temps: default-catalog schema directory only (no information_schema materialization).
        seen_temp: set[str] = set()
        try:
            temp_names = self._session.list_temp_view_names()
        except Exception:
            temp_names = []
        for table_name in temp_names:
            if _is_hidden_list_tables_name(table_name):
                continue
            if table_name in seen_temp:
                continue
            if not _pattern_matches(table_name, pattern):
                continue
            seen_temp.add(table_name)
            out.append(
                Table(
                    name=table_name,
                    catalog=None,
                    namespace=[],
                    description=None,
                    tableType="TEMPORARY",
                    isTemporary=True,
                )
            )
        return out

    listTables = list_tables  # noqa: N815

    # ===========================================================================================
    # Functions (scalar UDF registry surface — r23 C6 census cluster)
    # ===========================================================================================
    # === r23 C6: census-catalog-udf ===
    # Outside CACHE1 clear_cache band. registerFunction is the PySpark-deprecated alias
    # of spark.udf.register; functionExists probes the session UDF registry only
    # (CREATE FUNCTION / permanent catalog functions stay out of scope — no JVM).

    def register_function(
        self,
        name: str,
        f: Any,
        returnType: Any = None,  # noqa: N803 — PySpark camelCase
    ) -> Any:
        """Register a classic scalar Python UDF (PySpark ``Catalog.registerFunction``).

        Deprecated Spark alias of :meth:`spark.udf.register` — same registry, same
        return contract (the :class:`~repark.functions.UserDefinedFunction` callable).
        """
        return self._session.udf.register(name, f, returnType)

    registerFunction = register_function  # noqa: N815 — PySpark camelCase

    def function_exists(
        self,
        functionName: str,  # noqa: N803 — PySpark camelCase
        dbName: str | None = None,  # noqa: N803 — PySpark camelCase
    ) -> bool:
        """Whether a **session-registered** Python UDF exists (PySpark ``functionExists``).

        Probes the classic scalar UDF registry only (names from
        :meth:`registerFunction` / :meth:`spark.udf.register`). Permanent catalog
        functions via ``CREATE FUNCTION`` are not supported (no JVM) — those names
        always return ``False``. ``dbName`` is accepted for signature parity and
        ignored (temp/session UDFs are not database-scoped in repark v1).
        """
        _ = dbName
        name = _require_str(functionName, "functionName")
        # Multipart: spark_catalog.default.func1 → func1
        short = name.split(".")[-1].strip()
        if not short:
            return False
        registry = self._session._udf_registry()
        short_lower = short.lower()
        return any(key.lower() == short_lower for key in registry)

    functionExists = function_exists  # noqa: N815 — PySpark camelCase

    # ===========================================================================================
    # Internals
    # ===========================================================================================

    def _catalog_is_registered(self, name: str) -> bool:
        known = self._session._catalog_state()["known_catalogs"]
        if name in known:
            return True
        try:
            self._session.sql(f"SHOW NAMESPACES IN {_quote_ident(name)}").to_arrow()
        except Exception:
            return False
        known.add(name)
        return True

    def _namespace_exists(self, catalog: str, namespace: str) -> bool:
        try:
            table = self._session.sql(f"SHOW NAMESPACES IN {_quote_ident(catalog)}").to_arrow()
        except Exception:
            return False
        for row in table.to_pylist():
            if _unquote_show_namespace(row["namespace"]) == namespace:
                return True
        return False
