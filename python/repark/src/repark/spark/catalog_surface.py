"""Catalog method bodies for the 13-name catalog surface (CATALOG-SURFACE-1).

pins: catalog-surface-1/C-001, C-002, C-003, C-004, C-005, C-006
"""

from __future__ import annotations

import re
import warnings
import weakref
from typing import TYPE_CHECKING, Any, NoReturn

from repark.errors import AnalysisException, PySparkTypeError, PySparkValueError
from repark.spark._idents import quote_ident_if_needed as _quote_ident
from repark.spark._idents import sql_string_literal
from repark.spark.catalog import (
    Column,
    Function,
    Table,
    _pattern_matches,
    _require_str,
    _split_identifier,
)
from repark.spark.session.sql_relations import _sql_table_ref

if TYPE_CHECKING:
    from repark.spark.catalog import Catalog
    from repark.spark.dataframe import DataFrame
    from repark.spark.session import ReparkSession

_CACHED_TABLES_KEY = "catalog_cached_tables"
_FRAME_IDENTITIES_KEY = "catalog_table_frame_identities"
_BUILTIN_CLASS_NAME = "repark.builtin"
_UDF_CLASS_NAME = "repark.python_udf"


def _raise_table_or_view_not_found(table_name: str) -> NoReturn:
    raise AnalysisException(
        f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `{table_name}` cannot be found. "
        "Verify the spelling and correctness of the schema and catalog.\n"
        "If you did not qualify the name with a schema, verify the current_schema() "
        "output, or qualify the name with the correct schema and catalog.\n"
        "To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS."
    )


def _raise_table_or_view_already_exists(resolved: str) -> NoReturn:
    dotted = ".".join(f"`{part}`" for part in resolved.split("."))
    raise AnalysisException(
        f"[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view {dotted} because it "
        "already exists.\n"
        "Choose a different name, drop or replace the existing object, or add the "
        "IF NOT EXISTS clause to tolerate pre-existing objects."
    )


def _resolve_table_identity(session: ReparkSession, table_name: str) -> tuple[str, str]:
    resolved = session.resolve_table_name(table_name, prefer_temp_view=True)
    parts = _split_identifier(table_name)
    if len(parts) == 1 and parts[0] in set(session.list_temp_view_names()):
        return resolved, "view"
    return resolved, "table"


def _known_table(session: ReparkSession, resolved: str) -> bool:
    inner = session._ensure_alive()
    parts = resolved.split(".")
    try:
        if bool(inner.table_exists(resolved)):
            return True
    except RuntimeError:
        pass
    namespace = ".".join(parts[1:-1])
    try:
        if parts[-1] in set(session.list_iceberg_table_names(parts[0], namespace)):
            return True
    except Exception:
        try:
            return parts[-1] in set(session.list_df_schema_table_names(parts[0], namespace))
        except Exception:
            return False
    return False


def _describe_extended_rows(session: ReparkSession, resolved: str) -> list[tuple[Any, ...]]:
    frame = session.sql(f"DESCRIBE TABLE EXTENDED {_sql_table_ref(resolved)}")
    return [tuple(row) for row in frame.collect()]


def _table_comment(session: ReparkSession, resolved: str) -> str | None:
    for row in _describe_extended_rows(session, resolved):
        if len(row) >= 2 and row[0] == "Table Properties" and isinstance(row[1], str):
            match = re.search(r"comment=([^,\]]*)", row[1])
            if match is not None:
                return match.group(1)
    return None


def _identity_partition_columns(session: ReparkSession, resolved: str) -> set[str]:
    names: set[str] = set()
    in_partitioning = False
    for row in _describe_extended_rows(session, resolved):
        head = row[0] if row else ""
        if isinstance(head, str) and head.startswith("#"):
            in_partitioning = head == "# Partitioning"
            continue
        if in_partitioning and head:
            value = row[1] if len(row) > 1 else None
            if isinstance(value, str) and value:
                names.add(value)
    return names


def _cached_tables(token: dict[str, Any]) -> dict[str, Any]:
    cached = token.get(_CACHED_TABLES_KEY)
    if not isinstance(cached, dict):
        cached = {}
        token[_CACHED_TABLES_KEY] = cached
    return cached


def _frame_identities(token: dict[str, Any]) -> weakref.WeakKeyDictionary[Any, str]:
    identities = token.get(_FRAME_IDENTITIES_KEY)
    if not isinstance(identities, weakref.WeakKeyDictionary):
        identities = weakref.WeakKeyDictionary()
        token[_FRAME_IDENTITIES_KEY] = identities
    return identities


def _unpersist_identity_frames(token: dict[str, Any], resolved: str) -> None:
    identities = token.get(_FRAME_IDENTITIES_KEY)
    if not isinstance(identities, weakref.WeakKeyDictionary):
        return
    for frame, identity in list(identities.items()):
        if identity == resolved:
            frame.unpersist()


def _builtin_function_names() -> list[str]:
    from repark.spark import functions as spark_functions
    from repark.spark.functions_declared import DECLARED_REFUSE_NAMES

    return sorted(name for name in spark_functions.__all__ if name not in DECLARED_REFUSE_NAMES)


def session_table(session: ReparkSession, table_name: str) -> DataFrame:
    """spark.table body with the catalog cache overlay. pins: catalog-surface-1/C-004"""
    from repark.spark.dataframe import DataFrame

    resolved = session.resolve_table_name(table_name, prefer_temp_view=True)
    inner = session._ensure_alive()
    scan_ref = _sql_table_ref(resolved)
    cached = session._alive_token.get(_CACHED_TABLES_KEY)
    if isinstance(cached, dict):
        entry = cached.get(resolved)
        if entry is not None:
            if entry.is_cached and entry._cache_view is not None:
                scan_ref = entry._cache_view
            elif not entry.is_cached:
                cached.pop(resolved, None)
    frame = DataFrame(inner.sql(f"SELECT * FROM {scan_ref}"), inner, session._alive_token)
    _frame_identities(session._alive_token)[frame] = resolved
    return frame


def get_table(catalog: Catalog, table_name: str) -> Any:
    """Spark-shaped Table row for a table or temp view. pins: catalog-surface-1/C-001"""
    session = catalog._session
    session._ensure_alive()
    table_name = _require_str(table_name, "tableName")
    resolved, kind = _resolve_table_identity(session, table_name)
    if kind == "view":
        return Table(
            name=_split_identifier(table_name)[0],
            catalog=None,
            namespace=[],
            description=None,
            tableType="TEMPORARY",
            isTemporary=True,
        )
    if not _known_table(session, resolved):
        _raise_table_or_view_not_found(table_name)
    parts = resolved.split(".")
    return Table(
        name=parts[-1],
        catalog=parts[0],
        namespace=parts[1:-1],
        description=_table_comment(session, resolved),
        tableType="MANAGED",
        isTemporary=False,
    )


def list_columns(catalog: Catalog, table_name: str, db_name: str | None = None) -> list[Any]:
    """One Spark-shaped Column per schema field. pins: catalog-surface-1/C-002"""
    session = catalog._session
    session._ensure_alive()
    table_name = _require_str(table_name, "tableName")
    if db_name is not None:
        db_name = _require_str(db_name, "dbName")
        warnings.warn(
            "`dbName` has been deprecated since Spark 3.4 and might be removed in a "
            "future version. Use listColumns(`dbName.tableName`) instead.",
            FutureWarning,
            stacklevel=3,
        )
        table_name = f"{db_name}.{table_name}"
    resolved, kind = _resolve_table_identity(session, table_name)
    if kind == "table" and not _known_table(session, resolved):
        _raise_table_or_view_not_found(table_name)
    schema = session.table(table_name).schema
    partition_names = _identity_partition_columns(session, resolved) if kind == "table" else set()
    return [
        Column(
            name=field.name,
            description=None,
            dataType=field.dataType.simpleString(),
            nullable=field.nullable,
            isPartition=field.name in partition_names,
            isBucket=False,
            isCluster=False,
        )
        for field in schema.fields
    ]


def list_functions(
    catalog: Catalog,
    db_name: str | None = None,
    pattern: str | None = None,
) -> list[Any]:
    """Built-in plus session-UDF Function rows sorted by name. pins: catalog-surface-1/C-003"""
    session = catalog._session
    session._ensure_alive()
    if db_name is not None:
        _require_str(db_name, "dbName")
    if pattern is not None:
        pattern = _require_str(pattern, "pattern")
    rows = [
        Function(
            name=name,
            catalog=None,
            namespace=None,
            description="",
            className=_BUILTIN_CLASS_NAME,
            isTemporary=True,
        )
        for name in _builtin_function_names()
    ]
    rows += [
        Function(
            name=name,
            catalog=None,
            namespace=None,
            description="N/A.",
            className=_UDF_CLASS_NAME,
            isTemporary=True,
        )
        for name in sorted(session._udf_registry())
    ]
    rows.sort(key=lambda row: row.name)
    if pattern is not None:
        rows = [row for row in rows if _pattern_matches(row.name, pattern)]
    return rows


def get_function(catalog: Catalog, function_name: str) -> Any:
    """One Function row for a UDF or built-in. pins: catalog-surface-1/C-003"""
    from repark.spark import functions as spark_functions
    from repark.spark.functions_declared import DECLARED_REFUSE_NAMES

    session = catalog._session
    session._ensure_alive()
    function_name = _require_str(function_name, "functionName")
    short = function_name.split(".")[-1]
    for key in session._udf_registry():
        if key.split(".")[-1].lower() == short.lower():
            return Function(
                name=key.split(".")[-1],
                catalog=None,
                namespace=None,
                description="N/A.",
                className=_UDF_CLASS_NAME,
                isTemporary=True,
            )
    for name in spark_functions.__all__:
        if name not in DECLARED_REFUSE_NAMES and name.lower() == short.lower():
            return Function(
                name=name,
                catalog=None,
                namespace=None,
                description="",
                className=_BUILTIN_CLASS_NAME,
                isTemporary=True,
            )
    raise AnalysisException(
        f"[UNRESOLVED_ROUTINE] Cannot resolve routine `{short}` on search path "
        f"[`system`.`builtin`, `system`.`session`, "
        f"`{catalog.current_catalog()}`.`{catalog.current_database()}`]."
    )


def cache_table(
    catalog: Catalog,
    table_name: str,
    storage_level: Any = None,
) -> None:
    """Eagerly cache a table or temp view under its identity. pins: catalog-surface-1/C-004"""
    from repark.spark.dataframe import DataFrame

    session = catalog._session
    inner = session._ensure_alive()
    table_name = _require_str(table_name, "tableName")
    resolved, kind = _resolve_table_identity(session, table_name)
    if kind == "table" and not _known_table(session, resolved):
        _raise_table_or_view_not_found(table_name)
    frame = DataFrame(
        inner.sql(f"SELECT * FROM {_sql_table_ref(resolved)}"),
        inner,
        session._alive_token,
    )
    if storage_level is None:
        frame.cache()
    else:
        frame.persist(storage_level)
    frame.count()
    _cached_tables(session._alive_token)[resolved] = frame


def uncache_table(catalog: Catalog, table_name: str) -> None:
    """Release the catalog cache entry for a table or view. pins: catalog-surface-1/C-004"""
    session = catalog._session
    session._ensure_alive()
    table_name = _require_str(table_name, "tableName")
    resolved, kind = _resolve_table_identity(session, table_name)
    if kind == "table" and not _known_table(session, resolved):
        _raise_table_or_view_not_found(table_name)
    cached = session._alive_token.get(_CACHED_TABLES_KEY)
    if isinstance(cached, dict):
        entry = cached.pop(resolved, None)
        if entry is not None:
            entry.unpersist()
    _unpersist_identity_frames(session._alive_token, resolved)


def is_cached(catalog: Catalog, table_name: str) -> bool:
    """Whether the resolved identity has a live cache. pins: catalog-surface-1/C-004"""
    session = catalog._session
    session._ensure_alive()
    table_name = _require_str(table_name, "tableName")
    resolved, kind = _resolve_table_identity(session, table_name)
    if kind == "table" and not _known_table(session, resolved):
        _raise_table_or_view_not_found(table_name)
    cached = session._alive_token.get(_CACHED_TABLES_KEY)
    if isinstance(cached, dict):
        entry = cached.get(resolved)
        if entry is not None:
            if entry.is_cached:
                return True
            del cached[resolved]
    identities = session._alive_token.get(_FRAME_IDENTITIES_KEY)
    if isinstance(identities, weakref.WeakKeyDictionary):
        for frame, identity in list(identities.items()):
            if identity == resolved and frame.is_cached:
                return True
    return False


def create_table(
    catalog: Catalog,
    table_name: str,
    path: str | None = None,
    source: str | None = None,
    schema: Any = None,
    description: str | None = None,
    **options: Any,
) -> DataFrame:
    """Create an empty Iceberg table and return its DataFrame. pins: catalog-surface-1/C-005"""
    from repark.spark.types import StructType

    session = catalog._session
    session._ensure_alive()
    table_name = _require_str(table_name, "tableName")
    resolved = session.resolve_table_name(table_name, prefer_temp_view=False)
    if _known_table(session, resolved):
        _raise_table_or_view_already_exists(resolved)
    if path is not None or "path" in options:
        raise PySparkValueError(
            "repark.catalog.createTable supports only source('iceberg') managed tables; "
            "an external location is not supported, "
            f"got path={path if path is not None else options['path']!r}"
        )
    if source is not None and source.lower() != "iceberg":
        raise PySparkValueError(
            f"repark.catalog.createTable supports only source('iceberg'), got {source!r}"
        )
    if schema is None:
        raise AnalysisException(
            "[UNABLE_TO_INFER_SCHEMA] Unable to infer schema for Iceberg. "
            "It must be specified manually."
        )
    if isinstance(schema, StructType):
        column_defs = ", ".join(
            f"{_quote_ident(field.name)} {field.dataType.simpleString()}" for field in schema.fields
        )
    elif isinstance(schema, str):
        column_defs = schema
    else:
        raise PySparkTypeError(
            f"[CATALOG_ARG_TYPE] Argument `schema` must be a StructType or str, "
            f"got {type(schema).__name__}."
        )
    properties = dict(options)
    if description is not None:
        properties["comment"] = description
    ddl = f"CREATE TABLE {_sql_table_ref(resolved)} ({column_defs})"
    if properties:
        pairs = ", ".join(
            f"{sql_string_literal(str(key))}={sql_string_literal(str(value))}"
            for key, value in properties.items()
        )
        ddl += f" TBLPROPERTIES ({pairs})"
    session.sql(ddl).collect()
    return session.table(table_name)


def create_external_table(
    catalog: Catalog,
    table_name: str,
    path: str | None = None,
    source: str | None = None,
    schema: Any = None,
    description: str | None = None,
    **options: Any,
) -> DataFrame:
    """Deprecated alias of createTable with Spark's warning. pins: catalog-surface-1/C-005"""
    warnings.warn(
        "createExternalTable is deprecated since Spark 2.2, please use createTable instead.",
        FutureWarning,
        stacklevel=3,
    )
    return create_table(
        catalog,
        table_name,
        path=path,
        source=source,
        schema=schema,
        description=description,
        **options,
    )


def drop_global_temp_view(catalog: Catalog, view_name: str) -> bool:
    """False always: global temp views stay refused under EX-DF-2. pins: catalog-surface-1/C-006"""
    catalog._session._ensure_alive()
    _require_str(view_name, "viewName")
    return False


def recover_partitions(catalog: Catalog, table_name: str) -> None:
    """No-op for Iceberg tables; views and misses raise. pins: catalog-surface-1/C-006"""
    session = catalog._session
    session._ensure_alive()
    table_name = _require_str(table_name, "tableName")
    resolved, kind = _resolve_table_identity(session, table_name)
    if kind == "view":
        leaf = _split_identifier(table_name)[-1]
        raise AnalysisException(
            "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'recoverPartitions()' expects a "
            f"table but `{leaf}` is a view."
        )
    if not _known_table(session, resolved):
        _raise_table_or_view_not_found(table_name)


def refresh_table(catalog: Catalog, table_name: str) -> None:
    """Rebuild the table's catalog provider and drop its cache. pins: catalog-surface-1/C-006"""
    session = catalog._session
    session._ensure_alive()
    table_name = _require_str(table_name, "tableName")
    resolved, kind = _resolve_table_identity(session, table_name)
    if kind == "view":
        return
    if not _known_table(session, resolved):
        _raise_table_or_view_not_found(table_name)
    cached = session._alive_token.get(_CACHED_TABLES_KEY)
    if isinstance(cached, dict):
        entry = cached.pop(resolved, None)
        if entry is not None:
            entry.unpersist()
    _unpersist_identity_frames(session._alive_token, resolved)
    session.refresh_catalog_provider(resolved.split(".")[0])


def refresh_by_path(catalog: Catalog, path: str) -> None:
    """No-op: repark keeps no path-keyed cache. pins: catalog-surface-1/C-006"""
    catalog._session._ensure_alive()
    _require_str(path, "path")
