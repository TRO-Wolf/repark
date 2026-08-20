"""Session / identity / hint facade wrappers (FN-F).

Public names are re-exported from ``functions.py``. Catalog and user strings
are foldable session snapshots (ADR-0004: Session only, no env). ``version()``
is the repark distribution string, not DataFusion ``version()``. ``uuid()``
is non-deterministic (type + uniqueness, not value).
"""

from __future__ import annotations

from typing import Any

from repark import _native
from repark.errors import PySparkTypeError
from repark.spark.catalog import DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME
from repark.spark.column import Column
from repark.spark.functions import _as_column_arg, lit


def _foldable_session_string(value: str, *, display: str) -> Column:
    """A foldable string literal with a Spark function display name."""
    literal = lit(value)
    return Column(
        literal._inner,
        spark_display=display,
        projection_name=display,
        sql_expr=literal.sql_expr_part(),
        is_foldable=True,
    )


def _active_session() -> Any:
    """Live :class:`ReparkSession`, or ``None`` when none is active."""
    from repark.spark.session.session_core import ReparkSession

    return ReparkSession.getActiveSession()


def _current_catalog_name() -> str:
    """Session current catalog, or the facade default when none is active."""
    session = _active_session()
    if session is None:
        return DEFAULT_CATALOG_NAME
    return str(session.catalog.current_catalog())


def _current_database_name() -> str:
    """Session current namespace, or the facade default when none is active."""
    session = _active_session()
    if session is None:
        return DEFAULT_DATABASE_NAME
    return str(session.catalog.current_database())


def broadcast(col: Any) -> Any:
    """Broadcast hint (PySpark ``functions.broadcast``).

    A DataFrame is ``df.hint("broadcast")`` (single-node no-op). A column name
    or :class:`Column` is returned unchanged (charter identity hint).
    """
    hint = getattr(col, "hint", None)
    if callable(hint):
        return hint("broadcast")
    if isinstance(col, (Column, str)):
        return _as_column_arg(col, as_lit=False)
    raise PySparkTypeError(
        errorClass="NOT_COLUMN_OR_STR",
        messageParameters={"arg_name": "col", "arg_type": type(col).__name__},
    )


def current_user() -> Column:
    """Session user string (PySpark ``functions.current_user``).

    Foldable. ADR-0004 forbids an env / OS-user read; the session identity is
    the product default ``repark``. Pin type + in-session stability, not Spark's
    OS user.
    """
    return _foldable_session_string("repark", display="current_user()")


user = current_user
session_user = current_user
"""PySpark ``functions.session_user`` — the same session identity as ``current_user``/``user``.

Spark distinguishes ``current_user`` (the effective user) from ``session_user`` (the login user);
with no authentication layer there is one identity, so all three spellings return it. Recorded as
a divergence only if an identity layer ever makes them differ.
"""


def current_catalog() -> Column:
    """Session current catalog (PySpark ``functions.current_catalog``).

    Foldable snapshot of :meth:`Catalog.current_catalog` at construction
    (Session-only). A later ``setCurrentCatalog`` does not rewrite an already
    built Column — construct after the set.
    """
    return _foldable_session_string(_current_catalog_name(), display="current_catalog()")


def current_database() -> Column:
    """Session current database (PySpark ``functions.current_database``).

    Foldable snapshot of :meth:`Catalog.current_database` at construction.
    """
    return _foldable_session_string(_current_database_name(), display="current_database()")


def current_schema() -> Column:
    """Session current schema — Spark alias of :func:`current_database`."""
    return _foldable_session_string(_current_database_name(), display="current_schema()")


def version() -> Column:
    """Engine version string (PySpark ``functions.version``).

    SEMANTIC-HAZARD: DataFusion ``version()`` is ``Apache DataFusion …``.
    This is the repark distribution string (``repark-<pep440>``), matching
    :attr:`ReparkSession.version`.
    """
    from repark import __version__

    return _foldable_session_string(f"repark-{__version__}", display="version()")


def uuid() -> Column:
    """A per-row UUID string (PySpark ``functions.uuid``).

    Non-deterministic. Pin Arrow string type + uniqueness, not a golden value.
    """
    return Column(
        _native.PyColumn.sql("uuid()"),
        spark_display="uuid()",
        projection_name="uuid()",
        sql_expr="uuid()",
        is_foldable=False,
        has_ungroupable=True,
    )
