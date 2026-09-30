"""Run user-written or facade-built SQL through the shared ``spark.sql`` entry."""

from __future__ import annotations

from typing import TYPE_CHECKING

from repark import _native
from repark.spark.session.session_state import _promote_active
from repark.spark.session.sql_cache_statements import try_sql_cache_statement
from repark.spark.session.sql_relations import _is_catalog_state_statement
from repark.spark.session.sql_set_statements import try_sql_set_statement

if TYPE_CHECKING:
    from repark.spark.dataframe import DataFrame
    from repark.spark.session.session_core import ReparkSession


def run_sql(session: ReparkSession, query: str, *, built: bool) -> DataFrame:
    """Run one statement; facade-built text parses with default-mode literals.

    Args:
        session: The session executing the statement.
        query: User-written SQL (``built=False``) or facade-built SQL (``built=True``).
        built: True routes the engine parse through the built door, which forces
            ``escapedStringLiterals`` off for that one parse.

    Returns:
        The result frame.
    """
    from repark.spark.dataframe import DataFrame
    from repark.spark.udtf import try_sql_registered_udtf

    inner = session._ensure_alive()
    _promote_active(session)
    for rewrite in (
        try_sql_set_statement,
        try_sql_cache_statement,
        try_sql_registered_udtf,
    ):
        if (frame := rewrite(session, query)) is not None:
            return frame
    if (udf_frame := session._sql_with_registered_udfs(query)) is not None:
        return udf_frame
    expanded = session._expand_bare_table_names_in_sql(query)
    native = _native.session_sql_built(inner, expanded) if built else inner.sql(expanded)
    frame = DataFrame(native, inner, session._alive_token)
    if _is_catalog_state_statement(query):
        session._sync_catalog_state_from_engine()
    return frame
