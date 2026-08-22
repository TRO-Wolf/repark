"""TPC-H query texts from DuckDB ``tpch_queries()`` plus an optional rewrite table.

Rewrites are dialect compatibility only — every rewrite is a disclosed finding, never a
semantic weakening of the benchmark.
"""

from __future__ import annotations

from typing import Final

from pydantic import BaseModel, ConfigDict


class TpchQuery(BaseModel):
    """One TPC-H query with optional dialect rewrite applied for repark."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    query_nr: int
    original_sql: str
    sql_for_repark: str
    rewrite_note: str | None = None


# Dialect rewrites applied ONLY when repark cannot parse the DuckDB/canonical text.
# Keys are query numbers (1..22). Value: (rewritten_sql, disclosure note).
# Start empty — populate only after a verified dialect gap (not a wrong-result massage).
DIALECT_REWRITES: Final[dict[int, tuple[str, str]]] = {}


def load_queries() -> list[TpchQuery]:
    """Return the 22 canonical TPC-H queries (DuckDB ``tpch_queries()``), with rewrites."""
    import duckdb

    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute("INSTALL tpch")
        connection.execute("LOAD tpch")
        rows = connection.execute(
            "SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr"
        ).fetchall()
    finally:
        connection.close()

    if len(rows) != 22:
        msg = f"expected 22 TPC-H queries from DuckDB, got {len(rows)}"
        raise RuntimeError(msg)

    queries: list[TpchQuery] = []
    for query_nr, original_sql in rows:
        number = int(query_nr)
        text = str(original_sql)
        if number in DIALECT_REWRITES:
            rewritten, note = DIALECT_REWRITES[number]
            queries.append(
                TpchQuery(
                    query_nr=number,
                    original_sql=text,
                    sql_for_repark=rewritten,
                    rewrite_note=note,
                )
            )
        else:
            queries.append(
                TpchQuery(
                    query_nr=number,
                    original_sql=text,
                    sql_for_repark=text,
                    rewrite_note=None,
                )
            )
    return queries
