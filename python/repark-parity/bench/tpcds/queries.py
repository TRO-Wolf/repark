"""TPC-DS query texts from DuckDB ``tpcds_queries()`` plus an optional rewrite table.

Rewrites are dialect compatibility only — every rewrite is a disclosed finding, never a
semantic weakening of the benchmark. Query provenance: DuckDB's shipped ``tpcds``
extension (``tpcds_queries()``) — **not** vendored TPC Council specification text.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class TpcdsQuery:
    """One TPC-DS query with optional dialect rewrite applied for repark."""

    query_nr: int
    original_sql: str
    sql_for_repark: str
    rewrite_note: str | None = None

    @property
    def is_ordered(self) -> bool:
        """True when the (repark) SQL text contains an ``ORDER BY`` clause."""
        return sql_has_order_by(self.sql_for_repark)


# Dialect rewrites applied ONLY when repark cannot parse the DuckDB/canonical text.
# Keys are query numbers (1..99). Value: (rewritten_sql, disclosure note).
# Start empty — populate only after a verified dialect gap (not a wrong-result massage).
DIALECT_REWRITES: Final[dict[int, tuple[str, str]]] = {}

_ORDER_BY_RE: Final[re.Pattern[str]] = re.compile(r"\border\s+by\b", re.IGNORECASE)
# Strip line comments, block comments, and quoted strings so ORDER BY inside
# literals/comments does not force ordered compare (false WRONG-RESULT risk).
_SQL_NOISE_RE: Final[re.Pattern[str]] = re.compile(
    r"(--[^\n]*|/\*.*?\*/|'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\")",
    re.IGNORECASE | re.DOTALL,
)

EXPECTED_QUERY_COUNT: Final[int] = 99


def sql_has_order_by(sql: str) -> bool:
    """Heuristic: SQL text contains an ORDER BY keyword (case-insensitive).

    Comments and quoted string literals are stripped first so a TPC-DS string
    like ``'order by x'`` does not enable ordered compare.
    """
    stripped = _SQL_NOISE_RE.sub(" ", sql)
    return _ORDER_BY_RE.search(stripped) is not None


def load_queries() -> list[TpcdsQuery]:
    """Return the 99 canonical TPC-DS queries (DuckDB ``tpcds_queries()``), with rewrites."""
    import duckdb

    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute("INSTALL tpcds")
        connection.execute("LOAD tpcds")
        rows = connection.execute(
            "SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr"
        ).fetchall()
    finally:
        connection.close()

    if len(rows) != EXPECTED_QUERY_COUNT:
        msg = f"expected {EXPECTED_QUERY_COUNT} TPC-DS queries from DuckDB, got {len(rows)}"
        raise RuntimeError(msg)

    queries: list[TpcdsQuery] = []
    for query_nr, original_sql in rows:
        number = int(query_nr)
        text = str(original_sql)
        if number in DIALECT_REWRITES:
            rewritten, note = DIALECT_REWRITES[number]
            queries.append(
                TpcdsQuery(
                    query_nr=number,
                    original_sql=text,
                    sql_for_repark=rewritten,
                    rewrite_note=note,
                )
            )
        else:
            queries.append(
                TpcdsQuery(
                    query_nr=number,
                    original_sql=text,
                    sql_for_repark=text,
                    rewrite_note=None,
                )
            )
    return queries
