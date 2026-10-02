"""The ``lit`` rendering helpers."""

from __future__ import annotations

import math

from repark.spark._idents import sql_string_literal
from repark.spark.column import Scalar


def _lit_sql_expr(value: Scalar) -> str:
    """SQL literal fragment for embedding a ``lit`` into generated SQL (MERGE, etc.).

    Non-finite floats must not use bare ``nan`` / ``inf`` tokens — those bind as
    identifiers in free-SQL (select-global-agg, cube/rollup, MERGE) rather than float
    constants. Use CAST string forms DataFusion accepts.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "CAST('NaN' AS DOUBLE)"
        if value == math.inf:
            return "CAST('Infinity' AS DOUBLE)"
        if value == -math.inf:
            return "CAST('-Infinity' AS DOUBLE)"
        return repr(value)
    if isinstance(value, str):
        return sql_string_literal(value)
    return str(value)


def _lit_spark_display(value: Scalar) -> str:
    """PySpark-style literal fragment for display/agg names (not DataFusion's ``Int64(1)``).

    Live PySpark 4.1.2 renders string literals **without** surrounding quotes in both projection
    names (``df.select(F.lit("s")).columns == ['s']``) and aggregate embeds
    (``first(z)``, ``concat(s, z)``). Integer/float/bool/NULL follow Spark coercion.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        return value
    return str(value)
