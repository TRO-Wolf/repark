"""Expression-fragment rendering, bound as :class:`Column` methods."""

from __future__ import annotations

from typing import TYPE_CHECKING

from repark.spark import column_fields as _column_fields
from repark.spark._idents import escape_attr_token_quals as _escape_attr_token_quals

if TYPE_CHECKING:
    from repark.spark.column import Column


def sql_expr_part(column: Column) -> str:
    """SQL fragment for embedding this column into a generated SQL statement."""
    if column._sql_expr is not None:
        return column._sql_expr
    return spark_display_part(column)


def join_sql_part(column: Column) -> str:
    """SQL fragment for join ON rewrite (H1) — id-qualified tokens when present."""
    if column._join_sql_expr is not None:
        return column._join_sql_expr
    if column._attr_id is not None:
        quals = _escape_attr_token_quals(column._qualifiers)
        frame = _column_fields._column_frame_id(column) or 0
        return f"__REPARK_ATTR_{column._attr_id}__F{frame}__{quals}__"
    return sql_expr_part(column)


def sql_expr_without_alias(column: Column) -> str:
    """SQL fragment with a trailing NamedExpression ``AS name`` stripped (if present).

    ``Column.alias`` embeds ``… AS name`` into ``sql_expr`` for MERGE/select surfaces.
    Generator rewrites (``unnest`` / ``WHERE array_length(…)``) need the bare array
    expression only — never an illegal ``AS`` inside ``unnest(...)``.
    """
    text = sql_expr_part(column)
    if not column._stable_name or column._projection_name is None:
        return text
    suffix = f" AS {column._projection_name}"
    if text.endswith(suffix):
        return text[: -len(suffix)]
    return text


def spark_display_part(column: Column) -> str:
    """PySpark-style name fragment for this expression (aggregate output-name building)."""
    if column._spark_display is not None:
        return column._spark_display
    return column._inner.display_name()


def spark_wrap_display_part(column: Column) -> str:
    """Child fragment when this column is embedded inside an outer expression display.

    User ``.alias("v")`` stores ``spark_display`` as ``… AS v`` so aggregate arguments
    keep Spark's ``sum(x AS y)`` form via :meth:`spark_display_part`. Outer wrappers
    (``round`` / ``abs`` / arithmetic / cast / ``_scalar``) collapse that NamedExpression
    to the projection name so ``.alias("v").round(2)`` displays ``round(v, 2)`` rather
    than ``round((id * 1.234) AS v, 2)``.
    """
    if (
        column._stable_name
        and column._projection_name is not None
        and column._spark_display is not None
        and column._spark_display.endswith(f" AS {column._projection_name}")
    ):
        return column._projection_name
    return spark_display_part(column)
