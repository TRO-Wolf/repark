"""Written-name resolution behind the DataFrame name entry points."""

from __future__ import annotations

from typing import TYPE_CHECKING

from repark import _native
from repark.errors import AnalysisException
from repark.spark._idents import quote_ident as _quote_ident
from repark.spark.column import Column

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame


def _bind_written_column(frame: DataFrame, name: str) -> Column:
    """Bind one written name by the session rule, quoting its engine identifier.

    Preserve the requested display spelling and attach origin metadata for joins.
    """
    from repark.spark.functions import col as _written_col

    plan = frame._plan()
    columns = frame.columns
    if _native.frame_is_exact(plan):
        if "." in name and name not in columns and frame._display_names is None:
            _native.resolve_df_names(plan, [name])
        else:
            _native.match_display_names(plan, [name], columns)
        if frame._display_names is None:
            return _written_col(name)
        held = name
    elif name in columns and columns.count(name) == 1:
        held = name
    else:
        _written, hits, disposition = _native.match_display_names(plan, [name], columns)[0]
        if (
            disposition == "missing"
            and "." in name
            and frame._display_names is None
            and _native.resolve_df_names(plan, [name])[0][3] == "bound"
        ):
            return _written_col(name)
        if disposition == "missing":
            raise AnalysisException(
                f"A column with name `{name}` cannot be resolved; available columns: {columns}"
            )
        if disposition == "ambiguous" and name in columns:
            could_be = ", ".join(f"`{hit}`" for hit in hits)
            raise AnalysisException(
                f"[AMBIGUOUS_REFERENCE] Reference `{name}` is ambiguous, could be: [{could_be}]."
            )
        if disposition == "ambiguous":
            unique = list(dict.fromkeys(hits))
            raise AnalysisException(
                f"A column with name `{name}` is ambiguous among case-insensitive matches: "
                f"{unique}; available columns: {columns}"
            )
        held = hits[0]
    engine_field = frame._engine_field_for_display(held)
    quoted = _quote_ident(engine_field)
    return Column(
        _native.PyColumn.column(quoted).alias(name),
        spark_display=name,
        projection_name=name,
        stable_name=True,
        has_free_attribute=True,
        sql_expr=quoted,
        origin_plan_id=frame._plan_id,
        origin_field=held,
    )
