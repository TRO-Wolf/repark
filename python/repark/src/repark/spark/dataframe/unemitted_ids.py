"""Semi/anti unemitted-attribute refusal, bound as :class:`DataFrame` methods."""

from __future__ import annotations

from typing import TYPE_CHECKING

from repark.errors import AnalysisException
from repark.spark import column_fields as _column_fields
from repark.spark.dataframe.join_attr_tokens import _ATTR_TOKEN_RE

if TYPE_CHECKING:
    from repark.spark.column import Column
    from repark.spark.dataframe.core import DataFrame


def _remember_unemitted_right_ids(
    frame: DataFrame, left: DataFrame, right: DataFrame, *, left_only: bool = True
) -> None:
    """Record (semi/anti) or forget (emitting join) exclusive right attribute ids.

    ``left_only=True`` unions exclusive right ids into :attr:`_unemitted_attr_ids`.
    ``left_only=False`` removes them after an emitting join.
    """
    held, engines = _column_fields._stamped_ids_and_engines(right)
    displays = right.columns
    if len(displays) != len(held):
        displays = list(engines)
    left_ids = set(_column_fields._stamped_ids_and_engines(left)[0])
    exclusive = {
        held_id: displays[position]
        for position, held_id in enumerate(held)
        if held_id is not None and held_id not in left_ids
    }
    if exclusive:
        if left_only:
            frame._unemitted_attr_ids = {**frame._unemitted_attr_ids, **exclusive}
        else:
            frame._unemitted_attr_ids = {
                held_id: display
                for held_id, display in frame._unemitted_attr_ids.items()
                if held_id not in exclusive
            }


def _raise_if_id_not_emitted(frame: DataFrame, attr_id: str | None) -> None:
    """Raise Spark 4.1.2 ``MISSING_ATTRIBUTES`` when ``attr_id`` was not emitted."""
    if attr_id is None or attr_id not in frame._unemitted_attr_ids:
        return
    name = frame._unemitted_attr_ids[attr_id]
    available = ", ".join(f'"{column}"' for column in frame.columns)
    quoted = f'"{name}"'
    if name in frame.columns:
        raise AnalysisException(
            f"[MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION] "
            f"Resolved attribute(s) {quoted} missing from {available} in operator "
            f"!Project. Attribute(s) with the same name appear in the operation: "
            f"{quoted}. Please check if the right attribute(s) are used."
        )
    raise AnalysisException(
        f"[MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT] "
        f"Resolved attribute(s) {quoted} missing from {available} in operator !Project."
    )


def _raise_unemitted_attr_tokens(frame: DataFrame, join_sql: str) -> None:
    """Refuse attribute tokens whose id is in :attr:`_unemitted_attr_ids`."""
    if not frame._unemitted_attr_ids or "__REPARK_ATTR_" not in join_sql:
        return
    for match in _ATTR_TOKEN_RE.finditer(join_sql):
        _raise_if_id_not_emitted(frame, match.group(1))


def _refuse_unemitted_ids(frame: DataFrame, column: Column) -> Column:
    """Refuse a Column whose attribute was excluded by semi or anti join.

    A right-side attribute excluded by semi or anti raises ``MISSING_ATTRIBUTES``
    instead of falling back to the left side.
    """
    _raise_if_id_not_emitted(frame, column._attr_id)
    join_sql = column._join_sql_expr
    if join_sql is not None:
        _raise_unemitted_attr_tokens(frame, join_sql)
    return column
