"""By-name table writes for the DataFrame writers, including the schema-evolving append.

A frame whose columns are a subset of the table's writes them by name and fills the rest with
NULL. A frame carrying columns the table does not have is a schema-evolution question, and Spark
answers it on the table property ``write.spark.accept-any-schema`` and the ``mergeSchema`` write
option — so the append lowers to ``INSERT INTO … BY NAME``, the one engine path that owns that
decision, instead of being refused here.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from repark.errors import AnalysisException, PySparkTypeError
from repark.spark._idents import quote_ident as _quote_ident
from repark.spark.dataframe.core import _by_name_casefold_map

if TYPE_CHECKING:
    from repark.spark.column import Column


def _split_columns(
    session: Any, dataframe: Any, table_ref: str
) -> tuple[list[str], list[str], dict[str, str], list[str], list[str]]:
    """Target columns, source columns, the source case-fold map, the matched pair, the extras."""
    target_columns = list(session.sql(f"SELECT * FROM {table_ref} LIMIT 0").column_names())
    source_columns = list(dataframe.columns)
    source_by_case = _by_name_casefold_map(source_columns, surface="DataFrame")
    target_by_case = _by_name_casefold_map(target_columns, surface="table")
    present = [column for column in target_columns if column.casefold() in source_by_case]
    extra = [column for column in source_columns if column.casefold() not in target_by_case]
    return target_columns, source_columns, source_by_case, present, extra


def by_name_projection(
    session: Any, dataframe: Any, table_ref: str, *, display_name: str, surface: str
) -> tuple[str, str]:
    """A target column list plus a quoted source projection in target-table order."""
    target_columns, source_columns, source_by_case, present, extra = _split_columns(
        session, dataframe, table_ref
    )
    if extra:
        raise AnalysisException(
            f"cannot write DataFrame columns {source_columns} into table {display_name!r} "
            f"columns {target_columns} by name for {surface}"
            f"; extra in the DataFrame: {extra}"
        )
    columns = ", ".join(_quote_ident(column) for column in present)
    projection = ", ".join(_quote_ident(source_by_case[name.casefold()]) for name in present)
    return columns, projection


def append_statement(session: Any, dataframe: Any, table_ref: str) -> Callable[[str], str]:
    """Build the append SQL for one by-name table write, given the generated source view name.

    Extra source columns lower to ``INSERT INTO … BY NAME``, where the engine applies the
    ``write.spark.accept-any-schema`` gate and the merge-schema flag; the rest keep the explicit
    column list so a column missing from the frame is still filled with NULL.
    """
    _targets, source_columns, source_by_case, present, extra = _split_columns(
        session, dataframe, table_ref
    )
    if extra:
        wide = ", ".join(_quote_ident(column) for column in source_columns)
        return lambda view: f"INSERT INTO {table_ref} BY NAME SELECT {wide} FROM {view}"
    columns = ", ".join(_quote_ident(column) for column in present)
    projection = ", ".join(_quote_ident(name) for name in _matched_sources(source_by_case, present))
    return lambda view: f"INSERT INTO {table_ref} ({columns}) SELECT {projection} FROM {view}"


def replace_where_statement(
    session: Any, dataframe: Any, table_ref: str, condition: Column | str
) -> Callable[[str], str]:
    """Build ``INSERT INTO … REPLACE WHERE`` for ``DataFrameWriterV2.overwrite(condition)``.

    Args:
        session: The native session that reads the target's columns.
        dataframe: The source frame.
        table_ref: The quoted target table.
        condition: A Column, or a str that names a column, as PySpark reads it.

    Returns:
        The statement for a given source view name. Spark writes by name: the source follows
        table order and a column the frame lacks is NULL. Extra frame columns stay in frame
        order, so the engine answers Spark's arity refusal.

    Raises:
        PySparkTypeError: ``NOT_COLUMN_OR_STR`` when ``condition`` is neither.
    """
    from repark.spark.column import Column
    from repark.spark.functions import col

    if not isinstance(condition, (Column, str)):
        kind = type(condition).__name__
        raise PySparkTypeError(
            f"[NOT_COLUMN_OR_STR] Argument `col` should be a Column or str, got {kind}.",
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={"arg_name": "col", "arg_type": kind},
        )
    predicate = col(condition) if isinstance(condition, str) else condition
    targets, source_columns, source_by_case, _present, extra = _split_columns(
        session, dataframe, table_ref
    )
    if extra:
        items = [_quote_ident(column) for column in source_columns]
    else:
        items = [
            _quote_ident(source_by_case[target.casefold()])
            if target.casefold() in source_by_case
            else f"NULL AS {_quote_ident(target)}"
            for target in targets
        ]
    head = f"INSERT INTO {table_ref} REPLACE WHERE {predicate.sql_expr_part()}"
    return lambda view: f"{head} SELECT {', '.join(items)} FROM {view}"


def _matched_sources(source_by_case: dict[str, str], present: list[str]) -> list[str]:
    """The source spellings of the target columns the frame provides, in target order."""
    return [source_by_case[name.casefold()] for name in present]
