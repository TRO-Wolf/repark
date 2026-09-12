"""Spark ``UnresolvedRegex`` for ``colRegex`` — a marker ``Column`` that ``select`` expands."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from repark import _native
from repark.errors import AnalysisException
from repark.spark._idents import quote_ident
from repark.spark.column import Column

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame


class RegexColumn(Column):
    """The ``colRegex`` marker — a backticked-pattern column expanded only by ``select``."""

    __slots__ = ("_regex_pattern",)

    def __init__(self, inner: Any, regex_pattern: str) -> None:
        """Wrap a never-resolvable quoted ref and keep the stripped Java-style pattern."""
        super().__init__(inner)
        self._regex_pattern = regex_pattern


def col_regex_column(frame: DataFrame, col_name: str) -> Column:
    """Build the ``colRegex`` result: marker for a backticked pattern, literal name otherwise."""
    if len(col_name) > 2 and col_name.startswith("`") and col_name.endswith("`"):
        pattern = col_name[1:-1]
        return RegexColumn(_native.PyColumn.column(quote_ident(pattern)), pattern)
    from repark.spark.functions import col

    return col(frame._resolve_getitem_column_name(col_name))


def expand_col_regex(frame: DataFrame, column: RegexColumn) -> list[Column]:
    """Return bound Columns for every display name the pattern full-matches, in frame order."""
    try:
        pattern = re.compile(column._regex_pattern, re.IGNORECASE)
    except re.error as error:
        raise AnalysisException(
            f"colRegex pattern {column._regex_pattern!r} is not a valid regular expression "
            f"({error})"
        ) from error
    names = frame.columns
    if frame._display_names is None and len(set(names)) == len(names):
        return [frame._bind_schema_column(name, name) for name in names if pattern.fullmatch(name)]
    bound = frame._iter_bound_columns()
    return [
        bound_column
        for name, bound_column in zip(names, bound, strict=True)
        if pattern.fullmatch(name)
    ]
