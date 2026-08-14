"""Polars-**style** lazy API over repark DataFrame plans (``import repark.polars as rp``).

This is **not** an alias of real polars and must not import real polars except inside
:meth:`PolarsFrame.collect`. Expression builders use repark :class:`~repark.column.Column`
machinery under polars-like names.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.errors import PySparkTypeError, PySparkValueError
from repark.spark.column import Column
from repark.spark.functions import col as spark_col
from repark.spark.functions import lit as spark_lit
from repark.spark.functions import when as spark_when

if TYPE_CHECKING:
    from repark.spark.dataframe import DataFrame


def col(name: str) -> Column:
    """Column reference (polars-style ``rp.col`` → repark :func:`repark.functions.col`)."""
    return spark_col(name)


def lit(value: Any) -> Column:
    """Literal (polars-style ``rp.lit``)."""
    return spark_lit(value)


def when(condition: Column, value: Column | Any = None) -> Column:
    """Start a when/then/otherwise chain (polars-style naming over repark ``when``).

    Accepts ``rp.when(cond).then(v).otherwise(o)`` via a thin wrapper, or the Spark form
    ``rp.when(cond, v).otherwise(o)``.
    """
    if value is None:
        return _WhenBuilder(condition)
    return spark_when(condition, value)


class _WhenBuilder:
    """``rp.when(cond).then(v).otherwise(o)`` ergonomics."""

    def __init__(self, condition: Column) -> None:
        self._condition = condition

    def then(self, value: Column | Any) -> Column:
        return spark_when(self._condition, value)


def _reject_polars_expr(value: Any, *, surface: str) -> None:
    """Refuse real ``polars.Expr`` objects (version-coupled)."""
    module = type(value).__module__
    if module.startswith("polars"):
        raise PySparkTypeError(
            f"{surface} does not accept real polars objects ({type(value)!r}); "
            f"use repark.polars builders (rp.col / rp.lit / rp.when)"
        )


def _sort_key(column: Column, *, ascending: bool) -> Column:
    """Clone ``column`` as a sort key with an explicit direction (the same field-copy
    pattern as ``Column.asc``/``Column.desc``)."""
    return Column(
        column._inner,
        sort_ascending=ascending,
        spark_display=column._spark_display,
        projection_name=column._projection_name,
        stable_name=column._stable_name,
        agg_name=column._agg_name,
        is_aggregate=column._is_aggregate,
        is_foldable=column._is_foldable,
        has_free_attribute=column._has_free_attribute,
        has_ungroupable=column._has_ungroupable,
        is_aggregate_function=column._is_aggregate_function,
        partition_transform=column._partition_transform,
        # Keep structural sql_expr (parity with Column.asc/desc — octo C6-Q-002).
        sql_expr=column._sql_expr,
        # Keep generators sticky — orderBy refuse must still fire after polars sort-key
        # wrapping (combine octo C6-Q-002; matches Column.asc/desc).
        generator=column._generator,
        generator_cast=column._generator_cast,
        when_pairs=column._when_pairs,
    )


class PolarsFrame:
    """Polars-style wrapper around a repark :class:`~repark.dataframe.DataFrame`.

    Reach via ``df.pl``; return to Spark surface via :attr:`spark`.
    """

    __slots__ = ("_frame",)

    def __init__(self, frame: DataFrame) -> None:
        self._frame = frame

    @property
    def spark(self) -> DataFrame:
        """Underlying repark/PySpark-style :class:`~repark.dataframe.DataFrame`."""
        return self._frame

    def lazy(self) -> PolarsFrame:
        """No-op (plans are already lazy); returns self."""
        return self

    def select(self, *exprs: Column | str) -> PolarsFrame:
        return PolarsFrame(self._frame.select(*exprs))

    def with_columns(self, *exprs: Column, **named: Column) -> PolarsFrame:
        """Add/replace columns. Positional Columns need ``.alias``; kwargs become names."""
        frame = self._frame
        mapping: dict[str, Column] = {}
        for expression in exprs:
            _reject_polars_expr(expression, surface="with_columns")
            if not isinstance(expression, Column):
                raise PySparkTypeError(
                    f"with_columns expects Column, got {type(expression).__name__}"
                )
            name = expression.spark_display_part()
            # Prefer projection/alias name when present
            if expression._projection_name is not None:
                name = expression._projection_name
            if " AS " in name:
                name = name.split(" AS ")[-1]
            mapping[name] = expression
        for name, expression in named.items():
            _reject_polars_expr(expression, surface="with_columns")
            if not isinstance(expression, Column):
                raise PySparkTypeError(
                    f"with_columns kwargs must be Column, got {type(expression).__name__}"
                )
            mapping[name] = expression
        if not mapping:
            return PolarsFrame(frame)
        return PolarsFrame(frame.with_columns(mapping))

    def filter(self, predicate: Column | str) -> PolarsFrame:
        _reject_polars_expr(predicate, surface="filter")
        return PolarsFrame(self._frame.filter(predicate))

    def sort(
        self,
        *by: str | Column,
        descending: bool | list[bool] = False,
        nulls_last: bool = False,
    ) -> PolarsFrame:
        """Sort rows with POLARS null semantics: nulls first unless ``nulls_last=True``,
        regardless of direction (Spark ties null placement to direction — divergence honored
        here by marking every sort key explicitly)."""
        if not by:
            raise PySparkValueError("sort requires at least one column")
        flags = [descending] * len(by) if isinstance(descending, bool) else list(descending)
        if len(flags) != len(by):
            raise PySparkValueError(f"sort got {len(by)} columns but {len(flags)} descending flags")
        keys = []
        for column, desc_flag in zip(by, flags, strict=True):
            base = col(column) if isinstance(column, str) else column
            _reject_polars_expr(base, surface="sort")
            # Polars places nulls UNIFORMLY (first unless nulls_last), decoupled from
            # direction; the engine couples them (Spark rule). Emulate with an interleaved
            # null-indicator key: indicator ascending==nulls_last puts the null group where
            # polars puts it, and the value key then orders within each group.
            indicator = base.is_null()
            keys.append(_sort_key(indicator, ascending=nulls_last))
            keys.append(_sort_key(base, ascending=not desc_flag))
        return PolarsFrame(self._frame.order_by(*keys))

    def group_by(self, *by: str | Column) -> _PolarsGrouped:
        return _PolarsGrouped(self._frame.group_by(*by))

    def join(
        self,
        other: PolarsFrame | DataFrame,
        on: str | list[str] | None = None,
        how: str = "inner",
        *,
        left_on: str | None = None,
        right_on: str | None = None,
    ) -> PolarsFrame:
        """Join. Key-coalescing / suffix behavior follows Spark (divergence disclosed)."""
        from repark.spark.dataframe import DataFrame

        right = other.spark if isinstance(other, PolarsFrame) else other
        if not isinstance(right, DataFrame):
            raise PySparkTypeError("join other must be PolarsFrame or DataFrame")
        if how not in {"inner", "left", "left_outer"}:
            raise PySparkValueError(f"join how={how!r} not supported on repark (inner/left only)")
        join_kw = "LEFT JOIN" if how in {"left", "left_outer"} else "INNER JOIN"
        import uuid

        left_view = f"__rp_jl_{uuid.uuid4().hex}"
        right_view = f"__rp_jr_{uuid.uuid4().hex}"
        session = self._frame._session
        # Plan-stable MIA snapshots (combine C7-Q-002) — same as DataFrame.set-ops /
        # crossJoin. ``create_or_replace_temp_view`` → ``_native_for_registration`` is
        # action-like and would re-run a post-prepare mapInArrow UDF, diverging from
        # DataFrame.join (``_plan()``).
        self._frame._ensure_alive()
        right._ensure_alive()

        # === r23 QI1: idents ===
        # Polars join keys: bare-only validation (pre-existing) + always-quote SSOT.
        from repark.spark._idents import is_plain_ident
        from repark.spark._idents import quote_ident as _quote_ident_ssot

        def quote_ident(name: str) -> str:
            if not is_plain_ident(name):
                raise PySparkValueError(f"join column must be a bare SQL identifier, got {name!r}")
            return _quote_ident_ssot(name)

        try:
            session.create_or_replace_temp_view(left_view, self._frame._plan())
            right._session.create_or_replace_temp_view(right_view, right._plan())
            if on is None and left_on is not None and right_on is not None:
                on_sql = (
                    f"{left_view}.{quote_ident(left_on)} = {right_view}.{quote_ident(right_on)}"
                )
                # Keep right key (polars/Spark condition-join; octo C2-L-004).
                right_extra = list(right.columns)
            elif on is not None:
                on_list = [on] if isinstance(on, str) else list(on)
                on_sql = " AND ".join(
                    f"{left_view}.{quote_ident(key)} = {right_view}.{quote_ident(key)}"
                    for key in on_list
                )
                right_extra = [c for c in right.columns if c not in on_list]
            else:
                raise PySparkValueError("join requires on= or left_on/right_on")
            select_right = ", ".join(
                f"{right_view}.{quote_ident(c)} AS {quote_ident(c)}" for c in right_extra
            )
            select_clause = f"{left_view}.*" + (f", {select_right}" if select_right else "")
            sql = f"SELECT {select_clause} FROM {left_view} {join_kw} {right_view} ON {on_sql}"
            planned = session.sql(sql)
            return PolarsFrame(DataFrame(planned, session, self._frame._alive_token))
        finally:
            # Drop join staging views (octo C1-SEC-003); plan already holds MemTable/scan.
            session.drop_temp_view(left_view)
            session.drop_temp_view(right_view)

    def rename(self, mapping: dict[str, str]) -> PolarsFrame:
        return PolarsFrame(self._frame.with_columns_renamed(mapping))

    def unique(
        self,
        subset: str | list[str] | None = None,
        keep: str = "any",
    ) -> PolarsFrame:
        """Drop duplicate rows. ``keep`` is accepted; engine keeps an arbitrary row (disclosed)."""
        _ = keep
        if subset is None:
            return PolarsFrame(self._frame.drop_duplicates())
        if isinstance(subset, str):
            subset = [subset]
        return PolarsFrame(self._frame.drop_duplicates(list(subset)))

    def drop_nulls(self, subset: str | list[str] | None = None) -> PolarsFrame:
        return PolarsFrame(self._frame.dropna(subset=subset))

    def head(self, n: int = 5) -> PolarsFrame:
        return PolarsFrame(self._frame.limit(n))

    def limit(self, n: int) -> PolarsFrame:
        return PolarsFrame(self._frame.limit(n))

    def null_count(self) -> PolarsFrame:
        """Per-column null counts via SQL aggregates (plan-only)."""
        from repark.spark import functions as functions_api

        cols = self._frame.columns
        if not cols:
            raise PySparkValueError("null_count on a zero-column frame is undefined")
        aggs = [
            functions_api.sum(
                functions_api.when(
                    functions_api.col(name).isNull(), functions_api.lit(1)
                ).otherwise(functions_api.lit(0))
            ).alias(name)
            for name in cols
        ]
        return PolarsFrame(self._frame.agg(*aggs))

    def fill_null(
        self,
        value: Any = None,
        strategy: str | None = None,
    ) -> PolarsFrame:
        """Fill nulls with a scalar ``value`` only (forward/backward OUT — greylight B6)."""
        if strategy is not None:
            from repark.errors import UnsupportedOperationException

            raise UnsupportedOperationException(
                f"fill_null(strategy={strategy!r}) is not supported "
                "(forward/backward OUT; use fill_null(value=...); disclosed R-POLARS-NS)"
            )
        if value is None:
            raise PySparkValueError("fill_null requires value= (strategy forms OUT)")
        return PolarsFrame(self._frame.fillna(value))

    def with_row_index(self, name: str = "index", offset: int = 0) -> PolarsFrame:
        """Stretch: row numbers via SQL ``row_number`` over an ordered frame.

        Requires a stable order (uses all columns). ``offset`` shifts the starting index
        (polars default 0).
        """
        from repark.spark.functions import row_number
        from repark.spark.window import Window

        order_cols = list(self._frame.columns)
        if not order_cols:
            raise PySparkValueError("with_row_index on a zero-column frame is undefined")
        window = Window.orderBy(*order_cols)
        indexed = self._frame.with_column(name, row_number().over(window) + spark_lit(offset - 1))
        return PolarsFrame(indexed)

    def collect(self) -> Any:
        """Materialize to a real ``polars.DataFrame`` via Arrow (lazy import)."""
        try:
            import polars as pl
        except ImportError as error:
            raise ImportError(
                "repark.polars.collect() requires the optional polars package. "
                "Install with: pip install 'polars>=1.0'  (or repark[polars] when packaging allows)"
            ) from error
        return pl.from_arrow(self._frame.to_arrow())


class _PolarsGrouped:
    def __init__(self, grouped: Any) -> None:
        self._grouped = grouped

    def agg(self, *exprs: Column) -> PolarsFrame:
        return PolarsFrame(self._grouped.agg(*exprs))


class StringNameSpace:
    """Polars-style ``.str`` namespace lowering to repark ``functions`` (R-POLARS-NS)."""

    __slots__ = ("_column",)

    def __init__(self, column: Column) -> None:
        self._column = column

    def to_uppercase(self) -> Column:
        from repark.spark.functions import upper

        return upper(self._column)

    def to_lowercase(self) -> Column:
        from repark.spark.functions import lower

        return lower(self._column)

    def strip_chars(self) -> Column:
        from repark.spark.functions import trim

        return trim(self._column)

    def starts_with(self, prefix: str) -> Column:
        """Whether the string starts with ``prefix`` (DF ``starts_with`` via call_scalar)."""
        from repark.spark.functions import _scalar, lit

        if not isinstance(prefix, str):
            raise PySparkTypeError(
                f"str.starts_with prefix must be str, got {type(prefix).__name__}"
            )
        return _scalar("starts_with", self._column, lit(prefix))

    def ends_with(self, suffix: str) -> Column:
        """Whether the string ends with ``suffix`` (DF ``ends_with`` via call_scalar)."""
        from repark.spark.functions import _scalar, lit

        if not isinstance(suffix, str):
            raise PySparkTypeError(f"str.ends_with suffix must be str, got {type(suffix).__name__}")
        return _scalar("ends_with", self._column, lit(suffix))

    def contains(self, pattern: str) -> Column:
        """Whether the string contains ``pattern`` (DF ``contains`` via call_scalar)."""
        from repark.spark.functions import _scalar, lit

        if not isinstance(pattern, str):
            raise PySparkTypeError(
                f"str.contains pattern must be str, got {type(pattern).__name__}"
            )
        return _scalar("contains", self._column, lit(pattern))

    def replace(self, pattern: str, value: str) -> Column:
        from repark.spark.functions import regexp_replace

        return regexp_replace(self._column, pattern, value)

    def replace_all(self, pattern: str, value: str) -> Column:
        return self.replace(pattern, value)

    def slice(self, offset: int, length: int | None = None) -> Column:
        """Substring slice (polars-style 0-based ``offset`` → DF 1-based ``substr``)."""
        from repark.spark.functions import _scalar, lit

        if not isinstance(offset, int) or isinstance(offset, bool):
            raise PySparkTypeError(f"str.slice offset must be int, got {type(offset).__name__}")
        # Polars str.slice is 0-based; DataFusion substr is 1-based.
        position = offset + 1 if offset >= 0 else offset
        if length is None:
            return _scalar("substr", self._column, lit(position))
        if not isinstance(length, int) or isinstance(length, bool):
            raise PySparkTypeError(f"str.slice length must be int, got {type(length).__name__}")
        return _scalar("substr", self._column, lit(position), lit(length))

    def len_chars(self) -> Column:
        from repark.spark.functions import length

        return length(self._column)

    def zfill(self, length: int) -> Column:
        from repark.spark.functions import lpad

        return lpad(self._column, length, "0")

    def pad_start(self, length: int, fill_char: str = " ") -> Column:
        from repark.spark.functions import lpad

        return lpad(self._column, length, fill_char)

    def pad_end(self, length: int, fill_char: str = " ") -> Column:
        from repark.spark.functions import rpad

        return rpad(self._column, length, fill_char)

    def split(self, by: str) -> Column:
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "str.split is not supported yet (engine functions.split gap; disclosed R-POLARS-NS)"
        )


class DatetimeNameSpace:
    """Polars-style ``.dt`` namespace lowering to repark date functions (R-POLARS-NS)."""

    __slots__ = ("_column",)

    def __init__(self, column: Column) -> None:
        self._column = column

    def year(self) -> Column:
        from repark.spark.functions import year

        return year(self._column)

    def month(self) -> Column:
        from repark.spark.functions import month

        return month(self._column)

    def day(self) -> Column:
        from repark.spark.functions import dayofmonth

        return dayofmonth(self._column)

    def hour(self) -> Column:
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "dt.hour requires R-FN-BATCH3 hour(); not on this branch (disclosed R-POLARS-NS)"
        )

    def minute(self) -> Column:
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException("dt.minute not on this branch (disclosed R-POLARS-NS)")

    def second(self) -> Column:
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException("dt.second not on this branch (disclosed R-POLARS-NS)")

    def weekday(self) -> Column:
        from repark.spark.functions import weekday

        return weekday(self._column)

    def ordinal_day(self) -> Column:
        from repark.spark.functions import dayofyear

        return dayofyear(self._column)

    def truncate(self, every: str) -> Column:
        from repark.spark.functions import date_trunc

        return date_trunc(every, self._column)

    def offset_by(self, by: str) -> Column:
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "dt.offset_by is not supported yet (disclosed R-POLARS-NS)"
        )
