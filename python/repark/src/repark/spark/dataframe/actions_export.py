"""Actions/export region — DataFrameNaFunctions (r27 T0; technique A)."""

from __future__ import annotations

import logging
from typing import Any

from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkTypeError,
    PySparkValueError,
)
from repark.spark.column import Column
from repark.spark.dataframe.core import DataFrame, _normalize_subset
from repark.spark.types import DataType, StructField, StructType

logger = logging.getLogger("repark.spark.dataframe")


class DataFrameNaFunctions:
    """The missing-data surface (PySpark ``DataFrame.na``): :meth:`fill` and :meth:`drop`."""

    __slots__ = ("_dataframe",)

    def __init__(self, dataframe: DataFrame) -> None:
        """Bind the source DataFrame."""
        self._dataframe = dataframe

    def fill(
        self,
        value: Any,
        subset: str | list[str] | tuple[str, ...] | None = None,
    ) -> DataFrame:
        """Replace NULLs (PySpark ``DataFrame.na.fill``).

        ``value`` is a scalar (fills every column whose type matches the value's family — numeric,
        boolean, or string — restricted to ``subset`` if given) or a ``{column: value}`` dict (fills
        each named column; ``subset`` is ignored, and an unknown column raises an
        :class:`~repark.errors.AnalysisException`, Spark parity). A numeric value filled into an
        integer column keeps that column's exact type and fills the **truncated** value
        (``fillna(2.5)`` into a bigint → ``2``, still bigint — Spark parity), never widening the
        column to double. ``subset`` accepts a ``str`` (wrapped, not char-iterated), list, or tuple.

        C4: refuse non-scalar / non-dict ``value`` with Spark's
        ``NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR`` (Apache ``test_fillna`` list pin).
        """
        # === r20 C4: fillna value/subset errorClass parity ===
        if isinstance(value, dict):
            # subset ignored for dict form (Spark parity); still validate type if provided.
            _ = _normalize_subset(subset, accept_str=True, allowed_phrase="a list or tuple")
            return self._fill_dict(value)
        # bool is an int subclass — accepted; list/tuple/None/object → Spark error class.
        if not isinstance(value, (bool, int, float, str)):
            raise PySparkTypeError(
                errorClass="NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_STR",
                messageParameters={
                    "arg_name": "value",
                    "arg_type": type(value).__name__,
                },
            )
        names = _normalize_subset(subset, accept_str=True, allowed_phrase="a list or tuple")
        return self._fill_scalar(value, names)

    def _type_keys(self) -> dict[str, str]:
        """Map each source column to its native logical type key (``int`` / ``long`` / ``double`` /
        ``decimal(p,s)`` / …) — the exact width the fill literal casts to.

        H1: keys are **engine** field names (unique). Pair with display names via
        ``_engine_names`` when multi-name identity is present.
        """
        return {
            name: type_key for name, type_key, _ in self._dataframe._inner.logical_schema_fields()
        }

    def _fill_expr_for_bound(self, bound: Column, value: Any, type_key: str) -> Column:
        """``coalesce(bound, lit)`` for one already-bound column (H1 multi-name safe).

        Preserves origin on the result so ``select`` multi-name identity still treats
        colliding display names as origin-qualified (octo H1-C2-003).
        """
        from repark.spark import functions as F  # noqa: N812 — PySpark idiom

        literal = F.lit(value)
        if (
            type_key in ("int", "long")
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
        ):
            literal = literal.cast(type_key)
        filled = F.coalesce(bound, literal)
        display = bound._projection_name or bound.spark_display_part()
        if bound._origin_plan_id is None or bound._origin_field is None:
            return filled.alias(display) if display else filled
        return Column(
            filled._inner.alias(display),
            spark_display=display,
            projection_name=display,
            stable_name=True,
            has_free_attribute=True,
            sql_expr=filled._sql_expr,
            join_sql_expr=(f"coalesce({bound.join_sql_part()}, {literal.join_sql_part()})"),
            origin_plan_id=bound._origin_plan_id,
            origin_field=bound._origin_field,
        )

    def _fill_expr(self, column_name: str, value: Any, type_key: str) -> Column:
        """The ``coalesce(col, lit)`` fill for one column, preserving an integer column's width.

        Spark's ``na.fill`` casts the fill value to the column's type: filling a numeric value into
        an ``int`` / ``long`` column keeps that width and **truncates** a float toward zero
        (``2.5`` → ``2``). We cast the literal to the column's exact integer type (``"int"`` /
        ``"long"``) so ``coalesce`` stays that type, instead of DataFusion widening the whole column
        to double. Non-integer targets (double / decimal / string / boolean) keep the plain literal.
        The column ref is a quoted schema bind so mixed-case fields after a requested-spelling
        projection still resolve (octo r4 C3-L-008).
        """
        return self._fill_expr_for_bound(
            self._dataframe._bind_schema_column(column_name), value, type_key
        )

    def _fill_dict(self, replacements: dict[str, Any]) -> DataFrame:
        """Fill each ``{column: value}`` entry, width-preserving per the column's type."""
        known = set(self._dataframe.columns)
        for column_name in replacements:
            if column_name not in known:
                raise AnalysisException(
                    f"A column with name `{column_name}` cannot be resolved for fillna; "
                    f"available columns: {sorted(known)}"
                )
        type_keys = self._type_keys()
        # R-FACADE-HYGIENE (W7): multi-col fill = ONE projection (not N withColumn).
        # H1: bind by engine/display pairs so multi-name frames do not AMBIGUOUS (C2-003).
        projections: list[Column | str] = []
        for bound in self._dataframe._iter_bound_columns():
            display = bound._projection_name or bound.spark_display_part()
            engine = None
            # Match bound to engine field via quoted sql_expr (H1 multi-name).
            if bound._sql_expr is not None and bound._sql_expr.startswith('"'):
                engine = bound._sql_expr.strip('"').replace('""', '"')
            type_key = type_keys.get(engine or display, type_keys.get(display, ""))
            if display in replacements:
                # _fill_expr_for_bound already names the projection.
                projections.append(
                    self._fill_expr_for_bound(bound, replacements[display], type_key)
                )
            else:
                projections.append(bound)
        return self._dataframe.select(*projections)

    def _fill_scalar(self, value: Any, subset: list[str] | None) -> DataFrame:
        """Fill each type-matching column (within ``subset`` if set) with the scalar ``value``."""
        target_columns = set(self._columns_for_fill_value(value, subset))
        type_keys = self._type_keys()
        # H1: one projection over engine-bound columns (avoids withColumn + display/engine skew).
        projections: list[Column] = []
        for bound in self._dataframe._iter_bound_columns():
            display = bound._projection_name or bound.spark_display_part()
            engine = None
            if bound._sql_expr is not None and bound._sql_expr.startswith('"'):
                engine = bound._sql_expr.strip('"').replace('""', '"')
            type_key = type_keys.get(engine or display, type_keys.get(display, ""))
            if display in target_columns:
                projections.append(self._fill_expr_for_bound(bound, value, type_key))
            else:
                projections.append(bound)
        return self._dataframe.select(*projections)

    def _columns_for_fill_value(self, value: Any, subset: list[str] | None) -> list[str]:
        """Return the columns a scalar fill applies to: those whose type-family matches ``value``.

        Spark fills a numeric value into numeric columns only, a boolean into boolean columns, and a
        string into string columns — never across families. ``bool`` is checked before ``int``
        because Python's ``bool`` is an ``int`` subclass.
        """
        from repark.spark.types import (
            BooleanType,
            ByteType,
            DecimalType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            ShortType,
            StringType,
        )

        if isinstance(value, bool):
            allowed: tuple[type[DataType], ...] = (BooleanType,)
        elif isinstance(value, (int, float)):
            # The full numeric family: X2 split Arrow Int64→LongType (and Int8/16/Float32
            # to their own classes), so matching IntegerType alone silently skipped
            # bigint columns (r16 combine S1).
            allowed = (
                ByteType,
                ShortType,
                IntegerType,
                LongType,
                FloatType,
                DoubleType,
                DecimalType,
            )
        elif isinstance(value, str):
            allowed = (StringType,)
        else:
            raise PySparkTypeError(
                f"fillna value must be int, float, bool, str, or dict; got {type(value).__name__}"
            )
        # H1: schema.fields use engine names on multi-name frames; pair with display names
        # so fillna target list matches user-facing ``columns`` (octo H1-C2-003).
        frame = self._dataframe
        if frame._display_names is not None and frame._engine_names is not None:
            # Pair by position with logical engine schema (display overlay on .schema
            # must not key type lookup — octo H1-C6).
            engine_types = {
                name: type_key for name, type_key, _ in frame._inner.logical_schema_fields()
            }
            from repark.spark.types import (
                BooleanType,
                DoubleType,
                FloatType,
                IntegerType,
                LongType,
                StringType,
            )

            key_to_cls = {
                "int": IntegerType,
                "long": LongType,
                "double": DoubleType,
                "float": FloatType,
                "boolean": BooleanType,
                "string": StringType,
            }
            names_out: list[str] = []
            for display, engine in zip(frame._display_names, frame._engine_names, strict=True):
                if subset is not None and display not in subset:
                    continue
                type_key = engine_types.get(engine, "")
                type_cls = key_to_cls.get(type_key.split("(")[0])
                if type_cls is not None and issubclass(type_cls, allowed):
                    names_out.append(display)
            return names_out
        fields = frame.schema.fields
        if subset is not None:
            subset_set = set(subset)
            fields = [field for field in fields if field.name in subset_set]
        return [field.name for field in fields if isinstance(field.dataType, allowed)]

    def drop(
        self,
        how: str = "any",
        thresh: int | None = None,
        subset: str | list[str] | tuple[str, ...] | None = None,
    ) -> DataFrame:
        """Drop rows with NULLs (PySpark ``DataFrame.na.drop``).

        ``how='any'`` drops a row with any NULL in the considered columns; ``how='all'`` drops a row
        only when every considered column is NULL. ``thresh`` (a non-NULL-count floor) overrides
        ``how`` when set. ``subset`` limits which columns are considered (default: all) and accepts
        a ``str`` (wrapped, not char-iterated), list, or tuple.
        """
        if how not in ("any", "all"):
            raise PySparkValueError(f"how must be 'any' or 'all', got {how!r}")
        names = _normalize_subset(
            subset,
            accept_str=True,
            allowed_phrase="a list, str or tuple",
            error_class="NOT_LIST_OR_STR_OR_TUPLE",
        )
        if names is not None and not names:
            return self._dataframe
        # H1: multi-name frames bind by engine/display pairs (octo H1-C2-002). Subset by
        # display name still works; ambiguous subset names include every matching side.
        if names is None:
            bound_cols = self._dataframe._iter_bound_columns()
        elif (
            self._dataframe._display_names is not None and self._dataframe._engine_names is not None
        ):
            want = set(names)
            bound_cols = [
                self._dataframe._bind_engine_display_column(display, engine)
                for display, engine in zip(
                    self._dataframe._display_names,
                    self._dataframe._engine_names,
                    strict=True,
                )
                if display in want
            ]
        else:
            bound_cols = [self._dataframe._bind_schema_column(name) for name in names]
        if not bound_cols:
            return self._dataframe
        # Quoted schema bind (not bare ``F.col``) for mixed-case fields (octo r4 C3-L-008).
        not_null_flags = [column.isNotNull() for column in bound_cols]
        if thresh is not None:
            # Keep rows with at least `thresh` non-NULL values: sum the boolean flags cast to int.
            non_null_count = not_null_flags[0].cast("int")
            for flag in not_null_flags[1:]:
                non_null_count = non_null_count + flag.cast("int")
            predicate = non_null_count >= thresh
        elif how == "all":
            # Drop only all-NULL rows → keep a row with at least one non-NULL (OR of the flags).
            predicate = not_null_flags[0]
            for flag in not_null_flags[1:]:
                predicate = predicate | flag
        else:  # how == "any": keep only fully-non-NULL rows (AND of the flags).
            predicate = not_null_flags[0]
            for flag in not_null_flags[1:]:
                predicate = predicate & flag
        return self._dataframe.filter(predicate)
