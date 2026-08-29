"""Actions and exports for the DataFrame missing-data API."""

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
        """Replace NULL values using Spark's scalar or mapping rules.

        Args:
            value: A scalar or a mapping from column names to replacement values.
            subset: Optional column name, list, or tuple for scalar replacement.

        Returns:
            A lazy DataFrame with matching NULL values replaced.

        Raises:
            PySparkTypeError: If ``value`` has an unsupported type.
            AnalysisException: If a mapping names an unknown column.

        Notes:
            Numeric replacement preserves integer width and truncates toward zero.
            Mapping form ignores ``subset`` and follows Spark error behavior.
        """
        # Spark rejects unsupported values with a stable error class.
        if isinstance(value, dict):
            # Mapping form ignores subset, but still validates its type.
            _ = _normalize_subset(subset, accept_str=True, allowed_phrase="a list or tuple")
            return self._fill_dict(value)
        # bool is an int subclass, but Spark accepts it as its own scalar family.
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
        """Return native type keys by engine field name for width-preserving casts."""
        return {
            name: type_key for name, type_key, _ in self._dataframe._inner.logical_schema_fields()
        }

    def _fill_expr_for_bound(self, bound: Column, value: Any, type_key: str) -> Column:
        """Build a fill expression while preserving origin identity across projections."""
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
        """Build a fill expression that preserves integer width and mixed-case field binding.

        Spark truncates numeric replacements for integer columns. A quoted schema bind keeps
        requested-spelling projections resolvable.
        """
        return self._fill_expr_for_bound(
            self._dataframe._bind_schema_column(column_name), value, type_key
        )

    def _fill_dict(self, replacements: dict[str, Any]) -> DataFrame:
        """Fill mapping entries in one projection."""
        known = set(self._dataframe.columns)
        for column_name in replacements:
            if column_name not in known:
                raise AnalysisException(
                    f"A column with name `{column_name}` cannot be resolved for fillna; "
                    f"available columns: {sorted(known)}"
                )
        type_keys = self._type_keys()
        # One projection preserves the frame's display and engine-name pairing.
        projections: list[Column | str] = []
        for bound in self._dataframe._iter_bound_columns():
            display = bound._projection_name or bound.spark_display_part()
            engine = None
            # Match the bound display name to its engine field.
            if bound._sql_expr is not None and bound._sql_expr.startswith('"'):
                engine = bound._sql_expr.strip('"').replace('""', '"')
            type_key = type_keys.get(engine or display, type_keys.get(display, ""))
            if display in replacements:
                projections.append(
                    self._fill_expr_for_bound(bound, replacements[display], type_key)
                )
            else:
                projections.append(bound)
        return self._dataframe.select(*projections)

    def _fill_scalar(self, value: Any, subset: list[str] | None) -> DataFrame:
        """Fill scalar-compatible columns in one projection."""
        target_columns = set(self._columns_for_fill_value(value, subset))
        type_keys = self._type_keys()
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
        """Return columns whose type family accepts ``value``.

        Numeric, boolean, and string values do not cross families. Check ``bool`` before ``int``
        because Python treats ``bool`` as an integer subclass.
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
            # Include every numeric width so long columns are not skipped.
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
        # Multi-name frames need display names for target matching and engine names for types.
        frame = self._dataframe
        if frame._display_names is not None and frame._engine_names is not None:
            # The display overlay must not drive type lookup.
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
        # Match subset names against the display overlay, including ambiguous sides.
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
        # Quoted binds preserve mixed-case field resolution.
        not_null_flags = [column.isNotNull() for column in bound_cols]
        if thresh is not None:
            non_null_count = not_null_flags[0].cast("int")
            for flag in not_null_flags[1:]:
                non_null_count = non_null_count + flag.cast("int")
            predicate = non_null_count >= thresh
        elif how == "all":
            predicate = not_null_flags[0]
            for flag in not_null_flags[1:]:
                predicate = predicate | flag
        else:
            predicate = not_null_flags[0]
            for flag in not_null_flags[1:]:
                predicate = predicate & flag
        return self._dataframe.filter(predicate)
