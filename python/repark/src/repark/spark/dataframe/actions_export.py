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
from repark.spark.dataframe.replace_expr import _NO_VALUE
from repark.spark.types import DataType, StructField, StructType

logger = logging.getLogger("repark.spark.dataframe")


class DataFrameNaFunctions:
    """The missing-data surface (PySpark ``DataFrame.na``): fill, drop, and replace."""

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
        """Return native type keys by engine field name for schema readers."""
        return {
            name: type_key for name, type_key, _ in self._dataframe._inner.logical_schema_fields()
        }

    def _fill_expr_for_bound(
        self, bound: Column, value: Any, field_name: str, fallback_name: str
    ) -> Column:
        """Build a fill expression while preserving origin identity across projections."""
        from repark import _native
        from repark.spark import functions as F  # noqa: N812 — PySpark idiom

        probe = F.lit(value)
        filled_inner, cast_literal = _native.fill_expr_for_column(
            self._dataframe._inner,
            bound._inner,
            field_name,
            fallback_name,
            probe._inner,
            (
                probe.spark_display_part(),
                probe.sql_expr_part(),
                probe.join_sql_part(),
            ),
        )
        if cast_literal is None:
            literal = probe
        else:
            lit_inner, (display, sql, join) = cast_literal
            literal = Column(
                lit_inner,
                spark_display=display,
                projection_name=display,
                sql_expr=sql,
                join_sql_expr=join,
                is_foldable=True,
            )
        filled = F.coalesce(bound, literal)
        display = bound._projection_name or bound.spark_display_part()
        if bound._origin_plan_id is None or bound._origin_field is None:
            return filled.alias(display) if display else filled
        return Column(
            filled_inner.alias(display),
            spark_display=display,
            projection_name=display,
            stable_name=True,
            has_free_attribute=True,
            sql_expr=filled._sql_expr,
            join_sql_expr=(f"coalesce({bound.join_sql_part()}, {literal.join_sql_part()})"),
            origin_plan_id=bound._origin_plan_id,
            origin_field=bound._origin_field,
        )

    def _fill_dict(self, replacements: dict[str, Any]) -> DataFrame:
        """Fill mapping entries in one projection."""
        from repark.spark import subset_resolve

        bounds = self._dataframe._iter_bound_columns()
        bindings = subset_resolve._bindings(self._dataframe)
        values: dict[int, Any] = {}
        if bindings is None:
            known = set(self._dataframe.columns)
            for column_name in replacements:
                if column_name not in known:
                    raise AnalysisException(
                        f"A column with name `{column_name}` cannot be resolved for fillna; "
                        f"available columns: {sorted(known)}"
                    )
            for position, bound in enumerate(bounds):
                display = bound._projection_name or bound.spark_display_part()
                if display in replacements:
                    values[position] = replacements[display]
        else:
            for column_name, replacement in replacements.items():
                for position in subset_resolve._bound_subset_positions(
                    self._dataframe, column_name, bindings
                ):
                    values[position] = replacement
        projections: list[Column | str] = []
        for position, bound in enumerate(bounds):
            display = bound._projection_name or bound.spark_display_part()
            if position not in values:
                projections.append(bound)
                continue
            engine = None
            if bound._sql_expr is not None and bound._sql_expr.startswith('"'):
                engine = bound._sql_expr.strip('"').replace('""', '"')
            projections.append(
                self._fill_expr_for_bound(bound, values[position], engine or display, display)
            )
        return self._dataframe.select(*projections)

    def _fill_scalar(self, value: Any, subset: list[str] | None) -> DataFrame:
        """Fill scalar-compatible columns in one projection."""
        target_positions = self._columns_for_fill_value(value, subset)
        projections: list[Column] = []
        for position, bound in enumerate(self._dataframe._iter_bound_columns()):
            display = bound._projection_name or bound.spark_display_part()
            engine = None
            if bound._sql_expr is not None and bound._sql_expr.startswith('"'):
                engine = bound._sql_expr.strip('"').replace('""', '"')
            if position in target_positions:
                projections.append(
                    self._fill_expr_for_bound(bound, value, engine or display, display)
                )
            else:
                projections.append(bound)
        return self._dataframe.select(*projections)

    def _columns_for_fill_value(self, value: Any, subset: list[str] | None) -> set[int]:
        """Return positions whose type family accepts ``value``.

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
        from repark.spark import subset_resolve

        frame = self._dataframe
        targets: set[int] | None = None
        if subset is not None:
            bindings = subset_resolve._bindings(frame)
            if bindings is None:
                return self._columns_for_fill_value_legacy(allowed, subset)
            targets = set()
            for key in subset:
                targets.update(subset_resolve._bound_subset_positions(frame, key, bindings))
        if frame._display_names is not None and frame._engine_names is not None:
            engine_types = {
                name: type_key for name, type_key, _ in frame._inner.logical_schema_fields()
            }
            from repark.spark.types import (
                BooleanType,
                ByteType,
                DoubleType,
                FloatType,
                IntegerType,
                LongType,
                ShortType,
                StringType,
            )

            key_to_cls = {
                "byte": ByteType,
                "short": ShortType,
                "int": IntegerType,
                "long": LongType,
                "double": DoubleType,
                "float": FloatType,
                "boolean": BooleanType,
                "string": StringType,
            }
            positions_out: set[int] = set()
            pairs = zip(frame._display_names, frame._engine_names, strict=True)
            for position, (_display, engine) in enumerate(pairs):
                if targets is not None and position not in targets:
                    continue
                type_key = engine_types.get(engine, "")
                type_cls = key_to_cls.get(type_key.split("(")[0])
                if type_cls is not None and issubclass(type_cls, allowed):
                    positions_out.add(position)
            return positions_out
        fields = frame.schema.fields
        return {
            position
            for position, field in enumerate(fields)
            if (targets is None or position in targets) and isinstance(field.dataType, allowed)
        }

    def _columns_for_fill_value_legacy(
        self, allowed: tuple[type[DataType], ...], subset: list[str]
    ) -> set[int]:
        frame = self._dataframe
        names_out: list[str] = []
        if frame._display_names is not None and frame._engine_names is not None:
            engine_types = {
                name: type_key for name, type_key, _ in frame._inner.logical_schema_fields()
            }
            from repark.spark.types import (
                BooleanType,
                ByteType,
                DoubleType,
                FloatType,
                IntegerType,
                LongType,
                ShortType,
                StringType,
            )

            key_to_cls = {
                "byte": ByteType,
                "short": ShortType,
                "int": IntegerType,
                "long": LongType,
                "double": DoubleType,
                "float": FloatType,
                "boolean": BooleanType,
                "string": StringType,
            }
            for display, engine in zip(frame._display_names, frame._engine_names, strict=True):
                if display not in subset:
                    continue
                type_key = engine_types.get(engine, "")
                type_cls = key_to_cls.get(type_key.split("(")[0])
                if type_cls is not None and issubclass(type_cls, allowed):
                    names_out.append(display)
        else:
            fields = [field for field in frame.schema.fields if field.name in set(subset)]
            names_out = [field.name for field in fields if isinstance(field.dataType, allowed)]
        wanted = set(names_out)
        return {
            position
            for position, bound in enumerate(frame._iter_bound_columns())
            if (bound._projection_name or bound.spark_display_part()) in wanted
        }

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
        from repark.spark import subset_resolve

        if names is None:
            bound_cols = self._dataframe._iter_bound_columns()
        else:
            bindings = subset_resolve._bindings(self._dataframe)
            if bindings is None:
                bound_cols = self._bound_subset_columns_legacy(names)
            else:
                bounds = self._dataframe._iter_bound_columns()
                bound_cols = []
                for key in names:
                    for position in subset_resolve._bound_subset_positions(
                        self._dataframe, key, bindings
                    ):
                        bound_cols.append(bounds[position])
        if not bound_cols:
            return self._dataframe
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

    def _bound_subset_columns_legacy(self, names: list[str]) -> list[Column]:
        if self._dataframe._display_names is not None and self._dataframe._engine_names is not None:
            want = set(names)
            return [
                self._dataframe._bind_engine_display_column(display, engine)
                for display, engine in zip(
                    self._dataframe._display_names,
                    self._dataframe._engine_names,
                    strict=True,
                )
                if display in want
            ]
        return [self._dataframe._bind_schema_column(name) for name in names]

    def replace(
        self,
        to_replace: Any,
        value: Any = _NO_VALUE,
        subset: str | list[str] | tuple[str, ...] | None = None,
    ) -> DataFrame:
        """Replace value(s) exactly as ``DataFrame.replace``. pins: io-declared-1/C-004"""
        return self._dataframe.replace(to_replace, value, subset)
