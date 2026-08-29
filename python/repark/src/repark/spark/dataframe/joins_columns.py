"""Grouping, pivot, and pandas UDF support for the DataFrame facade."""

from __future__ import annotations

import contextlib
import functools
import logging
import math
import re
import traceback
import uuid
import warnings
from collections.abc import Callable, Iterator
from typing import Any, overload

from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkException,
    PySparkNotImplementedError,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark._idents import quote_ident as _quote_ident
from repark.spark._temp_views import scratch_view_name
from repark.spark.column import Column
from repark.spark.dataframe.core import (
    DataFrame,
    _coerce_map_in_arrow_schema,
    _global_agg_sql_parts,
    _is_numeric_type_key,
    _iter_apply_in_pandas_group_tables,
    _null_safe_equi_join_sql,
    _parse_count_distinct_simple_names,
    _parse_list_element_sql_type,
    _reject_partition_transform,
    _validate_apply_in_pandas_result_columns,
)
from repark.spark.row import Row
from repark.spark.types import DataType, StructField, StructType

logger = logging.getLogger("repark.spark.dataframe")


def _grouped_agg_pandas(pdf: Any, *, keys: list[str], specs: list[dict[str, Any]]) -> Any:
    """Return one output row for a GROUPED_AGG pandas UDF group."""
    try:
        import pandas as pd
    except ImportError as error:
        raise ImportError(
            "GROUPED_AGG pandas_udf requires pandas (pip install 'repark[pandas]')"
        ) from error

    row: dict[str, Any] = {}
    for key_name in keys:
        if key_name not in pdf.columns:
            raise PySparkException(
                f"GROUPED_AGG pandas_udf missing group key {key_name!r} in group frame"
            )
        row[key_name] = pdf[key_name].iloc[0] if len(pdf) > 0 else None
    for spec in specs:
        series_args: list[Any] = []
        for input_name in spec["input_inter_names"]:
            if input_name not in pdf.columns:
                raise PySparkException(
                    f"GROUPED_AGG pandas_udf input column missing from group frame: {input_name!r}"
                )
            series_args.append(pdf[input_name])
        try:
            value = spec["user_func"](*series_args)
        except PySparkException:
            raise
        except Exception as error:
            detail = traceback.format_exc()
            raise PySparkException(
                "GROUPED_AGG pandas_udf "
                f"{spec['function_name']!r} raised {type(error).__name__}: "
                f"{error}\n{detail}"
            ) from error
        if value is None:
            row[spec["out_name"]] = None
        elif isinstance(value, pd.Series):
            raise PySparkException(
                f"GROUPED_AGG pandas_udf {spec['function_name']!r} must return a "
                f"scalar; got pandas.Series (length {len(value)})"
            )
        elif isinstance(value, pd.DataFrame):
            raise PySparkException(
                f"GROUPED_AGG pandas_udf {spec['function_name']!r} must return a "
                f"scalar; got pandas.DataFrame"
            )
        else:
            row[spec["out_name"]] = value
    return pd.DataFrame([row], columns=[*keys, *[spec["out_name"] for spec in specs]])


def _apply_in_pandas_arrow_batches(
    input_batches: Iterator[Any],
    *,
    user_func: Callable[[Any], Any],
    key_names: list[str],
    expected_names: list[str],
    expected_arrow: Any,
) -> Iterator[Any]:
    """Run an applyInPandas callback for each streamed Arrow group."""
    import pandas as pd
    import pyarrow as pa

    for group_table in _iter_apply_in_pandas_group_tables(input_batches, key_names):
        pdf = group_table.to_pandas()
        try:
            out_pdf = user_func(pdf)
        except PySparkException:
            raise
        except Exception as error:
            detail = traceback.format_exc()
            raise PySparkException(
                f"applyInPandas user function raised {type(error).__name__}: {error}\n{detail}"
            ) from error
        if out_pdf is None:
            raise PySparkException(
                "applyInPandas user function must return a pandas.DataFrame (got None)"
            )
        if not isinstance(out_pdf, pd.DataFrame):
            raise PySparkException(
                "applyInPandas user function must return a pandas.DataFrame; "
                f"got {type(out_pdf).__name__}"
            )
        # Validate names before casting so empty wrong frames cannot hide mismatches.
        _validate_apply_in_pandas_result_columns(out_pdf, expected_names)
        # Preserve the declared schema for Spark's accepted empty group result.
        if len(out_pdf) == 0 and len(out_pdf.columns) == 0:
            yield pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in expected_arrow],
                schema=expected_arrow,
            )
            continue
        try:
            out_table = pa.Table.from_pandas(out_pdf, schema=expected_arrow, preserve_index=False)
        except (
            pa.ArrowInvalid,
            pa.ArrowTypeError,
            pa.ArrowNotImplementedError,
            ValueError,
            TypeError,
            KeyError,
        ) as error:
            # Name conversion failures at the offending field instead of a later mismatch.
            error_text = str(error)
            if (
                "Conversion failed" in error_text
                or "not in range" in error_text
                or "Could not convert" in error_text
            ):
                raise PySparkException(
                    f"applyInPandas failed converting pandas output to declared schema: {error}"
                ) from error
            try:
                out_table = pa.Table.from_pandas(out_pdf, preserve_index=False)
            except Exception:
                raise PySparkException(
                    f"applyInPandas failed converting pandas output to Arrow: {error}"
                ) from error
        output_batches = out_table.to_batches()
        if not output_batches:
            # Keep zero-row output under the converted schema after name validation.
            yield pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in out_table.schema],
                schema=out_table.schema,
            )
        else:
            yield from output_batches


class GroupedData:
    """The result of :meth:`DataFrame.groupBy`.

    Finish with :meth:`agg` or a shortcut such as :meth:`count` or :meth:`sum`.
    Group columns lead the output, followed by aggregate columns.
    """

    __slots__ = (
        "_dataframe",
        "_group_columns",
        "_pivot_col",
        "_pivot_values",
        "_pivot_values_explicit",
        "_sql_group_clause",
    )

    def __init__(
        self,
        dataframe: DataFrame,
        group_columns: list[Column],
        sql_group_clause: str | None = None,
        *,
        pivot_col: str | None = None,
        pivot_values: list[Any] | None = None,
        pivot_values_explicit: bool = False,
    ) -> None:
        """Bind the source DataFrame and resolved grouping columns."""
        self._dataframe = dataframe
        self._group_columns = group_columns
        self._sql_group_clause = sql_group_clause
        self._pivot_col = pivot_col
        self._pivot_values = pivot_values
        self._pivot_values_explicit = pivot_values_explicit

    def agg(self, *exprs: Column | dict[str, str] | Any) -> DataFrame:
        """Aggregate groups with Column expressions or one function mapping.

        Simple name aggregates rebind against the source schema, so mixed-case projected fields
        resolve. Partition transforms and nested generators raise before plan construction.

        A pure GROUPED_AGG pandas UDF uses :meth:`applyInPandas`. A mixed UDF and builtin aggregate
        lowers the UDF side via :meth:`applyInPandas` and joins the native aggregate plan — never a
        Python-side merge; cube, rollup, grouping sets, and pivot use dedicated lowering paths.
        """
        from repark.spark.functions import PandasUDFColumn

        if any(isinstance(expr, PandasUDFColumn) for expr in exprs):
            return self._agg_via_pandas_udfs(exprs)

        if self._pivot_col is not None:
            return self._agg_via_pivot(*exprs)
        if self._sql_group_clause is not None:
            return self._agg_via_sql_group(*exprs)
        self._dataframe._prepare_for_plan()
        aggregate_columns = [
            self._rebind_simple_name_aggregate(column) for column in self._resolve_aggregates(exprs)
        ]
        for column in aggregate_columns:
            _reject_partition_transform(column)
            column._reject_nested_generator("agg")
        group_natives = [column._inner for column in self._group_columns]
        aggregate_natives = []
        for column in aggregate_columns:
            if column._agg_name is not None:
                aggregate_natives.append(column._inner.alias(column._agg_name))
            else:
                aggregate_natives.append(column._inner)
        native = self._dataframe._inner.aggregate(group_natives, aggregate_natives)
        return self._dataframe._spawn(native)

    def _agg_via_pandas_udfs(self, exprs: tuple[Any, ...]) -> DataFrame:
        """Evaluate GROUPED_AGG pandas UDFs through :meth:`applyInPandas`.

        Pure UDFs use one pass. Mixed UDF and builtin aggregates use two native plans joined on
        group keys. A global aggregation joins its two single-row plans with ``crossJoin``.
        """

        from repark.spark.functions import PandasUDFColumn, PandasUDFType
        from repark.spark.types import StructField

        if self._sql_group_clause is not None:
            raise AnalysisException(
                "GROUPED_AGG pandas_udf after cube/rollup/grouping sets is not supported; "
                "use groupBy(...).agg(pandas_udf(...)) instead"
            )
        if self._pivot_col is not None:
            raise AnalysisException(
                "GROUPED_AGG pandas_udf after pivot is not supported; "
                "use groupBy(...).agg(pandas_udf(...)) without pivot"
            )

        ordered_slots: list[dict[str, Any]] = []
        pudf_markers: list[Any] = []
        other_exprs: list[Any] = []
        for expr in exprs:
            if isinstance(expr, PandasUDFColumn):
                pudf_markers.append(expr)
                ordered_slots.append({"kind": "pudf", "marker": expr})
            else:
                other_exprs.append(expr)
                ordered_slots.append({"kind": "builtin", "expr": expr})

        if not pudf_markers:
            raise PySparkTypeError("groupBy().agg(pandas_udf...) requires at least one pandas_udf")

        for marker in pudf_markers:
            function_type = int(getattr(marker, "_function_type", PandasUDFType.SCALAR))
            if function_type != PandasUDFType.GROUPED_AGG:
                raise AnalysisException(
                    "pandas_udf in groupBy().agg requires functionType=GROUPED_AGG "
                    f"(got functionType={function_type!r} for {marker._function_name!r}); "
                    "SCALAR / SCALAR_ITER use select/withColumn"
                )
            if getattr(marker, "_window_spec", None) is not None:
                raise AnalysisException(
                    "windowed GROUPED_AGG pandas_udf cannot be used in groupBy().agg; "
                    "use select/withColumn(... .over(Window.partitionBy(...))) instead"
                )

        frame = self._dataframe
        frame._ensure_alive()
        key_names = self._apply_in_pandas_group_key_names()

        intermediate_columns: list[Column] = []
        seen_inter: set[str] = set()
        for key_name in key_names:
            if key_name not in seen_inter:
                intermediate_columns.append(frame._bind_schema_column(key_name))
                seen_inter.add(key_name)

        udf_specs: list[dict[str, Any]] = []
        seen_out: dict[str, int] = dict.fromkeys(key_names, 1)
        for marker in pudf_markers:
            input_inter_names: list[str] = []
            for input_column in marker._inputs:
                _reject_partition_transform(input_column)
                if getattr(input_column, "_generator", None) is not None:
                    raise AnalysisException(
                        "GROUPED_AGG pandas_udf input cannot be explode/posexplode generator; "
                        f"unnest first (got generator on input to {marker._function_name!r})"
                    )
                if bool(getattr(input_column, "_is_aggregate", False)):
                    raise AnalysisException(
                        "GROUPED_AGG pandas_udf input cannot be an aggregate expression "
                        f"(got aggregate on input to {marker._function_name!r})"
                    )
                temp_name = f"__repark_gagg_in_{uuid.uuid4().hex}"
                if input_column._stable_name and not input_column._is_aggregate:
                    bound = frame._rebind_stable_name_column(input_column)
                else:
                    bound = input_column
                intermediate_columns.append(bound.alias(temp_name).for_select())
                input_inter_names.append(temp_name)
            out_name = marker.output_name()
            seen_out[out_name] = seen_out.get(out_name, 0) + 1
            udf_specs.append(
                {
                    "user_func": marker._user_func,
                    "function_name": marker._function_name,
                    "return_type_sql": marker._return_type_sql,
                    "input_inter_names": input_inter_names,
                    "out_name": out_name,
                }
            )

        for slot in ordered_slots:
            if slot["kind"] == "pudf":
                slot["out_name"] = slot["marker"].output_name()

        duplicates = [name for name, count in seen_out.items() if count > 1]
        if duplicates:
            raise AnalysisException(
                f"groupBy().agg would produce duplicate column names {duplicates}; "
                "use .alias(...) to disambiguate"
            )
        if not intermediate_columns:
            raise PySparkTypeError("GROUPED_AGG pandas_udf produced no intermediate columns")

        projected = frame._spawn(
            frame._plan().select([column._inner for column in intermediate_columns])
        )

        projected_arrow = projected._analyzed_arrow_schema()
        projected_by_name = {field.name: field for field in projected_arrow}
        from repark.spark.types import _arrow_type_to_repark

        struct_fields: list[StructField] = []
        for key_name in key_names:
            arrow_field = projected_by_name.get(key_name)
            if arrow_field is None:
                raise AnalysisException(
                    f"GROUPED_AGG intermediate missing group key column {key_name!r}"
                )
            struct_fields.append(
                StructField(key_name, _arrow_type_to_repark(arrow_field.type), True)
            )
        from repark.spark.functions import (
            _normalize_pandas_udf_return_type_sql,
            _pandas_udf_arrow_type_for_return,
        )

        for spec in udf_specs:
            validated_sql = _normalize_pandas_udf_return_type_sql(spec["return_type_sql"])
            spec["return_type_sql"] = validated_sql
            data_type = DataType.fromDDL(validated_sql)
            _pandas_udf_arrow_type_for_return(data_type)
            struct_fields.append(StructField(spec["out_name"], data_type, True))
        result_schema = StructType(struct_fields)

        specs = udf_specs
        keys = list(key_names)

        grouped = GroupedData(projected, [projected._bind_schema_column(name) for name in keys])
        udf_frame = grouped.applyInPandas(
            functools.partial(_grouped_agg_pandas, keys=keys, specs=specs),
            result_schema,
        )

        if not other_exprs:
            return udf_frame

        # Builtins must read the original frame, not the UDF intermediate projection.
        plain = GroupedData(self._dataframe, self._group_columns, sql_group_clause=None)
        builtin_resolved = [
            plain._rebind_simple_name_aggregate(column)
            for column in plain._resolve_aggregates(tuple(other_exprs))
        ]
        for column in builtin_resolved:
            _reject_partition_transform(column)
            column._reject_nested_generator("agg")
        group_natives = [column._inner for column in plain._group_columns]
        aggregate_natives = []
        builtin_out_names: list[str] = []
        for column in builtin_resolved:
            if column._agg_name is not None:
                aggregate_natives.append(column._inner.alias(column._agg_name))
                builtin_out_names.append(column._agg_name)
            elif column._projection_name is not None:
                aggregate_natives.append(column._inner.alias(column._projection_name))
                builtin_out_names.append(column._projection_name)
            else:
                aggregate_natives.append(column._inner)
                builtin_out_names.append(column.spark_display_part())
        for out_name in builtin_out_names:
            if out_name in seen_out:
                raise AnalysisException(
                    f"groupBy().agg would produce duplicate column names {[out_name]}; "
                    "use .alias(...) to disambiguate"
                )
            seen_out[out_name] = 1
        for slot, out_name in zip(
            [s for s in ordered_slots if s["kind"] == "builtin"],
            builtin_out_names,
            strict=True,
        ):
            slot["out_name"] = out_name
        native = plain._dataframe._inner.aggregate(group_natives, aggregate_natives)
        builtin_frame = plain._dataframe._spawn(native)

        # Views remove qualified bridge names. Null-safe joins keep NULL groups together.
        # Materialize the joined result before dropping intermediate views.
        session = frame._session
        udf_view = scratch_view_name(session, "__repark_mix_u_")
        builtin_view = scratch_view_name(session, "__repark_mix_b_")
        out_view = scratch_view_name(session, "__repark_mix_o_")
        try:
            udf_frame._prepare_for_plan()
            builtin_frame._prepare_for_plan()
            session.materialize_as_temp_view(udf_view, udf_frame._inner)
            session.materialize_as_temp_view(builtin_view, builtin_frame._inner)
            udf_clean = frame._spawn(session.sql(f"SELECT * FROM {udf_view}"))
            builtin_clean = frame._spawn(session.sql(f"SELECT * FROM {builtin_view}"))

            select_names: list[str] = list(key_names)
            for slot in ordered_slots:
                out_name = slot["out_name"]
                if out_name not in select_names:
                    select_names.append(out_name)

            if key_names:
                join_sql = _null_safe_equi_join_sql(
                    udf_view,
                    builtin_view,
                    list(key_names),
                    select_names,
                    left_column_names=list(udf_clean.columns),
                    right_column_names=list(builtin_clean.columns),
                )
                joined = frame._spawn(session.sql(join_sql))
            else:
                # Global aggregates produce one row per side, so crossJoin is exact.
                joined = udf_clean.crossJoin(builtin_clean).select(*select_names)

            joined._prepare_for_plan()
            session.materialize_as_temp_view(out_view, joined._inner)
            # The result view must outlive the intermediate views.
            return frame._spawn(session.sql(f"SELECT * FROM {out_view}"))
        finally:
            with contextlib.suppress(Exception):
                session.drop_temp_view(udf_view)
            with contextlib.suppress(Exception):
                session.drop_temp_view(builtin_view)

    def _agg_via_pivot(self, *exprs: Column | dict[str, str]) -> DataFrame:
        """Lower pivot values to conditional aggregates."""
        from repark.spark import functions as F  # noqa: N812 — PySpark idiom

        # Pivot aliases are not safe inside the SQL grouping-sets surface.
        if self._sql_group_clause is not None:
            raise AnalysisException(
                "pivot after cube/rollup/grouping sets is not supported; "
                "use groupBy(...).pivot(...) instead"
            )

        aggregate_columns = [
            self._rebind_simple_name_aggregate(column) for column in self._resolve_aggregates(exprs)
        ]
        if not aggregate_columns:
            raise AnalysisException("agg requires at least one aggregate expression after pivot")
        for column in aggregate_columns:
            _reject_partition_transform(column)

        pivot_col = self._pivot_col
        assert pivot_col is not None
        values = self._resolve_pivot_values(pivot_col)
        multi = len(aggregate_columns) > 1
        pivoted: list[Column] = []
        for value in values:
            cond = self._pivot_value_condition(pivot_col, value)
            value_name = _pivot_value_column_name(value)
            for aggregate in aggregate_columns:
                builder = _pivot_aggregate_builder(aggregate)
                input_column = _pivot_aggregate_input(aggregate, self._dataframe)
                conditional = builder(F.when(cond, input_column))
                out_name = (
                    f"{value_name}_{_pivot_agg_output_suffix(aggregate)}" if multi else value_name
                )
                pivoted.append(conditional.alias(out_name))

        plain = GroupedData(self._dataframe, self._group_columns, sql_group_clause=None)
        return plain.agg(*pivoted)

    def _resolve_pivot_values(self, pivot_col: str) -> list[Any]:
        """Return explicit values or capped inferred distinct values."""
        if self._pivot_values_explicit:
            return list(self._pivot_values or [])
        # Inferred form: distinct on pivot column.
        frame = self._dataframe
        frame._ensure_alive()
        max_values = _pivot_max_values(frame)
        # Limit discovery before sorting so high-cardinality pivots cannot force an unbounded sort.
        capped_frame = frame.select(pivot_col).distinct().limit(max_values + 1)
        table = capped_frame.to_arrow()
        values = table.column(0).to_pylist()
        if len(values) > max_values:
            raise AnalysisException(
                f"The pivot column {pivot_col} has more than {max_values} distinct values, "
                "this could indicate an error. If this was intended, set "
                f"spark.sql.pivotMaxValues to at least the number of distinct values of "
                f"the pivot column."
            )
        # Keep null last to match Spark pivot naming order.
        return _pivot_sort_discovered_values(values)

    def _pivot_value_condition(self, pivot_col: str, value: Any) -> Column:
        """Build a pivot condition with typed equality, NULL handling, and NaN handling."""

        from repark.spark import functions as F  # noqa: N812

        left = self._dataframe._bind_schema_column(pivot_col)
        if value is None:
            return left.is_null()
        if isinstance(value, float) and math.isnan(value):
            return F.isnan(left)
        right: Column = F.lit(value)
        cast_type = _pivot_column_engine_type(self._dataframe, pivot_col)
        if cast_type is not None:
            right = right.cast(cast_type)
        return left == right

    def _agg_via_sql_group(self, *exprs: Column | dict[str, str]) -> DataFrame:
        """Lower cube, rollup, and grouping sets through a stable SQL snapshot.

        The snapshot prevents a non-idempotent UDF from running again during SQL planning.
        Aggregate aliases match native ``groupBy().agg`` output.
        """

        aggregate_columns = self._resolve_aggregates(exprs)
        if not aggregate_columns:
            raise AnalysisException("agg requires at least one aggregate expression")
        for column in aggregate_columns:
            _reject_partition_transform(column)
            column._reject_nested_generator("agg")
        frame = self._dataframe
        frame._ensure_alive()
        view = scratch_view_name(frame._session, "__repark_gs_")
        # Use the plan snapshot so SQL planning cannot re-run a UDF.
        plan = frame._plan()
        frame._session.create_or_replace_temp_view(view, plan)
        try:
            group_sql = self._sql_group_clause or ""
            select_parts: list[str] = []
            for column in self._group_columns:
                select_parts.append(column.sql_expr_part())
            for column in aggregate_columns:
                # Use structural SQL so count literals are not confused with count(*).
                expression_sql, output_name = _global_agg_sql_parts(column)
                select_parts.append(f"{expression_sql} AS {_quote_ident(output_name)}")
            sql = f"SELECT {', '.join(select_parts)} FROM {view} GROUP BY {group_sql}"
            return frame._spawn(frame._session.sql(sql))
        finally:
            frame._session.drop_temp_view(view)

    def pivot(self, pivot_col: str, values: list[Any] | None = None) -> GroupedData:
        """Pivot on ``pivot_col`` using conditional aggregates.

        Returns a :class:`GroupedData` that lowers on the next aggregation.

        * A supplied values list avoids distinct discovery.
        * ``values=None`` discovers distinct values up to ``spark.sql.pivotMaxValues``.
          Overflow raises :class:`~repark.errors.AnalysisException`.

        A single aggregate uses the pivot value as its name. NULL becomes ``"null"``.
        Multiple aggregates use ``{value}_{aggname}`` or the explicit alias.

        Not supported after :meth:`DataFrame.cube` / :meth:`DataFrame.rollup` / grouping sets
        (SQL GROUP BY path + free-text pivot aliases is not a safe surface).
        """
        if not isinstance(pivot_col, str):
            raise PySparkTypeError(f"pivot_col must be a str, got {type(pivot_col).__name__}")
        if values is not None and not isinstance(values, list):
            raise PySparkTypeError(
                f"pivot values must be a list or None, got {type(values).__name__}"
            )
        if self._sql_group_clause is not None:
            raise AnalysisException(
                "pivot after cube/rollup/grouping sets is not supported; "
                "use groupBy(...).pivot(...) instead"
            )
        # A second pivot must fail instead of silently replacing pivot state.
        if self._pivot_col is not None:
            raise UnsupportedOperationException(
                "[REPEATED_CLAUSE] The PIVOT clause may be used at most once per "
                "SUBQUERY operation. SQLSTATE: 42614"
            )
        return GroupedData(
            self._dataframe,
            self._group_columns,
            sql_group_clause=None,
            pivot_col=pivot_col,
            pivot_values=list(values) if values is not None else None,
            pivot_values_explicit=values is not None,
        )

    def _rebind_simple_name_aggregate(self, column: Column) -> Column:
        """Rebind simple-name aggregates against the source schema.

        Quoted binds preserve mixed-case projected fields. Compound expressions and ``count(*)``
        remain unchanged. Alias recovery uses structural SQL when aggregate metadata is cleared.
        """
        from repark.spark import functions as functions_module

        rebound = self._try_rebind_unary_simple_aggregate(column, functions_module)
        if rebound is None:
            rebound = self._try_rebind_first_last_aggregate(column, functions_module)
        if rebound is None:
            rebound = self._try_rebind_binary_aggregate(column, functions_module)
        if rebound is None:
            rebound = self._try_rebind_count_distinct_aggregate(column, functions_module)
        if rebound is None:
            return column
        return self._finish_rebound_aggregate(column, rebound)

    def _match_af_text(self, column: Column, pattern: str) -> re.Match[str] | None:
        """Match aggregate metadata or structural SQL against ``pattern``."""
        if column._agg_name is not None:
            match = re.fullmatch(pattern, column._agg_name)
            if match is not None:
                return match
        if column._is_aggregate_function and column._sql_expr is not None:
            return re.fullmatch(pattern, column._sql_expr)
        return None

    def _try_rebind_unary_simple_aggregate(
        self, column: Column, functions_module: object
    ) -> Column | None:
        """Rebind a unary aggregate with a simple column input."""
        # Quoted structural names must match bare aggregate names.
        # collect_list and collect_set need their structural array forms after aliasing.
        simple_af = (
            r"(sum|avg|mean|min|max|count|stddev|stddev_samp|stddev_pop|"
            r"variance|var_samp|var_pop|median|bit_and|bit_or|bit_xor|"
            r"collect_list|collect_set)"
            r'\("?([A-Za-z_][A-Za-z0-9_]*)"?\)'
        )
        match = self._match_af_text(column, simple_af)
        if match is None and column._is_aggregate_function and column._sql_expr is not None:
            # Alias recovery uses the structural array aggregation forms.
            set_distinct = re.fullmatch(
                r'coalesce\(array_distinct\(array_agg\("?([A-Za-z_][A-Za-z0-9_]*)"?\)'
                r" IGNORE NULLS\),\s*make_array\(\)\)",
                column._sql_expr,
            )
            set_legacy = re.fullmatch(
                r'coalesce\(array_agg\(DISTINCT "?([A-Za-z_][A-Za-z0-9_]*)"?\)'
                r" IGNORE NULLS,\s*make_array\(\)\)",
                column._sql_expr,
            )
            list_match = re.fullmatch(
                r'coalesce\(array_agg\("?([A-Za-z_][A-Za-z0-9_]*)"?\)'
                r" IGNORE NULLS,\s*make_array\(\)\)",
                column._sql_expr,
            )
            if set_distinct is not None or set_legacy is not None:
                name = (set_distinct or set_legacy).group(1)  # type: ignore[union-attr]
                try:
                    bound = self._dataframe._bind_schema_column(name)
                except AnalysisException:
                    return None
                return functions_module.collect_set(bound)  # type: ignore[attr-defined]
            if list_match is not None:
                name = list_match.group(1)
                try:
                    bound = self._dataframe._bind_schema_column(name)
                except AnalysisException:
                    return None
                return functions_module.collect_list(bound)  # type: ignore[attr-defined]
        if match is None:
            return None
        function_name, name = match.group(1), match.group(2)
        try:
            bound = self._dataframe._bind_schema_column(name)
        except AnalysisException:
            return None
        builders = {
            "sum": functions_module.sum,  # type: ignore[attr-defined]
            "avg": functions_module.avg,  # type: ignore[attr-defined]
            "mean": functions_module.mean,  # type: ignore[attr-defined]
            "min": functions_module.min,  # type: ignore[attr-defined]
            "max": functions_module.max,  # type: ignore[attr-defined]
            "count": functions_module.count,  # type: ignore[attr-defined]
            "stddev": functions_module.stddev,  # type: ignore[attr-defined]
            "stddev_samp": functions_module.stddev_samp,  # type: ignore[attr-defined]
            "stddev_pop": functions_module.stddev_pop,  # type: ignore[attr-defined]
            "variance": functions_module.variance,  # type: ignore[attr-defined]
            "var_samp": functions_module.var_samp,  # type: ignore[attr-defined]
            "var_pop": functions_module.var_pop,  # type: ignore[attr-defined]
            "median": functions_module.median,  # type: ignore[attr-defined]
            "bit_and": functions_module.bit_and,  # type: ignore[attr-defined]
            "bit_or": functions_module.bit_or,  # type: ignore[attr-defined]
            "bit_xor": functions_module.bit_xor,  # type: ignore[attr-defined]
            "collect_list": functions_module.collect_list,  # type: ignore[attr-defined]
            "collect_set": functions_module.collect_set,  # type: ignore[attr-defined]
        }
        builder = builders.get(function_name)
        if builder is None:
            return None
        return builder(bound)

    def _try_rebind_first_last_aggregate(
        self, column: Column, functions_module: object
    ) -> Column | None:
        """Rebind ``first`` and ``last`` while preserving ``IGNORE NULLS``."""
        if not column._is_aggregate_function or column._sql_expr is None:
            name_match = None
            if column._agg_name is not None:
                name_match = re.fullmatch(
                    r'(first|last)\("?([A-Za-z_][A-Za-z0-9_]*)"?\)',
                    column._agg_name,
                )
            if name_match is None:
                return None
            function_name, name = name_match.group(1), name_match.group(2)
            ignore_nulls = False
        else:
            sql_match = re.fullmatch(
                r'(first|last)_value\("?([A-Za-z_][A-Za-z0-9_]*)"?\)( IGNORE NULLS)?',
                column._sql_expr,
            )
            if sql_match is None:
                return None
            function_name, name = sql_match.group(1), sql_match.group(2)
            ignore_nulls = sql_match.group(3) is not None
        try:
            bound = self._dataframe._bind_schema_column(name)
        except AnalysisException:
            return None
        if function_name == "first":
            return functions_module.first(bound, ignorenulls=ignore_nulls)  # type: ignore[attr-defined]
        return functions_module.last(bound, ignorenulls=ignore_nulls)  # type: ignore[attr-defined]

    def _try_rebind_binary_aggregate(
        self, column: Column, functions_module: object
    ) -> Column | None:
        """Rebind a binary aggregate with simple column inputs."""
        binary_af = (
            r"(corr|covar_pop|covar_samp)"
            r'\("?([A-Za-z_][A-Za-z0-9_]*)"?\s*,\s*"?([A-Za-z_][A-Za-z0-9_]*)"?\)'
        )
        match = self._match_af_text(column, binary_af)
        if match is None:
            return None
        function_name, left_name, right_name = match.group(1), match.group(2), match.group(3)
        try:
            left = self._dataframe._bind_schema_column(left_name)
            right = self._dataframe._bind_schema_column(right_name)
        except AnalysisException:
            return None
        builders = {
            "corr": functions_module.corr,  # type: ignore[attr-defined]
            "covar_pop": functions_module.covar_pop,  # type: ignore[attr-defined]
            "covar_samp": functions_module.covar_samp,  # type: ignore[attr-defined]
        }
        builder = builders.get(function_name)
        if builder is None:
            return None
        return builder(left, right)

    def _try_rebind_count_distinct_aggregate(
        self, column: Column, functions_module: object
    ) -> Column | None:
        """Rebind a count-distinct aggregate with simple column inputs."""
        names: list[str] | None = None
        if column._agg_name is not None:
            names = _parse_count_distinct_simple_names(column._agg_name)
        if names is None and column._is_aggregate_function and column._sql_expr is not None:
            names = _parse_count_distinct_simple_names(column._sql_expr)
        if not names:
            return None
        try:
            bounds = [self._dataframe._bind_schema_column(name) for name in names]
        except AnalysisException:
            return None
        return functions_module.count_distinct(*bounds)  # type: ignore[attr-defined]

    def _finish_rebound_aggregate(self, column: Column, rebound: Column) -> Column:
        """Preserve caller aggregate and alias names on a rebound column."""
        inner = rebound._inner
        if column._agg_name is None and column._projection_name is not None:
            inner = rebound._inner.alias(column._projection_name)
            spark_display = column._spark_display
            projection_name = column._projection_name
            stable_name = column._stable_name
        elif column._agg_name is not None:
            spark_display = column._agg_name
            projection_name = column._agg_name
            stable_name = rebound._stable_name
        else:
            spark_display = rebound._spark_display
            projection_name = rebound._projection_name
            stable_name = rebound._stable_name
        return Column(
            inner,
            agg_name=column._agg_name,
            spark_display=spark_display,
            projection_name=projection_name,
            stable_name=stable_name,
            sql_expr=rebound._sql_expr,
            is_aggregate=True,
            is_aggregate_function=True,
            has_free_attribute=False,
            partition_transform=column._partition_transform,
        )

    def count(self) -> DataFrame:
        """Row count per group as a ``count`` column (PySpark ``GroupedData.count``)."""
        from repark import _native

        # Structural count(*) avoids confusing row counts with a column named "1".
        aggregate = Column(
            _native.PyColumn.count_aggregate([_native.PyColumn.literal(1)], False),
            agg_name="count",
            sql_expr="count(*)",
            spark_display="count",
            projection_name="count",
        )
        return self.agg(aggregate)

    def sum(self, *cols: str) -> DataFrame:
        """Sum of each named column per group (PySpark ``GroupedData.sum``)."""
        return self._shortcut_aggregate("sum", cols)

    def avg(self, *cols: str) -> DataFrame:
        """Mean of each named column per group as ``DoubleType`` (PySpark ``GroupedData.avg``)."""
        return self._shortcut_aggregate("avg", cols)

    # PySpark exposes ``mean`` as an alias of ``avg``.
    mean = avg

    def min(self, *cols: str) -> DataFrame:
        """Minimum of each named column per group (PySpark ``GroupedData.min``)."""
        return self._shortcut_aggregate("min", cols)

    def max(self, *cols: str) -> DataFrame:
        """Maximum of each named column per group (PySpark ``GroupedData.max``)."""
        return self._shortcut_aggregate("max", cols)

    def _shortcut_aggregate(self, kind: str, cols: tuple[str, ...]) -> DataFrame:
        """Build a shortcut aggregate for named columns or every numeric source column.

        With no names, Spark includes numeric grouping keys in schema order and excludes
        non-numeric columns.
        """
        from repark.spark import functions as F  # noqa: N812 — PySpark idiom

        builders = {"sum": F.sum, "avg": F.avg, "min": F.min, "max": F.max}
        builder = builders[kind]
        names = list(cols) if cols else self._numeric_column_names()
        # Bind names against the source frame so mixed-case fields resolve.
        return self.agg(*[builder(self._dataframe._bind_schema_column(name)) for name in names])

    def _numeric_column_names(self) -> list[str]:
        """Return numeric source columns in schema order, including grouping keys."""
        return [
            name
            for name, type_key, _ in self._dataframe._inner.logical_schema_fields()
            if _is_numeric_type_key(type_key)
        ]

    def _resolve_aggregates(self, exprs: tuple[Column | dict[str, str], ...]) -> list[Column]:
        """Normalize aggregate expressions or one function mapping to columns."""
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            return [self._aggregate_from_dict(name, fn) for name, fn in exprs[0].items()]
        columns: list[Column] = []
        for expr in exprs:
            if not isinstance(expr, Column):
                raise PySparkTypeError(
                    "agg expects aggregate Column expressions or a single "
                    f"{{column: function}} dict, got {type(expr).__name__}"
                )
            columns.append(expr)
        return columns

    def _aggregate_from_dict(self, column_name: str, function_name: str) -> Column:
        """Build one aggregate from a ``{column: function_name}`` entry.

        Function names are case-insensitive. Unsupported spellings raise an analysis error.
        """
        from repark.spark import functions as F  # noqa: N812 — PySpark idiom

        builders = {
            "sum": F.sum,
            "avg": F.avg,
            "mean": F.mean,
            "min": F.min,
            "max": F.max,
            "count": F.count,
            "first": F.first,
            "last": F.last,
            "collect_list": F.collect_list,
            "collect_set": F.collect_set,
        }
        # Spark matches dictionary reducer names without case sensitivity.
        builder = builders.get(function_name.casefold())
        if builder is None:
            raise PySparkValueError(
                f"unsupported aggregate function {function_name!r} in agg dict "
                f"(supported: {sorted(builders)})"
            )
        # Quoted binds keep dictionary aggregates valid after case-preserving projections.
        return builder(self._dataframe._bind_schema_column(column_name))

    def _apply_in_pandas_group_key_names(self) -> list[str]:
        """Resolve simple named group keys for ``applyInPandas``.

        Expression keys are refused because boundary scanning needs concrete streamed columns.
        Project the expression first, then group by its resulting name.
        """
        names: list[str] = []
        for column in self._group_columns:
            if column._stable_name and column._projection_name is not None:
                names.append(column._projection_name)
                continue
            display = column.spark_display_part()
            raise AnalysisException(
                "applyInPandas v1 requires simple column-name group keys "
                f"(got non-NamedExpression group column {display!r}); "
                "project the expression first, then groupBy the resulting column name"
            )
        return names

    def applyInPandas(  # noqa: N802 — PySpark method name
        self,
        func: Callable[[Any], Any],
        schema: Any,
    ) -> DataFrame:
        """Apply a pandas UDF to each group through the lazy Arrow bridge.

        Groups are made contiguous by an engine-side sort, then streamed with boundary stitching.
        Memory is bounded by the largest group and one Arrow batch. Each returned DataFrame is
        cast and validated against ``schema``. Empty input produces no UDF calls.

        Args:
            func: Callable receiving one pandas DataFrame per group.
            schema: DDL string or ``StructType`` for the result.

        Returns:
            A lazy DataFrame containing the concatenated UDF results.

        Raises:
            PySparkException: If the UDF returns invalid columns or raises.

        Notes:
            Global grouping uses one group and can buffer the full input. Pivot, cube, rollup,
            grouping sets, and expression group keys are refused. User exceptions retain traceback
            text. The pandas extra is required.
        """

        if self._sql_group_clause is not None:
            raise AnalysisException(
                "applyInPandas after cube/rollup/grouping sets is not supported; "
                "use groupBy(...).applyInPandas(...) instead"
            )
        if self._pivot_col is not None:
            raise AnalysisException(
                "applyInPandas after pivot is not supported; "
                "use groupBy(...).applyInPandas(...) without pivot"
            )
        if not callable(func):
            raise PySparkTypeError(
                f"applyInPandas func must be callable, got {type(func).__name__}"
            )
        try:
            import pandas as pd
        except ImportError as error:
            raise ImportError(
                "applyInPandas requires pandas (pip install 'repark[pandas]')"
            ) from error
        import pyarrow as pa

        _ = (pd, pa)

        key_names = self._apply_in_pandas_group_key_names()
        # Coerce once so the Arrow path uses the declared schema.
        _declared, expected_arrow = _coerce_map_in_arrow_schema(schema)
        frame = self._dataframe
        frame._ensure_alive()
        # Sort only to make group keys contiguous. Global grouping needs no sort.
        sorted_parent = frame.order_by(*key_names) if key_names else frame

        expected_names = list(expected_arrow.names)

        return sorted_parent.mapInArrow(
            functools.partial(
                _apply_in_pandas_arrow_batches,
                user_func=func,
                key_names=key_names,
                expected_names=expected_names,
                expected_arrow=expected_arrow,
            ),
            schema,
        )

    apply_in_pandas = applyInPandas


def _pivot_max_values(frame: DataFrame) -> int:
    """Return the configured pivot cardinality limit."""
    token = getattr(frame, "_alive_token", {}) or {}
    conf = token.get("builder_config") or {}
    raw = "10000"
    for key, value in conf.items():
        if str(key).casefold() == "spark.sql.pivotmaxvalues" and value is not None:
            raw = str(value)
            break
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return 10000


def _pivot_value_column_name(value: Any) -> str:
    """Return Spark's pivot output name, including NULL and boolean spellings."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _pivot_column_engine_type(frame: DataFrame, pivot_col: str) -> str | None:
    """Return the native engine type used to cast explicit pivot values."""
    frame._ensure_alive()
    target = pivot_col.casefold()
    for name, type_key, _ in frame._inner.logical_schema_fields():
        if name == pivot_col or name.casefold() == target:
            return type_key
    return None


def _pivot_recover_agg_name(aggregate: Column) -> str:
    """Recover an aggregate name after optional ``.alias(...)``.

    Explicit aliases clear ``_agg_name``. The pre-alias name remains in ``spark_display``.
    """
    if aggregate._agg_name:
        return aggregate._agg_name
    if not aggregate._is_aggregate:
        return ""
    display = aggregate.spark_display_part()
    # ``sum(x) AS total`` / ``count AS c`` — take the left of the last top-level AS.
    match = re.fullmatch(r"(?is)(.+?)\s+AS\s+(.+)", display.strip())
    if match is not None:
        return match.group(1).strip()
    return display.strip()


def _pivot_agg_output_suffix(aggregate: Column) -> str:
    """Return the output suffix for a pivot aggregate."""
    if aggregate._agg_name is None and aggregate._stable_name and aggregate._projection_name:
        return aggregate._projection_name
    recovered = _pivot_recover_agg_name(aggregate)
    return recovered or aggregate.spark_display_part()


def _pivot_is_count_distinct_name(name: str) -> bool:
    """Return whether a recovered name is a count-distinct expression."""
    return name.casefold().startswith("count(distinct ")


def _pivot_aggregate_builder(aggregate: Column) -> Callable[..., Column]:
    """Return the aggregate builder matching a pivot expression."""
    from repark.spark import functions as F  # noqa: N812

    name = _pivot_recover_agg_name(aggregate).casefold()
    # Distinct counts need a separate refusal from conditional-count rebuilding.
    if _pivot_is_count_distinct_name(name):
        raise AnalysisException(
            "pivot does not support countDistinct yet "
            f"(got {aggregate._agg_name!r}); use count/sum/avg/min/max/first/last"
        )
    if name.startswith("sum("):
        return F.sum
    if name.startswith("avg(") or name.startswith("mean("):
        return F.avg
    if name.startswith("min("):
        return F.min
    if name.startswith("max("):
        return F.max
    # GroupedData.count() uses the bare aggregate name ``count``.
    if name == "count" or name.startswith("count("):
        return F.count
    # Conditional pivot rows inject NULLs, so first and last must ignore them.
    # Alias recovery sees DataFusion first_value and last_value names.
    if name.startswith("first(") or name.startswith("first_value("):
        return lambda column: F.first(column, ignorenulls=True)
    if name.startswith("last(") or name.startswith("last_value("):
        return lambda column: F.last(column, ignorenulls=True)
    raise AnalysisException(
        f"pivot does not support aggregate {aggregate._agg_name!r} yet "
        f"(supported: sum/avg/mean/min/max/count/first/last)"
    )


def _pivot_sort_discovered_values(values: list[Any]) -> list[Any]:
    """Sort discovered pivot values with NULL values last."""
    nulls = [value for value in values if value is None]
    non_nulls = [value for value in values if value is not None]
    try:
        non_nulls.sort()
    except TypeError:
        non_nulls.sort(key=lambda value: (type(value).__name__, str(value)))
    return non_nulls + nulls


def _pivot_is_typed_scalar_inner(inner: str) -> bool:
    """Return whether ``inner`` is a typed scalar display rather than a compound expression."""
    return (
        re.fullmatch(
            r"(?is)(?:Int(?:8|16|32|64)|UInt(?:8|16|32|64)|Float(?:16|32|64)|"
            r"Utf8|LargeUtf8|Boolean|Decimal\d*(?:\(\d+,\s*\d+\))?|Date(?:32|64)|"
            r"Null)\(.*\)",
            inner.strip(),
        )
        is not None
    )


def _pivot_native_shows_typed_literal(aggregate: Column) -> bool:
    """Return whether native display distinguishes a typed literal from a named column."""
    if aggregate._agg_name is None:
        return False
    display = aggregate._inner.display_name()
    return (
        re.search(
            r"(?i)\((?:Int(?:8|16|32|64)|UInt(?:8|16|32|64)|Float(?:16|32|64)|"
            r"Utf8|LargeUtf8|Boolean|Decimal\d*|Date(?:32|64)|Null)\(",
            display,
        )
        is not None
    )


def _pivot_count_one_is_row_count(aggregate: Column) -> bool:
    """Return whether recovered ``count(1)`` denotes a measure column."""
    if aggregate._agg_name is not None:
        return aggregate._inner.display_name().casefold() != "count(1)"
    return False


def _pivot_aggregate_input(
    aggregate: Column,
    frame: DataFrame | None = None,
) -> Column:
    """Extract and rebind a simple aggregate input, refusing compound expressions."""
    from repark.spark import functions as F  # noqa: N812

    name = _pivot_recover_agg_name(aggregate)
    name_cf = name.casefold()
    # Require a word break after DISTINCT to avoid matching column names.
    if _pivot_is_count_distinct_name(name_cf):
        raise AnalysisException(
            "pivot does not support countDistinct yet "
            f"(got {name!r}); use count/sum/avg/min/max/first/last"
        )
    # Bare count and count(*) count every row under the pivot condition.
    # Keep count(1) separate because it can name a measure column.
    if name_cf in {"count", "count(*)"}:
        return F.lit(1)
    match = re.fullmatch(
        r"(?i)(sum|avg|mean|min|max|count|first_value|last_value|first|last)\((.+)\)",
        name,
    )
    if match is None:
        raise AnalysisException(
            "pivot requires aggregates with recoverable input names "
            f"(got {name!r}); use F.sum('col') form before pivot.agg"
        )
    kind = match.group(1).casefold()
    inner = match.group(2).strip()
    recovered_typed = _pivot_is_typed_scalar_inner(inner)
    native_typed = _pivot_native_shows_typed_literal(aggregate)
    if kind == "count":
        # Typed literals are non-null row-count inputs. Compound expressions are not.
        if inner == "*" or recovered_typed or native_typed:
            return F.lit(1)
        if inner == "1" and _pivot_count_one_is_row_count(aggregate):
            return F.lit(1)
    elif recovered_typed or native_typed:
        # Non-count literals must not bind digit-named measure columns.
        raise AnalysisException(
            "pivot requires simple column-name aggregate inputs "
            f"(got {name!r}); compound expressions, literals, and CAST are not supported yet"
        )
    if (inner.startswith('"') and inner.endswith('"')) or (
        inner.startswith("`") and inner.endswith("`")
    ):
        inner = inner[1:-1]
    # Simple identifiers only. Digit-leading names are valid Spark columns.
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*|[0-9][A-Za-z0-9_]*", inner):
        raise AnalysisException(
            "pivot requires simple column-name aggregate inputs "
            f"(got {name!r}); compound expressions, literals, and CAST are not supported yet"
        )
    if frame is not None:
        try:
            return frame._bind_schema_column(inner)
        except AnalysisException as bind_error:
            # Unresolvable names must fail here, not surface a later schema error.
            raise AnalysisException(
                "pivot requires simple column-name aggregate inputs "
                f"(got {name!r}); compound expressions, literals, and CAST are not supported yet"
            ) from bind_error
    return F.col(inner)
