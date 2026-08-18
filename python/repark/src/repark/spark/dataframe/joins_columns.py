"""Joins/columns region — GroupedData + pivot helpers (r27 T0; technique A)."""

from __future__ import annotations

import contextlib
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


class GroupedData:
    """The result of :meth:`DataFrame.groupBy` (near-drop-in for ``pyspark.sql.GroupedData``).

    Finish an aggregation with :meth:`agg` (Column-expression or dict form) or a PySpark shortcut
    (:meth:`count`, :meth:`sum`, :meth:`avg` / :meth:`mean`, :meth:`min`, :meth:`max`). The group
    columns lead the output, then the aggregates — matching PySpark.
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
        """Bind the source DataFrame and the resolved grouping columns.

        Optional pivot state (R-PIVOT): ``pivot_col`` + values list (or infer on
        :meth:`agg` when ``pivot_values_explicit`` is false and values is None).
        """
        self._dataframe = dataframe
        self._group_columns = group_columns
        self._sql_group_clause = sql_group_clause
        self._pivot_col = pivot_col
        self._pivot_values = pivot_values
        self._pivot_values_explicit = pivot_values_explicit

    def agg(self, *exprs: Column | dict[str, str] | Any) -> DataFrame:
        """Aggregate the groups (PySpark ``GroupedData.agg``).

        Accepts aggregate :class:`Column` expressions (``agg(F.sum("x"), F.count("*"))``) or a
        single ``{column: function_name}`` dict (``agg({"x": "sum", "y": "max"})``). Each aggregate
        is aliased to its PySpark output name unless the caller set an explicit ``.alias(...)``.
        Partition-transform Columns (``F.years`` / …, including inside ``F.sum(F.years(...))``)
        raise — valid only inside :meth:`DataFrameWriterV2.partitionedBy`. Simple name-only
        aggregates (``F.sum("X")``) are rebound against the source frame so mixed-case fields
        after a requested-spelling projection still resolve (octo r4 C3-L-008).

        **GROUPED_AGG pandas_udf (M5/M6):** pure form
        (``groupBy(...).agg(mean_udf("v").alias("m"))``) routes over :meth:`applyInPandas`.
        **Mixed** pandas_udf + builtin aggregate (M6) is a two-pass plan-built join on
        group keys (UDF pass + native ``aggregate`` pass + engine join — not a Python merge).

        CUBE/ROLLUP/GROUPING SETS paths (R-DF-BATCH2) lower via SQL ``GROUP BY …``.
        After :meth:`pivot`, lowers to conditional aggregates per pivot value (R-PIVOT).
        """
        from repark.spark.functions import PandasUDFColumn

        # M5/M6: GROUPED_AGG pandas_udf form (before pivot/cube paths that lack the bridge).
        if any(isinstance(expr, PandasUDFColumn) for expr in exprs):
            return self._agg_via_pandas_udfs(exprs)

        if self._pivot_col is not None:
            return self._agg_via_pivot(*exprs)
        if self._sql_group_clause is not None:
            return self._agg_via_sql_group(*exprs)
        # Ensure mapInArrow plan snapshot is live even if group_by prepare was skipped
        # (octo C5-Q-002 — raw empty placeholder would silently aggregate to zero rows).
        self._dataframe._prepare_for_plan()
        aggregate_columns = [
            self._rebind_simple_name_aggregate(column) for column in self._resolve_aggregates(exprs)
        ]
        for column in aggregate_columns:
            _reject_partition_transform(column)
            # Bare generators in agg would project arrays without unnest (octo C6-Q-002).
            # F.count/sum(F.explode(...)) already refuse in _aggregate_argument (C6-Q-001).
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
        """GROUPED_AGG ``pandas_udf`` via :meth:`applyInPandas` (M5 pure / M6 mixed).

        * **Pure** pandas_udf form — one-pass applyInPandas (M5).
        * **Mixed** pandas_udf + builtin aggregate — **two-pass plan-built join** on group
          keys (M6): UDF pass via applyInPandas, builtin pass via native ``aggregate``,
          then engine join on keys (not a Python multiset merge). Global ``groupBy()``
          (zero keys) joins via ``crossJoin`` of the two single-row frames.

        SCALAR / SCALAR_ITER markers are refused here (select/withColumn surface).
        Cube/rollup/pivot refuse like applyInPandas.
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

        # Preserve caller order for final projection (keys first, then agg slots).
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
            # Windowed GROUPED_AGG is select/withColumn + .over(Window), not groupBy.agg.
            if getattr(marker, "_window_spec", None) is not None:
                raise AnalysisException(
                    "windowed GROUPED_AGG pandas_udf cannot be used in groupBy().agg; "
                    "use select/withColumn(... .over(Window.partitionBy(...))) instead"
                )

        # ---- pure UDF pass (shared with mixed) -----------------------------------------
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

        # Early collision scan for UDF out names only; builtin names are finalized after
        # the native aggregate pass (alias / default Spark display names).
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

        def _grouped_agg_func(pdf: Any) -> Any:
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
                            "GROUPED_AGG pandas_udf input column missing from group frame: "
                            f"{input_name!r}"
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

        grouped = GroupedData(projected, [projected._bind_schema_column(name) for name in keys])
        udf_frame = grouped.applyInPandas(_grouped_agg_func, result_schema)

        if not other_exprs:
            return udf_frame

        # ---- M6 mixed: two-pass plan-built join on group keys --------------------------
        # Builtin pass uses the original GroupedData (native aggregate), not the UDF
        # intermediate projection — builtins see source columns.
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

        # Materialize both sides through SQL temp views so join sees unqualified field
        # names. applyInPandas / mapInArrow plans can surface qualified bridge field
        # names that make equi-join on bare key names ambiguous. Join with
        # ``IS NOT DISTINCT FROM`` so NULL group keys match (Spark treats null as a
        # group; name-list equi-join drops them — octo M6 C1). The **joined** result is
        # also materialized so intermediate views can be dropped without dangling plan refs.
        session = frame._session
        udf_view = scratch_view_name(session, "__repark_mix_u_")
        builtin_view = scratch_view_name(session, "__repark_mix_b_")
        out_view = scratch_view_name(session, "__repark_mix_o_")
        try:
            # Ensure mapInArrow / applyInPandas bridge is plan-ready before materialize.
            udf_frame._prepare_for_plan()
            builtin_frame._prepare_for_plan()
            session.materialize_as_temp_view(udf_view, udf_frame._inner)
            session.materialize_as_temp_view(builtin_view, builtin_frame._inner)
            udf_clean = frame._spawn(session.sql(f"SELECT * FROM {udf_view}"))
            builtin_clean = frame._spawn(session.sql(f"SELECT * FROM {builtin_view}"))

            # Final projection: group keys + aggregates in caller order.
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
                # Global agg: both sides are single-row; plan-built cross product.
                joined = udf_clean.crossJoin(builtin_clean).select(*select_names)

            joined._prepare_for_plan()
            session.materialize_as_temp_view(out_view, joined._inner)
            # Keep out_view registered for the session lifetime (same class as cache MemTable).
            return frame._spawn(session.sql(f"SELECT * FROM {out_view}"))
        finally:
            with contextlib.suppress(Exception):
                session.drop_temp_view(udf_view)
            with contextlib.suppress(Exception):
                session.drop_temp_view(builtin_view)

    def _agg_via_pivot(self, *exprs: Column | dict[str, str]) -> DataFrame:
        """Two-phase pivot: conditional aggregation per pivot value (R-PIVOT)."""
        from repark.spark import functions as F  # noqa: N812 — PySpark idiom

        # CUBE/ROLLUP/GROUPING SETS re-enter SQL GROUP BY; pivot CASE + free-text aliases
        # are not a safe SQL surface (octo R-PIVOT C1-SEC-001). Refuse loudly.
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

        # Re-enter the non-pivot native agg path with expanded conditionals.
        plain = GroupedData(self._dataframe, self._group_columns, sql_group_clause=None)
        return plain.agg(*pivoted)

    def _resolve_pivot_values(self, pivot_col: str) -> list[Any]:
        """Return explicit values or inferred distincts (capped at pivotMaxValues)."""
        if self._pivot_values_explicit:
            return list(self._pivot_values or [])
        # Inferred form: distinct on pivot column.
        frame = self._dataframe
        frame._ensure_alive()
        max_values = _pivot_max_values(frame)
        # Cap discover at max+1 so pivotMaxValues is a real safety valve (octo C1-SAF-001 /
        # C2-SAF-001): limit the distinct set *before* sorting so a high-cardinality pivot
        # column never forces an engine-side sort of the unbounded distinct (overflow raises
        # without sorting max+1 → ∞). Sort only the ≤max discovered set for column order.
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
        # Spark orders nulls typically last for pivot cols.
        return _pivot_sort_discovered_values(values)

    def _pivot_value_condition(self, pivot_col: str, value: Any) -> Column:
        """Boolean column: pivot_col equals value (NULL-safe; Spark Cast + equality).

        Explicit values are cast to the pivot column's engine type before compare so
        type-mismatched lists (``pivot([1,2])`` on a string column of ``\"1\"``/``\"2\"``)
        still match (octo C3-L-001). NULL uses ``IS NULL``; NaN uses ``isnan`` when the
        pivot type is floating (octo C3-L-003 defense; DataFusion ``==`` is also NaN-aware).
        """

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
        """SQL GROUP BY CUBE/ROLLUP/GROUPING SETS for R-DF-BATCH2.

        Registers a plan-stable mapInArrow snapshot via :meth:`DataFrame._plan` (combine
        octo C5-L-001) — action-like :meth:`~DataFrame.create_or_replace_temp_view` would
        re-run a non-idempotent UDF after prepare and disagree with ``select(F.sum)``.
        Aggregate expressions are ``AS``-aliased to Spark default / explicit names
        (combine C5-L-002) matching native ``groupBy().agg`` and select-global-agg SQL.
        """

        aggregate_columns = self._resolve_aggregates(exprs)
        if not aggregate_columns:
            raise AnalysisException("agg requires at least one aggregate expression")
        for column in aggregate_columns:
            _reject_partition_transform(column)
            # Bare generators would embed array sql_expr without unnest (octo C7-L-002;
            # Spark UNSUPPORTED_GENERATOR). Mirrors native GroupedData.agg refuse (C6-Q-002).
            column._reject_nested_generator("agg")
        frame = self._dataframe
        frame._ensure_alive()
        view = scratch_view_name(frame._session, "__repark_gs_")
        # Plan-stable MIA snapshot (combine C5-L-001) — not DF createOrReplaceTempView
        # action re-run. Mirrors selectExpr / global-agg SQL (C2 / C4).
        plan = frame._plan()
        frame._session.create_or_replace_temp_view(view, plan)
        try:
            group_sql = self._sql_group_clause or ""
            select_parts: list[str] = []
            for column in self._group_columns:
                select_parts.append(column.sql_expr_part())
            for column in aggregate_columns:
                # Expression + Spark default / explicit alias (combine C5-L-002).
                # Structural sql_expr only — never substring-rewrite count(Int64(1))
                # (combine C6-SAF-001 / select-path C2-SAF-001). F.count("*") and
                # GroupedData.count carry sql_expr="count(*)"; a blind replace corrupted
                # first(lit('count(Int64(1))')) → first_value('count(*)').
                expression_sql, output_name = _global_agg_sql_parts(column)
                select_parts.append(f"{expression_sql} AS {_quote_ident(output_name)}")
            sql = f"SELECT {', '.join(select_parts)} FROM {view} GROUP BY {group_sql}"
            return frame._spawn(frame._session.sql(sql))
        finally:
            frame._session.drop_temp_view(view)

    def pivot(self, pivot_col: str, values: list[Any] | None = None) -> GroupedData:
        """Pivot on ``pivot_col`` (PySpark ``GroupedData.pivot``) — two-phase (R-PIVOT).

        Returns a :class:`GroupedData` that, on the next :meth:`agg` / shortcut, lowers to
        conditional aggregates (``CASE WHEN pivot = v THEN input END``) per distinct value.

        * **Values-list form** (``values`` provided): no distinct query.
        * **Inferred form** (``values is None``): runs a distinct query at ``agg`` time, capped
          at ``spark.sql.pivotMaxValues`` (default 10000). Overflow raises
          :class:`~repark.errors.AnalysisException` (Spark wording).

        Output column naming (live PySpark 4.1.2): single aggregate → pivot value as name
        (NULL → ``"null"``); multi-aggregate → ``{value}_{aggname}`` (or ``{value}_{alias}``
        when the aggregate was explicitly ``.alias(...)``).

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
        # Spark: PIVOT at most once per subquery (REPEATED_CLAUSE). A second .pivot()
        # must not silently overwrite pivot state (octo C3-Q-001).
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
        """Rebuild name-shaped aggregates with a quoted schema bind when possible.

        ``functions._aggregate_argument`` builds unquoted ``col(name)`` (needed so
        ``F.sum("X")`` still CI-resolves on a lowercase schema). After ``select("X")`` the
        field is case-preserved ``"X"`` and the unquoted leaf fails — rebind here using the
        source frame's schema (octo r4 C3-L-008 / C3-003). Compounds (``sum((X + 1))``) and
        ``count(*)`` stay unbound.

        Matching prefers ``_agg_name``; when ``.alias`` cleared it, structural ``_sql_expr``
        still identifies pure AggregateFunction builders (octo C4-Q-001). Allowlist covers
        unary batch-4 AFs, ``collect_list``/``collect_set``, binary ``corr``/``covar_*``,
        ``count_distinct`` (single + multi simple names), and ``first``/``last`` via
        structural ``first_value``/``last_value`` + optional ``IGNORE NULLS`` (octo C5-Q-001 /
        C5-L-002 / C4-L-001).
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
        """Match ``pattern`` against ``_agg_name`` then pure-AF structural ``_sql_expr``."""
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
        """Unary simple-name AFs including ``collect_list``/``collect_set`` (octo C5-Q-001)."""
        # Optional double-quotes so structural sql_expr (``sum("X")``) matches bare agg_name.
        # collect_list/set use complex sql_expr — match their agg_name form, or the coalesce
        # array_agg structural forms after .alias clears agg_name.
        simple_af = (
            r"(sum|avg|mean|min|max|count|stddev|stddev_samp|stddev_pop|"
            r"variance|var_samp|var_pop|median|bit_and|bit_or|bit_xor|"
            r"collect_list|collect_set)"
            r'\("?([A-Za-z_][A-Za-z0-9_]*)"?\)'
        )
        match = self._match_af_text(column, simple_af)
        if match is None and column._is_aggregate_function and column._sql_expr is not None:
            # Post-alias structural sql_expr for collect_list / collect_set (C5-Q-002).
            # list: coalesce(array_agg("X") IGNORE NULLS, make_array())
            # set:  coalesce(array_distinct(array_agg("X") IGNORE NULLS), make_array())
            # legacy set: coalesce(array_agg(DISTINCT "X") IGNORE NULLS, make_array())
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
        """``first``/``last`` via structural ``first_value``/``last_value`` (octo C5-Q-001).

        ``ignorenulls`` is recovered from optional ``IGNORE NULLS`` on structural sql_expr
        (not from ``agg_name`` alone — C4-L-001 / C5-Q-003).
        """
        if not column._is_aggregate_function or column._sql_expr is None:
            # Fall back: agg_name ``first(X)`` defaults ignorenulls=False.
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
        """Binary ``corr`` / ``covar_pop`` / ``covar_samp`` simple-name rebind (octo C5-L-002)."""
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
        """Single/multi-col simple-name ``count_distinct`` rebind (C5-Q-001 / C5-L-001)."""
        names: list[str] | None = None
        # agg_name: ``count(DISTINCT a, b)`` (unquoted leaves, space after commas).
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
        """Preserve caller agg/alias names on a rebound AggregateFunction column."""
        # Keep the caller's default agg name when present; after ``.alias`` the native
        # already carries the user name — re-apply it onto the rebound expression so
        # pure native ``groupBy().agg`` does not drop the alias (octo C4-Q-001).
        inner = rebound._inner
        if column._agg_name is None and column._projection_name is not None:
            inner = rebound._inner.alias(column._projection_name)
            spark_display = column._spark_display
            projection_name = column._projection_name
            stable_name = column._stable_name
        elif column._agg_name is not None:
            # Preserve requested spelling in the embed (``sum(X)`` not rebound ``sum(x)``).
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
            # Rebound structural sql_expr carries the case-correct quoted leaf.
            sql_expr=rebound._sql_expr,
            is_aggregate=True,
            is_aggregate_function=True,
            has_free_attribute=False,
            partition_transform=column._partition_transform,
        )

    def count(self) -> DataFrame:
        """Row count per group as a ``count`` column (PySpark ``GroupedData.count``)."""
        from repark import _native

        # Structural count(*) for free-SQL cube/rollup/groupingSets (combine C6-SAF-001) —
        # never rely on native display ``count(Int64(1))`` + substring rewrite.
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
        """Build ``F.<kind>(c)`` per column and aggregate (the sum/avg/min/max shortcuts).

        With **no** column names (``df.groupBy(g).sum()``) Spark aggregates EVERY numeric column in
        schema order — **including the grouping key** — so the output is
        ``[g, sum(g), sum(x), sum(y)]``; string / non-numeric columns are excluded. Verified against
        live PySpark 4.1.2: ``sum`` / ``avg`` / ``mean`` / ``min`` / ``max`` all follow this
        numeric-column rule (``count()`` is separate and already correct).
        """
        from repark.spark import functions as F  # noqa: N812 — PySpark idiom

        builders = {"sum": F.sum, "avg": F.avg, "min": F.min, "max": F.max}
        builder = builders[kind]
        names = list(cols) if cols else self._numeric_column_names()
        # Bind each name against the source frame so mixed-case fields resolve
        # (``F.sum("X")`` via bare ``col`` folds — octo r4 C3-L-008).
        return self.agg(*[builder(self._dataframe._bind_schema_column(name)) for name in names])

    def _numeric_column_names(self) -> list[str]:
        """The source frame's numeric columns (int / long / double / decimal), in schema order.

        Includes the grouping keys — the Spark zero-arg-shortcut rule (``groupBy(g).sum()`` emits
        ``sum(g)`` too). Reads the native analyzed schema (metadata only, no execution).
        """
        return [
            name
            for name, type_key, _ in self._dataframe._inner.logical_schema_fields()
            if _is_numeric_type_key(type_key)
        ]

    def _resolve_aggregates(self, exprs: tuple[Column | dict[str, str], ...]) -> list[Column]:
        """Normalize the ``agg`` args (Column expressions or a single dict) to a Column list."""
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
        """Build the aggregate Column for one ``{column: function_name}`` dict entry.

        Function names are matched case-insensitively (live PySpark 4.1.2 accepts
        ``COLLECT_LIST`` / ``SUM`` / … and still emits the snake_case output name). CamelCase
        spellings that are not in the allow-list (e.g. ``collectList``) still fail loud — Spark
        also rejects those as unresolved routines.
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
        # Spark's dict form is case-insensitive on the reducer name (oracle 4.1.2).
        builder = builders.get(function_name.casefold())
        if builder is None:
            raise PySparkValueError(
                f"unsupported aggregate function {function_name!r} in agg dict "
                f"(supported: {sorted(builders)})"
            )
        # Quoted bind so dict-form agg works after requested-spelling projections
        # (octo r4 C3-L-008).
        return builder(self._dataframe._bind_schema_column(column_name))

    def _apply_in_pandas_group_key_names(self) -> list[str]:
        """Resolve simple NamedExpression group keys for applyInPandas v1.

        Expression group keys (non-stable ``Column``s) are refused: the single-pass
        boundary scan needs concrete column names present in the streamed batches.
        Project the expression first, then ``groupBy`` the resulting name.
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
        """Per-group pandas UDF (PySpark ``GroupedData.applyInPandas``) over mapInArrow.

        **v1 contract (U6 / R-APPLYINPANDAS — facade bridge, not an engine physical operator):**

        1. **Engine-side sort** on the group keys (``orderBy``) so streamed Arrow batches are
           key-contiguous. ``repartition`` is a documented single-node no-op; a full sort is
           enough for contiguity on one partition.
        2. **mapInArrow bridge** streams sorted batches through a single-pass group boundary
           scan with **boundary-stitch** buffering (merge adjacent batches when one group
           straddles a batch edge) — memory **O(largest group + one batch)**, never a
           full-stream re-sort or re-group in the facade.
        3. Each completed group becomes one ``pandas.DataFrame`` passed to ``func``; the
           returned frame is re-ingested as Arrow RecordBatches (same C-stream / IPC path as
           mapInArrow).
        4. **Lazy:** returns a deferred DataFrame; nothing runs until an action. Actions
           re-run the bridge unless ``cache``/``persist`` pins.
        5. **Schema:** DDL or ``StructType``; every yielded batch is validated (loud mismatch
           naming field/type) — same as mapInArrow.
        6. **Errors:** user exceptions surface as :class:`~repark.errors.PySparkException`
           with traceback text; upstream stream closed best-effort.

        Empty input → empty output (no ``func`` calls). Groups that do not appear in the
        data are never invoked (Spark parity). Global ``groupBy()`` (no keys) treats the
        whole frame as one group — memory is honestly O(dataset) for that one group.
        Cube/rollup/grouping-sets and pivot paths are refused. Expression group keys that
        are not NamedExpressions are refused (project first). Scalar / SCALAR_ITER
        ``pandas_udf`` in select/withColumn is U7/M5 (separate path); pure GROUPED_AGG
        ``pandas_udf`` in ``groupBy().agg`` routes over this machinery (M5).

        Requires the optional ``pandas`` extra.
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

        key_names = self._apply_in_pandas_group_key_names()
        # Coerce once so the pandas→Arrow path can cast to the declared schema (Spark
        # applyInPandas converts via schema; bare from_pandas yields int64/large_string).
        _declared, expected_arrow = _coerce_map_in_arrow_schema(schema)
        frame = self._dataframe
        frame._ensure_alive()
        # Engine-side key contiguity: full sort on group keys (nulls first, ascending —
        # Spark group key order is non-contractual for applyInPandas output multiset; we
        # only need contiguity). Empty keys → global one-group path (no sort).
        sorted_parent = frame.order_by(*key_names) if key_names else frame

        expected_names = list(expected_arrow.names)

        def _arrow_grouped_func(input_batches: Iterator[Any]) -> Iterator[Any]:
            for group_table in _iter_apply_in_pandas_group_tables(input_batches, key_names):
                pdf = group_table.to_pandas()
                try:
                    out_pdf = func(pdf)
                except PySparkException:
                    raise
                except Exception as error:
                    detail = traceback.format_exc()
                    raise PySparkException(
                        "applyInPandas user function raised "
                        f"{type(error).__name__}: {error}\n{detail}"
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
                # Spark RESULT_COLUMN_NAMES_MISMATCH class — before Arrow cast so empty
                # wrong/partial/extra frames cannot be re-labeled as the declared schema.
                _validate_apply_in_pandas_result_columns(out_pdf, expected_names)
                # Zero-column empty frame (Spark-accepted empty group result): emit a 0-row
                # batch under the declared schema so mapInArrow validation still runs.
                if len(out_pdf) == 0 and len(out_pdf.columns) == 0:
                    yield pa.RecordBatch.from_arrays(
                        [pa.array([], type=field.type) for field in expected_arrow],
                        schema=expected_arrow,
                    )
                    continue
                try:
                    out_table = pa.Table.from_pandas(
                        out_pdf, schema=expected_arrow, preserve_index=False
                    )
                except (
                    pa.ArrowInvalid,
                    pa.ArrowTypeError,
                    pa.ArrowNotImplementedError,
                    ValueError,
                    TypeError,
                    KeyError,
                ) as error:
                    # Prefer a loud, column-naming cast error for overflow / conversion
                    # failures (octo U6 C3). Falling through to untyped Arrow would only
                    # surface a later mapInArrow type mismatch on an unrelated field
                    # (e.g. int64 vs int32 on the first column while ``total`` overflowed).
                    error_text = str(error)
                    if (
                        "Conversion failed" in error_text
                        or "not in range" in error_text
                        or "Could not convert" in error_text
                    ):
                        raise PySparkException(
                            "applyInPandas failed converting pandas output to declared "
                            f"schema: {error}"
                        ) from error
                    # Otherwise fall through to untyped conversion so mapInArrow validation
                    # names the field/type mismatch (same loud class as mapInArrow pins).
                    try:
                        out_table = pa.Table.from_pandas(out_pdf, preserve_index=False)
                    except Exception:
                        raise PySparkException(
                            f"applyInPandas failed converting pandas output to Arrow: {error}"
                        ) from error
                output_batches = out_table.to_batches()
                if not output_batches:
                    # Zero-row group result: yield under *out_table* schema so a mismatched
                    # empty frame cannot be silently rewritten as expected_arrow (C6-L-001 /
                    # U6 C1). Name check above already accepted column sets.
                    yield pa.RecordBatch.from_arrays(
                        [pa.array([], type=field.type) for field in out_table.schema],
                        schema=out_table.schema,
                    )
                else:
                    yield from output_batches

        return sorted_parent.mapInArrow(_arrow_grouped_func, schema)

    apply_in_pandas = applyInPandas


def _pivot_max_values(frame: DataFrame) -> int:
    """Spark ``spark.sql.pivotMaxValues`` (default 10000)."""
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
    """Spark pivot output column name for a pivot value (NULL → ``null``).

    Boolean values use Spark Cast-to-string spellings ``true``/``false`` (not Python
    ``str(True)`` → ``\"True\"``) — octo C3-Q-004 / C3-L-002. Check ``bool`` before
    other numeric branches: ``bool`` is a subclass of ``int``.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _pivot_column_engine_type(frame: DataFrame, pivot_col: str) -> str | None:
    """Engine type string for ``pivot_col`` (for Cast of pivot values — C3-L-001 / C4-Q-001).

    Uses native ``logical_schema_fields`` type keys (``int`` / ``long`` / …), not
    ``frame.schema``: the facade maps logical ``long`` → :class:`IntegerType` (near-drop-in
    Int64 surfaces), so ``schema`` would emit ``cast(\"int\")`` / Int32 and miss BIGINT
    pivot keys outside int32 (e.g. ``3_000_000_000``). Same pattern as ``DataFrameNaFunctions``
    fill width preservation.
    """
    frame._ensure_alive()
    target = pivot_col.casefold()
    for name, type_key, _ in frame._inner.logical_schema_fields():
        if name == pivot_col or name.casefold() == target:
            return type_key
    return None


def _pivot_recover_agg_name(aggregate: Column) -> str:
    """Recover ``sum(x)`` / bare ``count`` after optional ``.alias(...)`` (octo C1-L-003).

    Explicit ``.alias`` clears ``_agg_name``; the pre-alias identity remains in
    ``spark_display`` as ``{agg} AS {alias}``.
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
    """Multi-agg pivot column suffix: user alias wins, else default ``sum(x)`` name."""
    if aggregate._agg_name is None and aggregate._stable_name and aggregate._projection_name:
        return aggregate._projection_name
    recovered = _pivot_recover_agg_name(aggregate)
    return recovered or aggregate.spark_display_part()


def _pivot_is_count_distinct_name(name: str) -> bool:
    """True when recovered agg name is Spark ``count(DISTINCT …)``, not ``count(distinct_id)``.

    ``startswith(\"count(distinct\")`` false-matches measure columns named ``distinct`` /
    ``distinct_id`` (octo C7-L-001). True countDistinct always has a word break after
    ``DISTINCT`` (space before the first argument: ``count(DISTINCT x)``).
    """
    return name.casefold().startswith("count(distinct ")


def _pivot_aggregate_builder(aggregate: Column) -> Callable[..., Column]:
    """Return F.sum/F.count/... matching the aggregate's default name prefix."""
    from repark.spark import functions as F  # noqa: N812

    name = _pivot_recover_agg_name(aggregate).casefold()
    # countDistinct before bare count( — distinct is not a conditional-count rebuild.
    # Require space after ``distinct`` (octo C7-L-001) — not ``count(distinct_id)``.
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
    # GroupedData.count() uses bare agg_name ``count`` (octo C1-Q-001 / C1-L-001).
    if name == "count" or name.startswith("count("):
        return F.count
    # Spark PivotTransformer always rewrites First/Last with ignoreNulls=true because
    # CASE WHEN injects NULLs for non-matching pivot rows (octo C2-L-001 / C2-Q-002).
    # After ``.alias``, recovery sees DataFusion ``first_value``/``last_value`` because
    # ``alias`` freezes native display_name and clears ``_agg_name`` (octo C7-Q-001).
    if name.startswith("first(") or name.startswith("first_value("):
        return lambda column: F.first(column, ignorenulls=True)
    if name.startswith("last(") or name.startswith("last_value("):
        return lambda column: F.last(column, ignorenulls=True)
    raise AnalysisException(
        f"pivot does not support aggregate {aggregate._agg_name!r} yet "
        f"(supported: sum/avg/mean/min/max/count/first/last)"
    )


def _pivot_sort_discovered_values(values: list[Any]) -> list[Any]:
    """Sort a small discovered pivot-value set (nulls last), never an unbounded distinct."""
    nulls = [value for value in values if value is None]
    non_nulls = [value for value in values if value is not None]
    try:
        non_nulls.sort()
    except TypeError:
        non_nulls.sort(key=lambda value: (type(value).__name__, str(value)))
    return non_nulls + nulls


def _pivot_is_typed_scalar_inner(inner: str) -> bool:
    """True when ``inner`` is a DataFusion typed scalar display (``Int64(1)``, ``Utf8(\"x\")``).

    Used after ``.alias`` recovery embeds the typed form in the recovered agg name
    (e.g. ``count(Int64(1))``). Not CAST/abs/coalesce — those are compound expressions.
    """
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
    """True when pre-alias native display wraps a typed scalar lit, not a bare column id.

    ``F.sum(F.lit(1))`` recovers as ``sum(1)`` — the same string as ``F.sum(\"1\")`` for a
    measure column named ``\"1\"`` — but native display is ``sum(Int64(1))`` (octo C6-L-001).
    Same for ``count`` / ``avg`` / ``first_value`` / ``last_value``. After ``.alias``,
    ``_agg_name`` is cleared and recovery already embeds the typed form in the name.
    """
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
    """Disambiguate recovered name ``count(1)``: row-count vs measure column ``\"1\"``.

    ``F.count(\"*\")`` / ``F.count(lit(1))`` store Spark's default output name ``count(1)`` —
    the same string as ``F.count(\"1\")`` for a column named ``\"1\"`` (octo C5-L-001 /
    C5-Q-001). Pre-alias, the native display distinguishes them: column ``\"1\"`` renders
    ``count(1)``; literals render typed wrappers (``count(Int64(1))``, ``count(Utf8(\"1\"))``,
    …). After ``.alias``, ``count(*)`` recovers as ``count(Int64(1))`` (not ``count(1)``), so
    a recovered ``count(1)`` is the measure-column form.
    """
    if aggregate._agg_name is not None:
        return aggregate._inner.display_name().casefold() != "count(1)"
    return False


def _pivot_aggregate_input(
    aggregate: Column,
    frame: DataFrame | None = None,
) -> Column:
    """Extract a simple-name aggregate input from ``agg_name`` like ``sum(x)``.

    When ``frame`` is provided, simple name inputs rebind via the source schema so
    mixed-case fields survive (octo C1-L-002 — do not discard GroupedData rebind).

    Compound / lit / CAST inputs fail loud (octo C2-Q-001) — never rebuild as ``F.col`` of
    the raw expression text (that either misbinds or dies with a Schema error).
    """
    from repark.spark import functions as F  # noqa: N812

    name = _pivot_recover_agg_name(aggregate)
    name_cf = name.casefold()
    # Space after DISTINCT only (octo C7-L-001) — not measure columns ``distinct``/``distinct_*``.
    if _pivot_is_count_distinct_name(name_cf):
        raise AnalysisException(
            "pivot does not support countDistinct yet "
            f"(got {name!r}); use count/sum/avg/min/max/first/last"
        )
    # Bare GroupedData.count() / count(*) → every row under the pivot condition.
    # Do NOT treat bare ``count(1)`` as row-count here (octo C5-L-001 / C5-Q-001): that
    # name is also ``F.count(\"1\")`` for a measure column named ``\"1\"``. Exact match
    # only for unambiguous forms (octo C4-L-001): never ``startswith("count(1")``.
    if name_cf in {"count", "count(*)"}:
        return F.lit(1)
    # ``first_value``/``last_value``: alias-recovery path (octo C7-Q-001).
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
        # count(*) always rows. Typed DF lit displays (Int64(1), Utf8("1"), …) are
        # never-null so rebuild as row-count under the pivot condition.
        # ONLY typed-scalar constructors — never CAST/abs/coalesce/… (octo C6-Q-001):
        # the former broad Ident(...) regex mapped those to lit(1) and silently
        # over-counted null measures while F.sum(cast) still refused.
        if inner == "*" or recovered_typed or native_typed:
            return F.lit(1)
        # Bare ``1`` is ambiguous: row-count default name vs measure column ``\"1\"``.
        if inner == "1" and _pivot_count_one_is_row_count(aggregate):
            return F.lit(1)
        # else fall through and bind column ``\"1\"`` (non-null count), or refuse compounds.
    elif recovered_typed or native_typed:
        # Non-count: F.sum/avg/min/max/first/last(F.lit(1)) must not bind digit-named
        # measure columns (octo C6-L-001). count already peeled typed literals above.
        raise AnalysisException(
            "pivot requires simple column-name aggregate inputs "
            f"(got {name!r}); compound expressions, literals, and CAST are not supported yet"
        )
    if (inner.startswith('"') and inner.endswith('"')) or (
        inner.startswith("`") and inner.endswith("`")
    ):
        inner = inner[1:-1]
    # Simple identifiers only (octo C2-Q-001). Compounds, CAST, lit fail loud.
    # Digit-leading / all-digit names allowed (octo C4-L-001 / C5-L-001) — Spark permits
    # columns named ``\"10\"`` / ``\"1\"`` / ``\"1x\"``; true row-count forms peel above.
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*|[0-9][A-Za-z0-9_]*", inner):
        raise AnalysisException(
            "pivot requires simple column-name aggregate inputs "
            f"(got {name!r}); compound expressions, literals, and CAST are not supported yet"
        )
    if frame is not None:
        try:
            return frame._bind_schema_column(inner)
        except AnalysisException as bind_error:
            # Unresolvable name (missing col, or lit-shaped ``sum(1)``) must not fail-open
            # as ``F.col`` and surface a later Schema error (octo C2-Q-001 / C4-L-001).
            raise AnalysisException(
                "pivot requires simple column-name aggregate inputs "
                f"(got {name!r}); compound expressions, literals, and CAST are not supported yet"
            ) from bind_error
    return F.col(inner)
