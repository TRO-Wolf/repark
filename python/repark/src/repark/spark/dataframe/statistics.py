"""DataFrame statistics bodies behind the public stat wrappers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.errors import AnalysisException, UnsupportedOperationException
from repark.spark._idents import quote_ident as _quote_ident_sql
from repark.spark._idents import sql_string_literal

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame


def _describe(frame: DataFrame, *cols: str) -> DataFrame:
    """Basic stats (count/mean/stddev/min/max) as a DataFrame (PySpark ``describe``)."""
    return frame.summary("count", "mean", "stddev", "min", "max", _columns=cols or None)


def _summary(
    frame: DataFrame, *statistics: str, _columns: tuple[str, ...] | None = None
) -> DataFrame:
    """Summary statistics as a DataFrame (PySpark ``DataFrame.summary``).

    Supports count/mean/stddev/min/max. Percentile stats (``25%``/``50%``/``75%``) raise
    loud unsupported (engine gap — disclosed).
    """
    frame._ensure_alive()
    if not statistics:
        raise UnsupportedOperationException(
            "DataFrame.summary() without statistics is not Spark-shaped "
            "(engine lacks percentile rows); call summary('count','mean','stddev','min','max') "
            "or describe()"
        )
    stats = list(statistics)
    supported = {"count", "mean", "stddev", "min", "max"}
    bad = [item for item in stats if item not in supported]
    if bad:
        raise UnsupportedOperationException(
            f"summary statistics not supported yet: {bad} "
            f"(supported: {sorted(supported)}; percentiles are an engine gap)"
        )
    from repark.errors import PySparkValueError
    from repark.spark.types import (
        ByteType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
        StringType,
    )

    if _columns:
        target_pairs: list[tuple[str, str]] = [(name, name) for name in _columns]
        if frame._display_names is not None and frame._engine_names is not None:
            target_pairs = []
            want = set(_columns)
            for display, engine in zip(frame._display_names, frame._engine_names, strict=True):
                if display in want:
                    target_pairs.append((display, engine))
    elif frame._display_names is not None and frame._engine_names is not None:
        target_pairs = list(zip(frame._display_names, frame._engine_names, strict=True))
    else:
        target_pairs = [(name, name) for name in frame.columns]
    numeric_types = (
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        DecimalType,
    )
    describable_types = (*numeric_types, StringType)
    fields = frame.schema.fields
    engine_order = (
        list(frame._engine_names)
        if frame._engine_names is not None
        else [field.name for field in fields]
    )
    kind_by_engine = dict(zip(engine_order, (field.dataType for field in fields), strict=True))
    if _columns:
        refused = [
            display
            for display, engine in target_pairs
            if not isinstance(kind_by_engine.get(engine), describable_types)
        ]
        if refused:
            raise PySparkValueError(
                f"describe/summary columns must be numeric or string; refused: {refused}"
            )
    else:
        target_pairs = [
            pair
            for pair in target_pairs
            if isinstance(kind_by_engine.get(pair[1]), describable_types)
        ]
    if not target_pairs:
        raise AnalysisException("summary/describe on a zero-column frame is undefined")
    from repark import _native
    from repark.spark import functions as spark_functions
    from repark.spark.column import Column

    integer_types = (ByteType, ShortType, IntegerType, LongType)
    needed = list(dict.fromkeys(stats))
    aggregate_exprs: list[Any] = []
    cell_positions: dict[tuple[str, int], int] = {}
    for column_index, (_display, engine) in enumerate(target_pairs):
        quoted_engine = _quote_ident_sql(engine)
        data_type = kind_by_engine.get(engine)
        base_column = Column(
            _native.PyColumn.column(quoted_engine),
            spark_display=engine,
            projection_name=engine,
            stable_name=True,
            sql_expr=quoted_engine,
        )
        for stat in needed:
            position = len(aggregate_exprs)
            cell_positions[(stat, column_index)] = position
            if stat == "count":
                aggregated = spark_functions.count(base_column)
            elif stat in ("mean", "stddev"):
                operand = (
                    base_column
                    if isinstance(data_type, numeric_types)
                    else base_column.try_cast("double")
                )
                aggregated = (spark_functions.avg if stat == "mean" else spark_functions.stddev)(
                    operand
                )
            elif stat == "min":
                aggregated = spark_functions.min(base_column)
            else:
                aggregated = spark_functions.max(base_column)
            if stat in ("mean", "stddev") or not isinstance(
                data_type, (*integer_types, StringType)
            ):
                aggregated = aggregated.cast("string")
            aggregate_exprs.append(aggregated._inner.alias(f"__repark_stat_{position}"))
    cells_table = frame._spawn(frame._plan().aggregate([], aggregate_exprs)).to_arrow()
    cells = [
        cells_table.column(position).to_pylist()[0] for position in range(cells_table.num_columns)
    ]
    literal_rows = []
    for stat in stats:
        row_cells = [f"'{stat}'"]
        for column_index in range(len(target_pairs)):
            value = cells[cell_positions[(stat, column_index)]]
            row_cells.append(
                "NULL"
                if value is None
                else sql_string_literal(value if isinstance(value, str) else str(value))
            )
        literal_rows.append(f"({', '.join(row_cells)})")
    column_names = ["summary"] + [engine for _display, engine in target_pairs]
    alias = ", ".join(_quote_ident_sql(name) for name in column_names)
    projection = ", ".join(
        f"CAST({_quote_ident_sql(name)} AS VARCHAR) AS {_quote_ident_sql(name)}"
        for name in column_names
    )
    sql = f"SELECT {projection} FROM (VALUES {', '.join(literal_rows)}) AS t({alias})"
    child = frame._spawn(frame._session.sql(sql))
    if frame._display_names is not None or any(
        display != engine for display, engine in target_pairs
    ):
        child._display_names = ["summary"] + [display for display, _engine in target_pairs]
        child._engine_names = ["summary"] + [engine for _display, engine in target_pairs]
    return child


def _approx_quantile(
    frame: DataFrame,
    col: str | list[str] | tuple[str, ...],
    probabilities: list[float] | tuple[float, ...],
    relativeError: float,  # noqa: N803
) -> list[float] | list[list[float]]:
    """Approximate quantiles of numeric columns (PySpark ``DataFrame.approxQuantile``).

    Lowers to engine ``approx_percentile_cont`` via :func:`repark.functions.percentile_approx`
    ( FAIL-MISSING family). ``relativeError`` is validated (non-negative number) for API
    parity; the engine path is fixed-accuracy today (t-digest accuracy).
    """
    from repark.errors import PySparkTypeError, PySparkValueError
    from repark.spark.functions import percentile_approx

    if isinstance(relativeError, bool) or not isinstance(relativeError, (int, float)):
        raise PySparkTypeError(
            errorClass="NOT_FLOAT_OR_INT",
            messageParameters={
                "arg_name": "relativeError",
                "arg_type": type(relativeError).__name__,
            },
        )
    relative_error_value = float(relativeError)
    if relative_error_value != relative_error_value or relative_error_value < 0.0:
        raise PySparkValueError(
            errorClass="NEGATIVE_VALUE",
            messageParameters={
                "arg_name": "relativeError",
                "arg_value": str(relativeError),
            },
        )
    if not isinstance(col, (str, list, tuple)):
        raise PySparkTypeError(
            errorClass="NOT_LIST_OR_STR_OR_TUPLE",
            messageParameters={"arg_name": "col", "arg_type": type(col).__name__},
        )
    single = isinstance(col, str)
    columns: list[str] = [col] if single else list(col)
    for name in columns:
        if not isinstance(name, str):
            raise PySparkTypeError(
                errorClass="DISALLOWED_TYPE_FOR_CONTAINER",
                messageParameters={
                    "arg_name": "col",
                    "arg_type": type(col).__name__,
                    "allowed_types": "str",
                    "item_type": type(name).__name__,
                },
            )
    if not isinstance(probabilities, (list, tuple)):
        raise PySparkTypeError(
            errorClass="NOT_LIST_OR_TUPLE",
            messageParameters={
                "arg_name": "probabilities",
                "arg_type": type(probabilities).__name__,
            },
        )
    probs = list(probabilities)
    for probability in probs:
        if not isinstance(probability, (float, int)) or isinstance(probability, bool):
            raise PySparkTypeError(
                errorClass="NOT_LIST_OF_FLOAT_OR_INT",
                messageParameters={
                    "arg_name": "probabilities",
                    "arg_type": type(probability).__name__,
                },
            )
        probability_value = float(probability)
        if (
            probability_value != probability_value
            or probability_value < 0.0
            or probability_value > 1.0
        ):
            raise PySparkValueError(
                errorClass="VALUE_OUT_OF_BOUND",
                messageParameters={
                    "arg_name": "probabilities",
                    "arg_value": str(probability),
                },
            )

    if not columns or not probs:
        empty: list[list[float]] = [[] for _ in columns]
        return empty[0] if single else empty
    cells = frame.agg(
        *[percentile_approx(name, probs).alias(f"_q{index}") for index, name in enumerate(columns)]
    ).collect()
    row = list(cells[0]) if cells else [None] * len(columns)
    results: list[list[float]] = []
    for raw in row:
        values = raw if raw is not None else [None] * len(probs)
        results.append([float("nan") if value is None else float(value) for value in values])
    return results[0] if single else results


def _corr(frame: DataFrame, col1: str, col2: str, method: str | None = None) -> float:
    """Pearson correlation of two columns (PySpark ``DataFrame.corr`` / ``stat.corr``)."""
    from repark.errors import PySparkTypeError, PySparkValueError
    from repark.spark.functions import corr as f_corr

    if not isinstance(col1, str):
        raise PySparkTypeError(
            errorClass="NOT_STR",
            messageParameters={"arg_name": "col1", "arg_type": type(col1).__name__},
        )
    if not isinstance(col2, str):
        raise PySparkTypeError(
            errorClass="NOT_STR",
            messageParameters={"arg_name": "col2", "arg_type": type(col2).__name__},
        )
    resolved_method = method if method else "pearson"
    if resolved_method != "pearson":
        raise PySparkValueError(
            errorClass="VALUE_NOT_PEARSON",
            messageParameters={"arg_name": "method", "arg_value": str(resolved_method)},
        )
    rows = frame.agg(f_corr(col1, col2).alias("_corr")).collect()
    value = rows[0][0] if rows else None
    return float("nan") if value is None else float(value)


def _cov(frame: DataFrame, col1: str, col2: str) -> float:
    """Sample covariance of two columns (PySpark ``DataFrame.cov`` / ``stat.cov``)."""
    from repark.errors import PySparkTypeError
    from repark.spark.functions import covar_samp

    if not isinstance(col1, str):
        raise PySparkTypeError(
            errorClass="NOT_STR",
            messageParameters={"arg_name": "col1", "arg_type": type(col1).__name__},
        )
    if not isinstance(col2, str):
        raise PySparkTypeError(
            errorClass="NOT_STR",
            messageParameters={"arg_name": "col2", "arg_type": type(col2).__name__},
        )
    rows = frame.agg(covar_samp(col1, col2).alias("_cov")).collect()
    value = rows[0][0] if rows else None
    return float("nan") if value is None else float(value)


def _crosstab(frame: DataFrame, col1: str, col2: str) -> DataFrame:
    """Pair-wise frequency table (PySpark ``DataFrame.crosstab`` / ``stat.crosstab``).

    First column is named ``{col1}_{col2}``; remaining columns are the distinct
    string forms of ``col2`` values with occurrence counts (missing pairs → 0).
    """
    from repark.errors import PySparkTypeError
    from repark.spark.functions import col

    if not isinstance(col1, str):
        raise PySparkTypeError(
            errorClass="NOT_STR",
            messageParameters={"arg_name": "col1", "arg_type": type(col1).__name__},
        )
    if not isinstance(col2, str):
        raise PySparkTypeError(
            errorClass="NOT_STR",
            messageParameters={"arg_name": "col2", "arg_type": type(col2).__name__},
        )
    left_name = f"{col1}_{col2}"
    staged = frame.select(
        col(col1).cast("string").alias(left_name),
        col(col2).cast("string").alias(col2),
    )
    pivoted = staged.groupBy(left_name).pivot(col2).count()
    return pivoted.na.fill(0)


def _freq_items(frame: DataFrame, cols: list[str], support: float | None = None) -> DataFrame:
    """Reject frequent-item discovery because it is not implemented."""
    del cols, support
    raise UnsupportedOperationException(
        "DataFrame.stat.freqItems is not supported yet (disclosed R-DF-BATCH2)"
    )
