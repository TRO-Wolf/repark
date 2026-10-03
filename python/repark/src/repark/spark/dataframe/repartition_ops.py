"""Single-node repartition no-ops, bound as :class:`DataFrame` methods."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.errors import AnalysisException, PySparkTypeError, PySparkValueError
from repark.spark.column import Column

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame


def repartition(frame: DataFrame, numPartitions: Any, *cols: Any) -> DataFrame:  # noqa: N803
    """Accept ``repartition`` as a no-op (single-node; plan unchanged — disclosed).

    Validates Spark-shaped first-arg types so Apache ``test_repartition`` error-class
    pins land before the identity child is returned. List / bool / other non
    int-or-Column-or-str first args raise ``NOT_COLUMN_OR_STR`` whether or not
    ``*cols`` is present (Spark parity — sole-arg list must not silently no-op).
    """
    frame._ensure_alive()
    if isinstance(numPartitions, bool) or (
        not isinstance(numPartitions, (int, str)) and not isinstance(numPartitions, Column)
    ):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={
                "arg_name": "numPartitions",
                "arg_type": type(numPartitions).__name__,
            },
        )
    _ = cols
    frame._refuse_self_join_refs(
        [column for column in (numPartitions, *cols) if isinstance(column, Column)]
    )
    return frame._identity_child()


def repartitionByRange(  # noqa: N802
    frame: DataFrame,
    numPartitions: Any,  # noqa: N803 — PySpark parameter name
    *cols: Any,
) -> DataFrame:
    """Accept ``repartitionByRange`` as a no-op (single-node; disclosed).

    Type-checks the first argument against Spark's
    ``NOT_COLUMN_OR_INT_OR_STR`` surface (Apache ``test_repartition_by_range``). Real
    multi-partition range assignment is an engine seed (``spark_partition_id`` family).
    """
    frame._ensure_alive()
    if isinstance(numPartitions, list):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_INT_OR_STR",
            messageParameters={
                "arg_name": "numPartitions",
                "arg_type": "list",
            },
        )
    if isinstance(numPartitions, bool) or (
        not isinstance(numPartitions, (int, str)) and not isinstance(numPartitions, Column)
    ):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_INT_OR_STR",
            messageParameters={
                "arg_name": "numPartitions",
                "arg_type": type(numPartitions).__name__,
            },
        )
    _ = cols
    frame._refuse_self_join_refs(
        [column for column in (numPartitions, *cols) if isinstance(column, Column)]
    )
    return frame._identity_child()


def repartitionById(  # noqa: N802
    frame: DataFrame,
    numPartitions: Any,  # noqa: N803
    partitionIdExpr: Any,  # noqa: N803
) -> DataFrame:
    """Accept ``repartitionById`` as a single-node no-op after Spark-shaped validation.

    Validates ``numPartitions`` (``NOT_INT`` / ``VALUE_NOT_POSITIVE``) so Apache error
    pins pass. A bare string / simple-name :class:`Column` whose schema type is not
    integer-family raises :class:`~repark.errors.AnalysisException` at plan time
    (Apache ``test_repartition_by_id_error_non_int_type``). Actual partition-id
    routing needs multi-partition execution + ``spark_partition_id`` (engine seed).
    """
    frame._ensure_alive()
    if isinstance(numPartitions, bool) or not isinstance(numPartitions, int):
        raise PySparkTypeError(
            errorClass="NOT_INT",
            messageParameters={
                "arg_name": "numPartitions",
                "arg_type": type(numPartitions).__name__,
            },
        )
    if numPartitions <= 0:
        raise PySparkValueError(
            errorClass="VALUE_NOT_POSITIVE",
            messageParameters={
                "arg_name": "numPartitions",
                "arg_value": str(numPartitions),
            },
        )
    column_name: str | None = None
    if isinstance(partitionIdExpr, str):
        column_name = partitionIdExpr
    elif isinstance(partitionIdExpr, Column):
        display = partitionIdExpr.spark_display_part()
        if display.isidentifier() and display in frame.columns:
            column_name = display
    if column_name is not None:
        type_keys = {name: type_key for name, type_key, _ in frame._inner.logical_schema_fields()}
        type_key = type_keys.get(column_name, "")
        if type_key not in {"int", "long", "byte", "short"}:
            raise AnalysisException(
                f"repartitionById requires an integer partition expression; "
                f"column `{column_name}` has type `{type_key or 'unknown'}`"
            )
    return frame._identity_child()
