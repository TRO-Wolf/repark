"""Writer layout — bucketBy, sortBy, clusterBy state, action-time checks, and write helpers."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from pathlib import Path
from types import MethodType
from typing import TYPE_CHECKING, Any, NoReturn

from repark import _native
from repark.errors import (
    AnalysisException,
    PySparkNotImplementedError,
    PySparkTypeError,
    PySparkValueError,
)
from repark.spark._idents import escape_sql_single_quotes
from repark.spark._integral import (
    _attached_error_class,
    _attached_message_parameters,
    _attached_sql_state,
)
from repark.spark._temp_views import scratch_view_name

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame
    from repark.spark.dataframe.writer_readwriter import DataFrameWriter, DataFrameWriterV2

_MAX_BUCKET_COUNT = 100000


def _raise_analysis(
    message: str,
    error_class: str,
    message_parameters: dict[str, str] | None = None,
    sql_state: str | None = None,
) -> NoReturn:
    """Raise ``AnalysisException`` carrying Spark's errorClass, params, and SQLSTATE."""
    error = AnalysisException(message)
    error._spark_error_class = error_class
    error._spark_message_parameters = message_parameters
    error._spark_sql_state = sql_state
    error.getErrorClass = MethodType(_attached_error_class, error)
    error.getCondition = MethodType(_attached_error_class, error)
    error.getMessageParameters = MethodType(_attached_message_parameters, error)
    error.getSqlState = MethodType(_attached_sql_state, error)
    raise error


def run_through_temp_view(
    dataframe: DataFrame,
    build_sql: Callable[[str], str],
    options: dict[str, str] | None,
    prefix: str,
    static_overwrite: bool = False,
    dynamic_overwrite: bool = False,
) -> None:
    """Register a temp view, run the built write SQL with options, drop the view."""
    dataframe._ensure_alive()
    session = dataframe._session
    view_name = scratch_view_name(session, prefix)
    session.create_or_replace_temp_view(view_name, dataframe._native_for_registration())
    try:
        _native.session_sql_with_write_options(
            session, build_sql(view_name), options or {}, static_overwrite, dynamic_overwrite
        )
    finally:
        session.drop_temp_view(view_name)


def run_overwrite_partitions(writer: DataFrameWriterV2, build_sql: Callable[[str], str]) -> None:
    """Run ``writeTo(t).overwritePartitions()`` SQL as a dynamic overwrite in every session mode."""
    run_through_temp_view(
        writer._dataframe, build_sql, writer._options, "_repark_writer_v2_", dynamic_overwrite=True
    )


def store_writer_option(options: dict[str, str], key: object, value: object) -> None:
    """Store one writer option with case-insensitive last-wins dedup."""
    key_str = str(key)
    for existing in list(options):
        if existing.lower() == key_str.lower():
            del options[existing]
    options[key_str] = str(value)


def _raise_operation_not_support_bucketing(operation: str) -> NoReturn:
    """Raise Spark's ``_LEGACY_ERROR_TEMP_1312`` for a bucketed action."""
    _raise_analysis(
        f"'{operation}' does not support bucketBy right now.",
        "_LEGACY_ERROR_TEMP_1312",
        message_parameters={"operation": operation},
    )


def _raise_operation_not_support_bucketing_and_sorting(operation: str) -> NoReturn:
    """Raise Spark's ``_LEGACY_ERROR_TEMP_1313`` for a bucketed and sorted action."""
    _raise_analysis(
        f"'{operation}' does not support bucketBy and sortBy right now.",
        "_LEGACY_ERROR_TEMP_1313",
        message_parameters={"operation": operation},
    )


def _raise_sort_by_without_bucketing() -> NoReturn:
    """Raise Spark's ``SORT_BY_WITHOUT_BUCKETING`` refusal."""
    _raise_analysis(
        "[SORT_BY_WITHOUT_BUCKETING] sortBy must be used together with bucketBy. SQLSTATE: 42601",
        "SORT_BY_WITHOUT_BUCKETING",
        message_parameters={},
        sql_state="42601",
    )


def _raise_cluster_by_with_partitioned_by() -> NoReturn:
    """Raise Spark's ``SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED`` refusal."""
    _raise_analysis(
        "[SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED] Cannot specify both "
        "CLUSTER BY and PARTITIONED BY. SQLSTATE: 42908",
        "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED",
        message_parameters={},
        sql_state="42908",
    )


def _raise_cluster_by_with_bucketing() -> NoReturn:
    """Raise Spark's ``SPECIFY_CLUSTER_BY_WITH_BUCKETING_IS_NOT_ALLOWED`` refusal."""
    _raise_analysis(
        "[SPECIFY_CLUSTER_BY_WITH_BUCKETING_IS_NOT_ALLOWED] Cannot specify both "
        "CLUSTER BY and CLUSTERED BY INTO BUCKETS. SQLSTATE: 42908",
        "SPECIFY_CLUSTER_BY_WITH_BUCKETING_IS_NOT_ALLOWED",
        message_parameters={},
        sql_state="42908",
    )


def _bucketed(writer: DataFrameWriter | DataFrameWriterV2) -> bool:
    """Return whether the writer carries a bucketBy spec."""
    return writer._num_buckets is not None


def _unpack_column_args(col: Any, cols: tuple[Any, ...]) -> tuple[str, ...]:
    """Unpack Spark's ``(col, *cols)`` shape, raising its list/tuple and str errors."""
    if isinstance(col, (list, tuple)):
        if len(cols) > 0:
            raise PySparkValueError(
                f"[CANNOT_SET_TOGETHER] `col` of type {type(col).__name__} and `cols` "
                "should not be set together.",
                errorClass="CANNOT_SET_TOGETHER",
                messageParameters={"arg_list": f"`col` of type {type(col).__name__} and `cols`"},
            )
        first = col[0]
        rest = tuple(col[1:])
    else:
        first = col
        rest = cols
    for item in rest:
        if not isinstance(item, str):
            _refuse_not_list_of_str("cols", item)
    if not isinstance(first, str):
        _refuse_not_list_of_str("col", first)
    return (first, *rest)


def _refuse_not_list_of_str(arg_name: str, value: Any) -> None:
    """Raise Spark's NOT_LIST_OF_STR with its rendered sentence."""
    arg_type = type(value).__name__
    raise PySparkTypeError(
        f"[NOT_LIST_OF_STR] Argument `{arg_name}` should be a list[str], got {arg_type}.",
        errorClass="NOT_LIST_OF_STR",
        messageParameters={"arg_name": arg_name, "arg_type": arg_type},
    )


def _backticked_table_name(qualified: str) -> str:
    """Render a resolved multipart table name in Spark's backticked error form."""
    return ".".join(f"`{segment}`" for segment in qualified.split("."))


def bucket_by(writer: DataFrameWriter, num_buckets: Any, col: Any, *cols: Any) -> DataFrameWriter:
    """Record Hive bucketing columns after Spark's call-time checks; chain the writer."""
    if not isinstance(num_buckets, int):
        raise PySparkTypeError(
            f"[NOT_INT] Argument `numBuckets` should be an int, got {type(num_buckets).__name__}.",
            errorClass="NOT_INT",
            messageParameters={
                "arg_name": "numBuckets",
                "arg_type": type(num_buckets).__name__,
            },
        )
    writer._num_buckets = num_buckets
    writer._bucket_columns = list(_unpack_column_args(col, cols))
    return writer


def sort_by(writer: DataFrameWriter, col: Any, *cols: Any) -> DataFrameWriter:
    """Record per-bucket sort columns after Spark's call-time checks."""
    writer._sort_columns = list(_unpack_column_args(col, cols))
    return writer


def cluster_by(writer: DataFrameWriter, *cols: Any) -> DataFrameWriter:
    """Record clustering columns; Spark's bare assert refuses an empty call."""
    if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
        cols = tuple(cols[0])
    assert len(cols) > 0, "clusterBy needs one or more clustering columns."
    writer._cluster_columns = [str(item) for item in cols]
    return writer


def v2_cluster_by(writer: DataFrameWriterV2, col: Any, *cols: Any) -> DataFrameWriterV2:
    """Record V2 clustering column names; non-str names refuse Spark-shaped."""
    for item in (col, *cols):
        if not isinstance(item, str):
            raise PySparkTypeError(
                f"clusterBy expects column names (str), got {type(item).__name__}"
            )
    writer._cluster_columns = [str(item) for item in (col, *cols)]
    return writer


def refuse_bucketed_action(writer: DataFrameWriter, operation: str) -> None:
    """Refuse a bucketed or sorted non-catalog action (Spark's ``assertNotBucketed``)."""
    if _bucketed(writer):
        if writer._sort_columns:
            _raise_operation_not_support_bucketing_and_sorting(operation)
        _raise_operation_not_support_bucketing(operation)
    if writer._sort_columns:
        _raise_sort_by_without_bucketing()


def assert_no_sort_without_bucketing(writer: DataFrameWriter) -> None:
    """Refuse a table write whose sortBy has no bucketBy."""
    if writer._sort_columns and not _bucketed(writer):
        _raise_sort_by_without_bucketing()


def assert_no_cluster_conflicts(writer: DataFrameWriter) -> None:
    """Refuse a table write that combines clusterBy with partitionBy or bucketBy."""
    if not writer._cluster_columns:
        return
    if writer._partition_columns:
        _raise_cluster_by_with_partitioned_by()
    if _bucketed(writer):
        _raise_cluster_by_with_bucketing()


def assert_bucket_spec_valid_for_table_write(writer: DataFrameWriter, qualified_table: str) -> None:
    """Refuse an out-of-range bucket count or a bucket column missing from the frame."""
    if not _bucketed(writer):
        return
    num_buckets = writer._num_buckets
    if num_buckets <= 0 or num_buckets > _MAX_BUCKET_COUNT:
        _raise_analysis(
            f"[INVALID_BUCKET_COUNT] Number of buckets should be greater than 0 but less than "
            f"or equal to bucketing.maxBuckets (`{_MAX_BUCKET_COUNT}`). Got `{num_buckets}`. "
            "SQLSTATE: 22003",
            "INVALID_BUCKET_COUNT",
            message_parameters={
                "bucketingMaxBuckets": str(_MAX_BUCKET_COUNT),
                "numBuckets": str(num_buckets),
            },
            sql_state="22003",
        )
    frame_columns = list(writer._dataframe.columns)
    present = {str(name).casefold() for name in frame_columns}
    for column in writer._bucket_columns:
        if str(column).casefold() not in present:
            table_name = _backticked_table_name(qualified_table)
            table_cols = ", ".join(f"`{name}`" for name in frame_columns)
            _raise_analysis(
                f"[COLUMN_NOT_DEFINED_IN_TABLE] bucket column `{column}` is not defined in "
                f"table {table_name}, defined table columns are: {table_cols}. SQLSTATE: 42703",
                "COLUMN_NOT_DEFINED_IN_TABLE",
                message_parameters={
                    "colName": f"`{column}`",
                    "colType": "bucket",
                    "tableCols": table_cols,
                    "tableName": table_name,
                },
                sql_state="42703",
            )


def refuse_bucketed_table_write(writer: DataFrameWriter) -> None:
    """Refuse a bucketed Iceberg table write (Ruling R-1) pointing at ``F.bucket``."""
    if not _bucketed(writer):
        return
    feature = "bucketBy on an Iceberg table (use writeTo(...).partitionedBy(F.bucket(n, col)))"
    raise PySparkNotImplementedError(
        f"[NOT_IMPLEMENTED] {feature} is not implemented.",
        errorClass="NOT_IMPLEMENTED",
        messageParameters={"feature": feature},
    )


def refuse_clustered_table_write(
    writer: DataFrameWriter | DataFrameWriterV2,
) -> None:
    """Refuse a clustered Iceberg table write (Ruling R-2)."""
    if not writer._cluster_columns:
        return
    feature = "clusterBy on an Iceberg table"
    raise PySparkNotImplementedError(
        f"[NOT_IMPLEMENTED] {feature} is not implemented.",
        errorClass="NOT_IMPLEMENTED",
        messageParameters={"feature": feature},
    )


def refuse_bucketed_or_clustered_table_write(writer: DataFrameWriter, qualified_table: str) -> None:
    """Run the table-write bucketing and clustering checks in Spark's order."""
    assert_bucket_spec_valid_for_table_write(writer, qualified_table)
    refuse_bucketed_table_write(writer)
    refuse_clustered_table_write(writer)


def assert_v2_cluster_conflicts(writer: DataFrameWriterV2) -> None:
    """Refuse a V2 create/replace action that combines clusterBy with partitionedBy."""
    if writer._cluster_columns and writer._partition_exprs:
        _raise_cluster_by_with_partitioned_by()


def _sql_option_escape(value: str) -> str:
    """Escape a single-quoted COPY OPTIONS value."""
    return escape_sql_single_quotes(str(value))


def _normalize_write_compression(raw: str) -> str:
    """Map Spark compression names to DataFusion CSV or JSON compression tokens."""
    lowered = str(raw).strip().lower()
    if lowered in {"", "none", "uncompressed"}:
        return "uncompressed"
    if lowered in {"gzip", "gz"}:
        return "gzip"
    if lowered in {"bzip2", "bz2"}:
        return "bzip2"
    if lowered == "xz":
        return "xz"
    if lowered in {"zstd", "zst"}:
        return "zstd"
    raise AnalysisException(
        f"unsupported write compression {raw!r}; "
        "repark supports gzip, bzip2, xz, zstd, none/uncompressed"
    )


def _normalize_parquet_write_compression(raw: str) -> str:
    """Map Spark Parquet compression names to DataFusion tokens."""
    lowered = str(raw).strip().lower()
    if lowered in {"", "none", "uncompressed"}:
        return "uncompressed"
    if lowered == "snappy":
        return "snappy"
    if lowered in {"gzip", "gz"}:
        return "gzip(6)"
    if lowered in {"zstd", "zst"}:
        return "zstd(3)"
    if lowered == "lz4":
        return "lz4"
    if lowered.startswith("gzip(") or lowered.startswith("zstd(") or lowered.startswith("brotli("):
        return lowered
    raise AnalysisException(
        f"unsupported parquet write compression {raw!r}; "
        "repark supports snappy, gzip, zstd, lz4, none/uncompressed"
    )


def _merge_path_write_tree(staging: Any, destination: Any) -> None:
    """Merge a staged COPY tree into a destination without replacing existing parts."""
    import shutil

    staging_path = Path(staging)
    destination_path = Path(destination)
    destination_path.mkdir(parents=True, exist_ok=True)
    if not staging_path.is_dir():
        target = destination_path / staging_path.name
        if target.exists():
            target = destination_path / f"part-append-{uuid.uuid4().hex[:12]}{staging_path.suffix}"
        shutil.move(str(staging_path), str(target))
        return
    for item in staging_path.iterdir():
        target = destination_path / item.name
        if item.is_dir():
            _merge_path_write_tree(item, target)
            continue
        if target.exists():
            target = destination_path / f"part-append-{uuid.uuid4().hex[:12]}{item.suffix}"
        shutil.move(str(item), str(target))
