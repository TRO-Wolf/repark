"""Iceberg ``DataFrameWriter.save(name)`` and bucketed ``saveAsTable`` routing.

The native kernels own every decision: ``writer_save_target`` maps a save target and mode to
create, append, overwrite, or skip (raising Spark's refusals), and ``writer_check_layout``
compares the writer's partitionBy/bucketBy layout with an existing table's partitioning.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from repark import _native
from repark.spark._idents import quote_ident
from repark.spark.dataframe import writer_schema

if TYPE_CHECKING:
    from repark.spark.dataframe.writer_readwriter import DataFrameWriter


def bucket_clause(writer: DataFrameWriter) -> str:
    """Render the writer's bucketBy/sortBy state as Spark's CREATE TABLE bucket clause."""
    if writer._num_buckets is None:
        return ""
    buckets = ", ".join(quote_ident(column) for column in writer._bucket_columns)
    sorted_by = ""
    if writer._sort_columns:
        sorted_by = " SORTED BY (" + ", ".join(map(quote_ident, writer._sort_columns)) + ")"
    return f" CLUSTERED BY ({buckets}){sorted_by} INTO {writer._num_buckets} BUCKETS"


def ctas_sql(
    writer: DataFrameWriter, table_ref: str, *, view: str, or_replace: bool = False
) -> str:
    """Build the quoted Iceberg CTAS (or RTAS) statement for the writer's layout.

    A bucketed overwrite always replaces, as Spark's ``ReplaceTableAsSelect`` does, so a new
    table records an ``overwrite`` snapshot rather than an ``append``.
    """
    bucketed_overwrite = writer._num_buckets is not None and writer._mode == "overwrite"
    verb = "CREATE OR REPLACE TABLE" if or_replace or bucketed_overwrite else "CREATE TABLE"
    partition_clause = ""
    if writer._partition_columns:
        quoted_parts = ", ".join(quote_ident(column) for column in writer._partition_columns)
        partition_clause = f" PARTITIONED BY ({quoted_parts})"
    return (
        f"{verb} {table_ref} USING iceberg{partition_clause}{bucket_clause(writer)} "
        f"AS SELECT * FROM {view}"
    )


def check_layout(writer: DataFrameWriter, qualified: str) -> None:
    """Refuse a write whose partitionBy/bucketBy layout differs from the existing table's."""
    from repark.spark.session.sql_relations import _parse_table_identifier_segments

    _native.writer_check_layout(
        writer._dataframe._session,
        _parse_table_identifier_segments(qualified),
        [str(column) for column in writer._partition_columns],
        writer._num_buckets,
        [str(column) for column in writer._bucket_columns],
        [str(column) for column in writer._sort_columns],
    )


def write_bucketed_existing(
    writer: DataFrameWriter, qualified: str, table_ref: str, mode: str
) -> None:
    """Bucketed saveAsTable onto an existing table: RTAS on overwrite, checked append else."""
    if mode == "overwrite":
        writer._dataframe._refuse_tightened_iceberg_create()
        writer._run_through_temp_view(
            lambda view: ctas_sql(writer, table_ref, view=view, or_replace=True), writer._options
        )
        return
    check_layout(writer, qualified)
    session = writer._dataframe._session
    writer._run_through_temp_view(
        writer_schema.append_statement(session, writer._dataframe, table_ref), writer._options
    )


def save_iceberg(writer: DataFrameWriter, target: str) -> None:
    """Write ``df.write.format('iceberg').save(target)`` where the target names a table.

    Without an explicit ``format('iceberg')`` the kernel keeps the declared default-format
    refusal: Spark's default source is parquet, not a table write.
    """
    from repark.spark.dataframe.writer_readwriter import _resolve_writer_table

    session = writer._dataframe._session
    explicit = writer._format_explicit
    qualified = table_ref = None
    if explicit and "/" not in target:
        qualified, table_ref = _resolve_writer_table(writer._dataframe, target)
    action = _native.writer_save_target(session, target, qualified, writer._mode, explicit)
    if action == "skip" or qualified is None or table_ref is None:
        return
    if action == "create":
        writer._dataframe._refuse_tightened_iceberg_create()
        writer._run_through_temp_view(
            lambda view: ctas_sql(writer, table_ref, view=view), writer._options
        )
        return
    check_layout(writer, qualified)
    if action == "overwrite":
        columns, projection = writer._by_name_projection(session, table_ref, display_name=target)
        insert = f"INSERT OVERWRITE {table_ref} ({columns}) SELECT {projection} FROM"
        writer._run_through_temp_view(
            lambda view: f"{insert} {view}",
            writer._options,
            static_overwrite=True,
        )
        return
    writer._run_through_temp_view(
        writer_schema.append_statement(session, writer._dataframe, table_ref), writer._options
    )
