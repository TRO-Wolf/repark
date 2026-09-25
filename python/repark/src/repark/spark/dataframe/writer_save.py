"""Iceberg ``DataFrameWriter.saveAsTable`` and ``save(name)`` statement composition.

The native ``writer_plan`` kernel owns every decision: it maps the action, target, mode and
partitionBy/bucketBy layout to ``ctas``, ``rtas``, ``append``, ``overwrite`` or ``skip``,
compares the layout with an existing table's partitioning, and raises Spark's refusals.
``writer_target_is_table`` says whether a ``save()`` target names a table. This module only
composes the SQL for the kernel's answer.
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
    """Build the quoted Iceberg CTAS (or RTAS) statement for the writer's layout."""
    verb = "CREATE OR REPLACE TABLE" if or_replace else "CREATE TABLE"
    partition_clause = ""
    if writer._partition_columns:
        quoted_parts = ", ".join(quote_ident(column) for column in writer._partition_columns)
        partition_clause = f" PARTITIONED BY ({quoted_parts})"
    return (
        f"{verb} {table_ref} USING iceberg{partition_clause}{bucket_clause(writer)} "
        f"AS SELECT * FROM {view}"
    )


def plan_statement(writer: DataFrameWriter, action: str, target: str, qualified: str | None) -> str:
    """Ask the native kernel which statement the write runs, raising Spark's refusals."""
    layout = (
        [str(column) for column in writer._partition_columns],
        writer._num_buckets,
        [str(column) for column in writer._bucket_columns],
        [str(column) for column in writer._sort_columns],
    )
    return str(
        _native.writer_plan(
            writer._dataframe._session,
            action,
            target,
            qualified,
            writer._mode,
            writer._format_explicit,
            writer._dataframe._analyzed_arrow_schema(),
            layout,
        )
    )


def write_table(
    writer: DataFrameWriter, action: str, target: str, qualified: str | None, table_ref: str
) -> None:
    """Run the kernel's statement for a ``saveAsTable`` or ``save(name)`` table write."""
    statement = plan_statement(writer, action, target, qualified)
    if statement == "skip":
        return
    if statement in ("ctas", "rtas"):
        writer._dataframe._refuse_tightened_iceberg_create()
        writer._run_through_temp_view(
            lambda view: ctas_sql(writer, table_ref, view=view, or_replace=statement == "rtas"),
            writer._options,
        )
        return
    session = writer._dataframe._session
    if statement == "overwrite":
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


def save_iceberg(writer: DataFrameWriter, target: str) -> None:
    """Write ``df.write.format('iceberg').save(target)``; the kernel refuses path targets."""
    from repark.spark.dataframe.writer_readwriter import _resolve_writer_table

    qualified = None
    table_ref = target
    if _native.writer_target_is_table(target, writer._format_explicit):
        qualified, table_ref = _resolve_writer_table(writer._dataframe, target)
    write_table(writer, "save", target, qualified, table_ref)
