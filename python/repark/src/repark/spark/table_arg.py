"""``TableArg`` — a DataFrame as a table argument to a TVF/UDTF (PySpark ``table_arg``).

Obtained via ``DataFrame.asTable()``; carries the frame plus partitioning / ordering /
single-partition constraints that the UDTF call path honors when feeding rows to the
handler's ``eval``.

pins: df-subquery-1
"""

from __future__ import annotations

import functools
from collections.abc import Iterator
from typing import Any

from repark.errors import (
    IllegalArgumentException,
    PySparkException,
    PySparkTypeError,
    UnsupportedOperationException,
)
from repark.spark.udtf import (
    _TABLE_ARG_BLOCKED_MESSAGE,
    _build_output_batch,
    _normalize_eval_rows,
    _return_type_to_map_schema,
)

_ORDER_BEFORE_PARTITION = "Please call partitionBy() or withSinglePartition() before orderBy()."
_SINGLE_AFTER_PARTITION = (
    "Cannot call withSinglePartition() after partitionBy() or "
    "withSinglePartition() has been called."
)
_PARTITION_AFTER_SINGLE = "Cannot call partitionBy() after withSinglePartition() has been called."


def _column_names(cols: tuple[Any, ...]) -> list[str]:
    """Flatten ``str | Column | list`` arguments into column names."""
    from repark.spark.column import Column

    if len(cols) == 1 and isinstance(cols[0], list):
        cols = tuple(cols[0])
    names: list[str] = []
    for item in cols:
        if isinstance(item, str):
            names.append(item)
        elif isinstance(item, Column):
            names.append(item.spark_display_part())
        else:
            raise PySparkTypeError(
                f"table argument columns must be str or Column, got {type(item).__name__}"
            )
    return names


class TableArg:
    """A table argument to a TVF/UDTF (PySpark ``table_arg.TableArg``).

    Holds the source frame plus partitioning (``partitionBy``), in-partition ordering
    (``orderBy``) and single-partition (``withSinglePartition``) constraints.
    """

    __slots__ = ("_frame", "_order_cols", "_partition_cols", "_partitioned", "_single")

    def __init__(
        self,
        frame: Any,
        *,
        partition_cols: list[str] | None = None,
        order_cols: list[str] | None = None,
        partitioned: bool = False,
        single: bool = False,
    ) -> None:
        """Wrap ``frame`` as a table argument; internal constraints are keyword-only."""
        self._frame = frame
        self._partition_cols = list(partition_cols or [])
        self._order_cols = list(order_cols or [])
        self._partitioned = partitioned
        self._single = single

    def partitionBy(self, *cols: Any) -> TableArg:  # noqa: N802 — PySpark method name
        """Partition the table argument by the given columns (PySpark ``TableArg.partitionBy``)."""
        if self._single:
            raise IllegalArgumentException(_PARTITION_AFTER_SINGLE)
        return TableArg(
            self._frame,
            partition_cols=self._partition_cols + _column_names(cols),
            order_cols=self._order_cols,
            partitioned=True,
        )

    def orderBy(self, *cols: Any) -> TableArg:  # noqa: N802 — PySpark method name
        """Order the table argument within partitions (PySpark ``TableArg.orderBy``)."""
        if not self._partitioned and not self._single:
            raise IllegalArgumentException(_ORDER_BEFORE_PARTITION)
        return TableArg(
            self._frame,
            partition_cols=self._partition_cols,
            order_cols=self._order_cols + _column_names(cols),
            partitioned=self._partitioned,
            single=self._single,
        )

    def withSinglePartition(self) -> TableArg:  # noqa: N802 — PySpark method name
        """Treat the table argument as a single partition (``TableArg.withSinglePartition``)."""
        if self._partitioned or self._single:
            raise IllegalArgumentException(_SINGLE_AFTER_PARTITION)
        return TableArg(
            self._frame,
            partition_cols=self._partition_cols,
            order_cols=self._order_cols,
            partitioned=self._partitioned,
            single=True,
        )


def _as_table_arg(arg: Any, *, surface: str) -> Any:
    """Coerce a ``TableArg`` or bare DataFrame into a :class:`TableArg`."""
    if isinstance(arg, TableArg):
        return arg
    if type(arg).__name__ != "DataFrame":
        raise UnsupportedOperationException(f"{surface}: {_TABLE_ARG_BLOCKED_MESSAGE}")
    return TableArg(arg)


def _order_entry_key(
    entry: tuple[tuple[Any, ...], tuple[Any, ...]],
    order_index: list[int],
) -> tuple[tuple[int, Any], ...]:
    """Sort key for in-partition ``orderBy`` — ascending, nulls first."""
    values, _scalars = entry
    return tuple(
        (0, None) if values[index] is None else (1, values[index]) for index in order_index
    )


def _map_table_udtf_batches(
    batches: Iterator[Any],
    *,
    handler_cls: type[Any],
    table_arg: Any,
    layout: list[tuple[str, int | None]],
    table_field_names: list[str],
    scalar_width: int,
    field_names: list[str],
    arrow_schema: Any,
    surface: str,
) -> Iterator[Any]:
    """Feed table-arg rows (as ``Row``) plus scalar arg values into ``eval`` per partition.

    ``partitionBy`` groups rows across batches so each key's rows reach one fresh handler
    instance (``start``/``eval`` x n/``terminate`` lifecycle); ``orderBy`` sorts within each
    group ascending nulls-first. Buffering is inherent — a table arg feeds every row to
    the handler anyway.
    """
    import traceback

    from repark.spark.row import Row

    table_width = len(table_field_names)
    name_to_index = {name.casefold(): index for index, name in enumerate(table_field_names)}
    partition_index: list[int] = []
    for name in table_arg._partition_cols:
        index = name_to_index.get(name.casefold())
        if index is None:
            raise PySparkException(
                f"UDTF {surface}: partitionBy column {name!r} not found in table argument"
            )
        partition_index.append(index)
    order_index: list[int] = []
    for name in table_arg._order_cols:
        index = name_to_index.get(name.casefold())
        if index is None:
            raise PySparkException(
                f"UDTF {surface}: orderBy column {name!r} not found in table argument"
            )
        order_index.append(index)

    groups: dict[tuple[Any, ...], list[tuple[tuple[Any, ...], tuple[Any, ...]]]] = {}
    for batch in batches:
        for row_index in range(batch.num_rows):
            values = tuple(
                batch.column(column_index)[row_index].as_py() for column_index in range(table_width)
            )
            scalars = tuple(
                batch.column(table_width + column_index)[row_index].as_py()
                for column_index in range(scalar_width)
            )
            key = tuple(values[index] for index in partition_index)
            groups.setdefault(key, []).append((values, scalars))

    out_rows: list[tuple[Any, ...]] = []
    for rows in groups.values():
        if order_index:
            rows.sort(key=lambda entry: _order_entry_key(entry, order_index))
        handler = handler_cls()
        try:
            start = getattr(handler, "start", None)
            if callable(start):
                try:
                    start()
                except Exception as error:
                    detail = traceback.format_exc()
                    raise PySparkException(
                        f"UDTF {surface} start() raised {type(error).__name__}: {error}\n{detail}"
                    ) from error
            for values, scalars in rows:
                row = Row(**dict(zip(table_field_names, values, strict=True)))
                python_args = tuple(
                    row if kind == "table" else scalars[int(index)] for kind, index in layout
                )
                try:
                    result = handler.eval(*python_args)
                except PySparkException:
                    raise
                except Exception as error:
                    detail = traceback.format_exc()
                    raise PySparkException(
                        f"UDTF {surface} eval() raised {type(error).__name__}: {error}\n{detail}"
                    ) from error
                out_rows.extend(
                    _normalize_eval_rows(
                        result,
                        expected_width=len(field_names),
                        surface=surface,
                    )
                )
        finally:
            terminate = getattr(handler, "terminate", None)
            if callable(terminate):
                try:
                    terminate()
                except Exception as error:
                    detail = traceback.format_exc()
                    raise PySparkException(
                        f"UDTF {surface} terminate() raised "
                        f"{type(error).__name__}: {error}\n{detail}"
                    ) from error

    yield _build_output_batch(out_rows, field_names, arrow_schema)


def _execute_table_udtf(
    *,
    session: Any,
    handler_cls: type[Any],
    return_struct: Any,
    layout: list[tuple[str, int | None]],
    table_arg: Any,
    scalar_columns: list[Any],
    surface: str,
) -> Any:
    """Run a table-arg UDTF: the table frame plus lit-appended scalar columns feed ``eval``."""
    schema_ddl = _return_type_to_map_schema(return_struct)
    frame = table_arg._frame
    for index, column in enumerate(scalar_columns):
        frame = frame.with_column(f"__repark_udtf_sarg_{index}", column)

    from repark.spark.dataframe import _coerce_map_in_arrow_schema

    _declared, arrow_schema = _coerce_map_in_arrow_schema(schema_ddl)
    field_names = [field.name for field in return_struct.fields]
    return frame.mapInArrow(
        functools.partial(
            _map_table_udtf_batches,
            handler_cls=handler_cls,
            table_arg=table_arg,
            layout=layout,
            table_field_names=list(table_arg._frame.columns),
            scalar_width=len(scalar_columns),
            field_names=field_names,
            arrow_schema=arrow_schema,
            surface=surface,
        ),
        schema_ddl,
    )


__all__ = ["TableArg"]
