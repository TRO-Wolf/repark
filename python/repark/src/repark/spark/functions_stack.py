"""Spark ``stack`` generator (PERF-UNPIVOT-1)."""

from __future__ import annotations

import re
from typing import Any

from repark.errors import AnalysisException, PySparkTypeError
from repark.spark.column import Column
from repark.spark.functions import _column_argument

STACK_NAMES: tuple[str, ...] = ("stack",)

_INT_LITERAL = re.compile(r"^(?:Int(?:32|64)\()?(-?\d+)\)?$")


class StackCall:
    """Marker for ``F.stack`` lowered at ``DataFrame.select``."""

    def __init__(
        self,
        n: int,
        args: tuple[Column, ...],
        output_names: tuple[str, ...] | None = None,
    ) -> None:
        """Store n, the stacked arguments, and optional output names."""
        self.n = n
        self.args = args
        self.output_names = output_names
        self._generator = "stack"

    def alias(self, *names: str) -> StackCall:
        """Name the stacked output columns (PySpark ``stack(...).alias(x, y)``)."""
        if not names:
            raise PySparkTypeError(
                errorClass="CANNOT_BE_EMPTY",
                messageParameters={"item": "alias"},
            )
        return StackCall(self.n, self.args, names)


def _stack_n(n: Column | str | int) -> int:
    if isinstance(n, bool):
        raise AnalysisException(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve stack due to data type "
            'mismatch: The first parameter requires the "INT" type, however "BOOLEAN" was '
            "given. SQLSTATE: 42K09"
        )
    if isinstance(n, int):
        if n <= 0 or n > 2_147_483_647:
            raise AnalysisException(
                "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] Cannot resolve stack due to data type "
                f"mismatch: The `n` must be between (0, 2147483647] (current value = {n}). "
                "SQLSTATE: 42K09"
            )
        return n
    if isinstance(n, str):
        raise AnalysisException(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve stack due to data type "
            'mismatch: The first parameter requires the "INT" type. SQLSTATE: 42K09'
        )
    if isinstance(n, Column) and n._is_foldable:
        text = n.spark_display_part().strip()
        match = _INT_LITERAL.match(text)
        if match is not None:
            return _stack_n(int(match.group(1)))
    raise AnalysisException(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve stack due to data type "
        'mismatch: The first parameter requires the "INT" type. SQLSTATE: 42K09'
    )


def stack(n: Column | str | int, *cols: Column | str) -> StackCall:
    """Separate ``cols`` into ``n`` rows (PySpark ``functions.stack``)."""
    if not cols:
        raise AnalysisException(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `stack` requires > 1 parameters but the "
            "actual number is 1. SQLSTATE: 42605"
        )
    return StackCall(_stack_n(n), tuple(_column_argument(col) for col in cols))


def select_with_stack_if_present(frame: Any, expanded: list[Any]) -> Any | None:
    """Lower a select list that contains one ``StackCall``, else ``None``."""
    stack_items = [item for item in expanded if isinstance(item, StackCall)]
    if not stack_items:
        return None
    if len(stack_items) > 1:
        raise AnalysisException(
            "Only one generator allowed per select list "
            "(Spark: only one explode/posexplode family generator)"
        )
    return _select_with_stack(frame, expanded, stack_items[0])


def _select_with_stack(frame: Any, expanded: list[Any], call: StackCall) -> Any:
    from repark.spark.column import Column as FrameColumn

    passthrough = [item for item in expanded if item is not call]
    if any(getattr(item, "_generator", None) for item in passthrough):
        raise AnalysisException(
            "Only one generator allowed per select list "
            "(Spark: only one explode/posexplode family generator)"
        )
    bound_passthrough = [frame._column_of(item) for item in passthrough]
    bound_args = [
        frame._column_of(item) if not isinstance(item, FrameColumn) else item for item in call.args
    ]
    bound_args = [frame._rebind_origin_column(item) for item in bound_args]
    passthrough_count = len(bound_passthrough)
    current_names = list(frame.columns)
    arg_names = [column._projection_name or column.spark_display_part() for column in bound_args]
    skip_project = (
        passthrough_count == 0
        and arg_names == current_names
        and all(column._stable_name for column in bound_args)
    )
    if skip_project:
        host = frame
    else:
        natives = [column.for_select()._inner for column in bound_passthrough + bound_args]
        host = frame._spawn(frame._plan().select(natives))
    names = list(call.output_names) if call.output_names is not None else None
    from repark import _native

    inner = _native.stack_dataframe(host._plan(), call.n, passthrough_count, names)
    return frame._spawn(inner)


def install_into(namespace: dict[str, Any], exported: list[str]) -> None:
    """Copy ``stack`` onto the canonical functions module."""
    namespace["stack"] = stack
    if "stack" not in exported:
        exported.append("stack")
