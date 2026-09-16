"""Spark generator wrappers — ``posexplode*`` and ``inline*`` answer through the analyzer."""

from __future__ import annotations

from typing import Any

from repark import _native
from repark.errors import AnalysisException, PySparkTypeError, PySparkValueError
from repark.spark.column import Column
from repark.spark.functions import _column_argument

GENERATOR_NAMES: tuple[str, ...] = (
    "posexplode",
    "posexplode_outer",
    "inline",
    "inline_outer",
)


class _GeneratorColumn(Column):
    """Column whose ``_inner`` is a registered generator call expanded by the analyzer rewrite."""

    _repark_generator = True

    def __init__(self, inner: Any, **kwargs: Any) -> None:
        super().__init__(inner, **kwargs)
        self._projection_name = None

    def _reject_nested_generator(self, operation: str) -> None:
        raise AnalysisException(
            "[UNSUPPORTED_GENERATOR] The generator expression cannot be nested inside "
            f"{operation} (Spark: a generator is only valid as a top-level select projection)"
        )

    def for_select(self) -> Column:
        """Keep the generator call bare so the analyzer rewrite names its outputs."""
        return self

    def alias(self, *alias: str, metadata: dict[str, Any] | None = None) -> Column:
        """Name every generator output column (PySpark multi-name ``alias``)."""
        if not alias:
            raise PySparkTypeError(
                errorClass="CANNOT_BE_EMPTY",
                messageParameters={"item": "alias"},
            )
        if metadata is not None and len(alias) != 1:
            raise PySparkValueError(
                errorClass="ONLY_ALLOWED_FOR_SINGLE_COLUMN",
                messageParameters={"arg_name": "metadata"},
            )
        name_inners = [_native.PyColumn.literal(name) for name in alias]
        parts = _native.PyColumnParts.call_scalar(
            "__repark_gen_alias",
            [self._inner, *name_inners],
            [self.spark_wrap_display_part(), *alias],
            [self.sql_expr_part(), *alias],
            [self.join_sql_part(), *alias],
            None,
        )
        return _GeneratorColumn(
            parts[0],
            spark_display=parts[1],
            sql_expr=parts[2],
            join_sql_expr=parts[3],
        )


def _generator_call(name: str, arg: Column | str) -> Column:
    column = _column_argument(arg)
    column._reject_nested_generator(f"function {name}")
    parts = _native.PyColumnParts.call_scalar(
        name,
        [column._inner],
        [column.spark_wrap_display_part()],
        [column.sql_expr_part()],
        [column.join_sql_part()],
        None,
    )
    return _GeneratorColumn(
        parts[0],
        spark_display=parts[1],
        sql_expr=parts[2],
        join_sql_expr=parts[3],
    )


def posexplode(col: Column | str) -> Column:
    """Generator emitting ``pos``/``col`` (arrays) or ``pos``/``key``/``value`` (maps)."""
    return _generator_call("posexplode", col)


def posexplode_outer(col: Column | str) -> Column:
    """``posexplode`` keeping a row of NULLs for a NULL or empty input."""
    return _generator_call("posexplode_outer", col)


def inline(col: Column | str) -> Column:
    """Generator over an array of structs emitting one column per struct field."""
    return _generator_call("inline", col)


def inline_outer(col: Column | str) -> Column:
    """``inline`` keeping a row of NULLs for a NULL or empty input."""
    return _generator_call("inline_outer", col)


def install_into(namespace: dict[str, Any], exported: list[str]) -> None:
    """Install the four generator wrappers onto the canonical functions module."""
    for function in (posexplode, posexplode_outer, inline, inline_outer):
        namespace[function.__name__] = function
        if function.__name__ not in exported:
            exported.append(function.__name__)
