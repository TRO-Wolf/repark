"""Bitwise facade wrappers (FN-F).

Public names are re-exported from ``functions.py``. ``call_scalar`` has no
``bitwise_not`` arm; the SHIM is ``Column.bitwiseXOR(lit(-1))`` (two's-complement
complement). Shift / ``bit_count`` / ``getbit`` stay deferred — no honest shim
without a ``call_scalar`` allow-list edit (crates/ closed).
"""

from __future__ import annotations

from repark.spark.column import Column
from repark.spark.functions import _as_column_arg, _thread_origin, lit


def bitwise_not(col: Column | str) -> Column:
    """Bitwise complement (PySpark ``functions.bitwise_not``).

    ``~x`` on :class:`Column` is boolean NOT. Complement is ``x XOR -1``.
    Python-int columns are Arrow int64; the result stays int64 (Spark INT
    would stay INT — pin the actual type, do not claim width preservation).
    """
    column = _as_column_arg(col, as_lit=False)
    result = column.bitwiseXOR(lit(-1))
    display = f"bitwise_not({column.spark_wrap_display_part()})"
    return Column(
        result._inner,
        spark_display=display,
        projection_name=display,
        sql_expr=f"bitwise_not({column.sql_expr_part()})",
        join_sql_expr=f"bitwise_not({column.join_sql_part()})",
        stable_name=False,
        is_aggregate=column._is_aggregate,
        is_foldable=column._is_foldable and not column._is_aggregate,
        has_free_attribute=column._has_free_attribute,
        has_ungroupable=column._has_ungroupable,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


bitwiseNOT = bitwise_not  # noqa: N816
