"""Bitwise facade wrappers (FN-F + FN-GT1 + FN-GT2 bitmap).

Public names are re-exported from ``functions.py``. ``bitwise_not`` is a SHIM
(``Column.bitwiseXOR(lit(-1))``). FN-GT1 wires ``bit_count`` / ``bit_get`` /
shifts through ``call_scalar``. FN-GT2 bitmap helpers live here (bit-family
sibling of the URL wrappers).
"""

from __future__ import annotations

from repark.spark.column import Column
from repark.spark.functions import _as_column_arg, _scalar, _thread_origin, lit


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


def bit_count(col: Column | str) -> Column:
    """Number of bits set (PySpark ``functions.bit_count``).

    Parameters
    ----------
    col : Column or str
        Integral column.

    Returns
    -------
    Column
        Population count.

    Examples
    --------
    ``F.bit_count(F.lit(7))`` is ``3``.
    """
    return _scalar("bit_count", col)


def bit_get(col: Column | str, pos: Column | str | int) -> Column:
    """Bit at a 0-based position from the right (PySpark ``functions.bit_get``).

    Parameters
    ----------
    col : Column or str
        Integral column.
    pos : Column or str or int
        Bit position (0 is the least-significant bit). A bare ``str`` is a
        **column name** (PySpark 4.1.2 ``ColumnOrName``).

    Returns
    -------
    Column
        ``0`` or ``1``.

    Examples
    --------
    ``F.bit_get(F.lit(6), F.lit(1))`` is ``1`` (``6`` is ``0b110``).
    """
    return _scalar("bit_get", col, pos)


def getbit(col: Column | str, pos: Column | str | int) -> Column:
    """Bit at a 0-based position (PySpark ``functions.getbit``).

    Same kernel as :func:`bit_get`. The name ``getbit`` goes down the wire so
    the projected column is ``getbit(...)``, not ``bit_get(...)``.
    """
    return _scalar("getbit", col, pos)


def shiftleft(col: Column | str, numBits: Column | str | int) -> Column:  # noqa: N803
    """Left shift (PySpark ``functions.shiftleft``).

    Parameters
    ----------
    col : Column or str
        Integral column.
    numBits : int
        Shift count (PySpark types this as a Python ``int``, not
        ``ColumnOrName``).

    Returns
    -------
    Column
        Shifted integer.

    Examples
    --------
    ``F.shiftleft(F.lit(2), 1)`` is ``4``.
    """
    return _scalar("shiftleft", col, numBits, lit_indices=frozenset({1}))


def shiftright(col: Column | str, numBits: Column | str | int) -> Column:  # noqa: N803
    """Signed right shift (PySpark ``functions.shiftright``).

    Parameters
    ----------
    col : Column or str
        Integral column.
    numBits : int
        Shift count (PySpark types this as a Python ``int``).

    Returns
    -------
    Column
        Arithmetic-shifted integer.

    Examples
    --------
    ``F.shiftright(F.lit(-2), 1)`` is ``-1``.
    """
    return _scalar("shiftright", col, numBits, lit_indices=frozenset({1}))


def shiftrightunsigned(col: Column | str, numBits: Column | str | int) -> Column:  # noqa: N803
    """Unsigned right shift (PySpark ``functions.shiftrightunsigned``).

    Parameters
    ----------
    col : Column or str
        Integral column.
    numBits : int
        Shift count (PySpark types this as a Python ``int``).

    Returns
    -------
    Column
        Logical-shifted integer.

    Examples
    --------
    ``F.shiftrightunsigned(F.lit(8), 1)`` is ``4``.
    """
    return _scalar("shiftrightunsigned", col, numBits, lit_indices=frozenset({1}))


def bitmap_bit_position(col: Column | str) -> Column:
    """Bit position for a bitmap child (PySpark ``functions.bitmap_bit_position``).

    Parameters
    ----------
    col : Column or str
        Integral input.

    Returns
    -------
    Column
        Int64 bit position (``bitmap_bit_position(1)`` is ``0``;
        ``bitmap_bit_position(123)`` is ``122``).

    Examples
    --------
    ``F.bitmap_bit_position(F.lit(1))`` is ``0``.
    ``F.bitmap_bit_position(F.lit(123))`` is ``122``.
    """
    return _scalar("bitmap_bit_position", col)


def bitmap_bucket_number(col: Column | str) -> Column:
    """Bucket number for a bitmap child (PySpark ``functions.bitmap_bucket_number``).

    Parameters
    ----------
    col : Column or str
        Integral input.

    Returns
    -------
    Column
        Int64 bucket (``bitmap_bucket_number(1)`` is ``1``;
        ``bitmap_bucket_number(0)`` is ``0``).

    Examples
    --------
    ``F.bitmap_bucket_number(F.lit(1))`` is ``1``.
    """
    return _scalar("bitmap_bucket_number", col)


def bitmap_count(col: Column | str) -> Column:
    """Number of set bits in a binary bitmap (PySpark ``functions.bitmap_count``).

    Parameters
    ----------
    col : Column or str
        Binary bitmap.

    Returns
    -------
    Column
        Int64 population count.

    Examples
    --------
    ``F.bitmap_count(F.unhex(F.lit('FF')))`` is ``8``.
    """
    return _scalar("bitmap_count", col)
