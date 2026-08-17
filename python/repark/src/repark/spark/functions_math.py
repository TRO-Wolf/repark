"""Math facade wrappers (FN-GT1).

Public names are re-exported from ``functions.py``. Each name is a thin
``call_scalar`` wire onto a DF 54.1 / datafusion-spark kernel.
"""

from __future__ import annotations

from repark.spark.column import Column
from repark.spark.functions import _scalar


def bin(col: Column | str) -> Column:
    """Binary string of a long (PySpark ``functions.bin``).

    Parameters
    ----------
    col : Column or str
        Integral column.

    Returns
    -------
    Column
        Binary digits as a string.

    Examples
    --------
    ``F.bin(13)`` is ``'1101'``.
    """
    return _scalar("bin", col)


def hex(col: Column | str) -> Column:
    """Hex string of a number, string, or binary (PySpark ``functions.hex``).

    Parameters
    ----------
    col : Column or str
        Numeric, string, or binary column.

    Returns
    -------
    Column
        Uppercase hex digits.

    Examples
    --------
    ``F.hex(17)`` is ``'11'``.
    """
    return _scalar("hex", col)


def unhex(col: Column | str) -> Column:
    """Decode a hex string to binary (PySpark ``functions.unhex``).

    Parameters
    ----------
    col : Column or str
        Hex string column.

    Returns
    -------
    Column
        Binary payload.

    Examples
    --------
    ``F.unhex(F.lit('48656C6C6F'))`` is the bytes of ``Hello``.
    """
    return _scalar("unhex", col)


def factorial(col: Column | str) -> Column:
    """Factorial (PySpark ``functions.factorial``).

    Spark domain is ``[0, 20]``; outside that range the result is NULL.

    Parameters
    ----------
    col : Column or str
        Integral column.

    Returns
    -------
    Column
        ``n!`` as a 64-bit integer, or NULL.

    Examples
    --------
    ``F.factorial(5)`` is ``120``; ``F.factorial(21)`` is NULL.
    """
    return _scalar("factorial", col)


def rint(col: Column | str) -> Column:
    """Nearest integer as a double (PySpark ``functions.rint``).

    Parameters
    ----------
    col : Column or str
        Numeric column.

    Returns
    -------
    Column
        Double equal to a mathematical integer.

    Examples
    --------
    ``F.rint(1.5)`` is ``2.0``.
    """
    return _scalar("rint", col)


def width_bucket(
    v: Column | str | float | int,
    min: Column | str | float | int,
    max: Column | str | float | int,
    num_bucket: Column | str | int,
) -> Column:
    """Histogram bucket number (PySpark ``functions.width_bucket``).

    Parameters
    ----------
    v : Column or str or number
        Value to bucket.
    min : Column or str or number
        Inclusive lower bound of the histogram range.
    max : Column or str or number
        Exclusive upper bound of the histogram range.
    num_bucket : Column or str or int
        Number of equal-width buckets.

    Returns
    -------
    Column
        Bucket number in ``0 ..= num_bucket + 1``.

    Examples
    --------
    ``F.width_bucket(5.0, 0.0, 10.0, 5)`` is ``3``.
    """
    return _scalar("width_bucket", v, min, max, num_bucket, lit_indices=frozenset({1, 2, 3}))
