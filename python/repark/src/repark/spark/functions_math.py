"""Mathematical and trigonometric function wrappers."""

from __future__ import annotations

from repark.spark.column import Column
from repark.spark.functions import _as_column_arg, _scalar


def bin(col: Column | str) -> Column:
    """Binary string of a long (PySpark ``functions.bin``).

    Spark casts the input to ``BIGINT`` (numeric and numeric-strings). The
    ``datafusion-spark`` kernel is Int64-exact, so the leading ``.cast("long")``
    is the unix_date mold for those accepted inputs. Spark analysis-refuses
    BOOLEAN; this wrapper's CAST still stringifies ``true``/``false`` to
    ``1``/``0`` (pinned, not claimed as parity).

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
    ``F.bin(F.lit(13))`` is ``'1101'``.
    """
    return _scalar("bin", _as_column_arg(col, as_lit=False).cast("long"))


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
    ``F.hex(F.lit(17))`` is ``'11'``.
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

    Spark domain is ``[0, 20]``; outside that range the result is NULL for
    values that survive the Int32 CAST (Int64 outside ``i32`` fail-louds).

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
    ``F.factorial(F.lit(5))`` is ``120``; ``F.factorial(F.lit(21))`` is NULL.
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
    ``F.rint(F.lit(1.5))`` is ``2.0``.
    """
    return _scalar("rint", _as_column_arg(col, as_lit=False).cast("double"))


def width_bucket(
    v: Column | str | float | int,
    min: Column | str | float | int,
    max: Column | str | float | int,
    numBucket: Column | str | int,  # noqa: N803
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
    numBucket : Column or str or int
        Number of equal-width buckets. A bare ``str`` is a **column name**
        (PySpark 4.1.2 ``ColumnOrName``).

    Returns
    -------
    Column
        Bucket number in ``0 ..= numBucket + 1``.

    Examples
    --------
    ``F.width_bucket(F.lit(5.0), F.lit(0.0), F.lit(10.0), 5)`` is ``3``.
    """
    return _scalar("width_bucket", v, min, max, numBucket)
