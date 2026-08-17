"""URL and bitmap facade wrappers (FN-GT2).

Public names are re-exported from ``functions.py``.
"""

from __future__ import annotations

from repark.spark.column import Column
from repark.spark.functions import _scalar


def parse_url(
    url: Column | str,
    part_to_extract: Column | str,
    key: Column | str | None = None,
) -> Column:
    """Extract a URL part (PySpark ``functions.parse_url``).

    Invalid URLs raise. Use :func:`try_parse_url` for NULL-on-invalid.

    Examples
    --------
    ``F.parse_url(F.lit('https://spark.apache.org/x'), 'HOST')`` is
    ``'spark.apache.org'``.
    """
    if key is None:
        return _scalar(
            "parse_url",
            url,
            part_to_extract,
            lit_indices=frozenset({1} if isinstance(part_to_extract, str) else ()),
        )
    return _scalar(
        "parse_url",
        url,
        part_to_extract,
        key,
        lit_indices=frozenset({1, 2}),
    )


def try_parse_url(
    url: Column | str,
    part_to_extract: Column | str,
    key: Column | str | None = None,
) -> Column:
    """Extract a URL part, NULL if invalid (PySpark ``functions.try_parse_url``).

    Examples
    --------
    ``F.try_parse_url(F.lit('not a url'), 'HOST')`` is NULL.
    """
    if key is None:
        return _scalar(
            "try_parse_url",
            url,
            part_to_extract,
            lit_indices=frozenset({1} if isinstance(part_to_extract, str) else ()),
        )
    return _scalar(
        "try_parse_url",
        url,
        part_to_extract,
        key,
        lit_indices=frozenset({1, 2}),
    )


def url_encode(col: Column | str) -> Column:
    """URL-encode a string (PySpark ``functions.url_encode``).

    Examples
    --------
    ``F.url_encode(F.lit('a b'))`` is ``'a+b'``.
    """
    return _scalar("url_encode", col)


def url_decode(col: Column | str) -> Column:
    """URL-decode a string (PySpark ``functions.url_decode``).

    Examples
    --------
    ``F.url_decode(F.lit('a+b'))`` is ``'a b'``.
    """
    return _scalar("url_decode", col)


def try_url_decode(col: Column | str) -> Column:
    """URL-decode, NULL if invalid (PySpark ``functions.try_url_decode``).

    Examples
    --------
    ``F.try_url_decode(F.lit('%ZZ'))`` is NULL.
    """
    return _scalar("try_url_decode", col)


def bitmap_bit_position(col: Column | str) -> Column:
    """Bit position for a bitmap child (PySpark ``functions.bitmap_bit_position``).

    Examples
    --------
    ``F.bitmap_bit_position(F.lit(1))`` is a non-null integer.
    """
    return _scalar("bitmap_bit_position", col)


def bitmap_bucket_number(col: Column | str) -> Column:
    """Bucket number for a bitmap child (PySpark ``functions.bitmap_bucket_number``).

    Examples
    --------
    ``F.bitmap_bucket_number(F.lit(1))`` is a non-null integer.
    """
    return _scalar("bitmap_bucket_number", col)


def bitmap_count(col: Column | str) -> Column:
    """Number of set bits in a binary bitmap (PySpark ``functions.bitmap_count``).

    Examples
    --------
    ``F.bitmap_count(F.unhex(F.lit('FF')))`` is ``8``.
    """
    return _scalar("bitmap_count", col)
