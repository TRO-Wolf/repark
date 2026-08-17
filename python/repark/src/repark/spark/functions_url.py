"""URL facade wrappers (FN-GT2).

Public names are re-exported from ``functions.py``. Bitmap helpers live
in :mod:`repark.spark.functions_bitwise` (bit-family sibling).
"""

from __future__ import annotations

from repark.spark.column import Column
from repark.spark.functions import _scalar


def parse_url(
    url: Column | str,
    partToExtract: Column | str,  # noqa: N803 — PySpark keyword
    key: Column | str | None = None,
) -> Column:
    """Extract a URL part (PySpark ``functions.parse_url``).

    Spark 4.1.2 raises ``INVALID_URL`` on invalid input (including schemeless
    ``'not a url'``). The DF kernel is mixed: schemeless text yields NULL
    (HOST missing on a relative URI); some ``://``-malformed URLs raise
    (``'inva lid://host'``). Do not claim a single throw-vs-NULL rule.
    Spark compiles a ``QUERY`` key as a Java ``Pattern``; the DF kernel is
    exact equality (``'f.o'`` matches ``foo`` on Spark, NULL here).

    Parameters
    ----------
    url : Column or str
        URL string.
    partToExtract : Column or str
        Part name (``HOST``, ``PATH``, ``QUERY``, …). A Python ``str`` is a literal.
    key : Column or str, optional
        Query-parameter key when extracting ``QUERY``.

    Returns
    -------
    Column
        The extracted string.

    Examples
    --------
    ``F.parse_url(F.lit('https://spark.apache.org/x'), 'HOST')`` is
    ``'spark.apache.org'``.
    """
    if key is None:
        return _scalar(
            "parse_url",
            url,
            partToExtract,
            lit_indices=frozenset({1} if isinstance(partToExtract, str) else ()),
        )
    return _scalar(
        "parse_url",
        url,
        partToExtract,
        key,
        lit_indices=frozenset({1, 2}),
    )


def try_parse_url(
    url: Column | str,
    partToExtract: Column | str,  # noqa: N803 — PySpark keyword
    key: Column | str | None = None,
) -> Column:
    """Extract a URL part, NULL if invalid (PySpark ``functions.try_parse_url``).

    Parameters
    ----------
    url : Column or str
        URL string.
    partToExtract : Column or str
        Part name. A Python ``str`` is a literal.
    key : Column or str, optional
        Query-parameter key when extracting ``QUERY``.

    Returns
    -------
    Column
        The extracted string, or NULL.

    Examples
    --------
    ``F.try_parse_url(F.lit('not a url'), 'HOST')`` is NULL.
    """
    if key is None:
        return _scalar(
            "try_parse_url",
            url,
            partToExtract,
            lit_indices=frozenset({1} if isinstance(partToExtract, str) else ()),
        )
    return _scalar(
        "try_parse_url",
        url,
        partToExtract,
        key,
        lit_indices=frozenset({1, 2}),
    )


def url_encode(col: Column | str) -> Column:
    """URL-encode a string (PySpark ``functions.url_encode``).

    Parameters
    ----------
    col : Column or str
        Input string.

    Returns
    -------
    Column
        Percent-encoded string (space → ``+``).

    Examples
    --------
    ``F.url_encode(F.lit('a b'))`` is ``'a+b'``.
    """
    return _scalar("url_encode", col)


def url_decode(col: Column | str) -> Column:
    """URL-decode a string (PySpark ``functions.url_decode``).

    Parameters
    ----------
    col : Column or str
        Encoded string.

    Returns
    -------
    Column
        Decoded string.

    Examples
    --------
    ``F.url_decode(F.lit('a+b'))`` is ``'a b'``.
    """
    return _scalar("url_decode", col)


def try_url_decode(col: Column | str) -> Column:
    """URL-decode, NULL if invalid (PySpark ``functions.try_url_decode``).

    Parameters
    ----------
    col : Column or str
        Encoded string.

    Returns
    -------
    Column
        Decoded string, or NULL on invalid input.

    Examples
    --------
    ``F.try_url_decode(F.lit('%ZZ'))`` is NULL.
    """
    return _scalar("try_url_decode", col)
