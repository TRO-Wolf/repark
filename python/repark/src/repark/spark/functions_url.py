"""URL parsing and encoding function wrappers."""

from __future__ import annotations

from repark.spark.column import Column
from repark.spark.functions import _scalar


def parse_url(
    url: Column | str,
    partToExtract: Column | str,  # noqa: N803 — PySpark keyword
    key: Column | str | None = None,
) -> Column:
    """Extract a URL part (PySpark ``functions.parse_url``).

    The extraction is `java.net.URI` component splitting, matching Spark: the
    text is **not** normalized. An explicit port stays in ``AUTHORITY``, scheme
    and host keep their case, ``.``/``..`` path segments are not resolved, an
    IDN host makes ``HOST`` NULL (registry-based authority), ``http://@h/``
    keeps its empty-userinfo punctuation, and an opaque URL (``mailto:a@b``)
    has no ``PATH``. Percent escapes are **never decoded**: Spark reads the
    ``Raw`` getters for ``PATH``, ``QUERY``, ``REF``, ``FILE``, ``AUTHORITY``
    and ``USERINFO`` (only ``HOST`` and ``PROTOCOL`` use a non-``Raw`` getter),
    so ``%2e`` reads back as ``%2e`` and ``%20`` as ``%20``.

    An unparsable URL raises ``INVALID_URL`` (Spark); use :func:`try_parse_url`
    for the NULL-tolerant spelling. ``'not a url'`` raises — a space is illegal
    in every RFC-2396 component.

    The ``QUERY`` key is a **Java regex** (Spark compiles ``(&|^)<key>=([^&]*)``
    and returns group 2, matched against the *raw* query), so ``'f.o'`` matches
    ``foo`` and a value holding ``%26`` is not cut short. A key that cannot
    compile (``'('``, ``'a{2,'``) raises, matching Spark — ``ParseUrl`` calls
    ``Pattern.compile`` with no ``try``/``catch``, so the syntax error escapes
    rather than becoming a NULL. :func:`try_parse_url` does **not** tolerate
    that one: it tolerates only an unparsable URL.

    **Residual — the key's regex dialect.** Spark compiles it with
    ``java.util.regex``; repark compiles it with the Rust ``regex`` crate, a
    finite automaton. Five constructs Java accepts cannot be expressed there and
    **raise** here (under this function and :func:`try_parse_url` alike) where
    Spark answers: ``a(?=1)`` lookahead (Spark NULL), ``(?<=&)b`` lookbehind,
    ``(a)\\1`` backreference, ``(?>a)`` atomic group, ``\\Qa\\E`` quoting.

    Order matters and is Spark's: a three-argument call whose part is not
    ``QUERY`` is NULL *before* the URL is parsed, so
    ``parse_url('not a url', 'HOST', 'k')`` is NULL rather than an error.

    Parameters
    ----------
    url : Column or str
        URL string. A ``str`` is a **column name**.
    partToExtract : Column or str
        Part name (``HOST``, ``PATH``, ``QUERY``, ``REF``, ``PROTOCOL``,
        ``FILE``, ``AUTHORITY``, ``USERINFO``). A ``str`` is a **column
        name** — use ``F.lit('HOST')`` for a constant. An unknown part is NULL.
    key : Column or str, optional
        Query-parameter key when extracting ``QUERY``. A ``str`` is a column
        name.

    Returns
    -------
    Column
        The extracted string, or NULL when the part is absent.

    Examples
    --------
    ``F.parse_url(F.lit('https://spark.apache.org/x'), F.lit('HOST'))`` is
    ``'spark.apache.org'``.
    """
    if key is None:
        return _scalar("parse_url", url, partToExtract)
    return _scalar("parse_url", url, partToExtract, key)


def try_parse_url(
    url: Column | str,
    partToExtract: Column | str,  # noqa: N803 — PySpark keyword
    key: Column | str | None = None,
) -> Column:
    """Extract a URL part, NULL if invalid (PySpark ``functions.try_parse_url``).

    Same kernel and same part semantics as :func:`parse_url`; the only
    difference is that an **unparsable URL** is NULL instead of raising. That
    mirrors Spark exactly, and so does the limit of it: ``TryParseUrl``'s
    replacement is ``ParseUrl(params, failOnError=False)``, not
    ``TryEval(ParseUrl)``, and ``failOnError`` guards only the
    ``java.net.URI`` construction. An **uncompilable ``QUERY`` key** therefore
    still raises here, exactly as it does on :func:`parse_url`.

    Parameters
    ----------
    url : Column or str
        URL string. A ``str`` is a **column name**.
    partToExtract : Column or str
        Part name. A ``str`` is a **column name** — use ``F.lit('HOST')``.
    key : Column or str, optional
        Query-parameter key when extracting ``QUERY``.

    Returns
    -------
    Column
        The extracted string, or NULL.

    Examples
    --------
    ``F.try_parse_url(F.lit('https://spark.apache.org/x'), F.lit('HOST'))`` is
    ``'spark.apache.org'``; ``F.try_parse_url(F.lit('not a url'), F.lit('HOST'))``
    is NULL.
    """
    if key is None:
        return _scalar("try_parse_url", url, partToExtract)
    return _scalar("try_parse_url", url, partToExtract, key)


def url_encode(str: Column | str) -> Column:
    """URL-encode a string (PySpark ``functions.url_encode``).

    Parameters
    ----------
    str : Column or str
        Input string. The keyword is ``str``, matching PySpark 4.1.2.

    Returns
    -------
    Column
        Percent-encoded string (space → ``+``).

    Examples
    --------
    ``F.url_encode(F.lit('a b'))`` is ``'a+b'``.
    """
    return _scalar("url_encode", str)


def url_decode(str: Column | str) -> Column:
    """URL-decode a string (PySpark ``functions.url_decode``).

    Parameters
    ----------
    str : Column or str
        Encoded string. The keyword is ``str``, matching PySpark 4.1.2.

    Returns
    -------
    Column
        Decoded string.

    Examples
    --------
    ``F.url_decode(F.lit('a+b'))`` is ``'a b'``.
    """
    return _scalar("url_decode", str)


def try_url_decode(str: Column | str) -> Column:
    """URL-decode, NULL if invalid (PySpark ``functions.try_url_decode``).

    Parameters
    ----------
    str : Column or str
        Encoded string. The keyword is ``str``, matching PySpark 4.1.2.

    Returns
    -------
    Column
        Decoded string, or NULL on invalid input.

    Examples
    --------
    ``F.try_url_decode(F.lit('a+b'))`` is ``'a b'``;
    ``F.try_url_decode(F.lit('%ZZ'))`` is NULL.
    """
    return _scalar("try_url_decode", str)
