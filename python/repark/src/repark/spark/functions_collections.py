"""Collection facade wrappers.

Public names are re-exported from ``functions.py``. The module keeps collection
``element_at`` / ``array_compact`` / ``shuffle`` / ``map_from_entries`` /
``str_to_map``.
"""

from __future__ import annotations

from repark import _native
from repark.errors import PySparkTypeError, PySparkValueError
from repark.spark._idents import sql_string_literal
from repark.spark.column import Column
from repark.spark.functions import (
    _as_column_arg,
    _scalar,
    collect_list,
    lit,
)
from repark.spark.functions_expr import (
    array,
    array_contains,
    array_except,
    array_intersect,
    flatten,
    isnull,
    map_keys,
    size,
    when,
)


def cardinality(col: Column | str) -> Column:
    """Array/map cardinality (PySpark ``functions.cardinality``; alias of ``size``)."""
    return _scalar("cardinality", col)


def array_size(col: Column | str) -> Column:
    """Array/map cardinality (PySpark ``functions.array_size``; alias of ``size``)."""
    return size(col)


def array_agg(col: Column | str) -> Column:
    """Collect non-NULL values into an array (PySpark ``array_agg``; alias of ``collect_list``)."""
    return collect_list(col)


def named_struct(*cols: Column | str | int | float | bool | None) -> Column:
    """Named struct from even-length name/value pairs (PySpark ``functions.named_struct``).

    ``call_scalar`` has no ``named_struct`` arm. SHIM via the same ``make_struct``
    path as :func:`repark.spark.functions_expr.struct` (DataFusion ``named_struct``).
    Field names must be Python strings or foldable string columns.
    """
    if len(cols) < 2 or len(cols) % 2 != 0:
        raise PySparkValueError(
            "named_struct requires an even number of arguments (name, value, ...)"
        )
    columns: list[Column] = []
    named_parts: list[str] = []
    display_parts: list[str] = []
    field_names: list[str] = []
    free = False
    for index in range(0, len(cols), 2):
        name_argument = cols[index]
        value_argument = cols[index + 1]
        if isinstance(name_argument, str):
            field_name = name_argument
        elif isinstance(name_argument, Column):
            if not name_argument._is_foldable:
                raise PySparkValueError("named_struct field names must be foldable string literals")
            field_name = name_argument.spark_display_part()
        else:
            raise PySparkTypeError(
                errorClass="NOT_COLUMN_OR_STR",
                messageParameters={
                    "arg_name": "name",
                    "arg_type": type(name_argument).__name__,
                },
            )
        value = _as_column_arg(
            value_argument,
            as_lit=not isinstance(value_argument, (Column, str)),
        )
        free = free or bool(value._has_free_attribute) or isinstance(value_argument, str)
        columns.append(value)
        field_names.append(field_name)
        named_parts.append(f"{sql_string_literal(str(field_name))}, {value.sql_expr_part()}")
        display_parts.append(str(field_name))
    sql = f"named_struct({', '.join(named_parts)})"
    display = f"named_struct({', '.join(display_parts)})"
    named_natives = [
        column._inner.alias(str(name)) for column, name in zip(columns, field_names, strict=True)
    ]
    return Column(
        _native.PyColumn.make_struct(named_natives),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=sql,
        has_free_attribute=free,
    )


def map_contains_key(col: Column | str, key: Column | str | int | float) -> Column:
    """True when the map contains ``key`` (PySpark ``functions.map_contains_key``).

    SHIM: ``array_contains(map_keys(m), k)``.
    """
    return array_contains(map_keys(col), key)


def _glue_element(array_col: Column, element: Column, *, prepend: bool) -> Column:
    """Concatenate one wrapped element onto ``array_col`` (NULL array → NULL).

    ``F.concat`` is string-only (Utf8 cast). ``flatten(array(arr, array(x)))`` is
    the honest array glue. A bare flatten of a NULL array would yield ``[x]``.
    """
    wrapped = array(element)
    pieces = (wrapped, array_col) if prepend else (array_col, wrapped)
    built = flatten(array(*pieces))
    return when(isnull(array_col), lit(None)).otherwise(built)


def array_append(
    col: Column | str,
    value: Column | str | int | float | bool | None,
) -> Column:
    """Append ``value`` to an array (PySpark ``functions.array_append``)."""
    array_col = _as_column_arg(col, as_lit=False)
    element = value if isinstance(value, Column) else lit(value)
    return _glue_element(array_col, element, prepend=False)


def array_prepend(
    col: Column | str,
    value: Column | str | int | float | bool | None,
) -> Column:
    """Prepend ``value`` to an array (PySpark ``functions.array_prepend``)."""
    array_col = _as_column_arg(col, as_lit=False)
    element = value if isinstance(value, Column) else lit(value)
    return _glue_element(array_col, element, prepend=True)


def arrays_overlap(a1: Column | str, a2: Column | str) -> Column:
    """True when the arrays share a non-NULL element (PySpark ``functions.arrays_overlap``).

    SHIM: ``size(array_except(array_intersect(a, b), array(NULL))) > 0``.
    Null-only intersection is not overlap (Spark). A NULL array yields NULL.
    """
    intersection = array_except(array_intersect(a1, a2), array(lit(None)))
    return size(intersection) > 0


def get(
    col: Column | str,
    index: Column | str | int,
) -> Column:
    """0-based array element or map value (PySpark ``functions.get``).

    Spark 4.1.2 ``get`` is array-only (maps refuse). This wrapper is
    ``getitem`` and currently also serves maps (``test_get_map_by_key``).
    Contrast ``element_at`` (1-based; index 0 raises
    ``INVALID_INDEX_OF_ZERO``; maps by key).

    Parameters
    ----------
    col : Column or str
        Array (or, in this engine, map) column. A ``str`` is a column name.
    index : Column or str or int
        0-based index. An ``int`` is a literal; a ``str`` is a **column name**
        (PySpark ``get`` is ``ColumnOrName``, and it only wraps ``int``), so
        ``F.get('a', 'i')`` indexes by column ``i``.

    Returns
    -------
    Column
        The element, or NULL when the index is out of range.

    Examples
    --------
    ``F.get(F.array(F.lit(10), F.lit(20)), 1)`` is ``20``.
    """
    return _scalar("getitem", _as_column_arg(col, as_lit=False), index)


def element_at(
    col: Column | str,
    extraction: Column | str | int,
) -> Column:
    """1-based array element or map value (PySpark ``functions.element_at``).

    A Python ``str`` extraction is a **literal** map key (or never a column
    name). Pass a :class:`Column` to extract by another column. Index ``0``
    raises ``INVALID_INDEX_OF_ZERO``. Contrast :func:`get` (0-based; ``getitem``,
    including maps).

    **Out of range is NULL here, not an error.** Spark under ANSI raises
    ``INVALID_ARRAY_INDEX_IN_ELEMENT_AT``; this engine returns NULL on both
    doors. repark's ``spark.sql.ansi.enabled`` defaults to ``true``, but the
    documented scope of that flag is ``/`` and ``%`` by zero — see
    ``docs/guide/session-and-conf.md``: "Do not read 'ANSI on' as 'every
    arithmetic fault raises'". Element-at out-of-range is a recorded divergence
    rather than silently changing behavior.

    Parameters
    ----------
    col : Column or str
        Array or map column.
    extraction : Column or str or int
        1-based array index, or map key. A bare ``str`` is ``lit(extraction)``.

    Returns
    -------
    Column
        The element or map value; **NULL** when missing / out of range.

    Examples
    --------
    ``F.element_at(F.array(10, 20, 30), 1)`` is ``10``.
    On a map, ``F.element_at(..., 'b')`` treats ``'b'`` as a literal key.
    """
    return _scalar(
        "element_at",
        col,
        extraction,
        lit_indices=frozenset({} if isinstance(extraction, Column) else {1}),
    )


def array_compact(col: Column | str) -> Column:
    """Drop NULL elements from an array (PySpark ``functions.array_compact``).

    Does not de-duplicate.

    Parameters
    ----------
    col : Column or str
        Array column.

    Returns
    -------
    Column
        Array with NULL elements removed (duplicates kept).

    Examples
    --------
    ``F.array_compact(F.array(1, None, 1))`` is ``[1, 1]``.
    """
    return _scalar("array_compact", col)


def shuffle(col: Column | str, seed: Column | int | None = None) -> Column:
    """Random permutation of an array (PySpark ``functions.shuffle``).

    Unseeded it is non-deterministic — pin type and length, not order. With
    ``seed`` (PySpark 4.0+) the permutation is reproducible, and the facade and
    the Spark SQL door produce the **same** permutation for the same seed
    because both resolve one UDF.

    ``NULL`` in is ``NULL`` out (Spark). Null array casts remain engine-defined.

    Parameters
    ----------
    col : Column or str
        Array column.
    seed : Column or int, optional
        Permutation seed. Omitted → a fresh permutation on every call.

    Returns
    -------
    Column
        An array of the same elements in random order.

    Examples
    --------
    ``F.shuffle(F.array(1, 2, 3))`` is a length-3 integer array whose
    sorted values are ``[1, 2, 3]``.
    """
    if seed is None:
        return _scalar("shuffle", col)
    return _scalar("shuffle", col, seed)


def map_from_entries(col: Column | str) -> Column:
    """Map from ``array<struct<key, value>>`` (PySpark ``functions.map_from_entries``).

    Parameters
    ----------
    col : Column or str
        Array of key/value structs.

    Returns
    -------
    Column
        A map.

    Raises
    ------
    PySparkException
        On a **duplicate key**. Spark's default
        ``spark.sql.mapKeyDedupPolicy`` is ``EXCEPTION``, so
        ``map_from_entries(array(struct('a', 1), struct('a', 2)))`` is an error,
        not last-write-wins — the same rule ``map()`` and :func:`str_to_map`
        already enforce here.

    Examples
    --------
    ``F.map_from_entries(F.array(F.struct(F.lit('a').alias('key'), F.lit(1).alias('value'))))``
    is ``{'a': 1}``.
    """
    return _scalar("map_from_entries", col)


def str_to_map(
    text: Column | str,
    pairDelim: Column | str | None = None,  # noqa: N803 — PySpark keyword
    keyValueDelim: Column | str | None = None,  # noqa: N803 — PySpark keyword
) -> Column:
    """Split a string into a map (PySpark ``functions.str_to_map``).

    Defaults: pair delimiter ``,`` and key/value delimiter ``:``.
    Both delimiters are **regular expressions** (Spark). Omitted delimiters
    stay the literal defaults ``,`` / ``:``. A user-supplied Python ``str``
    is a **column name** (PySpark 4.1.2 ``ColumnOrName``).

    The Perl classes are Java's, i.e. **ASCII-only**: ``\\s`` is
    ``[ \\t\\n\\x0B\\f\\r]``, so a non-breaking space (U+00A0) does
    *not* split. A duplicate key raises (``spark.sql.mapKeyDedupPolicy`` is
    ``EXCEPTION``).

    Parameters
    ----------
    text : Column or str
        Input string.
    pairDelim : Column or str, optional
        Regex between pairs (default ``,``). A bare ``str`` is a **column
        name** (PySpark 4.1.2 ``ColumnOrName``); pass ``F.lit(',')``.
    keyValueDelim : Column or str, optional
        Regex between key and value (default ``:``). Same column-name rule.

    Returns
    -------
    Column
        ``map<string, string>``.

    Examples
    --------
    ``F.str_to_map(F.lit('a:1,b:2'))`` is ``{'a': '1', 'b': '2'}``.
    ``F.str_to_map(F.lit('a:1,b:2c:3'), F.lit('[,c]'), F.lit(':'))`` is
    ``{'a': '1', 'b': '2', '': '3'}``.
    """
    pair = "," if pairDelim is None else pairDelim
    key_value = ":" if keyValueDelim is None else keyValueDelim
    # PySpark 4.1.2 types pairDelim / keyValueDelim as Optional[ColumnOrName]:
    # a bare str is a column name. Default "," / ":" stay Python str and
    # _as_column_arg still wraps them as literals (not str-as-column — they are the
    # default constants, not user-supplied names). User-supplied str names
    # a column.
    lit_indices: set[int] = set()
    if pairDelim is None:
        lit_indices.add(1)
    if keyValueDelim is None:
        lit_indices.add(2)
    return _scalar("str_to_map", text, pair, key_value, lit_indices=frozenset(lit_indices) or None)
