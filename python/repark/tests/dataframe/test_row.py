"""G-ROW — :class:`~repark.row.Row` API + E8 error-class pins vs live PySpark 4.1.2.

Oracle (2026-07-27): live PySpark 4.1.2 under
``JAVA_HOME=/usr/lib/jvm/zulu-17-amd64``, ``SPARK_LOCAL_IP=127.0.0.1``, ANSI on,
``/tmp/oracle-venv``. Measured:

* ``Row`` is a ``tuple`` subclass; repark is a near-drop-in with the same *user* surface
  (not a tuple subclass — deliberate, documented).
* ``__getitem__``: int (incl. negative) + OOB ``IndexError``; str field; slice → ``tuple``;
  missing str / wrong type → ``PySparkValueError`` (NOT ``KeyError`` / ``TypeError``).
* ``__contains__``: field names only (values are not searched).
* ``asDict(recursive=)``: nested ``Row`` / list / dict conversion when ``recursive=True``.
* equality: **values only** (``Row(a=1)==Row(b=1)==(1,)``); hash matches plain tuple
  (``hash(row)==hash((1,))``) so set/dict interop holds.
* ``__fields__``: ``list`` of names; iteration yields values; ``repr`` ``Row(a=1, b=2)``.
* missing attr → ``PySparkAttributeError`` with ``[ATTRIBUTE_NOT_SUPPORTED]``.
* ``Row(1, a=2)`` → ``PySparkValueError`` ``[CANNOT_SET_TOGETHER]``.

``PySparkKeyError`` (malformed Row: ``__fields__`` longer than values) has no reachable
raise in repark — fields and values stay lock-step; deferred (Group X enumeration).
"""

from __future__ import annotations

import pytest

from repark.errors import (
    PySparkAttributeError,
    PySparkException,
    PySparkTypeError,
    PySparkValueError,
)
from repark.spark.row import Row

# Construction + surface shape


def test_keyword_construction_fields_values_repr() -> None:
    """``Row(a=1, b='x')`` preserves insertion order (Spark 3.0+; live 4.1.2)."""
    row = Row(a=1, b="x", c=3)
    assert row.__fields__ == ["a", "b", "c"]
    assert list(row) == [1, "x", 3]
    assert len(row) == 3
    assert repr(row) == "Row(a=1, b='x', c=3)"
    assert str(row) == "Row(a=1, b='x', c=3)"


def test_from_mapping_preserves_order() -> None:
    row = Row.from_mapping({"z": 1, "a": 2})
    assert row.__fields__ == ["z", "a"]
    assert list(row) == [1, 2]


def test_positional_and_list_construction() -> None:
    """Positional values get synthetic ``_N`` names (repark); values still indexable.

    Live 4.1.2: a single list/tuple *argument* is ONE value; Spark omits
    ``__fields__`` for that form; repark still assigns synthetic ``_0`` (near-drop-in).
    """
    row = Row(10, 20)
    assert row[0] == 10
    assert row[1] == 20
    assert list(row) == [10, 20]
    assert row.__fields__ == ["_0", "_1"]

    from_list = Row([7, 8])
    assert len(from_list) == 1
    assert list(from_list) == [[7, 8]]
    assert from_list[0] == [7, 8]
    assert from_list.__fields__ == ["_0"]
    assert from_list.asDict() == {"_0": [7, 8]}

    from_tuple = Row((7, 8))
    assert len(from_tuple) == 1
    assert list(from_tuple) == [(7, 8)]
    assert from_tuple[0] == (7, 8)
    assert list(Row(7, 8)) == [7, 8]


def test_user_fields_named_fields_and_values_do_not_shadow() -> None:
    """Live 4.1.2: ``Row(_fields=1)._fields`` is the *value* 1.

    Mutation: rename slots back to ``_fields``/``_values`` → RED.
    """
    row = Row(_fields=1)
    assert row._fields == 1
    assert row["_fields"] == 1
    assert row.asDict() == {"_fields": 1}
    assert row.__fields__ == ["_fields"]
    assert list(row) == [1]
    assert repr(row) == "Row(_fields=1)"

    dual = Row(_fields=1, _values=2)
    assert dual._fields == 1
    assert dual._values == 2
    assert dual.asDict() == {"_fields": 1, "_values": 2}
    assert dual.__fields__ == ["_fields", "_values"]
    assert dual == (1, 2)

    mixed = Row(_values=99, a=1)
    assert mixed._values == 99
    assert mixed.a == 1
    assert mixed["a"] == 1
    assert mixed.asDict() == {"_values": 99, "a": 1}


def test_mixed_args_kwargs_raises_pyspark_value_error() -> None:
    """Live 4.1.2: ``Row(1, a=2)`` → ``PySparkValueError`` ``[CANNOT_SET_TOGETHER]``."""
    with pytest.raises(PySparkValueError, match=r"CANNOT_SET_TOGETHER") as caught:
        Row(1, a=2)  # type: ignore[misc]
    assert isinstance(caught.value, ValueError)
    assert isinstance(caught.value, PySparkException)


# __getitem__ — int / str / slice / negative / E8 classes


def test_getitem_int_and_negative() -> None:
    row = Row(a=1, b="x", c=3)
    assert row[0] == 1
    assert row[1] == "x"
    assert row[-1] == 3
    assert row[-3] == 1


def test_getitem_int_out_of_range_is_bare_index_error() -> None:
    """Live 4.1.2: OOB int → bare ``IndexError`` (tuple path), not a PySpark wrapper."""
    row = Row(a=1, b=2)
    with pytest.raises(IndexError):
        _ = row[99]
    with pytest.raises(IndexError):
        _ = row[-99]


def test_getitem_str_field() -> None:
    row = Row(a=1, b="x")
    assert row["a"] == 1
    assert row["b"] == "x"


def test_getitem_slice_returns_tuple() -> None:
    """Live 4.1.2: ``row[1:3]`` → plain ``tuple`` of values (not a Row)."""
    row = Row(a=1, b="x", c=3)
    assert row[1:3] == ("x", 3)
    assert type(row[1:3]) is tuple
    assert row[:2] == (1, "x")
    assert row[::] == (1, "x", 3)
    assert row[1:] == ("x", 3)
    assert row[:-1] == (1, "x")


def test_getitem_missing_str_raises_pyspark_value_error() -> None:
    """E8 / Group X residual: live 4.1.2 raises ``PySparkValueError``, not ``KeyError``."""
    row = Row(a=1, b=2)
    with pytest.raises(PySparkValueError) as caught:
        _ = row["zz"]
    assert isinstance(caught.value, ValueError)
    assert isinstance(caught.value, PySparkException)
    assert not isinstance(caught.value, KeyError)
    # Message carries the missing key (Spark re-raises ``PySparkValueError(item)``).
    assert "zz" in str(caught.value)


def test_getitem_wrong_type_raises_pyspark_value_error() -> None:
    """E8: live 4.1.2 funnels non-int/slice keys through ``__fields__.index`` → ValueError.

    Aligned to the live class (``PySparkValueError``); no new ``PySparkException`` leaf.
    """
    row = Row(a=1, b=2)
    for bad in (object(), 1.5, ["a"], None):
        with pytest.raises(PySparkValueError) as caught:
            _ = row[bad]  # type: ignore[index]
        assert isinstance(caught.value, ValueError)
        assert isinstance(caught.value, PySparkException)
        assert not isinstance(caught.value, TypeError)
        assert not isinstance(caught.value, PySparkTypeError)


# __contains__ / iteration / __fields__


def test_contains_is_field_names_only() -> None:
    """``'a' in row`` True; values are NOT searched (live 4.1.2)."""
    row = Row(a=1, b="x", c=3)
    assert "a" in row
    assert "b" in row
    assert "zz" not in row
    assert 1 not in row
    assert "x" not in row


def test_iteration_yields_values() -> None:
    row = Row(a=1, b="x", c=3)
    assert list(row) == [1, "x", 3]
    assert tuple(row) == (1, "x", 3)


def test_fields_property_is_list_copy() -> None:
    row = Row(a=1, b=2)
    fields = row.__fields__
    assert fields == ["a", "b"]
    assert isinstance(fields, list)
    # Mutating the returned list must not corrupt the row.
    fields.append("hack")
    assert row.__fields__ == ["a", "b"]
    assert "hack" not in row


# asDict (+ recursive)


def test_as_dict_flat() -> None:
    row = Row(a=1, b="x")
    assert row.asDict() == {"a": 1, "b": "x"}
    assert row.asDict(recursive=False) == {"a": 1, "b": "x"}
    assert row.as_dict() == row.asDict()  # snake_case alias


def test_as_dict_recursive_nested_row_list_dict() -> None:
    """Live 4.1.2: ``recursive=True`` converts nested Rows (and Rows in list/dict)."""
    nested = Row(x=10, y=20)
    row = Row(id=1, nested=nested, items=[Row(x=1, y=2), Row(x=3, y=4)], meta={"r": nested})
    assert row.asDict(recursive=False) == {
        "id": 1,
        "nested": nested,
        "items": [Row(x=1, y=2), Row(x=3, y=4)],
        "meta": {"r": nested},
    }
    assert row.asDict(recursive=True) == {
        "id": 1,
        "nested": {"x": 10, "y": 20},
        "items": [{"x": 1, "y": 2}, {"x": 3, "y": 4}],
        "meta": {"r": {"x": 10, "y": 20}},
    }


# equality / hash / attr


def test_equality_is_values_only_including_tuple() -> None:
    """Live 4.1.2: field names ignored; ``Row`` compares equal to a same-values tuple."""
    left = Row(a=1, b="x", c=3)
    right_same = Row(a=1, b="x", c=3)
    right_diff_names = Row(x=1, y="x", z=3)
    right_diff_vals = Row(a=1, b="y", c=3)
    assert left == right_same
    assert left == right_diff_names
    assert left != right_diff_vals
    assert left == (1, "x", 3)
    assert left != (1, "x")
    assert left != [1, "x", 3]
    assert left != {"a": 1, "b": "x", "c": 3}


def test_hash_matches_equality() -> None:
    """Hash is values-only and interoperates with plain tuples (live PySpark 4.1.2).

    Python's set/dict contract: equal objects share a hash. A mutation
    ``return hash(("repark.Row", values))`` keeps Row↔Row asserts green while breaking
    ``{(1, 2), row}`` / dict keys shared with tuples.
    """
    left = Row(a=1, b=2)
    right = Row(x=1, y=2)
    plain = (1, 2)
    assert left == plain  # precondition for hash interop
    assert hash(left) == hash(right)
    assert hash(left) == hash(plain)
    assert {left, right} == {left}
    assert {(1, 2), left} == {(1, 2)}
    assert {left: "v"}[(1, 2)] == "v"


def test_missing_attr_raises_pyspark_attribute_error() -> None:
    """Live 4.1.2: ``row.zz`` → ``PySparkAttributeError`` ``[ATTRIBUTE_NOT_SUPPORTED]``."""
    row = Row(a=1)
    with pytest.raises(PySparkAttributeError, match=r"ATTRIBUTE_NOT_SUPPORTED") as caught:
        _ = row.zz  # type: ignore[attr-defined]
    assert isinstance(caught.value, AttributeError)
    assert isinstance(caught.value, PySparkException)
    assert "`zz`" in str(caught.value)
    assert row.a == 1
    assert not hasattr(row, "zz")


# collect path (needs native module) — field access on a real collected row


def test_collect_row_surface() -> None:
    """Collected rows support the same access surface (values + names)."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("pytest-row-collect").getOrCreate()
    try:
        rows = spark.sql("SELECT 1 AS a, 'x' AS b, 3 AS c").collect()
        assert len(rows) == 1
        row = rows[0]
        assert isinstance(row, Row)
        assert row.__fields__ == ["a", "b", "c"]
        assert row.a == 1
        assert row["b"] == "x"
        assert row[0] == 1
        assert row[-1] == 3
        assert row[1:3] == ("x", 3)
        assert "a" in row
        assert 1 not in row
        assert list(row) == [1, "x", 3]
        assert row.asDict() == {"a": 1, "b": "x", "c": 3}
        assert row == (1, "x", 3)
        assert repr(row) == "Row(a=1, b='x', c=3)"
    finally:
        spark.stop()


# R-PARITY3 — Row factory form + pickling


def test_row_factory_form_callable_and_repr() -> None:
    """Live 4.1.2: Row('name','age') is a callable factory; repr is <Row('name', 'age')>."""
    factory = Row("name", "age")
    assert callable(factory)
    assert repr(factory) == "<Row('name', 'age')>"
    assert list(factory) == ["name", "age"]
    assert len(factory) == 2
    assert factory == Row("name", "age")
    row = factory("alice", 1)
    assert row == Row(name="alice", age=1)
    assert row.name == "alice"
    assert row.age == 1
    assert row.__fields__ == ["name", "age"]
    assert repr(row) == "Row(name='alice', age=1)"


def test_row_factory_and_value_pickle_round_trip() -> None:
    import pickle

    factory = Row("name", "age")
    restored_factory = pickle.loads(pickle.dumps(factory))
    assert restored_factory == factory
    assert restored_factory("bob", 2) == Row(name="bob", age=2)
    row = factory("alice", 1)
    assert pickle.loads(pickle.dumps(row)) == row


def test_row_mixed_positional_is_not_factory() -> None:
    """Row('x', 1) is a value row (synthetic names), not a factory."""
    row = Row("x", 1)
    assert row.__fields__ == ["_0", "_1"]
    assert list(row) == ["x", 1]
    with pytest.raises(TypeError, match="not callable"):
        row(1, 2)  # type: ignore[operator]
