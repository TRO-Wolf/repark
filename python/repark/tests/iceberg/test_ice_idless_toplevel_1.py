"""RP-65 — an id-less top-level column on the no-mapping route through the facade.

The facade's plain Parquet frame writer embeds no field ids; ``add_files`` admits the file and
``UNSET TBLPROPERTIES`` removes the name mapping, so every top-level column reads by position.
A string column read as binary keeps its bytes, a binary column read as string answers its
null test and prefix, and an equality filter across the pair refuses.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from test_ice_idless_struct_1 import _QUALIFIED, _admit_frame, _open_session

from repark import ReparkSession
from repark.errors import PySparkException

_UNSET = f"ALTER TABLE {_QUALIFIED} UNSET TBLPROPERTIES ('schema.name-mapping.default')"
_HINT = (
    "the data file has no field ids and the table has no name mapping, "
    "so its columns are read by position"
)


def _keys(session: ReparkSession, predicate: str) -> list[int]:
    """The `k` values of the rows that satisfy `predicate`, ordered."""
    rows = session.sql(f"SELECT k FROM {_QUALIFIED} WHERE {predicate} ORDER BY k").collect()
    return [row["k"] for row in rows]


def _unmapped(
    tmp_path: Path,
    name: str,
    table_type: str,
    frame_schema: str,
    rows: tuple[tuple[Any, ...], ...],
) -> ReparkSession:
    """A session over a table `(k INT, v table_type)` holding one unmapped id-less file."""
    session = _open_session(name, tmp_path / "wh")
    session.sql(
        f"CREATE TABLE {_QUALIFIED} (k INT, v {table_type}) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    ).collect()
    _admit_frame(session, tmp_path / "source", frame_schema, rows)
    session.sql(_UNSET).collect()
    return session


def test_a_string_column_read_as_binary_keeps_its_bytes_and_refuses_equality(
    tmp_path: Path,
) -> None:
    """`SELECT` reads the raw bytes, `IS NOT NULL` and `!=` answer, and `=` refuses."""
    session = _unmapped(
        tmp_path, "rp65-s2b", "BINARY", "k INT, v STRING", ((1, "abc"), (2, "xyz"), (3, None))
    )
    try:
        table = session.sql(f"SELECT k, v FROM {_QUALIFIED} ORDER BY k").to_arrow()
        assert table.column("v").to_pylist() == [b"abc", b"xyz", None]
        assert _keys(session, "v IS NOT NULL") == [1, 2]
        assert _keys(session, "v != X'616263'") == [2]
        with pytest.raises(PySparkException) as caught:
            _keys(session, "v = X'616263'")
        message = str(caught.value)
        assert "data file column 'v' at position 1 has type string" in message
        assert "cannot be filtered with `=` as table field 'v' (id 2) of type binary" in message
        assert _HINT in message
    finally:
        session.stop()


def test_a_binary_column_read_as_string_answers_its_null_test_and_prefixes(
    tmp_path: Path,
) -> None:
    """`IS NULL`, `!=`, `LIKE` and `NOT LIKE` answer, and `>` refuses."""
    session = _unmapped(
        tmp_path,
        "rp65-b2s",
        "STRING",
        "k INT, v BINARY",
        ((1, bytearray(b"abc")), (2, bytearray(b"xyz")), (3, None)),
    )
    try:
        rows = session.sql(f"SELECT k, v FROM {_QUALIFIED} ORDER BY k").collect()
        assert [(row["k"], row["v"]) for row in rows] == [(1, "abc"), (2, "xyz"), (3, None)]
        assert _keys(session, "v IS NULL") == [3]
        assert _keys(session, "v != 'abc'") == [2]
        assert _keys(session, "v LIKE 'a%'") == [1]
        assert _keys(session, "v NOT LIKE 'a%'") == [2]
        with pytest.raises(PySparkException) as caught:
            _keys(session, "v > 'b'")
        message = str(caught.value)
        assert "cannot be filtered with `>` as table field 'v' (id 2) of type string" in message
        assert _HINT in message
    finally:
        session.stop()
