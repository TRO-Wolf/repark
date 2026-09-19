"""CAST-MAP-SPELL-1 — ``CAST(… AS MAP<…>)`` and ``.cast(MapType)`` answer Spark 4.1.2.

Every cell reads its expected answer from the recorded
``cast_map_spell_1/cast_map_spell_1_spark_oracle.json`` fixture: schema string,
per-field nullability and rows, or the exception class and first error line. One pin
per cell on the facade SQL door, plus the native ANSI door wherever it can spell the
cell and the DataFrame door for the three ``.cast`` cells. The live tier re-derives
every cell from live Spark and asserts the committed fixture still matches.

The oracle serializes PySpark ``Row`` values through ``json`` (a ``Row`` is a tuple
subclass), so structs inside maps read positionally (``{"a": [1]}`` for
``{"a": {"x": 1}}``) while top-level maps keep their keys; the row comparator below
matches that encoding instead of re-recording it. Arrow map columns read as
key/value pair lists, which the comparator folds back to dicts.

pins: cast-map-spell-1/C-002, C-003, C-004
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import pytest

_HERE = Path(__file__).resolve().parent
_ORACLE: dict[str, Any] = json.loads(
    (_HERE / "cast_map_spell_1" / "cast_map_spell_1_spark_oracle.json").read_text(encoding="utf-8")
)
_CELLS: dict[str, dict[str, Any]] = _ORACLE["cells"]

_SQL_VALUE_KEYS: tuple[str, ...] = (
    "null_map_str_int",
    "null_map_str_list_int",
    "empty_map_str_int",
    "widen_value",
    "value_to_string",
    "key_int_to_string",
    "nested_map_in_map",
    "array_of_map",
    "struct_with_map",
    "map_list_value_widen",
    "map_colon_struct_spelling",
    "lowercase_spelling",
    "try_cast_invalid",
    "null_key_map_cast",
    "union_with_typed_null",
)

_SQL_ERROR_KEYS: tuple[tuple[str, tuple[str, str]], ...] = (
    ("invalid_value_cast_ansi", ("NumberFormatException", "CAST_INVALID_INPUT")),
    (
        "int_to_map_refuses",
        ("AnalysisException", "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION"),
    ),
)

_LEGACY_KEY = "invalid_value_cast_legacy"

_DF_KEYS: tuple[str, ...] = ("df_cast_maptype", "df_cast_ddl_string", "df_cast_nested")

_NATIVE_VALUE_CELLS: tuple[tuple[str, str, bool], ...] = (
    ("null_map_str_int", "map<string, int32>", True),
    ("null_map_str_list_int", "map<string, list<item: int32>>", True),
    ("widen_value", "map<string, int64>", True),
    ("value_to_string", "map<string, string>", True),
    ("key_int_to_string", "map<string, string>", True),
    ("nested_map_in_map", "map<string, map<string, int64>>", True),
    ("struct_with_map", "struct<m: map<string, int64>>", True),
    ("map_colon_struct_spelling", "map<string, struct<x: int64>>", True),
    ("lowercase_spelling", "map<string, int64>", True),
    ("try_cast_invalid", "map<string, int32>", True),
    ("null_key_map_cast", "map<string, int64>", True),
)

_NATIVE_ERROR_KEYS: tuple[str, str] = (
    "invalid_value_cast_ansi",
    "int_to_map_refuses",
)

_NATIVE_ERROR_TOKENS: dict[str, str] = {
    "invalid_value_cast_ansi": "CAST_INVALID_INPUT",
    "int_to_map_refuses": "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION",
}

_LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
_LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)"


def _spark_session(ansi: str) -> Any:
    """Return a facade session with the requested ANSI mode and UTC zone."""
    from repark import ReparkSession

    active = ReparkSession.getActiveSession()
    if active is not None:
        active.stop()
    return (
        ReparkSession.builder.appName("cast-map-spell-1")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _pairs_to_dict(pairs: list[Any]) -> dict[Any, Any]:
    """Fold an Arrow map pair list into a plain dict, normalizing values."""
    return {pair[0]: _normalize_value(pair[1]) for pair in pairs}


def _normalize_value(value: Any) -> Any:
    """Normalize an Arrow ``to_pylist`` value toward the oracle JSON shape."""
    if isinstance(value, dict):
        return {key: _normalize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_value(item) for item in value]
    return value


def _values_equal(expected: Any, actual: Any) -> bool:
    """Compare one oracle value against one Arrow value under the oracle encoding."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        return set(expected) == set(actual) and all(
            _values_equal(expected[key], actual[key]) for key in expected
        )
    if isinstance(expected, dict) and isinstance(actual, (list, tuple)):
        return _values_equal(expected, _pairs_to_dict(list(actual)))
    if isinstance(expected, (list, tuple)) and isinstance(actual, dict):
        if len(expected) != len(actual):
            return False
        return all(
            _values_equal(item, value)
            for item, value in zip(expected, actual.values(), strict=True)
        )
    if isinstance(expected, (list, tuple)) and isinstance(actual, (list, tuple)):
        if len(expected) != len(actual):
            return False
        return all(_values_equal(item, value) for item, value in zip(expected, actual, strict=True))
    return expected == actual


def _assert_value_cell_facade(session: Any, key: str) -> None:
    """Assert one SQL cell's schema, nullability and rows on the facade door."""
    cell = _CELLS[key]
    frame = session.sql(cell["sql"])
    assert frame.schema.simpleString() == cell["schema"], (key, "schema")
    assert [field.nullable for field in frame.schema.fields] == cell["nullable"], (
        key,
        "nullable",
    )
    _assert_rows_any_order(key, cell["rows"], frame.to_arrow())


def _assert_rows_any_order(key: str, expected_rows: list[Any], table: Any) -> None:
    """Assert the table holds the oracle rows as a multiset.

    ``UNION ALL`` promises no row order, so each oracle row claims the first unmatched
    output row equal to it; a single-row cell reads exactly like a positional check.
    """
    assert table.num_rows == len(expected_rows), (key, "row count")
    remaining = [
        [
            _normalize_value(table.column(position).to_pylist()[index])
            for position in range(table.num_columns)
        ]
        for index in range(table.num_rows)
    ]
    for expected_row in expected_rows:
        match = next(
            (
                index
                for index, actual_row in enumerate(remaining)
                if _values_equal(expected_row, actual_row)
            ),
            None,
        )
        assert match is not None, (key, "rows", expected_row, table.to_pylist())
        del remaining[match]


@pytest.mark.parametrize("key", _SQL_VALUE_KEYS)
def test_sql_cell_matches_spark_on_facade(key: str) -> None:
    """One recorded ``CAST(… AS MAP<…>)`` cell answers Spark on the facade SQL door."""
    session = _spark_session("true")
    try:
        _assert_value_cell_facade(session, key)
    finally:
        session.stop()


@pytest.mark.parametrize(("key", "tokens"), _SQL_ERROR_KEYS)
def test_sql_refusal_matches_spark_on_facade(key: str, tokens: tuple[str, str]) -> None:
    """One recorded refusal raises with Spark's exception class and error token."""
    cell = _CELLS[key]
    assert cell["error_class"] == tokens[0], (key, "oracle class")
    session = _spark_session("true")
    try:
        with pytest.raises(Exception, match=re.escape(tokens[1])):
            session.sql(cell["sql"]).to_arrow()
    finally:
        session.stop()


def test_invalid_value_legacy_yields_null_on_facade() -> None:
    """The ANSI-off twin of the invalid-value cast answers ``{'a': None}``."""
    cell = _CELLS[_LEGACY_KEY]
    assert cell["ansi"] is False
    session = _spark_session("false")
    try:
        frame = session.sql(cell["sql"])
        assert frame.schema.simpleString() == cell["schema"], "schema"
        table = frame.to_arrow()
        assert table.num_rows == len(cell["rows"]), "row count"
        for index, expected_row in enumerate(cell["rows"]):
            for position, expected in enumerate(expected_row):
                actual = _normalize_value(table.column(position).to_pylist()[index])
                assert _values_equal(expected, actual), ("rows", index, position)
    finally:
        session.stop()


def _df_cast_maptype_frame(session: Any) -> Any:
    """Select ``m`` cast through a ``MapType`` object (oracle ``df_cast_maptype``)."""
    from repark import functions as repark_functions
    from repark.spark.types import LongType, MapType, StringType

    base = session.sql("SELECT map('a', 1) AS m")
    return base.select(repark_functions.col("m").cast(MapType(StringType(), LongType())).alias("m"))


def _df_cast_ddl_string_frame(session: Any) -> Any:
    """Select ``m`` cast through a DDL string (oracle ``df_cast_ddl_string``)."""
    from repark import functions as repark_functions

    base = session.sql("SELECT map('a', 1) AS m")
    return base.select(repark_functions.col("m").cast("map<string,bigint>").alias("m"))


def _df_cast_nested_frame(session: Any) -> Any:
    """Select ``m`` cast through a nested DDL string (oracle ``df_cast_nested``)."""
    from repark import functions as repark_functions

    base = session.sql("SELECT map('k', array(1)) AS m")
    return base.select(repark_functions.col("m").cast("map<string,array<bigint>>").alias("m"))


_DF_FRAMES: dict[str, Any] = {
    "df_cast_maptype": _df_cast_maptype_frame,
    "df_cast_ddl_string": _df_cast_ddl_string_frame,
    "df_cast_nested": _df_cast_nested_frame,
}


@pytest.mark.parametrize("key", _DF_KEYS)
def test_dataframe_cast_matches_spark(key: str) -> None:
    """One recorded DataFrame-door ``.cast`` cell answers Spark's schema and rows."""
    cell = _CELLS[key]
    session = _spark_session("true")
    try:
        frame = _DF_FRAMES[key](session)
        assert frame.schema.simpleString() == cell["schema"], (key, "schema")
        assert [field.nullable for field in frame.schema.fields] == cell["nullable"], (
            key,
            "nullable",
        )
        table = frame.to_arrow()
        assert table.num_rows == len(cell["rows"]), (key, "row count")
        for index, expected_row in enumerate(cell["rows"]):
            for position, expected in enumerate(expected_row):
                actual = _normalize_value(table.column(position).to_pylist()[index])
                assert _values_equal(expected, actual), (key, "rows", index, position)
    finally:
        session.stop()


def _live_jsonable(value: Any) -> Any:
    """Render one live Spark value the way the recorder wrote the fixture."""
    if isinstance(value, dict):
        return {key: _live_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_live_jsonable(item) for item in value]
    if hasattr(value, "asDict"):
        return _live_jsonable(value.asDict(recursive=True))
    return value


def _live_cell_matches_fixture(session: Any, key: str, cell: dict[str, Any]) -> None:
    """Assert one live Spark answer equals the committed fixture cell."""
    if "error" in cell:
        try:
            session.sql(cell["sql"]).collect()
        except Exception as error:
            assert type(error).__name__ == cell["error_class"], (key, "live class")
            assert cell["error"] in str(error), (key, "live error")
        else:
            raise AssertionError((key, "live Spark unexpectedly answered"))
        return
    frame = session.sql(cell["sql"])
    assert frame.schema.simpleString() == cell["schema"], (key, "live schema")
    if "nullable" in cell:
        assert [field.nullable for field in frame.schema.fields] == cell["nullable"], (
            key,
            "live nullable",
        )
    rows = [[_live_jsonable(value) for value in output] for output in frame.collect()]
    assert json.loads(json.dumps(rows, default=str)) == cell["rows"], (key, "live rows")


@pytest.mark.parametrize(
    "key_arrow", _NATIVE_VALUE_CELLS, ids=[cell[0] for cell in _NATIVE_VALUE_CELLS]
)
def test_sql_cell_matches_spark_on_native_door(key_arrow: tuple[str, str, bool]) -> None:
    """One native-spellable ``CAST(… AS MAP<…>)`` cell answers on the ANSI door.

    Notes:
        The door keeps DataFusion nullability, so every cast of a nullable input
        reads nullable. The ``array(...)`` cells, the ``UNION ALL`` cell and the
        bare-``map()`` cell have no native spelling (``UNRESOLVED_ROUTINE`` for
        ``array``, zero-argument ``map`` refuses) and stay facade-only.
    """
    import repark

    key, arrow_type, nullable = key_arrow
    cell = _CELLS[key]
    table = repark.sql(cell["sql"]).to_arrow()
    assert str(table.schema[0].type) == arrow_type, (key, "arrow type")
    assert table.schema[0].nullable is nullable, (key, "nullable")
    assert table.num_rows == len(cell["rows"]), (key, "row count")
    for index, expected_row in enumerate(cell["rows"]):
        for position, expected in enumerate(expected_row):
            actual = _normalize_value(table.column(position).to_pylist()[index])
            assert _values_equal(expected, actual), (key, "rows", index, position)


@pytest.mark.parametrize("key", _NATIVE_ERROR_KEYS)
def test_sql_refusal_matches_spark_on_native_door(key: str) -> None:
    """One native-spellable refusal raises with Spark's error-class token."""
    import repark

    cell = _CELLS[key]
    with pytest.raises(Exception, match=re.escape(_NATIVE_ERROR_TOKENS[key])):
        repark.sql(cell["sql"]).to_arrow()


@pytest.mark.skipif(not _LIVE, reason=_LIVE_SKIP)
def test_live_oracle_matches_committed_fixture(spark_engine: Any) -> None:
    """Live Spark 4.1.2 re-derives every fixture cell (drift detector)."""
    session = spark_engine.session
    for key in _SQL_VALUE_KEYS:
        _live_cell_matches_fixture(session, key, _CELLS[key])
    for key, tokens in _SQL_ERROR_KEYS:
        cell = _CELLS[key]
        assert cell["error_class"] == tokens[0], (key, "oracle class")
        _live_cell_matches_fixture(session, key, cell)
    session.conf.set("spark.sql.ansi.enabled", "false")
    try:
        _live_cell_matches_fixture(session, _LEGACY_KEY, _CELLS[_LEGACY_KEY])
    finally:
        session.conf.set("spark.sql.ansi.enabled", "true")
    from pyspark.sql import functions as spark_functions
    from pyspark.sql import types as spark_types

    base = session.sql("SELECT map('a', 1) AS m")
    nested = session.sql("SELECT map('k', array(1)) AS m")
    calls = {
        "df_cast_maptype": base.select(
            spark_functions.col("m")
            .cast(spark_types.MapType(spark_types.StringType(), spark_types.LongType()))
            .alias("m")
        ),
        "df_cast_ddl_string": base.select(
            spark_functions.col("m").cast("map<string,bigint>").alias("m")
        ),
        "df_cast_nested": nested.select(
            spark_functions.col("m").cast("map<string,array<bigint>>").alias("m")
        ),
    }
    for key, frame in calls.items():
        cell = _CELLS[key]
        assert frame.schema.simpleString() == cell["schema"], (key, "live schema")
        assert [field.nullable for field in frame.schema.fields] == cell["nullable"], (
            key,
            "live nullable",
        )
        rows = [[row[0]] for row in frame.collect()]
        assert json.loads(json.dumps(rows, default=str)) == cell["rows"], (
            key,
            "live rows",
        )


_ROUND3: dict[str, dict[str, Any]] = json.loads(
    (_HERE / "cast_map_spell_1" / "cast_map_spell_1_round3_spark_oracle.json").read_text(
        encoding="utf-8"
    )
)["cells"]
_NATIVE_COMMENT_HINT_REASON = (
    "2026-09-19: the native door parses with stock DataFusion's Generic dialect, which "
    "expands MySQL /*! */ hints in every statement (SELECT 1 /*! 3 */, 2 refuses there with "
    "no map in sight); the map-cast rewrite itself leaves the hint alone"
)
_ROUND3_NATIVE_KEYS: tuple[Any, ...] = tuple(
    pytest.param(
        key,
        id=key,
        marks=pytest.mark.xfail(strict=True, reason=_NATIVE_COMMENT_HINT_REASON),
    )
    if key.startswith("comment_hint")
    else pytest.param(key, id=key)
    for key, cell in _ROUND3.items()
    if cell["ansi"]
)


def _round3_value(value: Any) -> Any:
    """Render one Arrow value the way the recorder's ``json`` pass rendered Spark's.

    Arrow maps read as key/value pairs; folding them into a dict keyed by ``str(key)``
    is what ``collect()`` plus ``json`` does to Spark's map (the later of two equal
    keys wins the dict slot), so duplicate keys compare exactly as Spark shows them.
    """
    if (
        isinstance(value, list)
        and value
        and all(isinstance(pair, tuple) and len(pair) == 2 for pair in value)
    ):
        return {str(key): _round3_value(item) for key, item in value}
    if isinstance(value, list):
        return [_round3_value(item) for item in value]
    return value


def _assert_round3_cell(key: str, run: Any) -> None:
    """Assert one round-3 cell's rows, or its Spark error token, through ``run(sql)``."""
    cell = _ROUND3[key]
    if "error" in cell:
        token = cell["error"].split("]")[0] + "]"
        with pytest.raises(Exception, match=re.escape(token)):
            run(cell["sql"]).to_arrow()
        return
    table = run(cell["sql"]).to_arrow()
    rows = [
        [
            _round3_value(table.column(position).to_pylist()[index])
            for position in range(table.num_columns)
        ]
        for index in range(table.num_rows)
    ]
    assert rows == cell["rows"], (key, rows)


@pytest.mark.parametrize("key", sorted(_ROUND3))
def test_round3_cell_matches_spark_on_facade(key: str) -> None:
    """One round-3 cell answers Spark on the facade door under its recorded ANSI mode.

    pins: cast-map-spell-1/C-011, C-012, C-013, C-014
    """
    session = _spark_session("true" if _ROUND3[key]["ansi"] else "false")
    try:
        _assert_round3_cell(key, session.sql)
        if "error" not in _ROUND3[key]:
            assert (
                session.sql(_ROUND3[key]["sql"]).schema.simpleString() == (_ROUND3[key]["schema"])
            ), key
    finally:
        session.stop()


@pytest.mark.parametrize("key", _ROUND3_NATIVE_KEYS)
def test_round3_cell_matches_spark_on_native_door(key: str) -> None:
    """One ANSI-on round-3 cell answers Spark on the native ``repark.sql`` door.

    pins: cast-map-spell-1/C-011, C-012, C-013, C-014
    """
    import repark

    _assert_round3_cell(key, repark.sql)


_ROUND4: dict[str, dict[str, Any]] = json.loads(
    (_HERE / "cast_map_spell_1" / "cast_map_spell_1_round4_spark_oracle.json").read_text(
        encoding="utf-8"
    )
)["cells"]
_NULL_KEY_REFUSAL = "a map key cast produced NULL"
_NATIVE_INT64_LITERAL_REASON = (
    "2026-09-19: the native door types an untyped integer literal as BIGINT (stock "
    "DataFusion), so its CAST_OVERFLOW names 128L of the type BIGINT; the facade names INT"
)


def _round4_ansi(key: str) -> bool:
    """Return the ANSI setting a round-4 cell was recorded under (its key suffix)."""
    return key.endswith("_ansi_true")


def _assert_round4_cell(key: str, run: Any) -> None:
    """Assert one round-4 cell through ``run(sql)``: Spark's JSON map, error or RePark's refusal.

    ``try_cast_overflow_key_*``: Spark answers a map holding a NULL key, which an Arrow map
    cannot hold, so RePark refuses loud (ruling Q-23b-9); the pin holds that refusal.
    """
    cell = _ROUND4[key]
    if key.startswith("try_cast_overflow_key"):
        with pytest.raises(Exception, match=re.escape(_NULL_KEY_REFUSAL)):
            run(cell["sql"]).to_arrow()
        return
    if "error" in cell:
        with pytest.raises(Exception, match=re.escape(cell["error"])):
            run(cell["sql"]).to_arrow()
        return
    value = run(cell["sql"]).to_arrow().column(0).to_pylist()[0]
    assert _round3_value(value) == json.loads(cell["json"]), (key, value)


@pytest.mark.parametrize("key", sorted(_ROUND4))
def test_round4_cell_matches_spark_on_facade(key: str) -> None:
    """One round-4 cell answers Spark on the facade door under its recorded ANSI mode.

    pins: cast-map-spell-1/C-013, C-015, C-016
    """
    session = _spark_session("true" if _round4_ansi(key) else "false")
    try:
        _assert_round4_cell(key, session.sql)
    finally:
        session.stop()


@pytest.mark.parametrize(
    "key",
    [
        pytest.param(
            key,
            id=key,
            marks=pytest.mark.xfail(strict=True, reason=_NATIVE_INT64_LITERAL_REASON),
        )
        if key == "overflow_leaf_ansi_true"
        else pytest.param(key, id=key)
        for key in sorted(_ROUND4)
        if _round4_ansi(key) and "char(" not in _ROUND4[key]["sql"]
    ],
)
def test_round4_cell_matches_spark_on_native_door(key: str) -> None:
    """One ANSI-on round-4 cell answers Spark on the native ``repark.sql`` door.

    The ``char(...)`` cells have no native spelling (``UNRESOLVED_ROUTINE``) and stay
    facade-only.

    pins: cast-map-spell-1/C-013, C-015, C-016
    """
    import repark

    _assert_round4_cell(key, repark.sql)
