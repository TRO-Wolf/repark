"""The FNP-9/10 live-oracle leg: the collections and JSON goldens re-derived from Spark.

Split out of ``test_parity_live.py`` when that file reached its 1000-line ceiling; it shares the
session-scoped ``spark_engine`` fixture from ``conftest.py``, so it co-collects and co-runs with
``test_live_disclosure_still_diverges`` in one JVM.
"""

from __future__ import annotations

import _live_parity as lp
import pytest


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_fnp9_collections_json(spark_engine: lp.Engine) -> None:
    """The FNP-9/10 goldens re-derived from live Spark 4.1.2."""
    oracle = spark_engine.session.sql(
        "SELECT get_json_object('{\"a\":1.50}', '$.a') AS number_spelling, "
        "get_json_object('{\"a\":[1,2,3]}', '$.a[*]') AS wildcard_many, "
        "get_json_object('{\"a\":[1]}', '$.a[*]') AS wildcard_one, "
        "get_json_object('{\"a\":[1,[2,3]]}', '$.a[*][*]') AS wildcard_flatten, "
        "json_array_length('[[1,2],3]') AS nested_length, "
        'json_object_keys(\'{"a":1,"b":2}\') AS keys, '
        "to_json(named_struct('a', CAST(NULL AS INT), 'b', 'x')) AS omitted_field, "
        "to_json(map('a', CAST(NULL AS INT))) AS written_value, "
        "to_json(named_struct('d', CAST(1e20 AS DOUBLE))) AS java_double, "
        "from_json('{\"a\":1.7}', 'a INT') AS fractional_into_int, "
        'schema_of_json(\'{"b":1,"a":2}\') AS sorted_fields, '
        "array_insert(array(1,2,3), -1, 9) AS negative_appends, "
        "array_insert(array(1,2), 5, 9) AS padded, "
        "arrays_zip(array(1,2,3), array('a')) AS zipped, "
        "map_concat(map('a',1), map('b',2)) AS joined"
    ).toArrow()
    assert oracle.column("number_spelling").to_pylist() == ["1.5"]
    assert oracle.column("wildcard_many").to_pylist() == ["[1,2,3]"]
    assert oracle.column("wildcard_one").to_pylist() == ["1"]
    assert oracle.column("wildcard_flatten").to_pylist() == ["[1,2,3]"]
    assert oracle.column("nested_length").to_pylist() == [2]
    assert oracle.column("keys").to_pylist() == [["a", "b"]]
    assert oracle.column("omitted_field").to_pylist() == ['{"b":"x"}']
    assert oracle.column("written_value").to_pylist() == ['{"a":null}']
    assert oracle.column("java_double").to_pylist() == ['{"d":1.0E20}']
    assert oracle.column("fractional_into_int").to_pylist() == [{"a": None}]
    assert oracle.column("sorted_fields").to_pylist() == ["STRUCT<a: BIGINT, b: BIGINT>"]
    round_two = spark_engine.session.sql(
        "SELECT get_json_object('{\"a\":[[1,2],[3]]}', '$.a[*][1]') AS single_unwraps, "
        "get_json_object('{\"a\":[[1]]}', '$.a[0][*]') AS index_then_wildcard, "
        "get_json_object('{\"a\":[[1,2],[3]]}', '$.a[*][*]') AS double_wildcard, "
        "from_json('{\"a\":1,\"a\":2}', 'a INT') AS last_key_wins, "
        "from_json('{\"a\":\"x\"}', 'a INT, _corrupt_record STRING') AS bad_record, "
        "from_json('', 'a INT') AS empty_row, "
        'from_json(\'{"a":[1,"x"],"d":2}\', \'a ARRAY<INT>, d INT\') AS shape_nulls, '
        "from_json('{\"a\":1.50}', 'a STRING') AS java_string, "
        "schema_of_json('{\"a b\":1}') AS quoted_name, "
        'schema_of_json(\'{"a":{},"b":1}\') AS pruned, '
        "array_insert(array(1,2), 1, CAST(1.5 AS DOUBLE)) AS widened"
    ).collect()
    row = round_two[0]
    assert row["single_unwraps"] == "2"
    assert row["index_then_wildcard"] == "[1]"
    assert row["double_wildcard"] == "[1,2,3]"
    assert row["last_key_wins"]["a"] == 2
    assert row["bad_record"]["a"] is None
    assert row["bad_record"]["_corrupt_record"] == '{"a":"x"}'
    assert row["empty_row"] is None
    assert row["shape_nulls"]["a"] is None
    assert row["shape_nulls"]["d"] == 2
    assert row["java_string"]["a"] == "1.5"
    assert row["quoted_name"] == "STRUCT<`a b`: BIGINT>"
    assert row["pruned"] == "STRUCT<b: BIGINT>"
    assert row["widened"] == [1.5, 1.0, 2.0]
    decimals = spark_engine.session.sql(
        "SELECT CAST(from_json('{\"d\":1.505}', 'd DECIMAL(5,2)').d AS STRING) AS half_up, "
        "CAST(from_json('{\"e\":123456.78}', 'e DECIMAL(5,2)').e AS STRING) AS overflow, "
        "CAST(from_json('{\"d\":\"2.50\"}', 'd DECIMAL(5,2)').d AS STRING) AS from_string"
    ).collect()
    assert [row["half_up"] for row in decimals] == ["1.51"]
    assert [row["overflow"] for row in decimals] == [None]
    assert [row["from_string"] for row in decimals] == ["2.50"]
    assert oracle.column("negative_appends").to_pylist() == [[1, 2, 3, 9]]
    assert oracle.column("padded").to_pylist() == [[1, 2, None, None, 9]]
    assert oracle.column("zipped").to_pylist() == [
        [{"0": 1, "1": "a"}, {"0": 2, "1": None}, {"0": 3, "1": None}]
    ]
    assert oracle.column("joined").to_pylist() == [[("a", 1), ("b", 2)]]
    for statement, condition in (
        ("SELECT array_insert(array(1,2), 0, 9)", "INVALID_INDEX_OF_ZERO"),
        ("SELECT map_concat(map('a',1), map('a',2))", "DUPLICATED_MAP_KEY"),
        ("SELECT schema_of_json('{bad')", "JsonParseException"),
        ("SELECT from_json('{bad', 'a INT', map('mode','DROPMALFORMED'))", "PARSE_MODE"),
        ("SELECT array_insert(array(1,2), 1, 'z')", "ARRAY_FUNCTION_DIFF_TYPES"),
        ("SELECT get_json_object(1, '$')", "DATATYPE_MISMATCH"),
        (
            "SELECT from_json('{\"a\":\"x\"}', 'a INT', map('mode','FAILFAST'))",
            "MALFORMED_RECORD_IN_PARSING",
        ),
    ):
        try:
            spark_engine.session.sql(statement).toArrow()
            raise AssertionError(f"{statement} must raise")
        except AssertionError:
            raise
        except Exception as exc:
            assert condition in str(exc), f"{statement}: {exc}"
