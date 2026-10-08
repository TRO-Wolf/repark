from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _sm2_shared as sm2
import pytest

from repark.errors import AnalysisException, UnsupportedOperationException
from repark.spark import functions as spark_functions

_HERE = Path(__file__).parent
_MIXED_SPARK = json.loads((_HERE / "using_per_side_keys_1_mixed_spark.json").read_text())
_MIXED_MAIN = json.loads((_HERE / "using_per_side_keys_1_mixed_main.json").read_text())
_SIDES = {
    "int": "SELECT CAST(c AS INT) AS id FROM VALUES (1), (2) AS t(c)",
    "smallint": "SELECT CAST(c AS SMALLINT) AS id FROM VALUES (2), (7) AS t(c)",
    "bigint": "SELECT CAST(c AS BIGINT) AS id FROM VALUES (2), (5000000000) AS t(c)",
    "decimal": "SELECT CAST(c AS DECIMAL(10,2)) AS id FROM VALUES (2.00), (3.50) AS t(c)",
    "double": "SELECT CAST(c AS DOUBLE) AS id FROM VALUES (2.0), (3.7) AS t(c)",
    "float": "SELECT CAST(c AS FLOAT) AS id FROM VALUES (2.0), (3.5) AS t(c)",
    "string": "SELECT CAST(c AS STRING) AS id FROM VALUES ('2'), ('3') AS t(c)",
    "date": "SELECT CAST(c AS DATE) AS id FROM VALUES ('2026-01-02'), ('2026-01-05') AS t(c)",
    "timestamp": "SELECT CAST(c AS TIMESTAMP) AS id "
    "FROM VALUES ('2026-01-02 00:00:00'), ('2026-01-03 12:34:56') AS t(c)",
}
_SPARK_TYPED = [
    name
    for name, cell in sorted(_MIXED_SPARK.items())
    if "refused" not in cell and name.rsplit("|", 1)[1] in ("right", "full")
]


def _frames(session: Any) -> tuple[Any, Any]:
    left = session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id INT, s STRING")
    right = session.createDataFrame([(2, "x"), (3, "y"), (4, "z")], "id INT, t STRING")
    return left, right


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    return sorted(
        (tuple(row) for row in frame.collect()),
        key=lambda row: tuple(str(value) for value in row),
    )


def _mixed(session: Any, name: str) -> dict[str, Any]:
    left, right, how = name.split("|")
    frame = session.sql(_SIDES[left]).join(session.sql(_SIDES[right]), "id", how)
    rows = sorted(str(row[0]) for row in frame.select(frame["id"].cast("string")).collect())
    return {"type": frame.schema.simpleString(), "rows": rows}


@pytest.fixture(scope="module")
def mixed(tmp_path_factory: pytest.TempPathFactory) -> dict[str, dict[str, Any]]:
    session = sm2._open(tmp_path_factory.mktemp("upsk-mixed"), "upsk-mixed")
    found = {name: _mixed(session, name) for name in _SPARK_TYPED}
    session.stop()
    return found


@pytest.mark.parametrize("name", _SPARK_TYPED)
def test_mixed_key_shows_sparks_type_and_value(mixed: dict[str, dict[str, Any]], name: str) -> None:
    assert mixed[name] == _MIXED_SPARK[name]


def test_mixed_key_table_covers_the_measured_pairs() -> None:
    assert len(_SPARK_TYPED) == 48
    assert set(_MIXED_SPARK) == set(_MIXED_MAIN)
    refused = sorted(name for name, cell in _MIXED_SPARK.items() if "refused" in cell)
    assert {name.rsplit("|", 1)[0] for name in refused} == {
        "date|string",
        "string|date",
        "timestamp|string",
        "string|timestamp",
    }


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_a_second_side_key_operation_keeps_the_reach(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"upsk-df-second-{how}")
    left, right = _frames(session)
    frame = left.alias("l").join(right.alias("r"), "id", how)
    over_two = {"left": [3], "right": [3, 4], "full": [3, 4]}[how]
    assert [
        row[0] for row in frame.filter("r.id > 2").sort(spark_functions.col("l.id")).collect()
    ] == sorted(over_two, key=lambda key: (key != 4 or how == "left", key))
    assert sorted(row[0] for row in frame.filter("r.id > 2").select("r.id").collect()) == over_two
    kept = frame.sort(spark_functions.col("r.id")).filter("l.id > 1")
    assert sorted(row[0] for row in kept.collect()) == [2, 3]
    twice = frame.filter("r.id > 2").filter("r.id < 4")
    assert _rows(twice) == [(3, "c", "y")]
    assert twice.columns == ["id", "s", "t"]
    mixed_sides = frame.filter("l.id > 1").filter("r.id > 2").sort("l.id").select("l.id", "r.id")
    assert _rows(mixed_sides) == [(3, 3)]
    session.stop()


@pytest.mark.parametrize("how", ["left", "right", "full"])
@pytest.mark.parametrize("action", ["cache", "persist"])
def test_side_keys_do_not_depend_on_cache_materialisation(
    tmp_path: Path, how: str, action: str
) -> None:
    session = sm2._open(tmp_path, f"upsk-df-cache-{how}-{action}")
    left, right = _frames(session)
    fresh = left.alias("l").join(right.alias("r"), "id", how)
    hidden = "l.id" if how == "right" else "r.id"
    before = _rows(fresh.select(hidden))
    filtered = _rows(fresh.filter(f"{hidden} > 2"))
    frame = left.alias("l").join(right.alias("r"), "id", how)
    getattr(frame, action)().count()
    assert frame.columns == ["id", "s", "t"]
    assert _rows(frame) == _rows(fresh)
    assert _rows(frame.select(hidden)) == before
    assert _rows(frame.filter(f"{hidden} > 2")) == filtered
    assert frame.filter(f"{hidden} > 2").columns == ["id", "s", "t"]
    assert list(frame.toPandas().columns) == ["id", "s", "t"]
    target = tmp_path / f"out-{how}-{action}"
    frame.write.parquet(str(target))
    sm2._assert_no_twin_bytes(target)
    frame.unpersist()
    assert _rows(frame.select(hidden)) == before
    session.stop()


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_side_keys_refuse_after_a_checkpoint_as_before(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"upsk-df-ckpt-{how}")
    left, right = _frames(session)
    fresh = left.alias("l").join(right.alias("r"), "id", how)
    frame = left.alias("l").join(right.alias("r"), "id", how).localCheckpoint()
    hidden = "l.id" if how == "right" else "r.id"
    assert frame.columns == ["id", "s", "t"]
    assert _rows(frame) == _rows(fresh)
    refused = sm2._refusal_of(lambda: frame.select(hidden).collect())
    assert isinstance(refused, UnsupportedOperationException)
    assert "to a USING join key is not supported in repark v1" in str(refused)
    session.stop()


@pytest.mark.parametrize("ansi", ["true", "false"])
def test_ansi_off_keeps_the_left_key_for_mixed_full_keys(tmp_path: Path, ansi: str) -> None:
    session = sm2._open(tmp_path, f"upsk-df-ansi-{ansi}")
    session.conf.set("spark.sql.ansi.enabled", ansi)
    left = session.sql("SELECT CAST(c AS INT) AS id FROM VALUES (1), (2) AS t(c)")
    right = session.sql("SELECT CAST(c AS STRING) AS id FROM VALUES ('2'), ('3') AS t(c)")
    frame = left.join(right, "id", "full")
    if ansi == "true":
        assert frame.schema.simpleString() == "struct<id:bigint>"
        assert _rows(frame) == [(1,), (2,), (3,)]
    else:
        assert frame.schema.simpleString() == "struct<id:int>"
        assert _rows(frame) == [(1,), (2,), (None,)]
    session.conf.set("spark.sql.ansi.enabled", "true")
    session.stop()


def test_a_user_column_with_the_reserved_prefix_refuses_side_keys(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "upsk-df-collide")
    _left, right = _frames(session)
    odd = session.createDataFrame(
        [(1, 100), (2, 200), (3, 300)], "id INT, `__repark_using__r__id` INT"
    )
    frame = odd.alias("l").join(right.alias("r"), "id", "full")
    assert frame.columns == ["id", "__repark_using__r__id", "t"]
    assert _rows(frame.select("id")) == [(1,), (2,), (3,), (None,)]
    assert _rows(frame.select("__repark_using__r__id")) == [(100,), (200,), (300,), (None,)]
    assert _rows(frame.filter("l.id > 1").select("id")) == [(2,), (3,)]
    assert _rows(frame.select("l.id")) == [(1,), (2,), (3,), (None,)]
    by_left = frame.sort(spark_functions.col("l.id").desc()).collect()
    assert [row[0] for row in by_left][:3] == [3, 2, 1]
    refused = sm2._refusal_of(lambda: frame.select("r.id").collect())
    assert isinstance(refused, UnsupportedOperationException)
    assert "qualified reference `r`.`id` to a USING join key" in str(refused)
    cached = odd.alias("l").join(right.alias("r"), "id", "full")
    cached.cache().count()
    assert cached.columns == ["id", "__repark_using__r__id", "t"]
    plain = session.createDataFrame([(1, 5)], "id INT, `__repark_using__zz` INT")
    plain.cache().count()
    assert plain.columns == ["id", "__repark_using__zz"]
    assert _rows(plain.filter("__repark_using__zz = 5")) == [(1, 5)]
    session.stop()


@pytest.mark.parametrize("how", ["left_semi", "left_anti"])
def test_semi_anti_stale_right_key_in_sort(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"upsk-df-semi-{how}")
    left, right = _frames(session)
    frame = left.join(right, "id", how)
    refused = sm2._refusal_of(lambda: frame.sort(right["id"].desc(), "s").collect())
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused).startswith("MISSING_ATTRIBUTES") or (
        "MISSING_ATTRIBUTES" in str(refused)
    )
    assert sorted(row[0] for row in frame.sort(left["id"]).collect()) == (
        [2, 3] if how == "left_semi" else [1]
    )
    session.stop()
