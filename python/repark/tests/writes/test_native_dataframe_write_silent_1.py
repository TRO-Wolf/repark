from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _native_dataframe_write_silent_1_cells import (
    EXPECTED_FILE,
    PATH_KINDS,
    Door,
    path_refusal,
    scrub,
    shape,
    table_refusal,
)

import repark
from repark import ReparkSession
from repark._native import PyReparkSession
from repark.spark.session import _reset_active_session_for_tests

EXPECTED: dict[str, Any] = json.loads(EXPECTED_FILE.read_text(encoding="utf-8"))
GUARDED = sorted(key for key, cell in EXPECTED.items() if cell["guard"])
KEPT = sorted(key for key, cell in EXPECTED.items() if not cell["guard"])


def quoted_table(target: str) -> str:
    return target if "." in target else f"datafusion.public.{target}"


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, dict[str, Any]]]:
    _reset_active_session_for_tests()
    root = tmp_path_factory.mktemp("native-dataframe-write-silent-1")
    cells: dict[str, dict[str, Any]] = {}
    previous = repark._ANSI_NATIVE
    try:
        for name in ("native", "spark-door"):
            door = Door(name, root / name)
            door.open()
            try:
                for cell in door.cells():
                    cell["root"] = door.root
                    cells[cell["key"]] = cell
            finally:
                door.close()
    finally:
        repark._ANSI_NATIVE = previous
    yield cells


def test_the_recorded_table_covers_every_measured_cell(
    measured: dict[str, dict[str, Any]],
) -> None:
    assert sorted(measured) == sorted(EXPECTED)
    assert len(GUARDED) == 399
    assert all(key.startswith("native|") for key in GUARDED)


@pytest.mark.parametrize("key", GUARDED)
def test_a_native_write_that_would_store_nothing_refuses_before_any_effect(
    measured: dict[str, dict[str, Any]], key: str
) -> None:
    cell = measured[key]
    _, kind, verb, _ = key.split("|")
    if kind in PATH_KINDS:
        text = path_refusal(cell["target"])
    else:
        text = table_refusal(verb, quoted_table(cell["target"]), kind)
    assert cell["outcome"] == {
        "kind": "refused",
        "class": "UnsupportedOperationException",
        "text": text,
    }, cell
    assert cell["after"] == cell["before"], cell
    if kind in PATH_KINDS:
        assert cell["after"]["staging"] == [], cell


@pytest.mark.parametrize("key", KEPT)
def test_every_other_cell_keeps_its_outcome_on_main(
    measured: dict[str, dict[str, Any]], key: str
) -> None:
    cell = measured[key]
    outcome = dict(cell["outcome"])
    if "text" in outcome:
        outcome["text"] = scrub(outcome["text"], cell["root"])
    assert outcome == EXPECTED[key]["outcome"], cell
    assert cell["after"]["rows"] == EXPECTED[key]["after_rows"], cell
    assert shape(cell["after"]) == EXPECTED[key]["after_shape"], cell


@pytest.mark.parametrize(
    "kind", ["ice_existing", "hd_existing", "ice_memory_schema", "memory_qualified"]
)
def test_native_merge_into_stays_a_loud_refusal(
    measured: dict[str, dict[str, Any]], kind: str
) -> None:
    for frame in ("sql", "join"):
        cell = measured[f"native|{kind}|merge_insert_all|{frame}"]
        assert cell["outcome"]["kind"] == "refused", cell
        assert cell["seat_returned"] == 0, cell
        assert cell["after"] == cell["before"], cell


@pytest.fixture
def native_door(tmp_path: Path) -> Iterator[Path]:
    previous = repark._ANSI_NATIVE
    native = PyReparkSession.native()
    native.register_memory_catalog("ice", str(tmp_path / "ice"))
    native.create_namespace("ice", "ns", str(tmp_path / "ice" / "ns"))
    repark._ANSI_NATIVE = native
    try:
        yield tmp_path
    finally:
        repark._ANSI_NATIVE = previous


def rows(sql: str) -> list[tuple[Any, ...]]:
    return sorted(tuple(row.values()) for row in repark.sql(sql).to_arrow().to_pylist())


WORKAROUND = re.compile(
    r"run ((?:INSERT INTO|INSERT OVERWRITE|CREATE OR REPLACE TABLE) (\S+) (?:AS )?SELECT \.\.\.) "
    r"through repark\.sql\(\.\.\.\) and collect the result"
)
WORKAROUND_CASES = {
    "iceberg_append": ("ice.ns.t", "ice_existing", "ins_append", [(7,), (8,), (9,)]),
    "iceberg_overwrite": ("ice.ns.t", "ice_existing", "ins_overwrite", [(9,)]),
    "iceberg_save_overwrite": ("ice.ns.t", "ice_existing", "save_name_overwrite", [(9,)]),
    "iceberg_overwrite_partitions": (
        "ice.ns.t",
        "ice_existing",
        "v2_overwrite_partitions",
        [(9,)],
    ),
    "memory_append": ("datafusion.public.m", "memory_qualified", "ins_append", [(7,), (8,), (9,)]),
    "memory_overwrite": ("datafusion.public.m", "memory_qualified", "ins_overwrite", [(9,)]),
    "memory_schema_overwrite": ("ice.ms.m", "ice_memory_schema", "ins_overwrite", [(9,)]),
}


@pytest.mark.parametrize("case", sorted(WORKAROUND_CASES))
def test_the_named_workaround_stores_the_rows_when_followed_literally(
    native_door: Path, case: str
) -> None:
    target, kind, verb, expected = WORKAROUND_CASES[case]
    rows("CREATE SCHEMA ice.ms")
    rows(f"CREATE TABLE {target} (id INT)")
    rows(f"INSERT INTO {target} VALUES (7), (8)")
    rows("CREATE TABLE ice.ns.src (id INT)")
    rows("INSERT INTO ice.ns.src VALUES (9)")
    frame = repark.sql("SELECT id FROM ice.ns.src")
    with pytest.raises(repark.errors.UnsupportedOperationException) as refused:
        if verb == "v2_overwrite_partitions":
            frame.writeTo(target).overwritePartitions()
        elif verb == "save_name_overwrite":
            frame.write.format("iceberg").mode("overwrite").save(target)
        else:
            frame.write.insertInto(target, overwrite=verb == "ins_overwrite")
    message = str(refused.value)
    assert message == table_refusal(verb, target, kind)
    assert rows(f"SELECT id FROM {target}") == [(7,), (8,)]
    named = WORKAROUND.search(message)
    assert named is not None, message
    assert named.group(2) == target
    frame.createOrReplaceTempView("frame_rows")
    rows(named.group(1).replace("SELECT ...", "SELECT * FROM frame_rows"))
    assert rows(f"SELECT id FROM {target}") == expected


ABSOLUTE = "<absolute>"


@pytest.mark.parametrize(
    ("spelling", "named"),
    [
        ("out", "out"),
        ("out/", "out"),
        ("data/out", "data/out"),
        (f"{ABSOLUTE}/out", f"{ABSOLUTE}/out"),
        (".", "."),
        ("..", ".."),
        ("relx", "relx"),
        ("sub/", "sub"),
        ("deep/er/", "deep/er"),
    ],
)
@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_a_path_is_named_as_the_user_wrote_it(
    native_door: Path, monkeypatch: pytest.MonkeyPatch, spelling: str, named: str, mode: str
) -> None:
    work = native_door / "work" / "inner"
    work.mkdir(parents=True)
    monkeypatch.chdir(work)
    absolute = str(native_door / "abs")
    spelling = spelling.replace(ABSOLUTE, absolute)
    named = named.replace(ABSOLUTE, absolute)
    before = sorted(str(p) for p in native_door.rglob("*"))
    frame = repark.sql("SELECT 1 AS id")
    with pytest.raises(repark.errors.UnsupportedOperationException) as refused:
        frame.write.mode(mode).parquet(spelling)
    assert str(refused.value) == path_refusal(named)
    assert sorted(str(p) for p in native_door.rglob("*")) == before


def test_the_path_workaround_stores_the_rows(native_door: Path) -> None:
    destination = native_door / "out"
    frame = repark.sql("SELECT 9 AS id")
    with pytest.raises(repark.errors.UnsupportedOperationException) as refused:
        frame.write.mode("overwrite").parquet(str(destination))
    assert str(refused.value) == path_refusal(str(destination))
    assert "write the frame from a ReparkSession" in str(refused.value)
    spark = ReparkSession.builder.appName("native-dataframe-write-silent-1").getOrCreate()
    try:
        spark.sql("SELECT 9 AS id").write.mode("overwrite").parquet(str(destination))
        stored = sorted(tuple(row) for row in spark.read.parquet(str(destination)).collect())
    finally:
        spark.stop()
    assert stored == [(9,)]
