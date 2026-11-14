from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _native_dataframe_write_silent_1_cells import (
    BY_NAME_VERBS,
    EXPECTED_FILE,
    PATH_KINDS,
    PATH_ROUTE,
    TABLE_VERBS,
    Door,
    hadoop_config,
    path_refusal,
    scrub,
    shape,
    table_refusal,
    view_refusal,
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
    r"register the frame as a temporary view <view> and run (.+?) "
    r"through repark\.sql\(\.\.\.\) and collect the result"
)


def followed(message: str, view: str) -> str:
    named = WORKAROUND.search(message)
    assert named is not None, message
    return named.group(1).replace("<view>", view)


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
        TABLE_VERBS[verb](frame, target)
    message = str(refused.value)
    assert message == table_refusal(verb, target, kind, ("id",), ("id",))
    assert rows(f"SELECT id FROM {target}") == [(7,), (8,)]
    frame.createOrReplaceTempView("frame_rows")
    rows(followed(message, "frame_rows"))
    assert rows(f"SELECT id FROM {target}") == expected


REORDERED = ("p", "id")
POSITIONAL_VERBS = ("ins_append", "ins_overwrite", "ins_partition_by")
REORDER_CELLS = [
    (verb, kind)
    for verb in sorted(BY_NAME_VERBS) + list(POSITIONAL_VERBS)
    for kind in ("ice_existing", "hd_existing")
] + [
    (verb, kind) for verb in POSITIONAL_VERBS for kind in ("ice_memory_schema", "memory_qualified")
]
SEED = [(7, 1), (8, 2)]
SOURCE = [(9, 1), (10, 3)]


def expected_rows(verb: str) -> list[tuple[int, int]]:
    stored = SOURCE if verb in BY_NAME_VERBS else [(p, i) for i, p in SOURCE]
    overwrite = verb in ("ins_overwrite", "save_name_overwrite", "v2_overwrite_partitions")
    return sorted(stored if overwrite else SEED + stored)


@pytest.fixture(scope="module")
def spark_door_rows(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[dict[str, list[tuple[int, int]]]]:
    _reset_active_session_for_tests()
    root = tmp_path_factory.mktemp("native-dataframe-write-silent-1-reordered")
    builder = ReparkSession.builder.appName("native-dataframe-write-silent-1-reordered")
    for key, value in hadoop_config(root).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    stored: dict[str, list[tuple[int, int]]] = {}
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS hd.ns").collect()
        spark.sql("CREATE TABLE hd.ns.src (id INT, p INT) USING iceberg").collect()
        spark.sql("INSERT INTO hd.ns.src VALUES (9, 1), (10, 3)").collect()
        for n, verb in enumerate(sorted(BY_NAME_VERBS) + list(POSITIONAL_VERBS)):
            target = f"hd.ns.t{n}"
            spark.sql(f"CREATE TABLE {target} (id INT, p INT) USING iceberg").collect()
            spark.sql(f"INSERT INTO {target} VALUES (7, 1), (8, 2)").collect()
            TABLE_VERBS[verb](spark.sql("SELECT p, id FROM hd.ns.src"), target)
            result = spark.sql(f"SELECT id, p FROM {target}").collect()
            stored[verb] = sorted((row[0], row[1]) for row in result)
    finally:
        spark.stop()
        _reset_active_session_for_tests()
    yield stored


@pytest.fixture
def both_catalogs(tmp_path: Path) -> Iterator[Path]:
    previous = repark._ANSI_NATIVE
    native = PyReparkSession.native(config=hadoop_config(tmp_path))
    native.register_memory_catalog("ice", str(tmp_path / "ice"))
    native.create_namespace("ice", "ns", str(tmp_path / "ice" / "ns"))
    native.create_namespace("hd", "ns", None)
    repark._ANSI_NATIVE = native
    try:
        rows("CREATE SCHEMA ice.ms")
        yield tmp_path
    finally:
        repark._ANSI_NATIVE = previous


NATIVE_TARGETS = {
    "ice_existing": "ice.ns.t",
    "hd_existing": "hd.ns.t",
    "ice_memory_schema": "ice.ms.t",
    "memory_qualified": "datafusion.public.t",
}


@pytest.mark.parametrize(("verb", "kind"), REORDER_CELLS)
def test_a_reordered_frame_stores_the_spark_doors_rows_when_the_route_is_followed(
    both_catalogs: Path,
    spark_door_rows: dict[str, list[tuple[int, int]]],
    verb: str,
    kind: str,
) -> None:
    target = NATIVE_TARGETS[kind]
    catalog = "hd" if kind == "hd_existing" else "ice"
    rows(f"CREATE TABLE {target} (id INT, p INT)")
    rows(f"INSERT INTO {target} VALUES (7, 1), (8, 2)")
    rows(f"CREATE TABLE {catalog}.ns.src (id INT, p INT)")
    rows(f"INSERT INTO {catalog}.ns.src VALUES (9, 1), (10, 3)")
    frame = repark.sql(f"SELECT p, id FROM {catalog}.ns.src")
    with pytest.raises(repark.errors.UnsupportedOperationException) as refused:
        TABLE_VERBS[verb](frame, target)
    message = str(refused.value)
    assert message == table_refusal(verb, target, kind, REORDERED)
    assert rows(f"SELECT id, p FROM {target}") == SEED
    frame.createOrReplaceTempView("frame_rows")
    rows(followed(message, "frame_rows"))
    assert rows(f"SELECT id, p FROM {target}") == expected_rows(verb)
    assert spark_door_rows[verb] == expected_rows(verb)


@pytest.mark.parametrize("target", ["datafusion.public.v1", "ice.ms.v2"])
@pytest.mark.parametrize("overwrite", [False, True])
def test_a_native_view_refuses_without_naming_a_statement(
    native_door: Path, target: str, overwrite: bool
) -> None:
    rows("CREATE SCHEMA ice.ms")
    rows(f"CREATE VIEW {target} AS SELECT * FROM (VALUES (7, 1), (8, 2)) AS x(id, p)")
    frame = repark.sql("SELECT 9 AS id, 1 AS p")
    with pytest.raises(repark.errors.UnsupportedOperationException) as refused:
        frame.write.insertInto(target, overwrite=overwrite)
    assert str(refused.value) == view_refusal(target)
    assert "INSERT" not in str(refused.value)
    assert rows(f"SELECT id, p FROM {target}") == SEED


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


@pytest.mark.parametrize(
    ("query", "mode"),
    [
        ("SELECT id, p FROM ice.ns.src", "overwrite"),
        ("SELECT id, p FROM ice.ns.src", "append"),
        ("SELECT id, p FROM ice.ns.src WHERE id < 0", "overwrite"),
        ("SELECT id, p FROM ice.ns.src WHERE id < 0", "append"),
    ],
)
def test_the_path_workaround_stores_the_rows(native_door: Path, query: str, mode: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows("CREATE TABLE ice.ns.src (id INT, p INT)")
    rows("INSERT INTO ice.ns.src VALUES (9, 1), (10, 3)")
    destination = native_door / "out"
    seed = pa.table({"id": pa.array([7], pa.int32()), "p": pa.array([1], pa.int32())})
    if mode == "append":
        destination.mkdir()
        pq.write_table(seed, destination / "part-seed.parquet")
    frame = repark.sql(query)
    with pytest.raises(repark.errors.UnsupportedOperationException) as refused:
        frame.write.mode(mode).parquet(str(destination))
    assert str(refused.value) == path_refusal(str(destination))
    assert PATH_ROUTE in str(refused.value)
    expected = sorted(tuple(row.values()) for row in frame.to_arrow().to_pylist())
    if mode == "append":
        expected = sorted([(7, 1), *expected])
    spark = ReparkSession.builder.appName("native-dataframe-write-silent-1").getOrCreate()
    try:
        spark.createDataFrame(frame.collect(), frame.schema).write.mode(mode).parquet(
            str(destination)
        )
        stored = pq.read_table(destination)
    finally:
        spark.stop()
    assert sorted(tuple(row.values()) for row in stored.to_pylist()) == expected
    assert [str(field.type) for field in stored.schema] == ["int32", "int32"]
