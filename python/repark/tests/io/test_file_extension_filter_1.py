from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession, _native
from repark._native import PyReparkSession
from repark.errors import PySparkException

_ANSI = ["false", "true"]
_DOORS = ["reader", "format_load"]
_UNWIRED_DOORS = ["default_load", "sql", "view"]
_REFUSED = None
_PARTITION_COLUMN_CELLS = {"layout/partitioned-card", "layout/partitioned-markers"}

_SPARK_PART = "part-00000-5e1f-c000.snappy.parquet"
_SPARK_PART_2 = "part-00001-5e1f-c000.snappy.parquet"

_CELLS: dict[str, tuple[dict[str, Any], str, list[int] | None, list[str] | None]] = {
    "name/b.parquet": (
        {"a.parquet": 10, "b.parquet": 11},
        "",
        [10, 11],
        ["a.parquet", "b.parquet"],
    ),
    "name/b.PARQUET": (
        {"a.parquet": 10, "b.PARQUET": 11},
        "",
        [10, 11],
        ["a.parquet", "b.PARQUET"],
    ),
    "name/b.snappy.parquet": (
        {"a.parquet": 10, "b.snappy.parquet": 11},
        "",
        [10, 11],
        ["a.parquet", "b.snappy.parquet"],
    ),
    "name/b.parq": ({"a.parquet": 10, "b.parq": 11}, "", [10, 11], ["a.parquet", "b.parq"]),
    "name/b": ({"a.parquet": 10, "b": 11}, "", [10, 11], ["a.parquet", "b"]),
    "name/b.bin": ({"a.parquet": 10, "b.bin": 11}, "", [10, 11], ["a.parquet", "b.bin"]),
    "name/b.x.y.z": ({"a.parquet": 10, "b.x.y.z": 11}, "", [10, 11], ["a.parquet", "b.x.y.z"]),
    "name/_b.parquet": ({"a.parquet": 10, "_b.parquet": 11}, "", [10], ["a.parquet"]),
    "name/.b.parquet": ({"a.parquet": 10, ".b.parquet": 11}, "", [10], ["a.parquet"]),
    "name/_k=1.parquet": (
        {"a.parquet": 10, "_k=1.parquet": 11},
        "",
        [10, 11],
        ["_k=1.parquet", "a.parquet"],
    ),
    "name/b.parquet._COPYING_": (
        {"a.parquet": 10, "b.parquet._COPYING_": 11},
        "",
        [10],
        ["a.parquet"],
    ),
    "marker/_SUCCESS": ({"a.parquet": 10, "_SUCCESS": b""}, "", [10], ["a.parquet"]),
    "marker/_committed_123": (
        {"a.parquet": 10, "_committed_123": b'{"added":["a.parquet"],"removed":[]}'},
        "",
        [10],
        ["a.parquet"],
    ),
    "marker/_started_123": ({"a.parquet": 10, "_started_123": b""}, "", [10], ["a.parquet"]),
    "marker/.z.parquet.crc": (
        {"a.parquet": 10, ".z.parquet.crc": b"crc-bytes-not-parquet"},
        "",
        [10],
        ["a.parquet"],
    ),
    "marker/_metadata": ({"a.parquet": 10, "_metadata": "meta"}, "", [10], None),
    "marker/_common_metadata": ({"a.parquet": 10, "_common_metadata": "meta"}, "", [10], None),
    "marker/README.txt": ({"a.parquet": 10, "README.txt": b"a text file\n"}, "", _REFUSED, None),
    "marker/c.parquet-empty": ({"a.parquet": 10, "c.parquet": b""}, "", [10], None),
    "marker/c-empty": ({"a.parquet": 10, "c": b""}, "", [10], None),
    "marker/spark-writer-dir": (
        {
            _SPARK_PART: 10,
            _SPARK_PART_2: 11,
            "_SUCCESS": b"",
            "._SUCCESS.crc": b"crc",
            f".{_SPARK_PART}.crc": b"crc",
            f".{_SPARK_PART_2}.crc": b"crc",
            "_committed_9": b"{}",
            "_started_9": b"",
        },
        "",
        [10, 11],
        [_SPARK_PART, _SPARK_PART_2],
    ),
    "layout/nested-plain": ({"sub1/a.parquet": 10, "sub2/b": 11}, "", _REFUSED, None),
    "layout/nested-mixed": ({"a.parquet": 10, "sub/b.parquet": 11}, "", [10], ["a.parquet"]),
    "layout/partitioned-card": (
        {
            "p=1/part-00000": 1,
            "p=2/part-00000.parquet": 2,
            "p=3/part-00000.snappy.parquet": 3,
            "p=4/data.bin": 4,
        },
        "",
        [1, 2, 3, 4],
        [
            "p=1/part-00000",
            "p=2/part-00000.parquet",
            "p=3/part-00000.snappy.parquet",
            "p=4/data.bin",
        ],
    ),
    "layout/partitioned-markers": (
        {
            "p=1/a.parquet": 1,
            "p=1/b": 2,
            "p=1/_SUCCESS": b"",
            "p=1/.z.parquet.crc": b"crc",
            "_SUCCESS": b"",
        },
        "",
        [1, 2],
        ["p=1/a.parquet", "p=1/b"],
    ),
    "layout/hidden-dirs": (
        {"a.parquet": 10, "_temporary/0/x.parquet": 11, ".hidden/y.parquet": 12, "_tmp/z": 13},
        "",
        [10],
        ["a.parquet"],
    ),
    "glob/star-flat": (
        {"a.parquet": 10, "b": 11, "_SUCCESS": b"", ".z.crc": b"x"},
        "*",
        [10, 11],
        ["a.parquet", "b"],
    ),
    "glob/star-parquet": (
        {"a.parquet": 10, "b": 11, "c.PARQUET": 12},
        "*.parquet",
        [10],
        ["a.parquet"],
    ),
    "glob/star-hidden-explicit": ({"a.parquet": 10, "_b.parquet": 11}, "_*", _REFUSED, None),
    "single/no-ext": ({"a.parquet": 10, "b": 11}, "b", [11], ["b"]),
    "single/bin": ({"b.bin": 11}, "b.bin", [11], ["b.bin"]),
    "single/upper": ({"b.PARQUET": 11}, "b.PARQUET", [11], ["b.PARQUET"]),
    "single/hidden-underscore": ({"_b.parquet": 11}, "_b.parquet", _REFUSED, None),
    "single/hidden-dot": ({".b.parquet": 11}, ".b.parquet", _REFUSED, None),
    "single/copying": ({"b.parquet._COPYING_": 11}, "b.parquet._COPYING_", _REFUSED, None),
    "root/underscore-dir": (
        {"_root/a.parquet": 10, "_root/b": 11},
        "_root",
        [10, 11],
        ["_root/a.parquet", "_root/b"],
    ),
    "root/dot-dir": (
        {".root/a.parquet": 10, ".root/b": 11},
        ".root",
        [10, 11],
        [".root/a.parquet", ".root/b"],
    ),
}

_NEIGHBOURS: dict[str, tuple[str, dict[str, bytes], list[str]]] = {
    "csv/markers": (
        "csv",
        {"a.csv": b"10\n", "_SUCCESS": b"", ".z.csv.crc": b"crc", "_committed_1": b"{}"},
        ["10"],
    ),
    "csv/copying": ("csv", {"a.csv": b"10\n", "b.csv._COPYING_": b"11\n"}, ["10"]),
    "json/markers": (
        "json",
        {"a.json": b'{"id": 10}\n', "_SUCCESS": b"", ".z.json.crc": b"crc", "_committed_1": b"{}"},
        ["10"],
    ),
    "json/copying": (
        "json",
        {"a.json": b'{"id": 10}\n', "b.json._COPYING_": b'{"id": 11}\n'},
        ["10"],
    ),
    "text/hidden": ("text", {"a.txt": b"10\n", "_b.txt": b"11\n", ".b.txt": b"12\n"}, ["10"]),
    "text/no-ext": ("text", {"a.txt": b"10\n", "b": b"11\n", "b.TXT": b"12\n"}, ["10", "11", "12"]),
}


def _write_parquet(path: Path, ident: int) -> None:
    pq.write_table(pa.table({"id": pa.array([ident], pa.int64())}), str(path))


def _build(root: Path, files: dict[str, Any]) -> None:
    for rel, spec in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(spec, int):
            _write_parquet(path, spec)
        elif spec == "meta":
            pq.write_metadata(pa.schema([("id", pa.int64())]), str(path))
        else:
            path.write_bytes(spec)


def _relative(files: list[str], root: Path) -> list[str]:
    base = str(root.resolve())
    out = []
    for item in files:
        text = item.removeprefix("file://").replace("%3D", "=")
        out.append(text.removeprefix(base).lstrip("/"))
    return sorted(out)


def _read(spark: ReparkSession, door: str, target: str) -> Any:
    if door == "reader":
        return spark.read.parquet(target)
    if door == "format_load":
        return spark.read.format("parquet").load(target)
    if door == "default_load":
        return spark.read.load(target)
    if door == "sql":
        return spark.sql(f"SELECT * FROM parquet.`{target}`")
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW fx_view USING parquet OPTIONS (path '{target}')")
    return spark.table("fx_view")


def _answer(frame: Any) -> list[int]:
    return sorted(row["id"] for row in frame.collect())


@pytest.mark.parametrize("ansi", _ANSI)
@pytest.mark.parametrize("door", _DOORS)
@pytest.mark.parametrize("cell", sorted(_CELLS))
def test_a_parquet_path_read_lists_what_spark_lists(
    spark: ReparkSession, tmp_path: Path, cell: str, door: str, ansi: str
) -> None:
    files, target, rows, listed = _CELLS[cell]
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    _build(tmp_path, files)
    path = str(tmp_path / target) if target else str(tmp_path)
    if rows is _REFUSED:
        with pytest.raises(PySparkException):
            _answer(_read(spark, door, path))
        return
    frame = _read(spark, door, path)
    assert _answer(frame) == rows
    assert frame.count() == len(rows)
    if cell not in _PARTITION_COLUMN_CELLS:
        assert frame.schema.simpleString() == "struct<id:bigint>"
    if listed is not None:
        assert _relative(frame.inputFiles(), tmp_path) == listed


@pytest.mark.parametrize("cell", sorted(_CELLS))
def test_the_native_door_lists_what_spark_lists(tmp_path: Path, cell: str) -> None:
    files, target, rows, listed = _CELLS[cell]
    _build(tmp_path, files)
    path = str(tmp_path / target) if target else str(tmp_path)
    session = PyReparkSession.native()
    if rows is _REFUSED:
        with pytest.raises(PySparkException):
            frame = session.read_parquet(path)
            session.create_or_replace_temp_view("fx_native", frame)
            pa.table(session.sql("SELECT * FROM fx_native"))
        return
    frame = session.read_parquet(path)
    session.create_or_replace_temp_view("fx_native", frame)
    table = pa.table(session.sql("SELECT id FROM fx_native"))
    assert sorted(table.column("id").to_pylist()) == rows
    if listed is not None:
        assert _relative(list(_native.input_files(frame)), tmp_path) == listed


@pytest.mark.parametrize("ansi", _ANSI)
@pytest.mark.parametrize("door", _UNWIRED_DOORS)
@pytest.mark.parametrize("cell", ["name/b", "marker/spark-writer-dir", "single/no-ext"])
def test_the_unwired_parquet_doors_stay_refused(
    spark: ReparkSession, tmp_path: Path, cell: str, door: str, ansi: str
) -> None:
    files, target, _rows, _listed = _CELLS[cell]
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    _build(tmp_path, files)
    path = str(tmp_path / target) if target else str(tmp_path)
    with pytest.raises(PySparkException):
        _answer(_read(spark, door, path))


@pytest.mark.parametrize("ansi", _ANSI)
@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_a_write_beside_a_file_without_the_suffix_reads_back_as_spark(
    spark: ReparkSession, tmp_path: Path, mode: str, ansi: str
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    _build(tmp_path, {"a.parquet": 10, "b": 11})
    spark.createDataFrame([(20,)], "id bigint").write.mode(mode).parquet(str(tmp_path))
    expected = [10, 11, 20] if mode == "append" else [20]
    assert _answer(spark.read.parquet(str(tmp_path))) == expected


@pytest.mark.parametrize("ansi", _ANSI)
@pytest.mark.parametrize("cell", sorted(_NEIGHBOURS))
def test_the_neighbour_formats_keep_their_spark_answers(
    spark: ReparkSession, tmp_path: Path, cell: str, ansi: str
) -> None:
    fmt, files, values = _NEIGHBOURS[cell]
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    _build(tmp_path, files)
    frame = getattr(spark.read, fmt)(str(tmp_path))
    got = sorted(str(row[0]) for row in frame.collect())
    assert got == values
