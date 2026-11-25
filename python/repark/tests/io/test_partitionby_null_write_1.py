from __future__ import annotations

import datetime
import gzip
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

ORACLE = Path(__file__).with_name("partitionby_null_write_1_spark.json")
CELLS: list[dict[str, Any]] = json.loads(ORACLE.read_text(encoding="utf-8"))


def _fields(schema: str) -> list[tuple[str, str]]:
    return [tuple(part.strip().split(" ", 1)) for part in schema.split(",")]


def _frame(spark: ReparkSession, cell: dict[str, Any], rows: list[list[Any]]) -> Any:
    fields = _fields(cell["schema"])
    if cell["fmt"] == "text":
        fields = [(name, "string" if name == "id" else kind) for name, kind in fields]
    shaped = []
    for row in rows:
        values = []
        for (name, kind), value in zip(fields, row, strict=True):
            if value is not None and kind == "date":
                value = datetime.date.fromisoformat(value)
            elif value is not None and name == "id" and kind == "string":
                value = str(value)
            values.append(value)
        shaped.append(tuple(values))
    return spark.createDataFrame(shaped, ", ".join(f"{name} {kind}" for name, kind in fields))


def _write(frame: Any, cell: dict[str, Any], target: Path, mode: str) -> None:
    if cell["door"] == "save":
        frame.write.format(cell["fmt"]).partitionBy(*cell["parts"]).mode(mode).save(str(target))
        return
    getattr(frame.write.partitionBy(*cell["parts"]).mode(mode), cell["fmt"])(str(target))


def _ids(path: Path, fmt: str) -> list[str]:
    if fmt == "parquet":
        return [str(row["id"]) for row in pq.read_table(str(path)).to_pylist()]
    raw = path.read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    lines = [line for line in raw.decode("utf-8").splitlines() if line]
    if fmt == "json":
        return [str(json.loads(line)["id"]) for line in lines]
    if fmt == "csv":
        return [line for line in lines if line.lstrip("-").isdigit()]
    return lines


def _landing(root: Path, fmt: str) -> dict[str, list[str]]:
    landed: dict[str, list[str]] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name.startswith((".", "_")) or path.name.endswith(".crc"):
            continue
        landed.setdefault(str(path.parent.relative_to(root)), []).extend(_ids(path, fmt))
    return {directory: sorted(ids) for directory, ids in sorted(landed.items())}


@pytest.mark.parametrize("cell", CELLS, ids=[cell["key"] for cell in CELLS])
def test_partitioned_path_write_lands_rows_in_spark_directories(
    spark: ReparkSession, tmp_path: Path, cell: dict[str, Any]
) -> None:
    target = tmp_path / "out"
    if cell["pre"] is not None:
        _write(_frame(spark, cell, cell["pre"]), cell, target, "errorifexists")
    frame = _frame(spark, cell, cell["rows"])
    if "refuses" in cell:
        with pytest.raises(AnalysisException, match=cell["refuses"]):
            _write(frame, cell, target, cell["mode"])
        return
    _write(frame, cell, target, cell["mode"])
    assert _landing(target, cell["fmt"]) == cell["dirs"]


def test_the_headline_null_int_partition_lands_in_the_hive_default_directory(
    spark: ReparkSession, tmp_path: Path
) -> None:
    target = tmp_path / "headline"
    frame = spark.createDataFrame([(1, None), (2, 5)], "id int, p int")
    frame.write.partitionBy("p").parquet(str(target))
    names = sorted(path.name for path in target.iterdir() if path.is_dir())
    assert names == ["p=5", "p=__HIVE_DEFAULT_PARTITION__"]
    assert pq.read_table(str(target / "p=__HIVE_DEFAULT_PARTITION__")).column_names == ["id"]
