from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _csv_json_partition_layouts as layouts
import pytest

from repark.spark import ReparkSession

RECORDED = json.loads(
    (Path(__file__).parent / "csv_json_partition_discovery_1_cases.json").read_text(
        encoding="utf-8"
    )
)
TARGET = [entry for entry in RECORDED if entry["case"]["kind"] == "target"]
PROTECTED = [entry for entry in RECORDED if entry["case"]["kind"] == "protected"]


@pytest.fixture(scope="module")
def tables(tmp_path_factory: pytest.TempPathFactory) -> Path:
    base = tmp_path_factory.mktemp("csv_json_partition_discovery_1")
    layouts.build(base)
    return base


def _check(spark: ReparkSession, tables: Path, tmp_path: Path, entry: dict[str, Any]) -> None:
    observed = layouts.run(spark, tables, tmp_path, entry["case"])
    assert observed == entry["expected"]


@pytest.mark.parametrize("entry", TARGET, ids=[entry["case"]["id"] for entry in TARGET])
def test_partitioned_csv_and_json_reads_answer_as_spark(
    spark: ReparkSession, tables: Path, tmp_path: Path, entry: dict[str, Any]
) -> None:
    _check(spark, tables, tmp_path, entry)


@pytest.mark.parametrize("entry", PROTECTED, ids=[entry["case"]["id"] for entry in PROTECTED])
def test_unpartitioned_csv_and_json_reads_keep_their_answer(
    spark: ReparkSession, tables: Path, tmp_path: Path, entry: dict[str, Any]
) -> None:
    _check(spark, tables, tmp_path, entry)


def test_recorded_cases_cover_every_reader_and_class() -> None:
    seen = {(entry["case"]["fmt"], entry["case"]["kind"]) for entry in RECORDED}
    assert seen == {(fmt, kind) for fmt in layouts.FORMATS for kind in ("target", "protected")}
    assert len(TARGET) > len(PROTECTED) > 0
