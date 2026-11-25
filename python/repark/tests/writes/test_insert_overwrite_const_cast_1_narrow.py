from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _insert_overwrite_const_cast_1_narrow_cells import build_cells, run_cell, setup

from repark.spark.sql import SparkSession, functions

FIXTURE = Path(__file__).with_name("insert_overwrite_const_cast_1_narrow_spark_oracle.json")
ORACLE: dict[str, dict[str, Any]] = json.loads(FIXTURE.read_text(encoding="utf-8"))["cells"]
CELLS = build_cells()
NAMESPACE = "ice.ioc"

pytestmark = pytest.mark.xdist_group("insert_overwrite_const_cast_1_narrow")


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    warehouse = tmp_path_factory.mktemp("ioc_narrow")
    session = (
        SparkSession.builder.appName("insert-overwrite-const-cast-1-narrow")
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    setup(session, NAMESPACE)
    yield session
    session.conf.set("spark.sql.ansi.enabled", "true")


def test_fixture_covers_every_cell() -> None:
    assert set(ORACLE) == set(CELLS)


@pytest.mark.parametrize("key", sorted(CELLS))
def test_overwrite_answers_as_recorded(spark: Any, key: str) -> None:
    recorded = ORACLE[key]
    expected = recorded[recorded["expect"]]
    actual = run_cell(spark, functions, NAMESPACE, key, CELLS[key])
    assert (actual["ok"], actual["cls"]) == (expected["ok"], expected["cls"]), actual
    assert actual["rows"] == expected["rows"]
