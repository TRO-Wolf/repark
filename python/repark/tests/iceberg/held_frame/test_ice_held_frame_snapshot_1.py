from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _ice_held_frame_snapshot_1_cells import Cells

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("ice_held_frame_snapshot_1")
ORACLE = json.loads(
    Path(__file__).with_name("ice_held_frame_snapshot_1_spark_oracle.json").read_text()
)
KEYS = [(ansi, key) for ansi in sorted(ORACLE) for key in sorted(ORACLE[ansi])]


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    found = {}
    for ansi in sorted(ORACLE):
        root = tmp_path_factory.mktemp("held-" + ansi)
        session = (
            ReparkSession.builder.appName("ice-held-frame-snapshot-1")
            .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lc.type", "hadoop")
            .config("spark.sql.catalog.lc.warehouse", str(root / "lc"))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", ansi)
            .getOrCreate()
        )
        session.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
        found[ansi] = Cells(session).run()
        session.stop()
        _reset_active_session_for_tests()
    yield found


@pytest.mark.parametrize(("ansi", "key"), KEYS)
def test_a_held_iceberg_frame_answers_as_spark(
    measured: dict[str, Any], ansi: str, key: str
) -> None:
    assert measured[ansi][key] == ORACLE[ansi][key], (ansi, key, measured[ansi][key])
