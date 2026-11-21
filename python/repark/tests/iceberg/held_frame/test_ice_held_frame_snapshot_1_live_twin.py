from __future__ import annotations

import importlib.util
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _acceptance import ICEBERG_TABLE_PROPERTIES

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("ice_held_frame_live_twin")
HELPER = Path(__file__).resolve().parents[2] / "parity" / "_acceptance_held_frame.py"
SPEC = importlib.util.spec_from_file_location("_acceptance_held_frame_twin", HELPER)
assert SPEC is not None and SPEC.loader is not None
live = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(live)
MODES = ["true", "false"]
HADOOP_ALIAS = (
    "ICE-CATALOG-SESSION-HADOOP-1: type=hadoop is the in-process memory catalog, so a second "
    "session cannot see the table and no external commit reaches the reader (domain table "
    "section 6); the cell runs live on Glue and S3 Tables"
)
NOT_FOUND = re.compile(
    r"ERROR AnalysisException: Error during planning: table 'lc\.db\.held_(control|refresh)_\w{8}'"
    r" not found"
)
SPARK_PAUSED = [
    ("refresh", [0.132, 0.0], [2, 2, 3, 3]),
    ("refresh", [17.891, 0.0], [2, 2, 3, 3]),
    ("refresh", [35.11, 0.0], [3, 3, 3, 3]),
    ("refresh", [0.124, 35.0], [2, 3, 3, 3]),
    ("refresh", [20.117, 20.0], [2, 2, 3, 3]),
    ("control", [0.0, 0.146, 0.0, 0.0], [2, 2, live.SEED, 2, 4, 4, 4, 4]),
    ("control", [0.0, 17.958, 0.0, 0.0], [2, 2, live.SEED, 2, 4, 4, 4, 4]),
    ("control", [0.0, 35.103, 0.0, 0.0], [2, 3, live.GROWN, 3, 4, 4, 4, 4]),
    ("control", [0.0, 0.118, 35.0, 0.0], [2, 2, live.GROWN, 3, 4, 4, 4, 4]),
    ("control", [0.0, 0.115, 0.0, 35.0], [2, 2, live.SEED, 3, 4, 4, 4, 4]),
    ("control", [0.0, 20.123, 20.0, 0.0], [2, 2, live.SEED, 2, 4, 4, 4, 4]),
]


def _session(root: Path, ansi: str) -> ReparkSession:
    return (
        ReparkSession.builder.appName("ice-held-frame-live-twin")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "lc"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    found: dict[str, Any] = {}
    for ansi in MODES:
        session = _session(tmp_path_factory.mktemp(f"live-twin-{ansi}"), ansi)
        session.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
        other = session.newSession()
        cells = live.HeldFrameCells(
            session, "lc", "db", "", ICEBERG_TABLE_PROPERTIES, live.session_writer(other)
        )
        found[ansi] = {"rows": cells.rows()}
        found[ansi].update({label: cells.external(label) for label in live.SPARK_EXTERNAL})
        if ansi == MODES[0]:
            found["cost"] = [cells.cost_held_read(), cells.cost_held_filter_count()]
        other.stop()
        session.stop()
        _reset_active_session_for_tests()
    yield found


@pytest.mark.parametrize("ansi", MODES)
@pytest.mark.parametrize("label", sorted(live.SPARK_ROWS))
def test_a_held_frame_answers_as_spark_after_a_same_session_write(
    measured: dict[str, Any], ansi: str, label: str
) -> None:
    found = measured[ansi]["rows"][label]
    assert found == live.SPARK_ROWS[label], (ansi, label, found)


@pytest.mark.xfail(strict=True, raises=AssertionError, reason=HADOOP_ALIAS)
@pytest.mark.parametrize("ansi", MODES)
@pytest.mark.parametrize("label", sorted(live.SPARK_EXTERNAL))
def test_a_held_frame_answers_as_spark_after_an_external_commit(
    measured: dict[str, Any], ansi: str, label: str
) -> None:
    found = measured[ansi][label]
    assert found["answer"] == live.spark_answer(label, found["idle_s"]), (ansi, label, found)


@pytest.mark.parametrize("ansi", MODES)
@pytest.mark.parametrize(("label", "before"), [("control", [2]), ("refresh", [])])
def test_the_local_catalog_refuses_the_second_writer_by_name(
    measured: dict[str, Any], ansi: str, label: str, before: list[Any]
) -> None:
    found = measured[ansi][label]["answer"]
    assert found[:-1] == before, (ansi, label, found)
    assert NOT_FOUND.fullmatch(found[-1]), (ansi, label, found)


@pytest.mark.parametrize(("label", "idle_s", "spark"), SPARK_PAUSED)
def test_the_pinned_spark_answer_follows_the_measured_cache_interval(
    label: str, idle_s: list[float], spark: list[Any]
) -> None:
    assert live.spark_answer(label, idle_s) == spark


def test_with_no_idle_gap_the_pinned_spark_answer_is_the_cached_one() -> None:
    for label, known in live.SPARK_EXTERNAL.items():
        assert live.spark_answer(label, [0.0] * 4) == known["stale"], label


def test_each_cost_row_prints_one_parseable_line(measured: dict[str, Any]) -> None:
    lines = [live.cost_line("local", row) for row in measured["cost"]]
    parsed = [live.parse_cost_line(line) for line in lines]
    assert [row["label"] for row in parsed] == ["held_read", "held_filter_count"], lines
    assert all(row["catalog"] == "local" for row in parsed), lines
    assert all(int(row["n"]) >= live.COST_CALLS for row in parsed), lines
    for line in lines:
        print(line)
