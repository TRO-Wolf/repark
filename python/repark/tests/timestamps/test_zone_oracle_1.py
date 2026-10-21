"""ZONE-ORACLE-1 — an expression-level Spark oracle for timestamps and zones.

Replays the corpus of ``_zone_oracle_1_corpus.py`` against the recorded Spark 4.1.2 answers of
``zone_oracle_1_spark_oracle.json``. Every cell is a plain ``SELECT`` run through
``ReparkSession.sql`` in a session set to the cell's zone. A cell is not skipped and not
xfailed: the cells that are not ``EQUAL`` on main are the dated list
``zone_oracle_1_main_difference.json``, recorded with the measured RePark answer per cell. The
set of non-``EQUAL`` cells must be exactly that list, so a cell that leaves it (a fix) or joins
it (a regression) fails the test, and a listed cell must still answer as recorded.

The live tier re-records twenty cells chosen by hash in a fresh interpreter, where the recorder
owns a ``local[1]`` SparkContext, and compares them with the fixture.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _record_zone_oracle_1 import (
    DIFFERENT,
    EQUAL,
    ERROR,
    MAIN_FILE,
    ORACLE_FILE,
    REFUSED,
    STATUSES,
    classify_all,
    replay,
)
from _zone_oracle_1_corpus import (
    SAMPLE_SIZE,
    ZONE_TAGS,
    ZONES,
    Cell,
    cells,
    corpus_hash,
    sample_ids,
)

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live Spark re-derivation is skipped (CI is JVM-free)"
RECORDER = Path(__file__).with_name("_record_zone_oracle_1.py")
RECORDER_TIMEOUT_SECONDS = 1800
ORACLE: dict[str, Any] = json.loads(ORACLE_FILE.read_text(encoding="utf-8"))
MAIN: dict[str, Any] = json.loads(MAIN_FILE.read_text(encoding="utf-8"))
CELLS: list[Cell] = cells()
LISTED: dict[str, dict[str, Any]] = MAIN["cells"]


@pytest.fixture(scope="module")
def measured() -> Iterator[dict[str, dict[str, Any]]]:
    """Replay every cell once on one RePark session and return the measured answers."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-zone-oracle-1").getOrCreate()
    answers = replay(session, CELLS)
    yield answers
    session.stop()
    _reset_active_session_for_tests()


@pytest.fixture(scope="module")
def statuses(measured: dict[str, dict[str, Any]]) -> dict[str, str]:
    """Classify every measured cell against the Spark fixture."""
    return classify_all(ORACLE["cells"], measured)


def _same_measurement(recorded: dict[str, Any], answer: dict[str, Any]) -> bool:
    """Return whether a listed cell still answers as recorded: the value, or an error."""
    if "error" in recorded:
        return "error" in answer
    return answer.get("type") == recorded["type"] and answer.get("value") == recorded["value"]


def test_corpus_has_unique_stable_ids() -> None:
    """The generator yields unique cell ids, in every zone, and a fixed hash."""
    ids = [cell.cell_id for cell in CELLS]
    assert len(set(ids)) == len(ids)
    assert {cell.zone for cell in CELLS} == set(ZONES)
    assert all(cell.cell_id.endswith("." + ZONE_TAGS[cell.zone]) for cell in CELLS)
    assert cells() == CELLS
    assert corpus_hash() == corpus_hash()


def test_spark_fixture_provenance_covers_every_cell() -> None:
    """The Spark fixture carries its provenance, the corpus hash and every cell's statement."""
    provenance = ORACLE["provenance"]
    assert provenance["spark"] == "4.1.2"
    assert provenance["master"] == "local[1]"
    assert provenance["ansi"] == "true"
    assert provenance["generator_sha256"] == corpus_hash()
    assert provenance["cells"] == len(CELLS) == len(ORACLE["cells"])
    assert list(ORACLE["cells"]) == [cell.cell_id for cell in CELLS]
    for cell in CELLS:
        recorded = ORACLE["cells"][cell.cell_id]
        assert recorded["sql"] == cell.sql, cell.cell_id
        assert recorded["zone"] == cell.zone, cell.cell_id
        assert ("error" in recorded) != ("value" in recorded), cell.cell_id


def test_main_difference_list_is_consistent() -> None:
    """The dated list names corpus cells, carries the counts, and holds no EQUAL cell."""
    provenance = MAIN["provenance"]
    assert provenance["generator_sha256"] == corpus_hash()
    assert provenance["cells"] == len(CELLS)
    assert set(LISTED) <= {cell.cell_id for cell in CELLS}
    assert {entry["status"] for entry in LISTED.values()} <= {DIFFERENT, REFUSED, ERROR}
    tally = Counter(entry["status"] for entry in LISTED.values())
    counts = provenance["counts"]
    assert {status: counts[status] for status in (DIFFERENT, REFUSED, ERROR)} == {
        status: tally.get(status, 0) for status in (DIFFERENT, REFUSED, ERROR)
    }
    assert sum(counts[status] for status in STATUSES) == len(CELLS)


def test_live_sample_is_twenty_distinct_corpus_cells() -> None:
    """The live tier re-records twenty cells chosen by hash, the same twenty on every run."""
    chosen = sample_ids()
    assert len(chosen) == len(set(chosen)) == SAMPLE_SIZE == 20
    assert chosen == sample_ids()
    assert set(chosen) <= {cell.cell_id for cell in CELLS}


def test_replay_answers_every_cell(measured: dict[str, dict[str, Any]]) -> None:
    """Every cell has one measured answer: a type and a value, or an error text."""
    assert list(measured) == [cell.cell_id for cell in CELLS]
    for key, answer in measured.items():
        assert ("error" in answer) != ("value" in answer), key


@pytest.mark.parametrize("zone", ZONES)
def test_cells_off_the_list_equal_spark(
    statuses: dict[str, str], measured: dict[str, dict[str, Any]], zone: str
) -> None:
    """A cell that is not on the dated list answers Spark's type and value, or its error."""
    regressed = [
        (cell.cell_id, statuses[cell.cell_id], measured[cell.cell_id])
        for cell in CELLS
        if cell.zone == zone and cell.cell_id not in LISTED and statuses[cell.cell_id] != EQUAL
    ]
    assert regressed == []


@pytest.mark.parametrize("zone", ZONES)
def test_cells_on_the_list_still_differ_as_recorded(
    statuses: dict[str, str], measured: dict[str, dict[str, Any]], zone: str
) -> None:
    """A listed cell keeps its status and its measured RePark answer; a fix removes it."""
    drifted = [
        (cell.cell_id, statuses[cell.cell_id], measured[cell.cell_id])
        for cell in CELLS
        if cell.zone == zone
        and cell.cell_id in LISTED
        and (
            statuses[cell.cell_id] != LISTED[cell.cell_id]["status"]
            or not _same_measurement(LISTED[cell.cell_id], measured[cell.cell_id])
        )
    ]
    assert drifted == []


def test_difference_set_is_exactly_the_recorded_list(statuses: dict[str, str]) -> None:
    """The non-EQUAL cells are exactly the dated list: none fixed, none newly broken."""
    now = {key for key, status in statuses.items() if status != EQUAL}
    assert sorted(now - set(LISTED)) == []
    assert sorted(set(LISTED) - now) == []


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_live_oracle_samples_reproduce() -> None:
    """Twenty hash-chosen cells re-recorded on live Spark equal the fixture, in a fresh interpreter.

    The serial live suite shares one SparkContext, and an earlier module builds it on ``local[2]``;
    the recorder runs as a script so it owns a ``local[1]`` context, and it raises on any other.
    """
    completed = subprocess.run(
        [sys.executable, str(RECORDER), "--check", "--sample", str(SAMPLE_SIZE)],
        capture_output=True,
        text=True,
        timeout=RECORDER_TIMEOUT_SECONDS,
        check=False,
    )
    assert completed.returncode == 0, (
        f"recorder --check exited {completed.returncode}\n"
        f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr[-4000:]}"
    )
    assert f"checked={SAMPLE_SIZE} drift=0" in completed.stdout
