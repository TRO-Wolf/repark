from __future__ import annotations

import importlib.util
import json
import sys
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
DOUBLE = Path(__file__).with_name("_held_frame_writer_double.py")
MODES = ["true", "false"]
LEGS = ("repark", "spark")
PINNED = (2, 2, 3)
FABLE_SPARK: dict[str, dict[str, tuple[int, int, int]]] = {
    "cache_off": {
        **dict.fromkeys(live.TABLE_SHAPES, PINNED),
        **dict.fromkeys(live.QUERY_SHAPES, (3, 3, 3)),
    },
    "inside_30_s": {
        **dict.fromkeys(live.TABLE_SHAPES, PINNED),
        **dict.fromkeys(live.QUERY_SHAPES, (2, 3, 3)),
    },
    "after_35_s": {
        **dict.fromkeys(live.TABLE_SHAPES, PINNED),
        **dict.fromkeys(live.QUERY_SHAPES, (3, 3, 3)),
    },
}
_HELD_TWO = {"held": 2, "fresh": 3, "new": 3, "held_again": 2}
_UNPERSISTED = {"held": 2, "fresh": 3, "new": 3, "held_again": 3}
_ALL_THREE = dict.fromkeys(live.READS, 3)
_DROPPED_AT_THE_NEW_FRAME = {
    "after_writer": _UNPERSISTED,
    "after_props": _ALL_THREE,
    "after_refresh": _ALL_THREE,
}
ADVISER_PREDICTION = {shape: dict.fromkeys(live.STEPS, _HELD_TWO) for shape in live.SHAPES}
CODE_READ_PREDICTION = {
    **ADVISER_PREDICTION,
    "bare": _DROPPED_AT_THE_NEW_FRAME,
    "df_view": _DROPPED_AT_THE_NEW_FRAME,
}
PREDICTIONS = {"adviser": ADVISER_PREDICTION, "code_read": CODE_READ_PREDICTION}
CODE_READ_ONLY = {
    ("bare", "after_writer", "held_again"),
    ("bare", "after_props", "held"),
    ("bare", "after_props", "held_again"),
}
_HELD_AFTER_COMMIT = [(step, read) for step in live.STEPS[1:] for read in ("held", "held_again")]
MAIN_BREAKS = {("alias", "after_refresh", read) for read in ("held", "held_again")} | {
    (shape, *read)
    for shape in ("bare", *live.QUERY_SHAPES)
    for read in [("after_writer", "held_again"), *_HELD_AFTER_COMMIT]
}
_TABLE_LEVEL_HEAD = {
    "after_writer": _HELD_TWO,
    "after_props": _HELD_TWO,
    "after_refresh": _ALL_THREE,
}
HEAD_PREDICTION = {
    **dict.fromkeys(live.SHAPES, _DROPPED_AT_THE_NEW_FRAME),
    "bare": _TABLE_LEVEL_HEAD,
    "alias": _TABLE_LEVEL_HEAD,
}


class _UnseenWriter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def __call__(self, table: str, values: str) -> dict[str, Any]:
        self.calls.append((table, values))
        return {"count": 3}


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


def _found(steps: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {"before": 2, "writer": {"count": 3}, "set_props": None, "refresh": None, **steps}


def _measured(after: tuple[int, int, int]) -> dict[str, Any]:
    reads = zip(live.STEPS, after, strict=True)
    return _found({step: dict.fromkeys(live.READS, value) for step, value in reads})


def _broken(prediction: dict[str, dict[str, dict[str, int]]]) -> set[tuple[str, ...]]:
    broken: set[tuple[str, ...]] = set()
    for shape in live.SHAPES:
        found = _found(prediction[shape])
        failed = live.grade(shape, found, "repark", pending=frozenset())["failed"]
        broken |= {(shape, *name.split(".")) for name in failed}
    return broken


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    found: dict[str, Any] = {}
    for ansi in MODES:
        session = _session(tmp_path_factory.mktemp(f"live-twin-{ansi}"), ansi)
        session.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
        writer = _UnseenWriter()
        cells = live.HeldFrameCells(session, "lc", "db", "", ICEBERG_TABLE_PROPERTIES, writer)
        found[ansi] = {"rows": cells.rows()}
        found[ansi]["shapes"] = {shape: cells.shape(shape) for shape in live.SHAPES}
        found[ansi]["writes"] = writer.calls
        if ansi == MODES[0]:
            found["cost"] = [cells.cost_held_read(), cells.cost_held_filter_count()]
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


@pytest.mark.parametrize("ansi", MODES)
@pytest.mark.parametrize("shape", live.SHAPES)
def test_every_shape_runs_on_repark_and_a_writer_it_cannot_see_fails_the_validity_read(
    measured: dict[str, Any], ansi: str, shape: str
) -> None:
    found = measured[ansi]["shapes"][shape]
    reads = [found[step][read] for step in live.STEPS for read in live.READS]
    assert reads == [2] * len(reads), (ansi, shape, found)
    assert found["before"] == 2 and found["writer"] == {"count": 3}, (ansi, shape, found)
    assert found["set_props"] is None and found["refresh"] is None, (ansi, shape, found)
    assert "error" not in found and len(found["idle_s"]) == len(reads), (ansi, shape, found)
    for leg in LEGS:
        graded = live.grade(shape, found, leg)
        assert "validity.fresh_after_refresh" in graded["failed"], (ansi, shape, leg, graded)
        line = live.shape_line("local", leg, shape, found, graded)
        assert json.loads(line.split(" ", 4)[4])["answer"] == found, line


def test_the_other_writer_inserts_one_row_into_each_shape_table(measured: dict[str, Any]) -> None:
    for ansi in MODES:
        writes = measured[ansi]["writes"]
        assert [values for _, values in writes] == [live.EXTERNAL_ROW] * len(live.SHAPES), writes
        assert len({table for table, _ in writes}) == len(live.SHAPES), writes


@pytest.mark.parametrize("window", sorted(FABLE_SPARK))
@pytest.mark.parametrize("shape", live.SHAPES)
def test_spark_measured_answers_pass_both_legs_in_every_window(window: str, shape: str) -> None:
    found = _measured(FABLE_SPARK[window][shape])
    assert live.grade(shape, found, "spark")["failed"] == {}, (window, shape)
    assert live.grade(shape, found, "repark", pending=frozenset())["failed"] == {}, (window, shape)


@pytest.mark.parametrize("shape", live.SHAPES)
def test_the_head_prediction_passes_the_repark_leg_with_no_pending_read(shape: str) -> None:
    found = _found(HEAD_PREDICTION[shape])
    assert live.grade(shape, found, "repark")["failed"] == {}, shape
    assert live.grade(shape, found, "repark")[live.PENDING] == {}, shape


def test_the_pending_list_is_empty_and_main_breaks_the_rule_where_it_was() -> None:
    adviser = _broken(ADVISER_PREDICTION)
    code_read = _broken(CODE_READ_PREDICTION)
    assert not live.PENDING_1394
    assert adviser | code_read == MAIN_BREAKS
    assert len(MAIN_BREAKS) == 27
    assert (adviser | code_read) - adviser == CODE_READ_ONLY
    assert _broken(HEAD_PREDICTION) == set()


def test_a_reader_that_never_sees_the_writer_fails_on_both_legs() -> None:
    for shape in live.SHAPES:
        for leg in LEGS:
            failed = live.grade(shape, _measured((2, 2, 2)), leg)["failed"]
            assert "validity.fresh_after_refresh" in failed, (shape, leg, failed)


@pytest.mark.parametrize(
    ("field", "refused"),
    [
        ("writer", {"error": "ERROR TABLE_OR_VIEW_NOT_FOUND"}),
        ("set_props", "ERROR CatalogCommitConflicts"),
        ("refresh", "ERROR TABLE_OR_VIEW_NOT_FOUND"),
        ("error", "ERROR AnalysisException: cache"),
    ],
)
def test_a_refused_write_commit_or_step_fails_the_cell(field: str, refused: Any) -> None:
    for shape in live.SHAPES:
        for leg in LEGS:
            found = {**_measured(PINNED), field: refused}
            assert field in live.grade(shape, found, leg)["failed"], (shape, leg)


def test_the_cache_table_reads_are_open_on_the_repark_leg_and_pinned_on_the_spark_leg() -> None:
    found = _measured((3, 3, 3))
    repark = live.grade("cache_table", found, "repark")
    assert repark["failed"] == {} and len(repark[live.OPEN]) == len(live.STEPS) * len(live.READS)
    spark = live.grade("cache_table", found, "spark")
    before_refresh = {f"{step}.{read}" for step in live.STEPS[:2] for read in live.READS}
    assert set(spark["failed"]) == before_refresh


def test_a_table_level_fresh_read_is_open_on_the_repark_leg_and_asserted_on_the_spark_leg() -> None:
    found = _measured(PINNED)
    found["after_writer"] = {**found["after_writer"], "fresh": 3, "new": 3}
    for shape in ("bare", "alias"):
        repark = live.grade(shape, found, "repark")
        assert repark["failed"] == {}, repark
        assert repark[live.OPEN]["after_writer.fresh"] == 3, repark
        spark = live.grade(shape, found, "spark")
        assert set(spark["failed"]) == {"after_writer.fresh", "after_writer.new"}, spark


def test_a_query_level_held_read_after_a_fresh_read_must_equal_it() -> None:
    found = _measured((2, 3, 3))
    found["after_writer"] = {"held": 2, "fresh": 3, "new": 2, "held_again": 2}
    failed = live.grade("sql_star", found, "repark", pending=frozenset())["failed"]
    assert set(failed) == {"after_writer.new", "after_writer.held_again"}, failed
    pending = live.grade("sql_star", found, "repark", pending=frozenset(MAIN_BREAKS))
    assert set(pending["failed"]) == {"after_writer.new"}, pending
    assert pending[live.PENDING]["after_writer.held_again"] == 2, pending
    assert set(live.grade("sql_star", found, "repark")["failed"]) == set(failed), failed


def test_the_writer_client_reads_replies_and_scrubs_the_writer_log(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with live.SparkProcessWriter(sys.executable, {}, DOUBLE, 60.0, 60.0) as writer:
        wrote = writer("lc.db.t", live.EXTERNAL_ROW)
        refused = writer("lc.db.t", "refuse")
    assert wrote == {"count": 3}
    assert refused == {"error": "ERROR TABLE_OR_VIEW_NOT_FOUND"}
    log = capsys.readouterr().out
    assert f"{live.WRITER_LOG_TAG} starting under <ARN> for <ACCOUNT>" in log, log
    assert "0" * 12 not in log and ":".join(["arn", "aws", "glue"]) not in log, log


def test_the_writer_client_raises_when_the_writer_process_exits() -> None:
    with (
        live.SparkProcessWriter(sys.executable, {}, DOUBLE, 60.0, 60.0) as writer,
        pytest.raises(RuntimeError, match="exited with 4"),
    ):
        writer("lc.db.t", "exit")


def test_each_cost_row_prints_one_parseable_line(measured: dict[str, Any]) -> None:
    lines = [live.cost_line("local", row) for row in measured["cost"]]
    parsed = [live.parse_cost_line(line) for line in lines]
    assert [row["label"] for row in parsed] == ["held_read", "held_filter_count"], lines
    assert all(row["catalog"] == "local" for row in parsed), lines
    assert all(int(row["n"]) >= live.COST_CALLS for row in parsed), lines
    for line in lines:
        print(line)
