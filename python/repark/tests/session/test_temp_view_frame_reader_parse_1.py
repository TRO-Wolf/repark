from __future__ import annotations

import contextlib
import io
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import _temp_view_frame_reader_parse_1_cells as cells
import pytest
from test_temp_view_session_settings_1 import assert_answer, build

from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("temp_view_frame_reader_parse_1")
HERE = Path(__file__)
ORACLE = json.loads(HERE.with_name("temp_view_frame_reader_parse_1_spark_oracle.json").read_text())
ROW_KEYS = sorted(key for key, want in ORACLE.items() if "value" not in want)
VALUE_KEYS = sorted(key for key, want in ORACLE.items() if "value" in want)


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp("rp1")
    session = build(root, "true")
    found = cells.Cells(session, str(root)).run()
    session.stop()
    _reset_active_session_for_tests()
    yield found


@pytest.mark.parametrize("key", ROW_KEYS)
def test_a_reader_lineage_cell_answers_as_spark(measured: dict[str, Any], key: str) -> None:
    found, want = measured[key], ORACLE[key]
    assert_answer(found, want, key)
    if "columns" in want:
        assert found["columns"] == want["columns"], (key, found)
        assert_answer(found["view"], want["view"], key + "/view")


@pytest.mark.parametrize("key", VALUE_KEYS)
def test_plan_identity_answers_as_spark(measured: dict[str, Any], key: str) -> None:
    assert measured[key] == ORACLE[key], (key, measured[key])


def test_a_headerless_csv_explains_its_parse_and_hides_the_node(tmp_path: Path) -> None:
    session = build(tmp_path, "true")
    table = cells.Cells(session, str(tmp_path))
    table.fixtures()
    frame = session.read.csv(str(tmp_path / "csv0"))
    frame.createOrReplaceTempView("headerless")
    plans = [
        "\n".join(str(row[-1]) for row in session.sql("EXPLAIN SELECT * FROM headerless").collect())
    ]
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        frame.explain()
        frame.explain(mode="formatted")
    plans.append(printed.getvalue())
    session.stop()
    _reset_active_session_for_tests()
    for plan in plans:
        assert "ReaderParse" not in plan, plan
        assert "_c0" in plan, plan
