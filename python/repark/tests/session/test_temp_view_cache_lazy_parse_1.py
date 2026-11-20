from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import _temp_view_cache_lazy_parse_1_cells as cells
import pytest
from test_temp_view_session_settings_1 import assert_answer, build

from repark.errors import IllegalArgumentException
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("temp_view_cache_lazy_parse_1")
HERE = Path(__file__)
ORACLE = json.loads(HERE.with_name("temp_view_cache_lazy_parse_1_spark_oracle.json").read_text())
KEYS = sorted(ORACLE)


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp("cl1")
    session = build(root, "true")
    found = cells.Cells(session, str(root)).run()
    session.stop()
    _reset_active_session_for_tests()
    yield found


@pytest.mark.parametrize("key", KEYS)
def test_a_lazy_cache_cell_answers_as_spark(measured: dict[str, Any], key: str) -> None:
    assert_answer(measured[key], ORACLE[key], key)


@pytest.fixture
def lazy(tmp_path: Path) -> Iterator[tuple[Any, Any]]:
    session = build(tmp_path, "true")
    table = cells.Cells(session, str(tmp_path))
    table.fixtures()
    yield session, table
    session.stop()
    _reset_active_session_for_tests()


def test_a_child_over_the_budget_raises_and_the_table_stays_armed(
    lazy: tuple[Any, Any],
) -> None:
    session, table = lazy
    session.conf.set("repark.cache.max_bytes", "1")
    parent = table.frame("json_s").cache()
    child = parent.filter("n > 1")
    for frame in (child, child, parent):
        with pytest.raises(IllegalArgumentException, match=r"repark\.cache\.max_bytes=1"):
            frame.count()
    session.conf.set("repark.cache.max_bytes", "0")
    parent.unpersist()
    assert parent.count() == 2


def test_repr_of_an_unfilled_lazy_cache_runs_nothing(lazy: tuple[Any, Any]) -> None:
    _, table = lazy
    frame = table.frame("json_s").cache()
    assert frame._session.lazy_cache_filled(frame._cache_view) is False
    assert repr(frame) == repr(table.frame("json_s"))
    assert str(frame) == str(table.frame("json_s"))
    assert frame._session.lazy_cache_filled(frame._cache_view) is False
    assert frame.count() == 2
    assert frame._session.lazy_cache_filled(frame._cache_view) is True


DEEP = textwrap.dedent(
    """
    import json, sys, threading
    import repark._native as native
    from repark import ReparkSession
    from repark.spark import functions as F
    print("NATIVE " + native.__file__, flush=True)
    root, out = sys.argv[1], []


    def work():
        s = ReparkSession.builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
        with open(root + "/d.json", "w") as handle:
            handle.write(json.dumps({"n": 1, "ts": "2024-01-01 03:00:00"}) + "\\n")
        s.sql("SET spark.sql.session.timeZone=America/Los_Angeles").collect()
        frame = s.read.schema("n INT, ts TIMESTAMP").json(root + "/d.json")
        for step in range(2000):
            frame = frame.filter(F.col("n") >= -1000000 - (step % 7))
        frame = frame.selectExpr("n", "unix_timestamp(ts) AS u").cache()
        frame.createOrReplaceTempView("deep")
        s.sql("SET spark.sql.session.timeZone=UTC").collect()
        out.extend(s.sql("SELECT * FROM deep").collect())
        frame.unpersist()
        out.extend(s.sql("SELECT * FROM deep").collect())
        s.stop()


    threading.stack_size(8 * 1024 * 1024)
    worker = threading.Thread(target=work)
    worker.start()
    worker.join()
    print("ROWS " + json.dumps([[r[0], r[1]] for r in out]), flush=True)
    """
)


def test_a_deep_cached_lineage_fills_and_releases_in_a_subprocess(tmp_path: Path) -> None:
    script = tmp_path / "deep.py"
    script.write_text(DEEP)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(sys.path)
    done = subprocess.run(
        [sys.executable, str(script), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
        check=False,
    )
    assert done.returncode == 0, done.stderr[-2000:]
    found = [line for line in done.stdout.splitlines() if line.startswith("ROWS ")]
    assert found == ["ROWS [[1, 1704078000], [1, 1704078000]]"], done.stdout[-2000:]
