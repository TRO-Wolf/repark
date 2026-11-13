from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import UnsupportedOperationException
from repark.spark.session import _reset_active_session_for_tests

KEY = "spark.sql.ansi.doubleQuotedIdentifiers"
NEEDLE = f'the configuration "{KEY}" = true in RePark'
ROOT = Path(__file__).parents[4]
DATA = Path(__file__).parent / "data" / "dq_identifiers_config_refuse_1_cells.jsonl"
AUDIT_CELLS = ROOT / "docs" / "corpus" / "reaudit-cells-2026-11-05.jsonl"
TABLE_NAME = re.compile(r"hd\.n\d+\.t\b")
DEFAULT_READ = {"cols": ["key", "value"], "rows": [[KEY, "false"]]}
FIXTURE_ROWS = [[1, "a"], [2, "s"], [3, "b"]]
AUD021_CELL_COUNT = 11


def _records(kind: str) -> list[dict[str, Any]]:
    with DATA.open(encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]
    return [row for row in rows if row["kind"] == kind]


def _aud021_cells() -> list[dict[str, Any]]:
    with AUDIT_CELLS.open(encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]
    return [
        row
        for row in rows
        if row.get("family") == "f2"
        and row.get("view") == "facade"
        and str(row.get("head", "")).startswith("dq_")
        and not str(row.get("head", "")).startswith("dq_off_")
    ]


def _answer(run: Callable[[], Any]) -> Any:
    try:
        table = run().to_arrow()
    except Exception as error:
        lines = str(error).strip().splitlines()
        return "ERR " + type(error).__name__ + ": " + (lines[0][:300] if lines else "")
    rows = [list(row.values()) for row in table.to_pylist()]
    return {"cols": table.column_names, "rows": sorted(rows, key=repr)}


def _sorted(answer: Any) -> Any:
    if isinstance(answer, dict):
        return {"cols": answer["cols"], "rows": sorted(answer["rows"], key=repr)}
    return answer


def _builder(warehouse: Path) -> Any:
    return (
        ReparkSession.builder.appName("dq-identifiers-config-refuse-1")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", str(warehouse))
    )


@pytest.fixture
def spark(tmp_path: Path) -> Iterator[ReparkSession]:
    _reset_active_session_for_tests()
    session = _builder(tmp_path / "hd").getOrCreate()
    session.sql("CREATE NAMESPACE IF NOT EXISTS hd.dq")
    try:
        yield session
    finally:
        session.stop()
        _reset_active_session_for_tests()


def _fixture_table(spark: ReparkSession, name: str) -> str:
    table = f"hd.dq.{name}"
    spark.sql(
        f"CREATE TABLE {table} (id INT, s STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 's'), (3, 'b')")
    return table


def _read_key(spark: ReparkSession) -> Any:
    return _answer(lambda: spark.sql(f"SET {KEY}"))


def _set_by_sql(spark: ReparkSession) -> None:
    spark.sql(f"SET {KEY}=true")


def _set_by_conf(spark: ReparkSession) -> None:
    spark.conf.set(KEY, "true")


def _set_by_builder(spark: ReparkSession) -> None:
    ReparkSession.builder.config(KEY, "true").getOrCreate()


SET_PATHS: dict[str, Callable[[ReparkSession], None]] = {
    "sql_set": _set_by_sql,
    "conf_set": _set_by_conf,
    "builder_config": _set_by_builder,
}


def _refuses(spark: ReparkSession, set_path: Callable[[ReparkSession], None]) -> None:
    with pytest.raises(UnsupportedOperationException, match=re.escape(NEEDLE)):
        set_path(spark)
    assert _read_key(spark) == DEFAULT_READ


AUD021_CELLS = _aud021_cells()


def test_the_aud021_cells_are_the_eleven_facade_dq_rows() -> None:
    assert len(AUD021_CELLS) == AUD021_CELL_COUNT


@pytest.mark.parametrize("set_path", sorted(SET_PATHS))
@pytest.mark.parametrize("cell", AUD021_CELLS, ids=[cell["head"] for cell in AUD021_CELLS])
def test_aud021_cell_set_paths_refuse_and_the_statement_runs_under_the_default(
    spark: ReparkSession, cell: dict[str, Any], set_path: str
) -> None:
    baseline_table = _fixture_table(spark, "base")
    baseline = _answer(lambda: spark.sql(TABLE_NAME.sub(baseline_table, cell["stmt"])))
    baseline_state = _answer(lambda: spark.sql(f"SELECT * FROM {baseline_table}"))
    refused_table = _fixture_table(spark, "refused")
    _refuses(spark, SET_PATHS[set_path])
    after = _answer(lambda: spark.sql(TABLE_NAME.sub(refused_table, cell["stmt"])))
    after_state = _answer(lambda: spark.sql(f"SELECT * FROM {refused_table}"))
    assert str(after).replace(refused_table, "<t>") == str(baseline).replace(baseline_table, "<t>")
    assert after_state == baseline_state


def test_sql_set_arms_answer_like_spark_and_true_refuses(spark: ReparkSession) -> None:
    for record in _records("sql"):
        got = _answer(lambda record=record: spark.sql(record["stmt"]))
        if record["repark"] == "refuse":
            assert isinstance(got, str), record["arm"]
            assert got.startswith("ERR UnsupportedOperationException: "), record["arm"]
            assert NEEDLE in got, record["arm"]
        elif record["repark"] == "default":
            assert got == DEFAULT_READ, record["arm"]
        else:
            assert got == _sorted(record["spark"]), record["arm"]


def _conf_set_then_probe(spark: ReparkSession, value: Any) -> Any:
    spark.conf.set(KEY, value)
    return spark.sql("SELECT 1 AS c")


def test_conf_set_arms_answer_like_spark_and_true_refuses(spark: ReparkSession) -> None:
    for record in _records("conf"):
        got = _answer(lambda record=record: _conf_set_then_probe(spark, record["value"]))
        if record["repark"] == "refuse":
            assert isinstance(got, str), record["arm"]
            assert got.startswith("ERR UnsupportedOperationException: "), record["arm"]
            assert NEEDLE in got, record["arm"]
            assert _read_key(spark) == DEFAULT_READ
        elif record["spark"].startswith("ERR "):
            assert got == record["spark"], record["arm"]
        else:
            assert got == {"cols": ["c"], "rows": [[1]]}, record["arm"]
            assert spark.conf.get(KEY) == record["spark"], record["arm"]


@pytest.mark.parametrize(
    "configure",
    [
        pytest.param(lambda builder: builder.config(KEY, "true"), id="kv_string"),
        pytest.param(lambda builder: builder.config(KEY, True), id="kv_bool"),
        pytest.param(lambda builder: builder.config(map={KEY: "TRUE"}), id="map_upper"),
    ],
)
def test_builder_config_true_refuses_on_reuse_and_on_a_fresh_build(
    tmp_path: Path, configure: Callable[[Any], Any]
) -> None:
    _reset_active_session_for_tests()
    with pytest.raises(UnsupportedOperationException, match=re.escape(NEEDLE)):
        configure(_builder(tmp_path / "fresh")).getOrCreate()
    _reset_active_session_for_tests()
    live = _builder(tmp_path / "live").getOrCreate()
    try:
        with pytest.raises(UnsupportedOperationException, match=re.escape(NEEDLE)):
            configure(ReparkSession.builder).getOrCreate()
        assert _read_key(live) == DEFAULT_READ
    finally:
        live.stop()
        _reset_active_session_for_tests()


def test_builder_config_false_builds_and_reset_answers_false(tmp_path: Path) -> None:
    _reset_active_session_for_tests()
    session = _builder(tmp_path / "hd").config(KEY, "false").getOrCreate()
    try:
        assert _read_key(session) == DEFAULT_READ
        _refuses(session, _set_by_sql)
        assert _answer(lambda: session.sql(f"RESET {KEY}")) == {"cols": [], "rows": []}
        assert _read_key(session) == DEFAULT_READ
    finally:
        session.stop()
        _reset_active_session_for_tests()


SIX = [record for record in _records("six") if record["mode"] == "off"]
SIX_ANSI_OFF = {
    record["arm"]: record for record in _records("six") if record["mode"] == "ansioff_on"
}
REFUSED_SETS = {
    "config_on": [f"SET {KEY}=true"],
    "ansi_off_config_on": ["SET spark.sql.ansi.enabled=false", f"SET {KEY}=true"],
}


@pytest.mark.parametrize("mode", sorted(REFUSED_SETS))
@pytest.mark.parametrize("record", SIX, ids=[record["arm"] for record in SIX])
def test_dml_reach_statement_after_a_refused_set_answers_spark_config_off(
    spark: ReparkSession, record: dict[str, Any], mode: str
) -> None:
    expected = record if mode == "config_on" else SIX_ANSI_OFF[record["arm"]]
    table = _fixture_table(spark, record["arm"])
    for statement in REFUSED_SETS[mode]:
        if KEY in statement:
            _refuses(spark, lambda session, statement=statement: session.sql(statement))
        else:
            spark.sql(statement)
    got = _answer(lambda: spark.sql(record["stmt"].replace("hd.dq.t", table)))
    state = _answer(lambda: spark.sql(f"SELECT * FROM {table}"))
    assert state == _sorted(expected["readback"])
    if record["stmt"].startswith("SELECT"):
        assert got == _sorted(expected["spark"])
        assert got != _sorted(record["spark_config_on"])
    else:
        assert state != _sorted(record["spark_config_on_readback"])
