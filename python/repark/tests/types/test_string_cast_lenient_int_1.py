from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _record_string_cast_lenient_int_1 import (
    ANSI_MODES,
    FIXTURE,
    HUNDRED_BAD,
    HUNDRED_SEATS,
    TARGETS,
    WRITE_KINDS,
    answer,
    cell_key,
    cell_texts,
    comparison,
    comparisons,
    hundred,
    implicit_cell,
    implicit_cells,
    stored,
    text_class,
    write_cell,
    write_cells,
    write_key,
)

from repark import ReparkSession
from repark.spark import functions
from repark.spark.session import _reset_active_session_for_tests

_ORACLE: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
_CAST_SEATS = ("sql-literal", "sql-values", "sql-column", "dataframe", "expr", "selectexpr")
_TRY_CAST_GAPS = frozenset({"whitespace", "limit-padded"})
_NAMED_SEATS = frozenset({"sql-literal", "sql-values", "dataframe", "selectexpr"})
_NUL = "\x003"
_GROUPS = [
    (seat, target, ansi) for seat in _CAST_SEATS for target in TARGETS for ansi in ANSI_MODES
]
_WRITE_GROUPS = [
    (kind, target, ansi) for kind in WRITE_KINDS for target in TARGETS for ansi in ANSI_MODES
]


def _open(warehouse: Path, ansi: str) -> ReparkSession:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("string-cast-lenient-int-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse / "spark-warehouse"))
        .config("spark.sql.catalog.lenient.type", "hadoop")
        .config("spark.sql.catalog.lenient.warehouse", str(warehouse / "lenient"))
        .getOrCreate()
    )
    session.conf.set("spark.sql.ansi.enabled", ansi)
    return session


@pytest.fixture
def session(tmp_path: Path) -> Iterator[ReparkSession]:
    built = _open(tmp_path, "true")
    yield built
    built.stop()
    _reset_active_session_for_tests()


def _miss(seat: str, expected: dict[str, Any], got: dict[str, Any]) -> bool:
    if "value" in expected:
        keys = ("value", "type", "nullable", "name") if seat in _NAMED_SEATS else ("value", "type")
    else:
        keys = ("error", "sqlstate", "message")
    return any(got.get(key) != expected[key] for key in keys)


def _cold_rows(warehouse: Path, table: str) -> list[list[Any]]:
    cold = _open(warehouse, "true")
    folder = warehouse / "lenient" / "ns" / table.rsplit(".", 1)[1]
    frame = cold.read.format("iceberg").load(str(folder)).orderBy("id")
    rows = [[row[0], row[1]] for row in frame.collect()]
    cold.stop()
    return rows


def test_the_fixture_holds_every_cell() -> None:
    assert _ORACLE["spark"] == "4.1.2"
    seats = (*_CAST_SEATS, "try-cast")
    keys = {
        cell_key(seat, target, ansi, text)
        for seat in seats
        for target in TARGETS
        for ansi in ANSI_MODES
        for text in cell_texts(target)
    }
    assert set(_ORACLE["cells"]) == keys
    assert {write_key(*cell) for cell in write_cells()} <= set(_ORACLE["writes"])


@pytest.mark.parametrize(("seat", "target", "ansi"), _GROUPS)
def test_a_string_cast_to_an_integer_answers_as_spark(
    session: ReparkSession, seat: str, target: str, ansi: str
) -> None:
    session.conf.set("spark.sql.ansi.enabled", ansi)
    misses = []
    for text in cell_texts(target):
        if text == _NUL:
            continue
        expected = _ORACLE["cells"][cell_key(seat, target, ansi, text)]
        got = answer(session, functions, seat, target, text)
        if _miss(seat, expected, got):
            misses.append(f"{text_class(target, text)} {text!r}: spark {expected} repark {got}")
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_try_cast_keeps_its_answers(session: ReparkSession, target: str, ansi: str) -> None:
    session.conf.set("spark.sql.ansi.enabled", ansi)
    misses = []
    for text in cell_texts(target):
        if text == _NUL:
            continue
        expected = _ORACLE["cells"][cell_key("try-cast", target, ansi, text)]
        got = answer(session, functions, "try-cast", target, text)
        if text_class(target, text) in _TRY_CAST_GAPS:
            assert got["value"] is None, (text, got)
            assert expected["value"] is not None, (text, expected)
        elif got.get("value") != expected["value"] or got.get("type") != expected["type"]:
            misses.append(f"{text!r}: spark {expected} repark {got}")
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("seat", HUNDRED_SEATS)
@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_one_bad_row_in_a_hundred_answers_as_spark(
    session: ReparkSession, seat: str, target: str, ansi: str
) -> None:
    session.conf.set("spark.sql.ansi.enabled", ansi)
    for bad in HUNDRED_BAD:
        expected = _ORACLE["hundred"][cell_key(seat, target, ansi, bad)]
        got = hundred(session, functions, seat, target, bad)
        if "values" in expected:
            assert got == expected, (bad, got)
        else:
            assert (got.get("error"), got.get("sqlstate")) == (
                expected["error"],
                expected["sqlstate"],
            ), (bad, got)


@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_comparisons_with_padded_text_answer_as_spark(
    session: ReparkSession, target: str, ansi: str
) -> None:
    session.conf.set("spark.sql.ansi.enabled", ansi)
    for name in comparisons(target):
        expected = _ORACLE["comparisons"][cell_key(name, target, ansi, None)]
        got = comparison(session, target, name)
        if name == "in-padded":
            assert "error" in got, (name, got)
        elif "values" in expected:
            assert got == expected, (name, got)
        else:
            assert (got.get("error"), got.get("sqlstate")) == (
                expected["error"],
                expected["sqlstate"],
            ), (name, got)


def _write_miss(expected: dict[str, Any], got: dict[str, Any], rows: Any) -> str | None:
    if got["refused"] != expected["refused"]:
        return f"refused {got} but Spark {expected.get('error')}"
    if expected["refused"] and (got.get("error"), got.get("sqlstate")) != (
        expected["error"],
        expected["sqlstate"],
    ):
        return f"{got} != {expected['error']}"
    if rows != expected["rows"]:
        return f"rows {rows} != {expected['rows']}"
    return None


@pytest.mark.parametrize(("kind", "target", "ansi"), _WRITE_GROUPS)
def test_a_write_of_a_cast_value_stores_as_spark_read_back_cold(
    tmp_path: Path, kind: str, target: str, ansi: str
) -> None:
    session = _open(tmp_path, ansi)
    session.sql("CREATE NAMESPACE IF NOT EXISTS lenient.ns").collect()
    cells = [cell for cell in write_cells() if cell[:3] == (kind, target, ansi)]
    written = []
    for slot, cell in enumerate(cells):
        got, table = write_cell(session, functions, "lenient", cell, slot)
        written.append((cell, got, table))
    session.stop()
    misses = []
    for cell, got, table in written:
        expected = _ORACLE["writes"][write_key(*cell)]
        miss = _write_miss(expected, got, _cold_rows(tmp_path, table))
        if miss:
            misses.append(f"{cell}: {miss}")
    _reset_active_session_for_tests()
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize(("kind", "target", "ansi"), _WRITE_GROUPS)
def test_a_write_of_an_uncast_string_still_refuses_as_spark(
    tmp_path: Path, kind: str, target: str, ansi: str
) -> None:
    session = _open(tmp_path, ansi)
    session.sql("CREATE NAMESPACE IF NOT EXISTS lenient.ns").collect()
    cells = [cell for cell in implicit_cells() if cell[:3] == (kind, target, ansi)]
    misses = []
    for slot, cell in enumerate(cells):
        got, table = implicit_cell(session, functions, "lenient", cell, slot)
        expected = _ORACLE["writes"]["implicit|" + write_key(*cell)]
        rows = stored(session, table)
        if not got["refused"] or rows != expected["rows"]:
            misses.append(f"{cell}: {got} rows {rows}; Spark refuses {expected['error']}")
    session.stop()
    _reset_active_session_for_tests()
    assert not misses, "\n".join(misses)


_NUL_SCRIPT = """
import json, sys
sys.path.insert(0, sys.argv[1])
from _record_string_cast_lenient_int_1 import SEATS, TARGETS, answer
from repark import ReparkSession
from repark.spark import functions
session = ReparkSession.builder.appName("lenient-int-nul").getOrCreate()
found = {}
for ansi in ("false", "true"):
    session.conf.set("spark.sql.ansi.enabled", ansi)
    for target in TARGETS:
        for seat in SEATS:
            key = "|".join((seat, target, ansi))
            found[key] = answer(session, functions, seat, target, "\\x003")
print(json.dumps(found))
"""


def test_a_nul_padded_integer_text_answers_without_aborting_the_process() -> None:
    run = subprocess.run(
        [sys.executable, "-c", _NUL_SCRIPT, str(Path(__file__).parent)],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert run.returncode == 0, run.stderr[-2000:]
    found = json.loads(run.stdout.strip().splitlines()[-1])
    misses = []
    for key, got in found.items():
        seat, target, ansi = key.split("|")
        if seat == "try-cast":
            continue
        expected = _ORACLE["cells"][cell_key(seat, target, ansi, _NUL)]
        if got.get("value") != expected["value"] or got.get("type") != expected["type"]:
            misses.append(f"{key}: spark {expected} repark {got}")
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_a_values_string_stored_into_an_integer_column_still_refuses(
    tmp_path: Path, ansi: str
) -> None:
    session = _open(tmp_path, ansi)
    session.sql("CREATE NAMESPACE IF NOT EXISTS lenient.ns").collect()
    session.sql("CREATE TABLE lenient.ns.values_t (id INT, c INT) USING iceberg").collect()
    for text in (" 42 ", "abc"):
        with pytest.raises(Exception, match=r"Cannot cast string|CANNOT_SAFELY_CAST"):
            session.sql(f"INSERT INTO lenient.ns.values_t VALUES (1, '{text}')").collect()
    session.stop()
    assert _cold_rows(tmp_path, "lenient.ns.values_t") == []
    _reset_active_session_for_tests()


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_the_json_reader_keeps_its_integer_schema_cast(
    session: ReparkSession, tmp_path: Path, ansi: str
) -> None:
    session.conf.set("spark.sql.ansi.enabled", ansi)
    path = tmp_path / "ints.json"
    path.write_text('{"id": 0, "n": "1"}\n{"id": 1, "n": " 2"}\n', encoding="utf-8")
    with pytest.raises(Exception, match=r"Cannot cast string ' 2' to value of Int32 type"):
        session.read.schema("id INT, n INT").json(str(path)).collect()
