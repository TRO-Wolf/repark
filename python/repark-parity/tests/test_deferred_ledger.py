"""EC-4 harness: the checked-in deferral ledger IS the comparator's allowlist.

`docs/design/python-facade.md` §3 EC-4: *"a harness test asserts the checked-in ledger and the
comparator's machine-readable allowlist are byte-identical — a ledger that can drift from the gate
it feeds is not a ledger."* There is exactly one file (`task/port/deferred-python-tests.txt`);
byte-identity is pinned by proving the single-file property, not by diffing two copies. The
remaining assertions close both failure directions EC-4 names: every deferred id must be
pin-collected (the subtraction actually removes something), absent from the ported tree (listed
AND ported would be a silent gate hole), and resolvable through the loader the facade cohort's
own ``--junit`` mode uses. Absence is checked statically (the file is gone, or carries no such
`def`), so this test needs no wheel and runs in the ordinary `make py-test` loop.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pytest

# compat/ lives next to src/ under python/repark-parity (not the wheel package).
_PARITY_ROOT = Path(__file__).resolve().parents[1]
if str(_PARITY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PARITY_ROOT))

from compat.compare_reports import junit_node_id, load_junit_report, load_ledger  # noqa: E402

_REPO = Path(__file__).resolve().parents[3]
LEDGER_PATH = _REPO / "task" / "port" / "deferred-python-tests.txt"
HUMAN_LEDGER_PATH = _REPO / "task" / "port" / "deferred-tests.md"
FACADE_TESTS = _REPO / "python" / "repark" / "tests"
_PIN_FACADE = _REPO / "task" / "census" / "baseline-fc3f48102" / "facade"
PIN_COLLECTED = _PIN_FACADE / "collected.txt"
PIN_JUNIT = _PIN_FACADE / "facade.xml"

# `path::name` — the node-id shape the recorded collection emits; a `[param]` suffix
# parses and is allowed (no parametrized ids are deferred).
_NODE_ID = re.compile(r"^(?P<path>tests/[\w./-]+\.py)::(?P<name>[A-Za-z_]\w*)(?P<param>\[.*\])?$")


def _ledger_ids() -> list[str]:
    return load_ledger(LEDGER_PATH)


def _pin_collected() -> set[str]:
    lines = PIN_COLLECTED.read_text(encoding="utf-8").splitlines()
    return {line.strip() for line in lines if line.strip().startswith("tests/")}


def _module_def_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def test_ledger_file_exists_and_is_the_documented_allowlist_path() -> None:
    """The comparator's documented invocation names THIS file — one file, no second copy."""
    assert LEDGER_PATH.is_file(), f"the deferral ledger must be checked in at {LEDGER_PATH}"
    comparator = (_PARITY_ROOT / "compat" / "compare_reports.py").read_text(encoding="utf-8")
    assert "--deferred task/port/deferred-python-tests.txt" in comparator, (
        "the comparator's documented acceptance invocation must name the checked-in ledger path; "
        "if it names any other path the ledger can drift from the gate it feeds"
    )


def test_ledger_parses_as_node_ids_through_the_comparators_own_loader() -> None:
    ids = _ledger_ids()
    assert ids, "an empty ledger is written as an empty file, but this phase defers 12 ids"
    for node_id in ids:
        assert _NODE_ID.match(node_id), f"not a pytest node id: {node_id!r}"
    assert ids == sorted(set(ids), key=ids.index), "duplicate node id in the ledger"


def test_every_deferred_id_is_a_pin_collected_name() -> None:
    """Under-subtraction guard: a row that names nothing on the baseline side removes nothing."""
    collected = _pin_collected()
    assert collected, "the recorded pin collection is the oracle; it must not be empty"
    missing = [node_id for node_id in _ledger_ids() if node_id not in collected]
    assert not missing, (
        f"deferred ids absent from the recorded pin collection {PIN_COLLECTED}: {missing}"
    )


def test_every_deferred_id_subtracts_through_the_junit_loader() -> None:
    """The gate the facade cohort actually runs is ``--junit`` — the ledger must bite THERE.

    The ledger is written in collect-only ``path::name`` form while JUnit keys rows
    ``classname::name``; untranslated, it subtracts nothing in the mode that runs it — the
    drift EC-4 forbids. This asserts the translation against the recorded baseline XML
    through the loader that mode uses.
    """
    side = load_junit_report(PIN_JUNIT, label="v1", manifest={})
    assert side.classes, "the recorded pin JUnit report is the oracle; it must not be empty"
    missing = [node_id for node_id in _ledger_ids() if junit_node_id(node_id) not in side.classes]
    assert not missing, (
        f"deferred ids that do not resolve to a row of the recorded pin JUnit report "
        f"{PIN_JUNIT} — in --junit mode they would subtract nothing: {missing}"
    )


def test_every_deferred_id_is_absent_from_the_ported_tree() -> None:
    """Over-subtraction guard: listed AND ported would subtract a row that still runs here."""
    still_present: list[str] = []
    for node_id in _ledger_ids():
        match = _NODE_ID.match(node_id)
        assert match is not None
        path = FACADE_TESTS.parent / match.group("path")
        if not path.is_file():
            continue  # whole-file deferral — nothing can run
        if match.group("name") in _module_def_names(path):
            still_present.append(node_id)
    assert not still_present, (
        f"deferred ids that are ALSO ported (they would run here while being subtracted from the "
        f"baseline): {still_present}"
    )


def test_the_human_summary_names_every_machine_readable_id() -> None:
    """The prose ledger and the allowlist are one record, not two that can drift apart."""
    prose = HUMAN_LEDGER_PATH.read_text(encoding="utf-8")
    unnamed = [node_id for node_id in _ledger_ids() if node_id not in prose]
    assert not unnamed, f"ids in the allowlist that the human ledger never names: {unnamed}"


@pytest.mark.parametrize("surface", ["test_excel_reader.py", "test_pg_catalog.py"])
def test_deferred_surfaces_are_named_in_the_ledger_comments(surface: str) -> None:
    """The blocking surface is recorded next to the ids, so a reader never has to guess."""
    text = LEDGER_PATH.read_text(encoding="utf-8")
    assert surface in text
    assert "WHERE THE EXCEPTION IS RAISED" in text
