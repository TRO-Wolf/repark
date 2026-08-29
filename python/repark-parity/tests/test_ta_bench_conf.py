"""Default-conf contract (no engine, no numpy, no native module)."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

_TA_DIR = Path(__file__).resolve().parents[1] / "bench" / "ta"
if str(_TA_DIR) not in sys.path:
    sys.path.insert(0, str(_TA_DIR))

from target_partition_contract import (  # noqa: E402 — sibling bench dir, not a package
    DEFAULT_TARGET_PARTITIONS_LABEL,
    ISOLATION_ROLE,
    emit_target_partition_fields,
    session_target_partitions,
)

PRIMARY_SCRIPTS = (
    "bench_kernel_race.py",
    "bench_wide_serving.py",
    "bench_null_lookback.py",
    "bench_last_row.py",
)


def _parse(name: str) -> ast.Module:
    return ast.parse((_TA_DIR / name).read_text(encoding="utf-8"), filename=name)


def _make_session_calls(tree: ast.AST) -> list[ast.Call]:
    calls: list[ast.Call] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "make_session":
            calls.append(node)
    return calls


def _kw(call: ast.Call, name: str) -> ast.expr | None:
    for keyword in call.keywords:
        if keyword.arg == name:
            return keyword.value
    return None


def test_default_cell_omits_session_knob_and_emits_default_label() -> None:
    assert session_target_partitions(isolation=False) is None
    fields = emit_target_partition_fields(isolation=False)
    assert fields == {"target_partitions": DEFAULT_TARGET_PARTITIONS_LABEL}
    assert DEFAULT_TARGET_PARTITIONS_LABEL == "default"
    assert " " not in DEFAULT_TARGET_PARTITIONS_LABEL


def test_isolation_cell_sets_tp1_and_single_core_token() -> None:
    assert session_target_partitions(isolation=True) == 1
    fields = emit_target_partition_fields(isolation=True)
    assert fields == {"target_partitions": 1, "isolation": ISOLATION_ROLE}
    assert ISOLATION_ROLE == "single_core"
    assert " " not in ISOLATION_ROLE


def test_primary_scripts_omit_literal_tp1_on_make_session() -> None:
    for name in PRIMARY_SCRIPTS:
        calls = _make_session_calls(_parse(name))
        assert calls, f"{name} must call make_session"
        for call in calls:
            value = _kw(call, "target_partitions")
            if value is None:
                continue
            if isinstance(value, ast.Constant):
                assert value.value != 1, f"{name} must not hardcode target_partitions=1"


def test_batch_size_session_is_isolation() -> None:
    calls = _make_session_calls(_parse("bench_batch_size.py"))
    assert calls, "bench_batch_size.py must call make_session"
    for call in calls:
        value = _kw(call, "target_partitions")
        assert value is not None, "batch_size isolation must set target_partitions"
        assert isinstance(value, ast.Call)
        assert isinstance(value.func, ast.Name)
        assert value.func.id == "session_target_partitions"
        flags = [keyword for keyword in value.keywords if keyword.arg == "isolation"]
        assert flags and isinstance(flags[0].value, ast.Constant)
        assert flags[0].value.value is True


def test_many_symbols_has_isolation_and_default_cells_no_cores_cell() -> None:
    source = (_TA_DIR / "bench_many_symbols.py").read_text(encoding="utf-8")
    assert "session_target_partitions(isolation=isolation)" in source
    assert "emit_target_partition_fields(isolation=isolation)" in source
    assert "cpu_core_count" not in source
    tree = _parse("bench_many_symbols.py")
    isolation_literals: list[bool] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Tuple) or len(node.elts) != 3:
            continue
        first = node.elts[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, bool):
            isolation_literals.append(first.value)
    assert isolation_literals.count(True) == 1
    assert isolation_literals.count(False) == 2
