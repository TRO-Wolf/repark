"""Integration pins for exact ratchets and the frozen SQP-1 record.

pins: pr-245-revalidation/C-004, C-005, C-006, C-007, C-008, C-009
"""

from __future__ import annotations

import ast
import hashlib
import os
import runpy
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

_REPO = Path(__file__).resolve().parents[3]
_PYTHON_MAIN_BASELINES = {
    "python/repark/src/repark/spark/dataframe/plan_collapse.py": 1422,
    "python/repark/src/repark/spark/dataframe/writer_readwriter.py": 1406,
    "python/repark/src/repark/spark/functions.py": 2033,
    "python/repark/src/repark/spark/ml/feature/_transformers.py": 2763,
    "python/repark/src/repark/spark/session/_funcs.py": 8390,
    "python/repark/tests/test_functions_gt2.py": 1050,
    "python/repark-parity/bench/tpcds/runner.py": 1263,
    "python/repark-parity/bench/tpch/runner.py": 1780,
}
_FROZEN_SQP_FILES = {
    "task/ledgers/archive/2026-08/2026-08-27-sqp-1-spark-string-literals-ledger.md": (
        "e36d503959d904946d444d4ecdc0ab90b2d22387458cac3256fd319e026102fc"
    ),
    "crates/repark-spark/src/tests/spark_string_literals.rs": (
        "58f2910bcde9f82d6b7450c601beb9fcd984300ac5e4be6cd10a40c6bde3b045"
    ),
    "crates/repark-spark/src/tests/cast_binary.rs": (
        "74909c272a090d18c3ac6d2eef4f355fa1be9c182baf4ec4e5133d9defd72da4"
    ),
    "crates/repark-sql/tests/ansi_door_string_literals.rs": (
        "917ddb77f710763e52927a5730c5c2c94867639d64ad61b46e4d4ce26986b95c"
    ),
    "python/repark/tests/test_sqp_1_string_literals.py": (
        "3c67eb43efac916cefe05725aba4e0ecd01e0a5b7defddc014e65dcb258e03ed"
    ),
    "python/repark-parity/tests/test_sqp_1_record.py": (
        "39584dfb5b8d3d73685a9d64146fc691a9dd2bbdf278d489bbbd930b55f6c519"
    ),
}
_SQL_LITERAL_CALLS = {
    "python/repark-parity/bench/tpcds/datagen.py": {"escape_sql_single_quotes": 1},
    "python/repark-parity/bench/tpcds/runner.py": {"escape_sql_single_quotes": 1},
    "python/repark-parity/bench/tpch/datagen.py": {"escape_sql_single_quotes": 1},
    "python/repark-parity/bench/tpch/runner.py": {"escape_sql_single_quotes": 1},
    "python/repark-parity/bench/write/merge_runner.py": {"sql_string_literal": 1},
    "python/repark-parity/bench/write/overwrite_runner.py": {"sql_string_literal": 1},
    "python/repark-parity/bench/write/runner.py": {"sql_string_literal": 1},
    "python/repark/src/repark/spark/catalog.py": {"sql_string_literal": 1},
    "python/repark/src/repark/spark/dataframe/core.py": {"_sql_string_literal": 1},
    "python/repark/src/repark/spark/dataframe/writer_readwriter.py": {
        "_sql_string_literal": 2,
        "escape_sql_single_quotes": 2,
    },
    "python/repark/src/repark/spark/functions.py": {"sql_string_literal": 4},
    "python/repark/src/repark/spark/functions_collections.py": {"sql_string_literal": 1},
    "python/repark/src/repark/spark/functions_expr.py": {"sql_string_literal": 1},
    "python/repark/src/repark/spark/ml/feature/_transformers.py": {"sql_string_literal": 7},
    "python/repark/src/repark/spark/session/create_dataframe_values.py": {"sql_string_literal": 1},
    "python/repark/src/repark/spark/session/session_configuration.py": {"sql_string_literal": 1},
}


def _line_count(path: Path) -> int:
    """Return the exact split-lines count used by the source-size gates."""
    return len(path.read_text(encoding="utf-8").splitlines())


def _script_globals(relative: str) -> dict[str, Any]:
    """Load one repository guard without running its command entry point."""
    return runpy.run_path(str(_REPO / relative))


def test_pr245_python_ratchets_are_exact_and_never_exceed_main() -> None:
    """Each affected Python file stays at or below main and matches any remaining exception."""
    exceptions = _script_globals("scripts/check_lib_py.py")["EXCEPTIONS"]
    for relative, main_baseline in _PYTHON_MAIN_BASELINES.items():
        count = _line_count(_REPO / relative)
        assert count <= main_baseline, relative
        if count > 1000:
            assert exceptions[relative][0] == count, relative


def test_pr245_changed_rust_sources_stay_cohesive_and_below_the_default() -> None:
    """Changed Rust files stay below the default and keep their named responsibility."""
    sources = {
        "crates/repark-spark/src/spark_literals.rs": "canonicalize",
        "crates/repark-spark/src/spark_ast.rs": "rewrite_binary_casts",
        "crates/repark-spark/src/router.rs": "spark_literals::canonicalize",
    }
    for relative, responsibility in sources.items():
        text = (_REPO / relative).read_text(encoding="utf-8")
        assert len(text.splitlines()) <= 1000, relative
        assert responsibility in text, relative


def test_pr245_sql_embed_guard_detects_a_bypass_and_has_one_home() -> None:
    """The receiver-blind detector rejects its bounded constant quote-doubling syntax."""
    guard = _script_globals("scripts/check_python_conventions.py")
    find_bypass = guard["find_sql_quote_doubling"]
    assert find_bypass(ast.parse('escaped = value.replace("\'", "\'\'")\n')) == [1]
    assert find_bypass(ast.parse("escaped = value.replace(chr(39), chr(39) * 2)\n")) == [1]
    arithmetic = "escaped = value.replace(chr(0x28 - 1), chr(0x28 - 1) * 2)\n"
    assert find_bypass(ast.parse(arithmetic)) == [1]
    receiver_blind = "escaped = unrelated.replace(chr(0x28 - 1), chr(0x28 - 1) * 2)\n"
    assert find_bypass(ast.parse(receiver_blind)) == [1]
    assert find_bypass(ast.parse("literal = sql_string_literal(value)\n")) == []
    assert guard["SQL_LITERAL_HELPER_FILES"] == {
        "python/repark-parity/src/repark_parity/sql.py",
        "python/repark/src/repark/spark/_idents.py",
    }


def test_pr245_parity_runners_import_without_the_product_package(tmp_path: Path) -> None:
    """Both parity runners import when the standalone environment blocks the product package."""
    script = """
import sys
sys.path[:0] = sys.argv[1:]
sys.modules["repark"] = None
import bench.tpcds.runner
import bench.tpch.runner
from repark_parity.sql import escape_sql_single_quotes
assert escape_sql_single_quotes("a'b\\c") == "a''b\\c"
"""
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment.pop("VIRTUAL_ENV", None)
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            script,
            str(_REPO / "python/repark-parity/src"),
            str(_REPO / "python/repark-parity"),
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_pr245_sql_embed_guard_rejects_unsafe_or_out_of_domain_constants() -> None:
    """The detector neither executes expressions nor broadens beyond its bounded whitelist."""
    find_bypass = _script_globals("scripts/check_python_conventions.py")["find_sql_quote_doubling"]
    safe_misses = [
        "value.replace(chr(0x20 - 1), chr(0x20 - 1) * 2)",
        "value.replace(chr(True), chr(True) * 2)",
        "value.replace(chr(0x110000), chr(0x110000) * 2)",
        "value.replace(chr(39, 40), chr(39) * 2)",
        "value.replace(chr(quote_code), chr(quote_code) * 2)",
        "value.replace(chr(side_effect()), chr(39) * 2)",
        "value.replace(chr(78 // 2), chr(39) * 2)",
        "value.replace(chr(39), chr(39) * 3)",
        "value.replace(chr(39), chr(39) * 2, 1)",
        "value.replace(chr(20 + 10 + 5 + 2 + 1 + 1), chr(39) * 2)",
    ]
    for source in safe_misses:
        assert find_bypass(ast.parse(source)) == [], source


def test_pr245_sql_embed_guard_bounds_constructed_constant_trees() -> None:
    """The constant evaluator misses deep text trees without recursion or large output."""
    guard = _script_globals("scripts/check_python_conventions.py")
    find_bypass = guard["find_sql_quote_doubling"]
    source = "value.replace(" + "+".join(["chr(39)", *(['""'] * 2_000)]) + ", \"''\")"
    parsed = ast.parse(source)
    parsed_call = parsed.body[0].value
    assert isinstance(parsed_call, ast.Call)
    deep_concat = parsed_call.args[0]
    replacement: ast.expr = ast.Constant(value="'")
    for _ in range(30):
        replacement = ast.BinOp(left=replacement, op=ast.Mult(), right=ast.Constant(value=2))
    calls = ast.Module(
        body=[
            ast.Expr(
                value=ast.Call(
                    func=ast.Attribute(
                        value=ast.Name(id="value", ctx=ast.Load()),
                        attr="replace",
                        ctx=ast.Load(),
                    ),
                    args=[deep_concat, ast.Constant(value="''")],
                    keywords=[],
                )
            ),
            ast.Expr(
                value=ast.Call(
                    func=ast.Attribute(
                        value=ast.Name(id="value", ctx=ast.Load()),
                        attr="replace",
                        ctx=ast.Load(),
                    ),
                    args=[ast.Constant(value="'"), replacement],
                    keywords=[],
                )
            ),
        ],
        type_ignores=[],
    )
    assert guard["_constant_text"](deep_concat) is None
    assert guard["_constant_text"](replacement) is None
    assert find_bypass(parsed) == []
    assert find_bypass(calls) == []


def test_pr245_python_guard_controls_parser_resource_failures(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A valid hostile file fails once while ordinary valid and invalid files stay classified."""
    guard = _script_globals("scripts/check_python_conventions.py")
    check_file = guard["check_file"]
    valid = tmp_path / "valid.py"
    valid.write_text("value = 1\n", encoding="utf-8")
    assert check_file(valid, tmp_path) == []
    invalid = tmp_path / "invalid.py"
    invalid.write_text("if:\n", encoding="utf-8")
    invalid_errors = check_file(invalid, tmp_path)
    assert len(invalid_errors) == 1
    assert invalid_errors[0].startswith("ERROR: invalid.py does not parse")
    hostile = tmp_path / "hostile.py"
    hostile.write_text("value = " + "+".join(["0"] * 10_000) + "\n", encoding="utf-8")
    assert check_file(hostile, tmp_path) == [
        "ERROR: hostile.py exceeds Python parser resource limits (RecursionError) — "
        "refuse to pass closed"
    ]
    for error_type in (MemoryError, OverflowError):
        with patch.object(guard["ast"], "parse", side_effect=error_type()):
            assert check_file(valid, tmp_path) == [
                f"ERROR: valid.py exceeds Python parser resource limits "
                f"({error_type.__name__}) — refuse to pass closed"
            ]
    gate_root = tmp_path / "gate"
    scan_root = gate_root / "scan"
    scan_root.mkdir(parents=True)
    gate_hostile = scan_root / "hostile.py"
    gate_hostile.write_text(hostile.read_text(encoding="utf-8"), encoding="utf-8")
    main_globals = guard["main"].__globals__
    main_globals["__file__"] = str(gate_root / "scripts" / "check_python_conventions.py")
    main_globals["SCAN_ROOTS"] = ("scan",)
    main_globals["NESTED_DEF_EXCEPTIONS"] = {}
    main_globals["DATACLASS_EXCEPTIONS"] = {}
    assert guard["main"]() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.splitlines() == [
        "ERROR: scan/hostile.py exceeds Python parser resource limits (RecursionError) — "
        "refuse to pass closed",
        "python-conventions: FAIL — 1 violation(s) across 1 files",
    ]


def test_pr245_shipped_sql_literal_helper_call_inventory_is_exact() -> None:
    """Pin the enumerable helper-call inventory without claiming semantic SQL completeness."""
    actual: dict[str, dict[str, int]] = {}
    helpers = {"sql_string_literal", "_sql_string_literal", "escape_sql_single_quotes"}
    for root in (_REPO / "python/repark/src", _REPO / "python/repark-parity/bench"):
        for path in root.rglob("*.py"):
            counts: dict[str, int] = {}
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = node.func.id if isinstance(node.func, ast.Name) else ""
                if name in helpers:
                    counts[name] = counts.get(name, 0) + 1
            if counts:
                actual[path.relative_to(_REPO).as_posix()] = counts
    assert actual == _SQL_LITERAL_CALLS


def test_pr245_router_comment_states_only_the_local_front_door_invariant() -> None:
    """Pin the two durable reasons in the canonicalization call-site comment."""
    text = (_REPO / "crates/repark-spark/src/router.rs").read_text(encoding="utf-8")
    expected = (
        "// Canonicalize once at the Spark SQL front door so later tokenizers cannot process "
        "escapes again.\n"
        "    // Translate downstream parser locations back to the caller's SQL before returning "
        "an error."
    )
    assert expected in text


def test_pr245_original_sqp_record_and_pin_family_are_byte_frozen() -> None:
    """The archived ledger and original pin files retain their frozen hashes."""
    for relative, expected in _FROZEN_SQP_FILES.items():
        digest = hashlib.sha256((_REPO / relative).read_bytes()).hexdigest()
        assert digest == expected, relative


def test_pr245_navigation_names_the_separate_revalidation_family() -> None:
    """Both test homes and the lifecycle map name this revalidation unit."""
    required = {
        "python/repark/tests/map.md": "test_pr_245_revalidation.py",
        "python/repark-parity/tests/map.md": "test_pr_245_revalidation_record.py",
    }
    for relative, name in required.items():
        assert name in (_REPO / relative).read_text(encoding="utf-8"), relative
    name = "pr-245-revalidation-ledger.md"
    live = [
        _REPO / "task/ledgers/staging" / name,
        _REPO / "task/ledgers/completed" / name,
    ]
    archived = sorted((_REPO / "task/ledgers/archive").glob(f"*/*-{name}"))
    ledgers = [ledger for ledger in (*live, *archived) if ledger.is_file()]
    assert len(ledgers) == 1, ledgers
    ledger_map = ledgers[0].parent / "map.md"
    assert ledgers[0].name in ledger_map.read_text(encoding="utf-8")
    staging_map = (_REPO / "task/ledgers/staging/map.md").read_text(encoding="utf-8")
    assert name not in staging_map
