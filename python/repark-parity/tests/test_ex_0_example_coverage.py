"""Pins for the v0.7 example-drift gate.

pins: ex-0-example-drift-gate/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
pins: ex-1-class-surfaces/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
"""

from __future__ import annotations

import ast
import contextlib
import importlib.util
import io
import shutil
import subprocess
from pathlib import Path
from types import ModuleType

from pytest import MonkeyPatch

_REPO = Path(__file__).resolve().parents[3]
_GATE = _REPO / "scripts" / "check_example_coverage.py"
_WRAPPER = _REPO / "scripts" / "check_example_coverage.sh"
_MAKEFILE = _REPO / "Makefile"


def _load_gate() -> ModuleType:
    """Load the example-coverage gate as a module."""
    spec = importlib.util.spec_from_file_location("check_example_coverage", _GATE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ex_0_enumerator_emits_five_families_and_repark_sql() -> None:
    """C-001: the AST walk names every family and the native SQL door.

    pins: ex-1-class-surfaces/C-001
    """
    gate = _load_gate()
    rows = gate.enumerate_public_surface(_REPO)
    families = {family for family, _name in rows}
    assert families == set(gate.FAMILIES)
    names = [name for _family, name in rows]
    assert rows == sorted(rows, key=lambda row: (row[0], row[1]))
    assert "repark.sql" in names
    assert any(name.startswith("F.") for name in names)
    assert any(name.startswith("DataFrame.") for name in names)
    assert any(name.startswith("ta.") for name in names)
    assert any(name.startswith("DataFrameReader.") for name in names)
    assert any(name.startswith("SparkSession.") for name in names)
    assert "F.try_divide" in names
    assert "F.zip_with" in names
    assert "F.xpath" in names
    assert "F.unwrap_udt" in names
    assert len(rows) == 913


def test_ex_0_uncovered_name_is_red() -> None:
    """C-003: an enumerated name with no COVERS, backlog, or exception is red."""
    gate = _load_gate()
    findings = gate.coverage_findings({"F.abs"}, set(), set(), set(), 0, 0)
    assert findings == [
        "public name F.abs has no example COVERS row and is not in the backlog or exceptions"
    ]


def test_ex_0_stale_backlog_name_is_red() -> None:
    """C-004: a backlog name that is not in the inventory is red."""
    gate = _load_gate()
    findings = gate.coverage_findings({"F.abs"}, {"F.abs"}, {"gone"}, set(), 1, 0)
    assert any("backlog names gone, which is not in the inventory" in item for item in findings)


def test_ex_0_covered_name_still_in_backlog_is_red() -> None:
    """C-004: a name an example now covers cannot stay in the backlog."""
    gate = _load_gate()
    findings = gate.coverage_findings({"F.abs"}, {"F.abs"}, {"F.abs"}, set(), 1, 0)
    needle = "backlog still lists F.abs, which an example now covers"
    assert any(needle in item for item in findings)


def test_ex_0_backlog_count_mismatch_is_red() -> None:
    """C-005: backlog length must equal BACKLOG_BASELINE (ratchet down only)."""
    gate = _load_gate()
    findings = gate.coverage_findings({"F.abs"}, set(), {"F.abs"}, set(), 0, 0)
    assert any("backlog count is 1, baseline is 0" in item for item in findings)


def test_ex_0_seed_examples_declare_covers_and_leave_the_backlog() -> None:
    """C-002 / C-008: seed scripts exist, declare COVERS, and those names are not backlogged.

    pins: ex-1-class-surfaces/C-006
    """
    gate = _load_gate()
    scripts = gate.example_scripts(_REPO)
    relatives = {path.relative_to(_REPO).as_posix() for path in scripts}
    assert "docs/examples/functions/abs.py" in relatives
    assert "docs/examples/dataframe/select_filter.py" in relatives
    assert "docs/examples/io/parquet_roundtrip.py" in relatives
    assert "docs/examples/ta/sma.py" in relatives
    covered: set[str] = set()
    for script in scripts:
        names = gate.covers_from_script(script)
        assert names
        assert gate.unused_cover_names(script) == []
        covered.update(names)
    assert "F.abs" in covered
    assert "DataFrame.select" in covered
    assert "DataFrameWriter.parquet" in covered
    assert "ta.sma" in covered
    backlog = set(gate.parse_named_lines(_REPO / gate.BACKLOG_RELATIVE, kind="backlog"))
    assert covered.isdisjoint(backlog)
    assert len(backlog) == gate.BACKLOG_BASELINE
    assert gate.BACKLOG_BASELINE <= 892


def test_ex_0_exceptions_file_names_only_inventory_rows() -> None:
    """C-006: cloud exceptions are inventoried names with a reason."""
    gate = _load_gate()
    mapping = gate.parse_exceptions_file(_REPO / gate.EXCEPTIONS_RELATIVE)
    enumerated = {name for _family, name in gate.enumerate_public_surface(_REPO)}
    assert mapping
    for name, reason in mapping.items():
        assert name in enumerated
        assert reason
    assert "SparkSession.read_postgres" in mapping
    assert "DataFrameReader.jdbc" in mapping
    assert len(mapping) == gate.EXCEPTIONS_BASELINE
    assert gate.EXCEPTIONS_BASELINE == 2


def test_ex_0_execute_nonzero_is_red(tmp_path: Path) -> None:
    """C-007: a script that exits nonzero is a finding."""
    gate = _load_gate()
    failing = tmp_path / "failing_example.py"
    failing.write_text("raise SystemExit(7)\n", encoding="utf-8")
    findings = gate.execute_examples([failing])
    assert len(findings) == 1
    assert "exited 7" in findings[0]


def test_ex_0_covers_without_body_use_is_red() -> None:
    """C-002: stuffing a COVERS name the script never uses is red."""
    gate = _load_gate()
    source = (_REPO / "docs" / "examples" / "functions" / "abs.py").read_text(encoding="utf-8")
    stuffed = source.replace(
        'COVERS: list[str] = ["F.abs", "F.col", "F.lit"]',
        'COVERS: list[str] = ["F.abs", "F.col", "F.lit", "DataFrame.agg"]',
    )
    tree = ast.parse(stuffed)
    assert gate.cover_is_used("F.abs", tree)
    assert not gate.cover_is_used("DataFrame.agg", tree)


def test_ex_0_object_agg_does_not_bind_class_covers() -> None:
    """C-002 / L-001: object().agg does not cover DataFrame.agg or GroupedData.agg."""
    gate = _load_gate()
    tree = ast.parse(
        "from repark.spark import SparkSession\n"
        'COVERS: list[str] = ["DataFrame.agg", "GroupedData.agg"]\n'
        "def main() -> None:\n"
        "    object().agg\n"
        "    None.agg\n"
    )
    assert not gate.cover_is_used("DataFrame.agg", tree)
    assert not gate.cover_is_used("GroupedData.agg", tree)


def test_ex_0_repark_sql_does_not_bind_spark_session_sql() -> None:
    """C-002 / L-001: repark.sql() does not cover SparkSession.sql."""
    gate = _load_gate()
    tree = ast.parse((_REPO / "docs" / "examples" / "session" / "sql.py").read_text())
    assert gate.cover_is_used("repark.sql", tree)
    assert not gate.cover_is_used("SparkSession.sql", tree)


def test_ex_0_run_gate_rejects_stuffed_covers(tmp_path: Path) -> None:
    """C-002 / Q-001: run_gate on a scratch tree reports unused COVERS and exits 1."""
    gate = _load_gate()
    scratch = tmp_path / "repo"
    (scratch / "docs").mkdir(parents=True)
    shutil.copytree(_REPO / "docs" / "examples", scratch / "docs" / "examples")
    (scratch / "python").symlink_to(_REPO / "python", target_is_directory=True)
    abs_script = scratch / "docs" / "examples" / "functions" / "abs.py"
    abs_script.write_text(
        abs_script.read_text(encoding="utf-8").replace(
            'COVERS: list[str] = ["F.abs", "F.col", "F.lit"]',
            'COVERS: list[str] = ["F.abs", "F.col", "F.lit", "DataFrame.agg"]',
        ),
        encoding="utf-8",
    )
    backlog = scratch / "docs" / "examples" / "backlog.txt"
    backlog.write_text(
        "".join(
            line
            for line in backlog.read_text(encoding="utf-8").splitlines(True)
            if line.strip() != "DataFrame.agg"
        ),
        encoding="utf-8",
    )
    captured = io.StringIO()
    with contextlib.redirect_stderr(captured):
        exit_code = gate.run_gate(
            scratch,
            write_snapshot=False,
            skip_execute=True,
            require_execute=False,
            backlog_baseline=gate.BACKLOG_BASELINE - 1,
        )
    assert exit_code == 1
    text = captured.getvalue()
    assert "never uses" in text
    assert "DataFrame.agg" in text


def test_ex_0_exceptions_baseline_mismatch_is_red() -> None:
    """C-006: exceptions length must equal EXCEPTIONS_BASELINE."""
    gate = _load_gate()
    findings = gate.coverage_findings(
        {"F.abs", "SparkSession.read_postgres"},
        set(),
        {"F.abs"},
        {"SparkSession.read_postgres"},
        1,
        0,
    )
    assert any("exceptions count is 1, baseline is 0" in item for item in findings)


def test_ex_0_exception_for_covered_name_is_red() -> None:
    """C-006: a name an example covers cannot stay in exceptions."""
    gate = _load_gate()
    findings = gate.coverage_findings({"F.abs"}, {"F.abs"}, set(), {"F.abs"}, 0, 1)
    needle = "exceptions still lists F.abs, which an example now covers"
    assert any(needle in item for item in findings)


def test_ex_0_makefile_wires_the_target_into_ci() -> None:
    """C-009: make ci depends on check-example-coverage; the wrapper uses isolated Python.

    pins: ex-1-class-surfaces/C-007
    """
    makefile = _MAKEFILE.read_text(encoding="utf-8")
    assert "check-example-coverage" in makefile
    ci_line = next(line for line in makefile.splitlines() if line.startswith("ci:"))
    assert "check-example-coverage" in ci_line
    wrapper = _WRAPPER.read_text(encoding="utf-8")
    assert 'exec python3 -I "$repo_root/scripts/check_example_coverage.py" "$@"' in wrapper
    help_run = subprocess.run(
        [str(_WRAPPER), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert help_run.returncode == 0
    assert "--require-execute" in help_run.stdout + help_run.stderr
    gate_source = _GATE.read_text(encoding="utf-8")
    assert "skipping example execution" in gate_source
    workflow = (_REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "check_example_coverage.sh" in workflow
    wheels = (_REPO / ".github" / "workflows" / "wheels.yml").read_text(encoding="utf-8")
    assert "--require-execute" in wheels
    assert "python -I scripts/check_example_coverage.py --require-execute" in wheels


def test_ex_0_execute_child_drops_python_path_overrides(monkeypatch: MonkeyPatch) -> None:
    """C-007 / SEC-001: example children cannot inherit PYTHONPATH/PYTHONHOME."""
    gate = _load_gate()
    monkeypatch.setenv("PYTHONPATH", "/tmp/bogus-repark")
    monkeypatch.setenv("PYTHONSTARTUP", "/tmp/startup")
    monkeypatch.setenv("PYTHONHOME", "/tmp/home")
    env = gate.execution_environment()
    assert "PYTHONPATH" not in env
    assert "PYTHONSTARTUP" not in env
    assert "PYTHONHOME" not in env


def test_ex_0_no_product_code_in_the_gate_script() -> None:
    """C-010: the gate does not import engine crates or edit product modules.

    pins: ex-1-class-surfaces/C-008
    """
    source = _GATE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".", 1)[0])
    assert "repark" not in imported
    assert "crates" not in source


def test_ex_1_class_surfaces_are_enumerated_with_their_counts() -> None:
    """EX-1 C-001: the seven ruled surfaces are in the inventory with their measured counts.

    pins: ex-1-class-surfaces/C-001
    """
    gate = _load_gate()
    rows = gate.enumerate_public_surface(_REPO)
    families: dict[str, int] = {}
    for family, _name in rows:
        families[family] = families.get(family, 0) + 1
    assert families["column"] == 40
    assert families["window"] == 22
    assert families["catalog"] == 28
    assert families["types"] == 32
    assert families["ml"] == 28
    names = {name for _family, name in rows}
    assert "Column.alias" in names
    assert "Window.partitionBy" in names
    assert "WindowSpec.rowsBetween" in names
    assert "Catalog.listTables" in names
    assert "Row.asDict" in names
    assert "types.StringType" in names
    assert "ml.Pipeline" in names
    assert "Column.__add__" not in names
    assert "Column._native" not in names


def test_ex_1_new_surfaces_reuse_the_existing_tables() -> None:
    """EX-1 C-002: class surfaces are CLASS_SURFACES rows, module surfaces MODULE_SURFACES rows.

    pins: ex-1-class-surfaces/C-002
    """
    gate = _load_gate()
    class_rows = {(row[0], row[1]) for row in gate.CLASS_SURFACES}
    assert ("column", "Column") in class_rows
    assert ("window", "Window") in class_rows
    assert ("window", "WindowSpec") in class_rows
    assert ("catalog", "Catalog") in class_rows
    assert ("types", "Row") in class_rows
    module_rows = {(row[0], row[1]) for row in gate.MODULE_SURFACES}
    assert module_rows == {("ta", "ta"), ("types", "types"), ("ml", "ml")}
    for _family, _prefix, relative in gate.MODULE_SURFACES:
        assert (_REPO / relative).is_file()
    for _family, _prefix, relative, class_name, _nested in gate.CLASS_SURFACES:
        assert (_REPO / relative).is_file()
        assert class_name


def test_ex_1_missing_class_or_all_is_a_hard_error(tmp_path: Path) -> None:
    """EX-1 C-002: shape drift on a surface raises, it never silently skips.

    pins: ex-1-class-surfaces/C-002
    """
    gate = _load_gate()
    empty = tmp_path / "empty.py"
    empty.write_text("x = 1\n", encoding="utf-8")
    tree = gate.parse_source(empty)
    try:
        gate.class_def(tree, "Column", where="empty.py")
    except RuntimeError as error:
        assert "class Column is missing" in str(error)
    else:
        raise AssertionError("a missing class must raise")
    try:
        gate.dunder_all(tree, where="empty.py")
    except RuntimeError as error:
        assert "no module-level __all__" in str(error)
    else:
        raise AssertionError("a missing __all__ must raise")


def test_ex_1_module_doors_are_cross_checked_against_live_all() -> None:
    """EX-1 C-003: every module door is live-cross-checked, so a dynamic table cannot hide.

    pins: ex-1-class-surfaces/C-003
    """
    gate = _load_gate()
    checked = {module for _prefix, module in gate.LIVE_ALL_MODULES}
    assert checked == {
        "repark.spark.functions",
        "repark.spark.ta",
        "repark.spark.types",
        "repark.spark.ml",
    }
    prefixes = {prefix for prefix, _module in gate.LIVE_ALL_MODULES}
    assert {prefix for _family, prefix, _relative in gate.MODULE_SURFACES} <= {
        prefix.rstrip(".") for prefix in prefixes
    }
    sources = [gate.TYPES_SOURCE, gate.ML_SOURCE, gate.CATALOG_SOURCE, gate.ROW_SOURCE]
    sources.extend(row[2] for row in gate.CLASS_SURFACES if row[0] in {"column", "window"})
    for source in sources:
        text = (_REPO / source).read_text(encoding="utf-8")
        assert "setattr(" not in text
        assert "install_into" not in text
        assert "globals()[" not in text


def test_ex_1_class_surface_binds_only_on_a_classified_receiver() -> None:
    """EX-1 C-004: Window binds on its class root, WindowSpec / Column / Row on a local.

    pins: ex-1-class-surfaces/C-004
    """
    gate = _load_gate()
    window = ast.parse(
        "from repark.spark.window import Window\n"
        'COVERS: list[str] = ["Window.partitionBy", "WindowSpec.rowsBetween"]\n'
        'spec = Window.partitionBy("g")\n'
        "print(spec.rowsBetween(-1, 1))\n"
    )
    assert gate.cover_is_used("Window.partitionBy", window)
    assert gate.cover_is_used("WindowSpec.rowsBetween", window)
    assert not gate.cover_is_used("Window.orderBy", window)
    row = ast.parse(
        "from repark.spark import SparkSession\n"
        'COVERS: list[str] = ["Row.asDict"]\n'
        "spark = SparkSession.builder.getOrCreate()\n"
        "frame = spark.range(1)\n"
        "print(frame.collect()[0].asDict())\n"
    )
    assert gate.cover_is_used("Row.asDict", row)
    unrooted = ast.parse(
        'COVERS: list[str] = ["Column.alias"]\n'
        "def main() -> None:\n"
        "    object().alias\n"
        "    object().partitionBy\n"
        "    object().asDict\n"
    )
    assert not gate.cover_is_used("Column.alias", unrooted)
    assert not gate.cover_is_used("Window.partitionBy", unrooted)
    assert not gate.cover_is_used("Row.asDict", unrooted)


def test_ex_1_module_surface_binds_only_on_its_own_door() -> None:
    """EX-1 C-005: a types cover does not bind on the ml door, and the reverse.

    pins: ex-1-class-surfaces/C-005
    """
    gate = _load_gate()
    types_alias = ast.parse(
        "from repark.spark import types as T\n"
        'COVERS: list[str] = ["types.StringType"]\n'
        "print(T.StringType())\n"
    )
    assert gate.cover_is_used("types.StringType", types_alias)
    types_import = ast.parse(
        "from repark.spark.types import StringType\n"
        'COVERS: list[str] = ["types.StringType"]\n'
        "print(StringType())\n"
    )
    assert gate.cover_is_used("types.StringType", types_import)
    ml_alias = ast.parse(
        "from repark.spark import ml\n"
        'COVERS: list[str] = ["ml.Pipeline"]\n'
        "print(ml.Pipeline(stages=[]))\n"
    )
    assert gate.cover_is_used("ml.Pipeline", ml_alias)
    crossed = ast.parse(
        "from repark.spark import ml\n"
        'COVERS: list[str] = ["types.StringType"]\n'
        "print(ml.StringType())\n"
    )
    assert not gate.cover_is_used("types.StringType", crossed)
    reversed_cross = ast.parse(
        "from repark.spark import types as T\n"
        'COVERS: list[str] = ["ml.Pipeline"]\n'
        "print(T.Pipeline())\n"
    )
    assert not gate.cover_is_used("ml.Pipeline", reversed_cross)


def test_ex_1_every_new_name_is_in_the_backlog() -> None:
    """EX-1 C-006: the widening covers nothing; every new name is a backlog row.

    pins: ex-1-class-surfaces/C-006
    """
    gate = _load_gate()
    rows = gate.enumerate_public_surface(_REPO)
    backlog = set(gate.parse_named_lines(_REPO / gate.BACKLOG_RELATIVE, kind="backlog"))
    widened = {"column", "window", "catalog", "ml"}
    for family, name in rows:
        if family in widened or name.startswith("types.") or name.startswith("Row."):
            assert name in backlog, name
    assert len(backlog) == gate.BACKLOG_BASELINE
