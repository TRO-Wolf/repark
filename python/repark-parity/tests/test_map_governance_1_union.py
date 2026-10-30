"""MAP-GOVERNANCE-1: the union binding model, explicit-id disclaimers, own-bin floors, and the
multi-line definition shapes."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest
from test_dl_2_ledger_grammar import _ATTESTATION
from test_map_governance_1 import (
    _GRAMMAR,
    _HEADER,
    _LEDGER,
    _MAP,
    _REPO,
    _ROWS,
    _SIDECAR,
    _SIDECAR_TEXT,
    _TEST,
    _UNIT,
    _git,
    _grammar,
    _migrate,
    _scratch_map_repo,
    _write,
    seed_repo,
)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    return seed_repo(tmp_path)


def test_an_id_less_disclaimer_keeps_the_binding_and_is_reported(tmp_path: Path) -> None:
    text = "\n".join(
        [
            "# map",
            "",
            f"- `pins.rs` — the battery, red then green. pins: {_UNIT}/C-001",
            "  The measurement of C-002 (two reads; then success) is cited here so the grammar"
            " gate counts it; this file pins none of it.",
            f"  pins: {_UNIT}/C-002",
            "  Their clause citations below stay so the grammar gate counts the merged ledger.",
            f"  pins: {_UNIT}/C-003",
            "",
        ]
    )
    repo = _scratch_map_repo(tmp_path, text, {_TEST: "#[test]\nfn demo_holds() {}\n"})
    report = _migrate(repo, "--check", "--report", "hand-candidates")
    assert "hand candidates: 1" in report.stdout
    assert f"  {_UNIT}/C-003\t{_TEST}\t{_MAP}:7\t" in report.stdout
    assert _migrate(repo).returncode == 0
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}",
        "C-002: none (records)",
        f"C-003: {_TEST}",
    ]


def test_a_row_named_test_file_is_added_beside_the_citations_file(tmp_path: Path) -> None:
    other = "crates/demo/tests/other_battery.rs"
    rows = _ROWS.replace(
        "| C-002 | The other thing holds. | A pin. | PROVEN | the battery |",
        "| C-002 | The other thing holds. | A pin. | PROVEN | `src/engine.rs`, "
        "`other_battery.rs::other_holds` and `demo_holds` |",
    )
    text = f"# map\n\n- `pins.rs` — the battery. pins: {_UNIT}/C-001, C-002\n"
    files = {
        _TEST: "#[test]\nfn demo_holds() {}\n",
        other: "#[test]\nfn other_holds() {}\n",
        "crates/demo/src/engine.rs": "pub fn run() {}\n",
    }
    repo = _scratch_map_repo(tmp_path, text, files)
    _write(repo, _LEDGER, rows)
    assert _migrate(repo).returncode == 0
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}",
        f"C-002: {other}::other_holds, {_TEST}::demo_holds",
    ]


def test_a_sidecar_in_another_bin_does_not_satisfy_the_floor(repo: Path) -> None:
    completed = f"task/ledgers/completed/{_UNIT}"
    _write(repo, f"{completed}-ledger.md", _ROWS)
    _write(repo, f"{completed}-pins.md", _SIDECAR_TEXT)
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo_holds\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{_LEDGER}: 1 PROVEN clause(s) with no binding (ceiling 0): C-002" in result.stderr


def test_more_shell_rust_and_decorator_shapes_define_nothing(repo: Path) -> None:
    script = "scripts/check_more.sh"
    body = (
        "cat <<\\EOF\nphantom_bs() {\nEOF\n"
        "cat <<'Q'\nphantom_sq() {\nQ\n"
        'cat <<"D"\nphantom_dq() {\nD\n'
        'echo "multi\nphantom_ml() {\n"\n'
        "x=$((1<<3))\nreal_after() {\n  :\n}\n"
    )
    _write(repo, script, body)
    rust = "crates/demo/tests/nested.rs"
    _write(repo, rust, "/* outer\n/* inner */\nfn phantom_nested() {}\n*/\n#[test]\nfn real() {}\n")
    for path, name in (
        (script, "phantom_bs"),
        (script, "phantom_sq"),
        (script, "phantom_dq"),
        (script, "phantom_ml"),
        (rust, "phantom_nested"),
    ):
        _write(repo, _SIDECAR, _HEADER + f"C-001: {path}::{name}\nC-002: {_TEST}\n")
        result = _grammar(repo)
        assert result.returncode == 1, name
        assert f"C-001 names `{name}`, which {path} does not define" in result.stderr
    _write(repo, _SIDECAR, _HEADER + f"C-001: {script}::real_after, {rust}::real\nC-002: {_TEST}\n")
    assert _grammar(repo).returncode == 0
    used = "crates/demo/tests/test_used.py"
    _write(
        repo, used, "import pytest\n\n\n@pytest.mark.usefixtures('db')\ndef helper():\n    pass\n"
    )
    _write(repo, _SIDECAR, _HEADER + f"C-001: {used}::helper\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"C-001 binds {used}, which is not a test file or a script" in result.stderr


_ITEM_TEST = "#[test]\nfn demo_holds() {}\n#[test]\nfn sub_case_holds() {}\n"


def _rows(*cells: str) -> str:
    head = (
        "| Clause | Proposition | Proof obligation | Verdict | Evidence |\n|---|---|---|---|---|\n"
    )
    return (
        head
        + "".join(
            f"| C-00{n} | A clause. | A pin. | PROVEN | {cell} |\n"
            for n, cell in enumerate(cells, 1)
        )
        + "| C-009 | Open. | Needs work. | OPEN | waiting |\n"
    )


def test_nested_shapes_bind_the_enclosing_items_file(tmp_path: Path) -> None:
    text = "\n".join(
        [
            "# map",
            "",
            "- `pins.rs` — the battery.",
            f"  **ROUND-2 (2026-10-05):** a later round. pins: {_UNIT}/C-001, C-002",
            f"  - `sub_case_holds` covers the sub case. pins: {_UNIT}/C-003",
            f"  - a sub-bullet with prose only. pins: {_UNIT}/C-004",
            "",
        ]
    )
    rows = _rows("`demo_holds` is green", "measured", "measured", "`sub_case_holds` too")
    repo = _scratch_map_repo(tmp_path, text, {_TEST: _ITEM_TEST})
    _write(repo, _LEDGER, rows)
    assert _migrate(repo).returncode == 0
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}::demo_holds",
        "C-002: none (records)",
        f"C-003: {_TEST}::sub_case_holds",
        f"C-004: {_TEST}::sub_case_holds",
    ]


def test_a_row_named_test_binds_by_intent_and_ambiguity_is_reported(tmp_path: Path) -> None:
    second = "crates/demo/tests/second.rs"
    files = {
        _TEST: _ITEM_TEST,
        second: "#[test]\nfn shared_name() {}\n",
        "crates/demo/tests/third.rs": "#[test]\nfn shared_name() {}\n",
    }
    text = f"# map\n\nNothing cited here. pins: {_UNIT}/C-003\n"
    repo = _scratch_map_repo(tmp_path, text, files)
    _write(repo, _LEDGER, _rows("`demo_holds`", "`shared_name`", "measured"))
    _write(repo, _SIDECAR, _HEADER + "C-001: none (records)\n")
    report = _migrate(repo, "--check", "--report", "hand-candidates")
    assert (
        f"  {_UNIT}/C-002\tshared_name\tcrates/demo/tests/second.rs, crates/demo/tests/third.rs"
        "\tambiguous row-named test"
    ) in report.stdout
    assert _migrate(repo).returncode == 0
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}::demo_holds",
        "C-003: none (records)",
    ]


def test_the_gate_reports_records_whose_row_names_a_defined_test(repo: Path) -> None:
    _write(repo, _LEDGER, _rows("`demo_holds`", "measured"))
    _write(repo, _SIDECAR, _HEADER + "C-001: none (records)\nC-002: none (records)\n")
    _git(repo, "add", "-A")
    result = subprocess.run(
        [sys.executable, str(_GRAMMAR), "--repo", str(repo), "--report", "row-named-records"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert (
        f"{_LEDGER}: C-001 row names `demo_holds`, which only {_TEST} defines, and no binding"
        f" names it: add `{_TEST}::demo_holds` to the clause's sidecar line"
    ) in result.stderr
    assert "row-named records: 1 (0 ambiguous)" in result.stdout
    assert f"  {_UNIT}/C-001\tdemo_holds\t{_TEST}" in result.stdout


def test_heredoc_herestring_escape_and_rust_comment_shapes(repo: Path) -> None:
    script = "scripts/check_shapes.sh"
    body = (
        "cat <<1EOF\nphantom_digit() {\n1EOF\n"
        "cat <<END-X\nphantom_hyphen() {\nEND-X\n"
        'grep x <<< "$v"\nreal_after_herestring() {\n  :\n}\n'
        "echo it\\'s\nreal_after_escape() {\n  :\n}\n"
    )
    _write(repo, script, body)
    rust = "crates/demo/tests/comments.rs"
    _write(
        repo,
        rust,
        '/* a " quote and // slashes */\n#[test]\nfn real_one() {}\n'
        'const S: &str = c"multi\nfn phantom_cstring() {}\n";\n#[test]\nfn real_two() {}\n',
    )
    for path, name in (
        (script, "phantom_digit"),
        (script, "phantom_hyphen"),
        (rust, "phantom_cstring"),
    ):
        _write(repo, _SIDECAR, _HEADER + f"C-001: {path}::{name}\nC-002: {_TEST}\n")
        result = _grammar(repo)
        assert result.returncode == 1, name
        assert f"C-001 names `{name}`, which {path} does not define" in result.stderr
    good = (
        f"{script}::real_after_herestring, {script}::real_after_escape, "
        f"{rust}::real_one, {rust}::real_two"
    )
    _write(repo, _SIDECAR, _HEADER + f"C-001: {good}\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr


def test_a_double_backtick_span_and_a_parametrize_id_still_bind(tmp_path: Path) -> None:
    pytest_file = "crates/demo/tests/test_cells.py"
    files = {
        _TEST: _ITEM_TEST,
        pytest_file: "def test_option_cell_values():\n    pass\n",
    }
    text = f"# map\n\nNothing named here. pins: {_UNIT}/C-003\n"
    repo = _scratch_map_repo(tmp_path, text, files)
    _write(
        repo,
        _LEDGER,
        _rows(
            "the message ``a `quoted` word`` then `demo_holds`",
            "`test_option_cell_values[COLL-00..08]` and `test_option_cell_values[*]`",
            "measured",
        ),
    )
    assert _migrate(repo).returncode == 0
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}::demo_holds",
        f"C-002: {pytest_file}::test_option_cell_values",
        "C-003: none (records)",
    ]
    _write(repo, _SIDECAR, _HEADER + "C-001: none (records)\nC-002: none (records)\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"C-002 row names `test_option_cell_values`, which only {pytest_file} defines" in (
        result.stderr
    )


def test_two_regenerations_are_byte_identical(tmp_path: Path) -> None:
    other = "crates/demo/tests/other.rs"
    text = f"# map\n\n- `pins.rs` — the battery. pins: {_UNIT}/C-001, C-002\n"
    outputs = []
    for attempt in ("first", "second"):
        (tmp_path / attempt).mkdir()
        repo = _scratch_map_repo(
            tmp_path / attempt, text, {_TEST: _ITEM_TEST, other: "#[test]\nfn other_holds() {}\n"}
        )
        _write(repo, _LEDGER, _rows("`other.rs` and `demo_holds`", "measured"))
        _write(repo, _SIDECAR, _HEADER + "C-001: none (records)\n")
        assert _migrate(repo).returncode == 0
        again = _migrate(repo)
        assert "(0 written by this run)" in again.stdout
        outputs.append((repo / _SIDECAR).read_text(encoding="utf-8"))
    assert outputs[0] == outputs[1]
    assert outputs[0].splitlines()[2:] == [
        f"C-001: {other}, {_TEST}::demo_holds",
        f"C-002: {_TEST}",
    ]


def test_arithmetic_ansi_quotes_extern_macro_and_cfg_shapes(repo: Path) -> None:
    script = "scripts/check_arith.sh"
    body = (
        "(( x << 2 ))\nreal_after_arith() {\n  :\n}\necho $'it\\'s'\nreal_after_ansi() {\n  :\n}\n"
    )
    _write(repo, script, body)
    rust = "crates/demo/tests/shapes.rs"
    _write(
        repo,
        rust,
        'extern "C" fn real_extern() {}\n'
        "macro_rules! battery {\n    () => {\n        fn phantom_macro() {}\n    };\n}\n"
        "#[cfg(any())]\nfn phantom_cfg() {}\n#[test]\nfn real_after() {}\n",
    )
    for path, name in ((rust, "phantom_macro"), (rust, "phantom_cfg")):
        _write(repo, _SIDECAR, _HEADER + f"C-001: {path}::{name}\nC-002: {_TEST}\n")
        result = _grammar(repo)
        assert result.returncode == 1, name
        assert f"C-001 names `{name}`, which {path} does not define" in result.stderr
    good = (
        f"{script}::real_after_arith, {script}::real_after_ansi, "
        f"{rust}::real_extern, {rust}::battery, {rust}::real_after"
    )
    _write(repo, _SIDECAR, _HEADER + f"C-001: {good}\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr


def test_a_row_named_test_binds_by_name_and_a_rename_goes_red(repo: Path) -> None:
    _write(repo, _LEDGER, _rows("`demo_holds`", "measured"))
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"C-001 row names `demo_holds`, which only {_TEST} defines" in result.stderr
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo_holds\nC-002: {_TEST}\n")
    assert _grammar(repo).returncode == 0
    _write(repo, _TEST, "#[test]\nfn demo_holds_renamed() {}\nfn other_holds() {}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"C-001 names `demo_holds`, which {_TEST} does not define" in result.stderr
    cited = "#[test]\nfn demo_holds() {}\n" + f'const PINS: &str = "pins: {_UNIT}/C-001";\n'
    _write(repo, _TEST, cited)
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}\nC-002: {_TEST}\n")
    assert _grammar(repo).returncode == 0


def test_a_reading_unit_skips_the_row_named_rule(repo: Path) -> None:
    reading = "task/ledgers/completed/mg-demo-reading-ledger.md"
    text = "\n".join(
        [
            "# Unit ledger — demo reading",
            "",
            "**Unit:** demo · **Path:** READING",
            "",
            "| Clause | Proposition | Verdict | Evidence |",
            "|---|---|---|---|",
            "| C-001 | Line counts by crate, tests split into `tests/` and `demo_holds` | PROVEN"
            " | docs: docs/audit.md#counts |",
            "",
        ]
    )
    _write(repo, reading, text + _ATTESTATION)
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr
    _write(repo, reading, text.replace("**Path:** READING", "**Path:** BUILD") + _ATTESTATION)
    result = _grammar(repo)
    assert result.returncode == 1
    assert "C-001 row names `demo_holds`" in result.stderr


def test_a_backslash_escaped_backtick_opens_no_span(repo: Path) -> None:
    text_module = _REPO / "scripts" / "migrate_pins_text.py"
    spec = importlib.util.spec_from_file_location("text", text_module)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.code_spans("a \\`b` c `d`") == ["c"]
    assert module.code_spans("`a\\`") == ["a\\"]
    assert module.code_spans("\\\\`x`") == ["x"]
    assert module.code_spans("\\``x``") == []
    _write(repo, _LEDGER, _rows("\\`demo_holds\\`", "measured"))
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr
