"""MAP-GOVERNANCE-1: the pin binding lives in ledger sidecars the grammar gate verifies."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
_GRAMMAR = _REPO / "scripts" / "check_ledger_grammar.py"
_STAGING = "task/ledgers/staging"
_UNIT = "mg-demo-charter"
_LEDGER = f"{_STAGING}/{_UNIT}-ledger.md"
_SIDECAR = f"{_STAGING}/{_UNIT}-pins.md"
_TEST = "crates/demo/tests/pins.rs"
_HEADER = f"# {_UNIT} pin binding, read by scripts/check_ledger_grammar.py rule B\n\n"
_SIDECAR_TEXT = _HEADER + f"C-001: {_TEST}::demo_holds\nC-002: {_TEST}::other_holds, {_TEST}\n"

_ROWS = """# Charter

| Clause | Proposition | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The thing holds. | A pin. | PROVEN | `pins.rs` |
| C-002 | The other thing holds. | A pin. | PROVEN | `pins.rs` |
| C-003 | Not yet known. | Needs the module. | OPEN | waiting |
"""


def _exceptions() -> dict[str, tuple[int, bool]]:
    spec = importlib.util.spec_from_file_location("check_ledger_grammar", _GRAMMAR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return dict(module.EXCEPTIONS)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, check=True, text=True
    ).stdout


def _write(repo: Path, path: str, text: str) -> None:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _grammar(repo: Path) -> subprocess.CompletedProcess[str]:
    _git(repo, "add", "-A")
    return subprocess.run(
        [sys.executable, str(_GRAMMAR), "--repo", str(repo)],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    _git(tmp_path, "init", "-q", "-b", "main")
    for name, (ceiling, _governed) in _exceptions().items():
        rows = "".join(f"| C-{n:03d} | legacy | x | PROVEN | x |\n" for n in range(1, ceiling + 1))
        _write(tmp_path, f"{_STAGING}/{name}", "# legacy\n\n" + rows)
    _write(tmp_path, _LEDGER, _ROWS)
    _write(tmp_path, _TEST, "#[test]\nfn demo_holds() {}\nfn other_holds() {}\n")
    _write(tmp_path, _SIDECAR, _SIDECAR_TEXT)
    return tmp_path


def test_sidecar_binding_pins_proven_clauses(repo: Path) -> None:
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr
    assert "2 pinned clause ids" in result.stdout


def test_sidecar_path_only_binding_counts(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr


def test_unbound_proven_clause_goes_red(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo_holds\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert "1 PROVEN clause(s) with no binding (ceiling 0): C-002 — add a" in result.stderr


def test_name_missing_from_the_file_goes_red(repo: Path) -> None:
    _write(repo, _TEST, "#[test]\nfn demo_holds() {}\nfn other_holds_renamed() {}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{_SIDECAR}:4: C-002 names `other_holds`, which {_TEST} does not define" in (
        result.stderr
    )


def test_name_must_be_a_whole_token(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo\nC-002: {_TEST}::other_holds\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"C-001 names `demo`, which {_TEST} does not define" in result.stderr


def test_a_deleted_test_whose_name_survives_in_a_docstring_goes_red(repo: Path) -> None:
    script = "crates/demo/tests/test_demo.py"
    _write(
        repo,
        script,
        '"""Covers test_demo_holds and test_other."""\n\n\ndef test_other():\n    pass\n',
    )
    _write(
        repo, _SIDECAR, _HEADER + f"C-001: {script}::test_other\nC-002: {script}::test_demo_holds\n"
    )
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"C-002 names `test_demo_holds`, which {script} does not define" in result.stderr
    assert "C-001 names" not in result.stderr


def test_unbindable_paths_go_red(repo: Path) -> None:
    source = "crates/demo/src/lib.rs"
    _write(repo, source, "fn demo_holds() {}\n")
    _write(repo, "crates/demo/tests/map.md", "# map\n")
    _write(repo, "README.md", "# demo\n")
    for path in (source, "crates/demo/tests/map.md", "README.md", _LEDGER, _SIDECAR):
        _write(repo, _SIDECAR, _HEADER + f"C-001: {path}\nC-002: {_TEST}\n")
        result = _grammar(repo)
        assert result.returncode == 1, path
        assert f"C-001 binds {path}, which is not a test file or a script" in result.stderr


def test_a_script_is_bindable(repo: Path) -> None:
    _write(repo, "scripts/check_demo.py", "def main():\n    pass\n")
    _write(repo, _SIDECAR, _HEADER + f"C-001: scripts/check_demo.py::main\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr


def test_records_form_counts_and_is_listed(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: none (records)\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr
    assert "1 clause(s) bound to `none (records)`, no test pins them" in result.stdout
    assert f"  {_UNIT}: C-001\n" in result.stdout
    _write(repo, _SIDECAR, _HEADER + f"C-001: none (records), {_TEST}\nC-002: {_TEST}\n")
    mixed = _grammar(repo)
    assert mixed.returncode == 1
    assert "C-001 `none (records)` must be the only entry" in mixed.stderr


def test_a_sidecar_counts_only_beside_its_own_ledger(repo: Path) -> None:
    stray = f"task/ledgers/completed/{_UNIT}-pins.md"
    _write(repo, stray, _SIDECAR_TEXT)
    (repo / _SIDECAR).unlink()
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{stray}: no ledger {_UNIT}-ledger.md beside it; it binds nothing" in result.stderr
    assert "2 PROVEN clause(s) with no binding (ceiling 0): C-001, C-002" in result.stderr


def test_an_unreadable_sidecar_exits_2(repo: Path) -> None:
    _git(repo, "add", "-A")
    (repo / _SIDECAR).unlink()
    gone = subprocess.run(
        [sys.executable, str(_GRAMMAR), "--repo", str(repo)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert gone.returncode == 2
    assert "tracked sidecar is unreadable" in gone.stderr
    (repo / _SIDECAR).write_bytes(b"C-001: \xff\n")
    garbled = subprocess.run(
        [sys.executable, str(_GRAMMAR), "--repo", str(repo)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert garbled.returncode == 2
    assert "tracked sidecar is unreadable" in garbled.stderr


def test_untracked_path_goes_red(repo: Path) -> None:
    gone = "crates/demo/tests/gone.rs"
    _write(repo, _SIDECAR, _HEADER + f"C-001: {gone}::demo_holds\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{_SIDECAR}:3: C-001 binds {gone}, which is not a tracked file" in result.stderr


def test_missing_clause_goes_red(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}\nC-002: {_TEST}\nC-009: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{_SIDECAR}:5: C-009 names no clause of {_UNIT} in any ledger" in result.stderr


def test_malformed_and_duplicate_lines_go_red(repo: Path) -> None:
    _write(
        repo,
        _SIDECAR,
        _HEADER
        + f"C-001: {_TEST}\nC-002: {_TEST}\nC-002: {_TEST}\nloose prose\nC-001:\n"
        + f"C-003:{_TEST}\nC-001: {_TEST}, {_TEST}\n",
    )
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{_SIDECAR}:5: C-002 is bound twice" in result.stderr
    assert f"{_SIDECAR}:6: not a binding line" in result.stderr
    assert f"{_SIDECAR}:7: not a binding line" in result.stderr
    assert f"{_SIDECAR}:8: not a binding line" in result.stderr
    assert f"{_SIDECAR}:9: C-001 repeats an entry" in result.stderr


def test_malformed_name_goes_red(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::not a name\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"{_SIDECAR}:3: C-001 entry `{_TEST}::not a name` is not <path> or <path>::<name>" in (
        result.stderr
    )


def test_archive_sidecar_strips_the_date_prefix(repo: Path) -> None:
    archived = "task/ledgers/archive/2026-08/2026-08-20-old-unit"
    _write(repo, f"{archived}-ledger.md", "| C-001 | Old. | x | PROVEN | x |\n")
    _write(repo, f"{archived}-pins.md", f"# old-unit pin binding\n\nC-001: {_TEST}\n")
    assert _grammar(repo).returncode == 0
    _write(repo, f"{archived}-pins.md", f"# old-unit pin binding\n\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert "C-002 names no clause of old-unit in any ledger" in result.stderr


def test_floor_keeps_its_meaning(repo: Path) -> None:
    for name, (ceiling, _governed) in _exceptions().items():
        if not ceiling:
            continue
        sidecar = f"{_STAGING}/{name.removesuffix('-ledger.md')}-pins.md"
        _write(repo, sidecar, f"# pins\n\nC-001: {_TEST}\n")
        result = _grammar(repo)
        assert result.returncode == 1
        assert f"ceiling {ceiling} is above the measured {ceiling - 1}" in result.stderr


def test_a_citation_in_source_still_counts(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo_holds\n")
    _write(repo, _TEST, f"// pins: {_UNIT}/C-002\n#[test]\nfn demo_holds() {{}}\n")
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr


def test_a_citation_in_a_map_no_longer_counts(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo_holds\n")
    _write(repo, "crates/demo/tests/map.md", f"# map\n\n- `pins.rs` pins: {_UNIT}/C-002\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert "1 PROVEN clause(s) with no binding (ceiling 0): C-002 — add a" in result.stderr


_MAP = "crates/demo/tests/map.md"
_MAP_TEXT = "\n".join(
    [
        "# map",
        "",
        "## Contents",
        "- [`pins.rs`](pins.rs) — the battery: `demo_holds` and `other_holds` and `absent_name`.",
        f"  pins: {_UNIT}/C-001",
        f"- `other.rs` — the second file, `other_holds` again. pins: {_UNIT}/C-002",
        f"- a row naming no file (pins: {_UNIT}/C-001). More prose.",
        f"- a row with a range. pins: {_UNIT}/C-001..C-002",
        "",
        "## Notes",
        f"Loose prose with no file. pins: {_UNIT}/C-002",
        "",
    ]
)


def _migrate(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    scripts = repo / "scripts"
    scripts.mkdir(exist_ok=True)
    for name in ("check_ledger_grammar.py", "migrate_pins_to_sidecars.py"):
        (scripts / name).write_text(
            (_REPO / "scripts" / name).read_text(encoding="utf-8"), encoding="utf-8"
        )
    _git(repo, "add", "-A")
    return subprocess.run(
        [sys.executable, str(scripts / "migrate_pins_to_sidecars.py"), "--repo", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def mapped(repo: Path) -> Path:
    (repo / _SIDECAR).unlink()
    _write(repo, "crates/demo/tests/other.rs", "#[test]\nfn other_holds() {}\n")
    _write(repo, _MAP, _MAP_TEXT)
    return repo


def test_migration_binds_rows_and_cuts_the_citations(mapped: Path) -> None:
    result = _migrate(mapped)
    assert result.returncode == 0, result.stderr
    assert "unplaceable citations: 0" in result.stdout
    sidecar = (mapped / _SIDECAR).read_text(encoding="utf-8").splitlines()
    assert sidecar[0].startswith(f"# {_UNIT} pin binding")
    assert sidecar[2:] == [
        f"C-001: {_TEST}::demo_holds, {_TEST}::other_holds",
        "C-002: crates/demo/tests/other.rs::other_holds",
    ]
    text = (mapped / _MAP).read_text(encoding="utf-8")
    assert "  pins:" not in text
    assert "`other_holds` again.\n" in text
    assert "- a row naming no file. More prose.\n" in text
    assert f"- a row with a range. pins: {_UNIT}/C-001..C-002\n" in text
    assert "Loose prose with no file.\n" in text
    assert _grammar(mapped).returncode == 0


def test_migration_is_idempotent(mapped: Path) -> None:
    assert _migrate(mapped).returncode == 0
    sidecar = (mapped / _SIDECAR).read_text(encoding="utf-8")
    text = (mapped / _MAP).read_text(encoding="utf-8")
    again = _migrate(mapped)
    assert again.returncode == 0, again.stderr
    assert "maps rewritten: 0" in again.stdout
    assert "(0 written by this run)" in again.stdout
    assert _migrate(mapped, "--check").returncode == 0
    assert (mapped / _SIDECAR).read_text(encoding="utf-8") == sidecar
    assert (mapped / _MAP).read_text(encoding="utf-8") == text


def test_migration_keeps_a_hand_edited_binding(mapped: Path) -> None:
    assert _migrate(mapped).returncode == 0
    first = (mapped / _SIDECAR).read_text(encoding="utf-8").splitlines()[2]
    edited = _HEADER + f"{first}\nC-002: none (records)\n"
    _write(mapped, _SIDECAR, edited)
    again = _migrate(mapped)
    assert again.returncode == 0, again.stderr
    assert (mapped / _SIDECAR).read_text(encoding="utf-8") == edited


def test_migration_check_mode_writes_nothing(mapped: Path) -> None:
    result = _migrate(mapped, "--check")
    assert result.returncode == 1
    assert "maps rewritten: 1" in result.stdout
    assert "pending: 1 map(s) and 1 sidecar(s) would change" in result.stderr
    assert not (mapped / _SIDECAR).exists()
    assert (mapped / _MAP).read_text(encoding="utf-8") == _MAP_TEXT


def test_migration_refuses_an_unplaceable_citation(mapped: Path) -> None:
    _write(mapped, _MAP, _MAP_TEXT + f"- `pins.rs` again. pins: {_UNIT}/C-099\n")
    for args in ((), ("--check",)):
        result = _migrate(mapped, *args)
        assert result.returncode == 1
        assert "unplaceable citations: 1" in result.stdout
        assert f"pins: {_UNIT}/C-099 names no clause in any ledger" in result.stderr
        assert not (mapped / _SIDECAR).exists()


def test_migration_binds_a_source_citation_and_leaves_it(mapped: Path) -> None:
    (mapped / _MAP).unlink()
    _write(mapped, _TEST, f"// pins: {_UNIT}/C-001, C-002\n#[test]\nfn demo_holds() {{}}\n")
    assert _migrate(mapped).returncode == 0
    assert (mapped / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}",
        f"C-002: {_TEST}",
    ]
    assert (mapped / _TEST).read_text(encoding="utf-8").startswith(f"// pins: {_UNIT}/C-001")


def test_migration_names_the_archive_sidecar_by_its_ledger(mapped: Path) -> None:
    old = "old-unit"
    archived = f"task/ledgers/archive/2026-08/2026-08-20-{old}"
    _write(mapped, f"{archived}-ledger.md", "| C-001 | Old. | x | PROVEN | x |\n")
    _write(mapped, _MAP, _MAP_TEXT + f"- `pins.rs` once more. pins: {old}/C-001\n")
    assert _migrate(mapped).returncode == 0
    assert (mapped / f"{archived}-pins.md").read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}"
    ]


_SIZE = _REPO / "scripts" / "check_map_size.py"
_GUARD = _REPO / "scripts" / "check_map_md.sh"


def _size_repo(tmp_path: Path, table: str) -> Path:
    repo = tmp_path / "sized"
    _git(repo.parent, "init", "-q", "-b", "main", str(repo))
    source = _SIZE.read_text(encoding="utf-8")
    start = source.index("\nEXCEPTIONS: dict[str, tuple[int, int]] = {") + 1
    end = source.index("\n}\n", start) + 3
    _write(repo, "scripts/check_map_size.py", source[:start] + table + source[end:])
    _write(repo, "map.md", "# root\n")
    _write(repo, "big/map.md", "".join(f"- row {n}\n" for n in range(250)))
    for exempt in ("staging", "completed", "archive"):
        _write(repo, f"task/ledgers/{exempt}/map.md", f"# {exempt}\n")
    return repo


def _size(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    _git(repo, "add", "-A")
    return subprocess.run(
        [sys.executable, str(repo / "scripts" / "check_map_size.py"), "--repo", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _big_bytes() -> int:
    return len("".join(f"- row {n}\n" for n in range(250)).encode())


def _table(rows: str) -> str:
    return "EXCEPTIONS: dict[str, tuple[int, int]] = {\n" + rows + "}\n"


def test_map_over_the_line_ceiling_goes_red(tmp_path: Path) -> None:
    repo = _size_repo(tmp_path, _table(""))
    result = _size(repo)
    assert result.returncode == 1
    assert "ERROR: big/map.md is 250 lines" in result.stderr


def test_map_over_the_byte_ceiling_goes_red(tmp_path: Path) -> None:
    repo = _size_repo(tmp_path, _table(""))
    _write(repo, "big/map.md", "x" * 16385 + "\n")
    result = _size(repo)
    assert result.returncode == 1
    assert "ERROR: big/map.md is 1 lines and 16386 bytes" in result.stderr


def test_excepted_map_at_its_baseline_is_green(tmp_path: Path) -> None:
    repo = _size_repo(tmp_path, _table(f'    "big/map.md": (250, {_big_bytes()}),\n'))
    result = _size(repo)
    assert result.returncode == 0, result.stderr
    assert "5 maps clean" in result.stdout
    assert "1 exceptions, 3 exempt" in result.stdout


def test_excepted_map_that_grows_goes_red(tmp_path: Path) -> None:
    repo = _size_repo(tmp_path, _table(f'    "big/map.md": (250, {_big_bytes()}),\n'))
    _write(repo, "big/map.md", "".join(f"- row {n}\n" for n in range(250)) + "- one more\n")
    result = _size(repo)
    assert result.returncode == 1
    assert "ERROR: big/map.md grew to 251 lines" in result.stderr
    assert _size(repo, "--update-baselines").returncode == 1
    assert f'"big/map.md": (250, {_big_bytes()}),' in (
        repo / "scripts/check_map_size.py"
    ).read_text(encoding="utf-8")


def test_excepted_map_that_shrinks_ratchets_down(tmp_path: Path) -> None:
    repo = _size_repo(tmp_path, _table(f'    "big/map.md": (250, {_big_bytes()}),\n'))
    shrunk = "".join(f"- row {n}\n" for n in range(240))
    _write(repo, "big/map.md", shrunk)
    result = _size(repo)
    assert result.returncode == 1
    assert "shrank to 240 lines" in result.stderr
    assert "lower the row" in result.stderr
    updated = _size(repo, "--update-baselines")
    assert updated.returncode == 0, updated.stderr
    script = (repo / "scripts/check_map_size.py").read_text(encoding="utf-8")
    assert f'    "big/map.md": (240, {len(shrunk.encode())}),\n' in script
    assert _size(repo).returncode == 0


def test_excepted_map_within_both_ceilings_retires_its_row(tmp_path: Path) -> None:
    repo = _size_repo(tmp_path, _table(f'    "big/map.md": (250, {_big_bytes()}),\n'))
    _write(repo, "big/map.md", "# small\n")
    result = _size(repo)
    assert result.returncode == 1
    assert "delete the row" in result.stderr
    assert _size(repo, "--update-baselines").returncode == 0
    script = (repo / "scripts/check_map_size.py").read_text(encoding="utf-8")
    assert "EXCEPTIONS: dict[str, tuple[int, int]] = {}\n" in script
    assert _size(repo).returncode == 0
    assert _size(repo, "--update-baselines").returncode == 0


def test_update_refuses_a_new_violator(tmp_path: Path) -> None:
    repo = _size_repo(tmp_path, _table(""))
    result = _size(repo, "--update-baselines")
    assert result.returncode == 1
    assert "REFUSED: big/map.md: 250 lines" in result.stderr
    assert "EXCEPTIONS: dict[str, tuple[int, int]] = {\n}\n" in (
        repo / "scripts/check_map_size.py"
    ).read_text(encoding="utf-8")


def test_stale_and_hollow_rows_go_red(tmp_path: Path) -> None:
    rows = f'    "big/map.md": (250, {_big_bytes()}),\n    "gone/map.md": (300, 1),\n'
    repo = _size_repo(tmp_path, _table(rows + '    "map.md": (7, 7),\n'))
    result = _size(repo)
    assert result.returncode == 1
    assert "EXCEPTIONS key is no tracked map: gone/map.md" in result.stderr
    assert "map.md: baseline (7, 7) is within both ceilings" in result.stderr


def test_real_table_is_clean_on_the_tree() -> None:
    result = subprocess.run(
        [sys.executable, str(_SIZE)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


def test_lockstep_branch_mode_warns_with_the_advisory_note(tmp_path: Path) -> None:
    repo = tmp_path / "lockstep"
    _git(tmp_path, "init", "-q", "-b", "main", str(repo))
    _write(repo, "pkg/mod.rs", "fn a() {}\n")
    _write(repo, "pkg/map.md", "# pkg\n")
    _git(repo, "add", "-A")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "base")
    base = _git(repo, "rev-parse", "HEAD").strip()
    _write(repo, "pkg/mod.rs", "fn b() {}\n")
    _git(repo, "add", "-A")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "work")
    result = subprocess.run(
        ["bash", str(_GUARD), "--base", base], capture_output=True, text=True, check=False, cwd=repo
    )
    assert result.returncode == 0
    lines = result.stderr.splitlines()
    assert lines[0] == "WARNING: pkg/map.md was not updated on this branch (map.md lockstep rule)."
    assert lines[-1].startswith("note: map.md lockstep is advisory")
    assert "ERROR" not in result.stderr


def test_migration_binds_a_defined_name_and_leaves_a_self_bound_map_path_only(
    mapped: Path,
) -> None:
    _write(mapped, _TEST, "#[test]\nfn demo_holds() {}\nfn other_holds() { helper_call(); }\n")
    _write(
        mapped,
        _MAP,
        "\n".join(
            [
                "# map",
                "",
                "## Contents",
                f"- `pins.rs` — `other_holds` calls `helper_call`. pins: {_UNIT}/C-001",
                "",
                "## Notes",
                f"Prose naming `demo_holds` with no file row. pins: {_UNIT}/C-002",
                "",
            ]
        ),
    )
    assert _migrate(mapped).returncode == 0
    assert (mapped / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}::other_holds",
        "C-002: none (records)",
    ]


def test_the_ledger_bin_maps_are_exempt_from_the_ratchet(tmp_path: Path) -> None:
    repo = _size_repo(tmp_path, _table(f'    "big/map.md": (250, {_big_bytes()}),\n'))
    for exempt in ("staging", "completed", "archive"):
        _write(repo, f"task/ledgers/{exempt}/map.md", "".join(f"- r{n}\n" for n in range(3000)))
    result = _size(repo)
    assert result.returncode == 0, result.stderr
    assert _size(repo, "--update-baselines").returncode == 0
    script = (repo / "scripts/check_map_size.py").read_text(encoding="utf-8")
    table = script.split("\nEXCEPTIONS: dict[str, tuple[int, int]] = {", 1)[1].split("\n}\n", 1)[0]
    assert "task/ledgers" not in table


def test_the_exempt_list_names_tracked_maps_only(tmp_path: Path) -> None:
    repo = _size_repo(tmp_path, _table(f'    "big/map.md": (250, {_big_bytes()}),\n'))
    (repo / "task/ledgers/archive/map.md").unlink()
    result = _size(repo)
    assert result.returncode == 1
    assert "EXEMPT key is no tracked map: task/ledgers/archive/map.md" in result.stderr


def _scratch_map_repo(tmp_path: Path, text: str, files: dict[str, str]) -> Path:
    repo = tmp_path / "mapped"
    _git(tmp_path, "init", "-q", "-b", "main", str(repo))
    _write(repo, _LEDGER, _ROWS)
    for path, body in files.items():
        _write(repo, path, body)
    _write(repo, _MAP, text)
    return repo


def test_migration_never_crosses_an_entry_into_a_neighbour(tmp_path: Path) -> None:
    moved = "crates/demo/tests/sub/moved_test.rs"
    text = "\n".join(
        [
            "# map",
            "",
            f"ALPHA-1 (2026-10-01): `pins.rs` pins the first. pins: {_UNIT}/C-001",
            f"BETA-1 (2026-10-02): `moved_test.rs` pins the second. pins: {_UNIT}/C-002",
            "",
        ]
    )
    repo = _scratch_map_repo(
        tmp_path, text, {_TEST: "#[test]\nfn a() {}\n", moved: "#[test]\nfn b() {}\n"}
    )
    result = _migrate(repo)
    assert result.returncode == 0, result.stderr
    assert "map citations resolved by basename: 1" in result.stdout
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}",
        f"C-002: {moved}",
    ]


def test_migration_stops_on_an_unresolvable_basename_until_bound_by_hand(tmp_path: Path) -> None:
    text = f"# map\n\nGAMMA-1 (2026-10-03): `ghost_test.rs` pins it. pins: {_UNIT}/C-001\n"
    repo = _scratch_map_repo(tmp_path, text, {_TEST: "#[test]\nfn demo_holds() {}\n"})
    stopped = _migrate(repo)
    assert stopped.returncode == 1
    assert f"{_MAP}:3: pins: {_UNIT}/C-001 names ghost_test.rs (0 matches here)" in stopped.stderr
    assert (repo / _MAP).read_text(encoding="utf-8") == text
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo_holds\n")
    placed = _migrate(repo)
    assert placed.returncode == 0, placed.stderr
    assert "hand-bound citations: 1" in placed.stdout
    assert (repo / _SIDECAR).read_text(encoding="utf-8") == (
        _HEADER + f"C-001: {_TEST}::demo_holds\n"
    )
    assert "pins:" not in (repo / _MAP).read_text(encoding="utf-8")


def test_migration_writes_records_for_a_production_row(tmp_path: Path) -> None:
    source_map = "crates/demo/src/map.md"
    repo = _scratch_map_repo(tmp_path, "# tests\n", {"crates/demo/src/lib.rs": "fn x() {}\n"})
    _write(repo, source_map, f"# map\n\n- `lib.rs` — the crate root. pins: {_UNIT}/C-001\n")
    result = _migrate(repo)
    assert result.returncode == 0, result.stderr
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        "C-001: none (records)"
    ]
    assert "none (records) lines: 1" in result.stdout


def test_migration_cut_keeps_indentation_and_drops_stray_punctuation(tmp_path: Path) -> None:
    text = "\n".join(
        [
            "# map",
            "",
            "- `pins.rs` — the battery,",
            f"  pins: {_UNIT}/C-001); a literal follows.",
            f"(pins: {_UNIT}/C-002). Cargo.toml banner condensed.",
            "",
        ]
    )
    repo = _scratch_map_repo(tmp_path, text, {_TEST: "#[test]\nfn a() {}\n"})
    assert _migrate(repo).returncode == 0
    lines = (repo / _MAP).read_text(encoding="utf-8").splitlines()
    assert lines[3] == "  a literal follows."
    assert lines[4] == "Cargo.toml banner condensed."


def test_ci_runs_the_map_size_ratchet_and_make_ci_lists_it() -> None:
    workflow = (_REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    guards = workflow.split("\n  guards:", 1)[1]
    step = guards.split("- name: map.md size ratchet guard (check_map_size)", 1)[1]
    run_line = next(line for line in step.splitlines() if line.strip().startswith("run:"))
    assert run_line.strip() == "run: python3 scripts/check_map_size.py"
    makefile = (_REPO / "Makefile").read_text(encoding="utf-8")
    ci_line = next(line for line in makefile.splitlines() if line.startswith("ci:"))
    assert " check-map-size " in ci_line
    lines = [line.strip() for line in makefile.splitlines()]
    assert "python3 scripts/check_map_size.py" in lines


def test_lockstep_refuses_a_bad_base_ref(tmp_path: Path) -> None:
    repo = tmp_path / "badref"
    _git(tmp_path, "init", "-q", "-b", "main", str(repo))
    result = subprocess.run(
        ["bash", str(_GUARD), "--base", "no-such-ref"],
        capture_output=True,
        text=True,
        check=False,
        cwd=repo,
    )
    assert result.returncode == 2
    assert "--base no-such-ref is not a commit" in result.stderr


def test_size_ratchet_exempts_the_archive_month_maps(tmp_path: Path) -> None:
    repo = _size_repo(tmp_path, _table(f'    "big/map.md": (250, {_big_bytes()}),\n'))
    month = "task/ledgers/archive/2026-10/map.md"
    _write(repo, month, "".join(f"- r{n}\n" for n in range(3000)))
    _write(
        repo, "task/ledgers/archive/2026-10/deeper/map.md", "".join(f"- r{n}\n" for n in range(300))
    )
    result = _size(repo)
    assert result.returncode == 1
    assert "task/ledgers/archive/2026-10/deeper/map.md is 300 lines" in result.stderr
    assert month not in result.stderr
    (repo / "task/ledgers/archive/2026-10/deeper/map.md").unlink()
    assert _size(repo).returncode == 0


def test_migration_keeps_an_indented_dated_paragraph_with_its_item_only_by_a_defined_name(
    tmp_path: Path,
) -> None:
    text = "\n".join(
        [
            "# map",
            "",
            "- `pins.rs` — the battery.",
            f"  **ROUND-2 (2026-10-05):** `other_holds` lands. pins: {_UNIT}/C-001",
            f"  **ROUND-3 (2026-10-06):** `ghost_fn` only. pins: {_UNIT}/C-002",
            "",
        ]
    )
    repo = _scratch_map_repo(
        tmp_path, text, {_TEST: "#[test]\nfn demo_holds() {}\nfn other_holds() {}\n"}
    )
    result = _migrate(repo)
    assert result.returncode == 0, result.stderr
    assert "map citations in an indented dated paragraph: 2" in result.stdout
    assert "  bound to their item's file by a defined name: 1" in result.stdout
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}::other_holds",
        "C-002: none (records)",
    ]


def test_a_definition_inside_a_string_or_comment_does_not_count(repo: Path) -> None:
    script = "crates/demo/tests/test_kinds.py"
    _write(repo, script, 'MESSAGE = "class Column is missing"\n\n\ndef test_real():\n    pass\n')
    rust = "crates/demo/tests/strings.rs"
    _write(
        repo,
        rust,
        'const S: &str = "#[test]\nfn ghost() {}";\n// fn commented() {}\n#[test]\nfn real() {}\n',
    )
    for path, name in ((script, "Column"), (rust, "ghost"), (rust, "commented")):
        _write(repo, _SIDECAR, _HEADER + f"C-001: {path}::{name}\nC-002: {_TEST}\n")
        result = _grammar(repo)
        assert result.returncode == 1, (path, name)
        assert f"C-001 names `{name}`, which {path} does not define" in result.stderr
    _write(repo, _SIDECAR, _HEADER + f"C-001: {script}::MESSAGE, {rust}::real\nC-002: {_TEST}\n")
    assert _grammar(repo).returncode == 0


def test_a_fixture_never_binds(repo: Path) -> None:
    fixture = "crates/demo/tests/oracle.json"
    _write(repo, fixture, "{}\n")
    _write(repo, _SIDECAR, _HEADER + f"C-001: {fixture}\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"C-001 binds {fixture}, which is not a test file or a script" in result.stderr


def test_a_citation_in_production_source_does_not_count(repo: Path) -> None:
    _write(repo, _SIDECAR, _HEADER + f"C-001: {_TEST}::demo_holds\n")
    _write(repo, "crates/demo/src/lib.rs", f"// pins: {_UNIT}/C-002\nfn x() {{}}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert "1 PROVEN clause(s) with no binding (ceiling 0): C-002" in result.stderr


def test_migration_binds_the_test_that_reads_a_fixture_else_records(tmp_path: Path) -> None:
    fixture = "crates/demo/tests/oracle.json"
    reader = "crates/demo/tests/reads.rs"
    text = "\n".join(
        [
            "# map",
            "",
            f"- `oracle.json` — the cells; `reads.rs` loads them. pins: {_UNIT}/C-001",
            f"- `oracle.json` — the same cells, no reader named. pins: {_UNIT}/C-002",
            "",
        ]
    )
    files = {fixture: "{}\n", reader: '#[test]\nfn load() { include_str!("oracle.json"); }\n'}
    repo = _scratch_map_repo(tmp_path, text, files)
    assert _migrate(repo).returncode == 0
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {reader}",
        "C-002: none (records)",
    ]


def test_migration_keeps_a_records_line_and_honours_a_disclaimer(tmp_path: Path) -> None:
    text = "\n".join(
        [
            "# map",
            "",
            "- `pins.rs` — the battery.",
            f"  pins: {_UNIT}/C-001",
            "  The merge (C-002) is cited here so the grammar gate counts it; this file pins none.",
            f"  pins: {_UNIT}/C-002",
            "",
        ]
    )
    repo = _scratch_map_repo(tmp_path, text, {_TEST: "#[test]\nfn a() {}\n"})
    _write(repo, _SIDECAR, _HEADER + "C-001: none (records)\n")
    result = _migrate(repo)
    assert result.returncode == 0, result.stderr
    assert "map citations whose own text disclaims a pin: 1" in result.stdout
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        "C-001: none (records)",
        "C-002: none (records)",
    ]
    assert _migrate(repo, "--check").returncode == 0


def test_migration_cut_keeps_a_parenthesis_balanced(tmp_path: Path) -> None:
    text = "\n".join(
        [
            "# map",
            "",
            "- `pins.rs` — the battery (the long note,",
            "  which runs on",
            f"  pins: {_UNIT}/C-001), `next` follows.",
            "",
        ]
    )
    repo = _scratch_map_repo(tmp_path, text, {_TEST: "#[test]\nfn a() {}\n"})
    assert _migrate(repo).returncode == 0
    lines = (repo / _MAP).read_text(encoding="utf-8").splitlines()
    assert lines[3] == "  which runs on), `next` follows."
    assert len(lines) == 4


def test_a_recorder_never_binds(repo: Path) -> None:
    recorder = "crates/demo/tests/_record_demo.py"
    _write(repo, recorder, "def record():\n    pass\n")
    _write(repo, _SIDECAR, _HEADER + f"C-001: {recorder}::record\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 1
    assert f"C-001 binds {recorder}, which is not a test file or a script" in result.stderr
    _write(repo, recorder, f"# pins: {_UNIT}/C-001\n")
    _write(repo, _SIDECAR, _HEADER + f"C-002: {_TEST}\n")
    unpinned = _grammar(repo)
    assert unpinned.returncode == 1
    assert "1 PROVEN clause(s) with no binding (ceiling 0): C-001" in unpinned.stderr


def test_migration_binds_nothing_to_a_fixture_whose_entry_names_only_its_recorder(
    tmp_path: Path,
) -> None:
    fixture = "crates/demo/tests/oracle.json"
    recorder = "crates/demo/tests/_record_oracle.py"
    text = f"# map\n\n- `oracle.json` — written by `_record_oracle.py`. pins: {_UNIT}/C-001\n"
    files = {fixture: "{}\n", recorder: 'OUT = "oracle.json"\n'}
    repo = _scratch_map_repo(tmp_path, text, files)
    assert _migrate(repo).returncode == 0
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        "C-001: none (records)"
    ]


def test_a_disclaimer_unbinds_only_the_clauses_it_names(tmp_path: Path) -> None:
    text = "\n".join(
        [
            "# map",
            "",
            "- `pins.rs` — the battery. This file proves C-001 and C-002 only; C-003 is the merge,"
            " cited here so the grammar gate counts it.",
            f"  pins: {_UNIT}/C-001, C-002, C-003",
            f"  pins: {_UNIT}/C-004 (the verification, cited here so the grammar gate counts it)",
            f"  pins: {_UNIT}/C-005",
            "",
        ]
    )
    rows = _ROWS + "".join(f"| C-00{n} | More. | A pin. | PROVEN | x |\n" for n in range(4, 6))
    repo = _scratch_map_repo(tmp_path, text, {_TEST: "#[test]\nfn demo_holds() {}\n"})
    _write(repo, _LEDGER, rows)
    result = _migrate(repo)
    assert result.returncode == 0, result.stderr
    assert "map citations whose own text disclaims a pin: 2" in result.stdout
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}",
        f"C-002: {_TEST}",
        "C-003: none (records)",
        "C-004: none (records)",
        f"C-005: {_TEST}",
    ]


def test_a_test_placed_helper_without_tests_never_binds(repo: Path) -> None:
    helper = "crates/demo/tests/oracle_writer.py"
    _write(repo, helper, "def write():\n    pass\n")
    rust_helper = "crates/demo/tests/common.rs"
    _write(repo, rust_helper, "pub fn setup() {}\n")
    for path, name in ((helper, "write"), (rust_helper, "setup")):
        _write(repo, _SIDECAR, _HEADER + f"C-001: {path}::{name}\nC-002: {_TEST}\n")
        result = _grammar(repo)
        assert result.returncode == 1, path
        assert f"C-001 binds {path}, which is not a test file or a script" in result.stderr
    fixture = "crates/demo/tests/conftest.py"
    _write(repo, fixture, "import pytest\n\n\n@pytest.fixture\ndef table():\n    return 1\n")
    _write(repo, _SIDECAR, _HEADER + f"C-001: {fixture}::table\nC-002: {_TEST}\n")
    assert _grammar(repo).returncode == 0


def test_shell_and_python_definitions_need_a_real_binding(repo: Path) -> None:
    script = "scripts/check_demo.sh"
    body = 'cat <<EOF\nphantom_heredoc() {\nEOF\necho "phantom_quoted() {"\nreal_fn() {\n  :\n}\n'
    _write(repo, script, body)
    module = "scripts/demo_registry.py"
    _write(
        repo,
        module,
        "import os\nREGISTRY = {}\nREGISTRY['phantom'] = 1\nos.x = 1\na, (b, *c) = 1, (2, 3)\n",
    )
    cases = (
        (script, "phantom_heredoc"),
        (script, "phantom_quoted"),
        (module, "phantom"),
        (module, "os"),
    )
    for path, name in cases:
        _write(repo, _SIDECAR, _HEADER + f"C-001: {path}::{name}\nC-002: {_TEST}\n")
        result = _grammar(repo)
        assert result.returncode == 1, name
        assert f"C-001 names `{name}`, which {path} does not define" in result.stderr
    good = f"{script}::real_fn, {module}::REGISTRY, {module}::a, {module}::b, {module}::c"
    _write(repo, _SIDECAR, _HEADER + f"C-001: {good}\nC-002: {_TEST}\n")
    result = _grammar(repo)
    assert result.returncode == 0, result.stderr


def test_a_shared_citation_binds_each_clause_by_its_own_row(tmp_path: Path) -> None:
    other = "crates/demo/tests/other_battery.rs"
    production = "crates/demo/src/engine.rs"
    rows = (
        "| Clause | Proposition | Proof obligation | Verdict | Evidence |\n"
        "|---|---|---|---|---|\n"
        "| C-001 | The battery. | A pin. | PROVEN | `pins.rs::demo_holds` |\n"
        "| C-002 | Elsewhere. | A pin. | PROVEN | `other_battery.rs::other_holds` |\n"
        "| C-003 | Production only. | A pin. | PROVEN | `engine.rs` is unchanged |\n"
        "| C-004 | Unnamed. | A pin. | PROVEN | measured |\n"
    )
    text = f"# map\n\n- `pins.rs` — the battery. pins: {_UNIT}/C-001, C-002, C-003, C-004\n"
    files = {
        _TEST: "#[test]\nfn demo_holds() {}\n",
        other: "#[test]\nfn other_holds() {}\n",
        production: "pub fn run() {}\n",
    }
    repo = _scratch_map_repo(tmp_path, text, files)
    _write(repo, _LEDGER, rows)
    result = _migrate(repo)
    assert result.returncode == 0, result.stderr
    assert "shared-citation clauses rebound by their own ledger row: 2" in result.stdout
    assert "  of which nothing bindable remains: 1" in result.stdout
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}::demo_holds",
        f"C-002: {other}::other_holds",
        "C-003: none (records)",
        f"C-004: {_TEST}",
    ]
