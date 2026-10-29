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
    _write(tmp_path, _TEST, "fn demo_holds() {}\nfn other_holds() {}\n")
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
    _write(repo, _TEST, "fn demo_holds() {}\nfn other_holds_renamed() {}\n")
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
    _write(repo, _TEST, f"// pins: {_UNIT}/C-002\nfn demo_holds() {{}}\n")
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
    _write(repo, "crates/demo/tests/other.rs", "fn other_holds() {}\n")
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
    _write(mapped, _TEST, f"// pins: {_UNIT}/C-001, C-002\nfn demo_holds() {{}}\n")
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
    _write(mapped, _TEST, "fn demo_holds() {}\nfn other_holds() { helper_call(); }\n")
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
    repo = _scratch_map_repo(tmp_path, text, {_TEST: "fn a() {}\n", moved: "fn b() {}\n"})
    result = _migrate(repo)
    assert result.returncode == 0, result.stderr
    assert "map citations resolved by basename: 1" in result.stdout
    assert (repo / _SIDECAR).read_text(encoding="utf-8").splitlines()[2:] == [
        f"C-001: {_TEST}",
        f"C-002: {moved}",
    ]


def test_migration_stops_on_an_unresolvable_basename_until_bound_by_hand(tmp_path: Path) -> None:
    text = f"# map\n\nGAMMA-1 (2026-10-03): `ghost_test.rs` pins it. pins: {_UNIT}/C-001\n"
    repo = _scratch_map_repo(tmp_path, text, {_TEST: "fn demo_holds() {}\n"})
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
    repo = _scratch_map_repo(tmp_path, text, {_TEST: "fn a() {}\n"})
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
