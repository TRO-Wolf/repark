from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

_REPO = Path(__file__).resolve().parents[3]
_SCRIPT = _REPO / "scripts/check_model_ban.py"


def _load_gate() -> ModuleType:
    specification = importlib.util.spec_from_file_location("check_model_ban", _SCRIPT)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_live_tree_names_no_banned_opus_id() -> None:
    """pins: the 2026-10-17 Opus 5.0 ban on the live tree."""
    gate = _load_gate()
    assert gate.findings(_REPO) == []


def test_every_banned_spelling_is_caught_and_the_allowed_id_passes() -> None:
    gate = _load_gate()
    assert gate.self_test() == []
    for spelling in gate.banned_spellings():
        assert gate.BANNED.search(f"model: {spelling}") is not None, spelling
    for allowed in (gate.ALLOWED_ID, "Opus 5.0 is banned forever", "claude-opus-5", "Opus 5"):
        assert gate.BANNED.search(allowed) is None, allowed


def test_a_tracked_hit_is_reported_with_its_line(tmp_path: Path) -> None:
    import subprocess

    gate = _load_gate()
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    hit = tmp_path / "brief.md"
    hit.write_text(f"model: {gate.banned_spellings()[0]}\n", encoding="utf-8")
    clean = tmp_path / "ok.md"
    clean.write_text(f"model: {gate.ALLOWED_ID}\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(tmp_path), "add", "brief.md", "ok.md"], check=True)
    found = gate.findings(tmp_path)
    assert len(found) == 1
    assert found[0].startswith("brief.md:1: model: ")


def test_gate_is_wired_and_named_everywhere() -> None:
    makefile = (_REPO / "Makefile").read_text(encoding="utf-8")
    ci_line = next(line for line in makefile.splitlines() if line.startswith("ci: "))
    workflow = (_REPO / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "check-model-ban" in ci_line
    assert "run: python3 scripts/check_model_ban.py" in workflow
    required = {
        "map.md": "check-model-ban",
        ".github/workflows/map.md": "check_model_ban.py",
        "scripts/map.md": "check_model_ban.py",
        "python/repark-parity/tests/map.md": "test_model_ban.py",
        "AGENTS.md": "OPUS 5.0 IS BANNED FOREVER",
        "CLAUDE.md": "OPUS 5.0 IS BANNED FOREVER",
    }
    for relative_path, artifact in required.items():
        text = (_REPO / relative_path).read_text(encoding="utf-8")
        assert artifact in text, relative_path
