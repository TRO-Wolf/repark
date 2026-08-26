from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_SCRIPT = _REPO / "scripts/check_comment_density.py"


def _tree(tmp_path: Path, body: str, ceiling: int | None) -> Path:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    src = tmp_path / "scripts"
    src.mkdir()
    (src / "probe.py").write_text(body, encoding="utf-8")
    if ceiling is not None:
        (src / "comment_ceilings.json").write_text(
            json.dumps({"scripts/probe.py": ceiling}) + "\n", encoding="utf-8"
        )
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True)
    return tmp_path


def _run(tree: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_SCRIPT), "--repo", str(tree)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_new_file_with_a_comment_is_red(tmp_path: Path) -> None:
    result = _run(_tree(tmp_path, "# why\nx = 1\n", None))
    assert result.returncode == 1
    assert "exceeds its ceiling of 0" in result.stdout


def test_file_at_its_ceiling_is_green(tmp_path: Path) -> None:
    result = _run(_tree(tmp_path, "# a\n# b\nx = 1\n", 2))
    assert result.returncode == 0


def test_file_above_its_ceiling_is_red(tmp_path: Path) -> None:
    result = _run(_tree(tmp_path, "# a\n# b\n# c\nx = 1\n", 2))
    assert result.returncode == 1


def test_shebang_is_not_a_comment(tmp_path: Path) -> None:
    result = _run(_tree(tmp_path, "#!/usr/bin/env python3\nx = 1\n", None))
    assert result.returncode == 0


def test_reseed_only_lowers(tmp_path: Path) -> None:
    tree = _tree(tmp_path, "# a\nx = 1\n", 5)
    subprocess.run([sys.executable, str(_SCRIPT), "--repo", str(tree), "--reseed"], check=True)
    table = json.loads((tree / "scripts/comment_ceilings.json").read_text(encoding="utf-8"))
    assert table == {"scripts/probe.py": 1}


def test_the_live_tree_is_under_its_ceilings() -> None:
    result = _run(_REPO)
    assert result.returncode == 0, result.stdout
