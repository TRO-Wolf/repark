"""RP-4 C-004: fork to_branch exists; no engine caller sets it.

pins: rp-4-fork-repin/C-004
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CRATES = _REPO_ROOT / "crates"
_FIXTURES = _REPO_ROOT / "crates" / "repark-spark" / "src" / "tests" / "fixtures"
_SETTER_NEEDLES = (".to_branch(",)


def test_engine_sources_do_not_call_to_branch() -> None:
    """Fail if an engine caller starts targeting SnapshotUpdate.to_branch."""
    hits: list[str] = []
    for path in _CRATES.rglob("*.rs"):
        if "target" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        for line_number, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("//"):
                continue
            if any(needle in stripped for needle in _SETTER_NEEDLES):
                hits.append(f"{path.relative_to(_REPO_ROOT)}:{line_number}:{stripped}")
    assert hits == [], "engine must not call to_branch:\n" + "\n".join(hits)


def test_checked_in_spark_fixtures_are_byte_flat() -> None:
    """Append and adopt fixtures on this branch match origin/main byte-for-byte."""
    import hashlib
    import subprocess

    import pytest

    base_ok = subprocess.run(
        ["git", "rev-parse", "--verify", "origin/main"],
        cwd=_REPO_ROOT,
        capture_output=True,
        check=False,
    )
    if base_ok.returncode != 0:
        pytest.skip("origin/main is not in this checkout")
    listed = subprocess.run(
        ["git", "ls-files", "-z", "crates/repark-spark/src/tests/fixtures"],
        cwd=_REPO_ROOT,
        check=True,
        capture_output=True,
    )
    paths = [item.decode() for item in listed.stdout.split(b"\0") if item]
    assert paths, "expected checked-in Spark fixtures"
    drifted: list[str] = []
    compared = 0
    for relative in paths:
        if Path(relative).name == "map.md":
            continue
        shown = subprocess.run(
            ["git", "show", f"origin/main:{relative}"],
            cwd=_REPO_ROOT,
            capture_output=True,
            check=True,
        )
        current = hashlib.sha256((_REPO_ROOT / relative).read_bytes()).hexdigest()
        base = hashlib.sha256(shown.stdout).hexdigest()
        compared += 1
        if current != base:
            drifted.append(relative)
    assert compared > 0, "expected Iceberg fixture bytes to compare"
    assert drifted == [], f"fixtures must stay byte-flat vs origin/main: {drifted}"
