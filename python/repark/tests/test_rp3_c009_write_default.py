"""RP-3 C-009: fork fills write_default; no engine caller sets it.

pins: rp-3-fork-repin/C-009
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CRATES = _REPO_ROOT / "crates"
_FIXTURES = _REPO_ROOT / "crates" / "repark-spark" / "src" / "tests" / "fixtures"
_SETTER_NEEDLES = ("with_write_default", "write_default(")


def test_engine_sources_do_not_set_write_default() -> None:
    """Fail if an engine caller starts setting Iceberg write_default."""
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
    assert hits == [], "engine must not set write_default:\n" + "\n".join(hits)


def test_checked_in_spark_fixtures_are_byte_flat() -> None:
    """Append and adopt fixtures on this branch match origin/main byte-for-byte."""
    import hashlib
    import subprocess

    listed = subprocess.run(
        ["git", "ls-files", "-z", "crates/repark-spark/src/tests/fixtures"],
        cwd=_REPO_ROOT,
        check=True,
        capture_output=True,
    )
    paths = [item.decode() for item in listed.stdout.split(b"\0") if item]
    assert paths, "expected checked-in Spark fixtures"
    drifted: list[str] = []
    for relative in paths:
        current = hashlib.sha256((_REPO_ROOT / relative).read_bytes()).hexdigest()
        shown = subprocess.run(
            ["git", "show", f"origin/main:{relative}"],
            cwd=_REPO_ROOT,
            check=True,
            capture_output=True,
        )
        base = hashlib.sha256(shown.stdout).hexdigest()
        if current != base:
            drifted.append(relative)
    assert drifted == [], f"fixtures must stay byte-flat vs origin/main: {drifted}"
