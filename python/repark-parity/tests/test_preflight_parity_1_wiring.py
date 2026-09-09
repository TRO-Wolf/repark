"""Pins PREFLIGHT-PARITY-1: the CAP-1 mirror joins `make preflight` as `py-test-parity-cap`."""

from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_MAKEFILE_LINES = (_REPO / "Makefile").read_text(encoding="utf-8").splitlines()
_CAP_MIRROR = "python/repark-parity/tests/test_cap_1_source_file_line_cap.py"
_ISOLATED_TOKEN = "--with pyarrow --with pytest --with 'pydantic>=2.10,<3'"


def _target_block(target: str) -> list[str]:
    header = next(line for line in _MAKEFILE_LINES if line.split(":")[0] == target)
    block = [header]
    for line in _MAKEFILE_LINES[_MAKEFILE_LINES.index(header) + 1 :]:
        if not line.startswith("\t"):
            break
        block.append(line)
    return block


def test_cap_mirror_target_runs_the_one_file_with_the_parity_interpreter() -> None:
    """pins: preflight-parity-1/C-001."""
    block = _target_block("py-test-parity-cap")
    recipe = "\n".join(block[1:])
    tested = [
        token
        for line in block[1:]
        for token in line.split()
        if token.startswith("python/repark-parity/tests/")
    ]
    assert tested == [_CAP_MIRROR]
    assert "PYTHONPATH=python/repark-parity/src" in recipe
    assert "uv run --no-project" in recipe
    assert _ISOLATED_TOKEN in recipe


def test_preflight_places_the_cap_mirror_after_facade_before_audit() -> None:
    """pins: preflight-parity-1/C-002."""
    prerequisites = _target_block("preflight")[0].split(":", 1)[1].split("##")[0].split()
    assert prerequisites.index("py-test-facade") < prerequisites.index("py-test-parity-cap")
    assert prerequisites.index("py-test-parity-cap") < prerequisites.index("audit")
    verify_prerequisites = _target_block("verify")[0].split(":", 1)[1].split("##")[0].split()
    assert "py-test-parity-cap" not in verify_prerequisites
