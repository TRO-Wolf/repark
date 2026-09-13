"""PLATFORM-1 abi3 wheel-matrix pins over the workflow YAML and the release doc."""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
_RELEASE_YML = _REPO / ".github" / "workflows" / "release.yml"
_WHEELS_YML = _REPO / ".github" / "workflows" / "wheels.yml"
_RELEASE_DOC = _REPO / "docs" / "release.md"

EXPECTED_RELEASE_LEGS: dict[str, str] = {
    "manylinux-x86_64": "ubuntu-latest",
    "manylinux-aarch64": "ubuntu-24.04-arm",
    "macos-arm64": "macos-latest",
    "macos-x86_64": "macos-15-intel",
    "windows-x86_64": "windows-latest",
}
EXPECTED_NIGHTLY_LEGS: dict[str, str] = {
    "manylinux-aarch64": "ubuntu-24.04-arm",
    "macos-arm64": "macos-latest",
    "macos-x86_64": "macos-15-intel",
    "windows-x86_64": "windows-latest",
}
EXPECTED_MATRIX_CONDITION = (
    "github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'"
)
EXPECTED_PR_CONDITION = "github.event_name == 'pull_request' || github.ref == 'refs/heads/main'"


def _job_block(text: str, job: str) -> str | None:
    match = re.search(rf"(?ms)^  {re.escape(job)}:\s*\n(.*?)(?=^  \S|\Z)", text)
    return match.group(1) if match is not None else None


def _matrix_leg_entries(job_block: str) -> dict[str, str] | None:
    matrix = re.search(r"(?ms)^ {6}matrix:\s*\n((?: {8,}[^\n]*\n?)+)", job_block)
    if matrix is None:
        return None
    entries: dict[str, str] = {}
    for entry in re.finditer(
        r"(?ms)^ {10}- leg:\s*(\S+)\s*\n((?: {12,}[^\n]*\n?)*)", matrix.group(1)
    ):
        runs_on = re.search(r"(?m)^ {12}runs-on:\s*(\S+)\s*$", entry.group(2))
        entries[entry.group(1)] = runs_on.group(1) if runs_on is not None else ""
    return entries


def _leg_findings(where: str, legs: dict[str, str] | None, expected: dict[str, str]) -> list[str]:
    if legs is None:
        return [f"{where}: no matrix include block"]
    findings: list[str] = []
    if list(legs) != list(expected):
        findings.append(f"{where}: legs {list(legs)} != expected {list(expected)}")
        return findings
    for leg, runs_on in expected.items():
        if legs[leg] != runs_on:
            findings.append(f"{where}: leg {leg} runs-on {legs[leg]!r} != {runs_on!r}")
    return findings


def _release_findings(text: str) -> list[str]:
    block = _job_block(text, "build-wheel")
    if block is None:
        return ["release.yml: no build-wheel job"]
    findings = _leg_findings(
        "release.yml build-wheel", _matrix_leg_entries(block), EXPECTED_RELEASE_LEGS
    )
    publish = _job_block(text, "publish-pypi")
    if publish is None:
        findings.append("release.yml: no publish-pypi job")
        return findings
    if not re.search(r"(?m)^\s+pattern:\s*release-wheel-\*\s*$", publish):
        findings.append("release.yml publish-pypi: download lacks `pattern: release-wheel-*`")
    if not re.search(r"(?m)^\s+merge-multiple:\s*true\s*$", publish):
        findings.append("release.yml publish-pypi: download lacks `merge-multiple: true`")
    return findings


def _nightly_findings(text: str) -> list[str]:
    triggers = re.search(r"(?ms)^on:\s*\n(.*?)(?=^\S|\Z)", text)
    findings: list[str] = []
    if triggers is None or not re.search(
        r"(?m)^ {2}schedule:\s*\n {4}- cron:\s*\S", triggers.group(1)
    ):
        findings.append("wheels.yml: no nightly schedule cron trigger")
    if triggers is None or not re.search(r"(?m)^ {2}workflow_dispatch:", triggers.group(1)):
        findings.append("wheels.yml: no workflow_dispatch trigger")
    block = _job_block(text, "platform-matrix")
    if block is None:
        findings.append("wheels.yml: no platform-matrix job")
        return findings
    condition = re.search(r"(?m)^ {4}if:\s*(.+?)\s*$", block)
    if condition is None or condition.group(1) != EXPECTED_MATRIX_CONDITION:
        found = condition.group(1) if condition is not None else "none"
        findings.append(f"wheels.yml platform-matrix: `if:` is {found!r}, not schedule-or-dispatch")
    findings.extend(
        _leg_findings(
            "wheels.yml platform-matrix", _matrix_leg_entries(block), EXPECTED_NIGHTLY_LEGS
        )
    )
    return findings


def test_release_matrix_names_exactly_the_five_legs() -> None:
    """pins: platform-1/C-003."""
    assert _release_findings(_RELEASE_YML.read_text(encoding="utf-8")) == []


def test_doctored_release_matrix_fails() -> None:
    """pins: platform-1/C-004."""
    text = _RELEASE_YML.read_text(encoding="utf-8")
    dropped = text.replace("          - leg: macos-arm64\n", "", 1)
    assert _release_findings(dropped) != []
    renamed = text.replace("          - leg: macos-arm64\n", "          - leg: mac-arm64\n", 1)
    assert _release_findings(renamed) != []
    runs_on = text.replace(
        "            runs-on: macos-15-intel\n", "            runs-on: macos-latest\n", 1
    )
    assert _release_findings(runs_on) != []
    merged_off = text.replace("          merge-multiple: true\n", "", 1)
    assert _release_findings(merged_off) != []
    appended = text.replace(
        "            runs-on: windows-latest\n",
        "            runs-on: windows-latest\n"
        "          - leg: linux-riscv64\n"
        "            runs-on: ubuntu-latest\n",
        1,
    )
    assert _release_findings(appended) != []


def test_nightly_matrix_names_the_four_new_legs_off_pull_request() -> None:
    """pins: platform-1/C-001, C-002."""
    assert _nightly_findings(_WHEELS_YML.read_text(encoding="utf-8")) == []


def test_doctored_nightly_matrix_fails() -> None:
    """pins: platform-1/C-004."""
    text = _WHEELS_YML.read_text(encoding="utf-8")
    dropped = text.replace("          - leg: windows-x86_64\n", "", 1)
    assert _nightly_findings(dropped) != []
    on_pr = text.replace(EXPECTED_MATRIX_CONDITION, "github.event_name == 'pull_request'", 1)
    assert _nightly_findings(on_pr) != []
    no_cron = text.replace("  schedule:\n    - cron:", "  weekly:\n    - cron:", 1)
    assert _nightly_findings(no_cron) != []


def test_pull_request_smoke_job_keeps_its_gate_and_host() -> None:
    """pins: platform-1/C-002."""
    block = _job_block(_WHEELS_YML.read_text(encoding="utf-8"), "smoke")
    assert block is not None
    condition = re.search(r"(?m)^ {4}if:\s*(.+?)\s*$", block)
    assert condition is not None and condition.group(1) == EXPECTED_PR_CONDITION
    assert re.search(r"(?m)^ {4}runs-on:\s*ubuntu-latest\s*$", block) is not None


def test_release_doc_lists_the_five_wheels() -> None:
    """pins: platform-1/C-005."""
    doc = _RELEASE_DOC.read_text(encoding="utf-8")
    for token in EXPECTED_RELEASE_LEGS:
        assert token in doc, token


def test_platform_1_dispatch_runs_are_not_cancelled_by_pushes() -> None:
    """The wheels concurrency group keys on the event: a push never cancels a matrix run."""
    text = (_REPO / ".github" / "workflows" / "wheels.yml").read_text(encoding="utf-8")
    assert "group: wheels-${{ github.workflow }}-${{ github.event_name }}-${{ github.ref }}" in text

