#!/usr/bin/env python3
"""Enforce public-docstring presence with a per-file ratchet.

SSOT for the five Ruff presence rules the owner ruled 2026-08-22: D101, D102,
D103, D105, D107. Style ``D`` is declined permanently and is not selected
here. Prose (AGENTS.md "Python", .agents/skills/code-quality/SKILL.md) points
here and never restates the table. Mirrors the check_python_conventions
dual-wire shape (py = logic + SSOT, sh = wrapper).

Ruff is the parser; this wrapper is the ratchet Ruff cannot express: per-file
ceilings that go DOWN only, seeded from the measured tree at arming
(2026-08-22: 136 findings across 39 files under SCAN_ROOTS excluding tests).
The ~266 figure in the slate included tests; tests keep the per-file ignore.

Scan: every ``*.py`` under SCAN_ROOTS except a ``tests`` path part and
``__pycache__``. A new undocumented public name in an unlisted file is red; a
listed file may not grow past its ceiling; a row whose file drops to zero is
deleted rather than kept at 0. Ruff treats a module whose filename starts
with ``_`` as private, so D103 does not apply there — that is the parser's
definition of public, not a second rule.

Exit 0 on clean; 1 on findings; 2 on environment or usage error. Fail-closed:
uvx/ruff missing, ruff exit other than 0/1, JSON parse miss, empty scan, a
missing scan root, a stale EXCEPTIONS key, a row whose measured count is 0.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path

# Trees the guard owns — same three as check_python_conventions.py. Tests are
# out of scope (pyproject.toml per-file-ignores keep ``D`` off ``**/tests/**``).
SCAN_ROOTS: tuple[str, ...] = (
    "python/repark/src",
    "python/repark-parity",
    "scripts",
)

# Must match the Makefile ``RUFF := uvx ruff@…`` pin. A test pins the two
# together so they cannot drift.
RUFF_PIN: str = "0.15.22"

# Presence only. Style codes (D401, D202, D205, D413, …) stay off this list.
PRESENCE_RULES: tuple[str, ...] = ("D101", "D102", "D103", "D105", "D107")

# repo-relative posix path -> (ceiling, reason). Keys sorted alphabetically.
# Seeded from ``uvx ruff@0.15.22 check`` over SCAN_ROOTS with
# ``--select D101,D102,D103,D105,D107 --extend-exclude '**/tests/**'`` at
# arming (2026-08-22). Ceilings equal the measured count (no slack: a new
# undocumented public name is exactly the debt). A row whose file drops to
# zero is deleted rather than kept at 0.
EXCEPTIONS: dict[str, tuple[int, str]] = {
    "python/repark-parity/bench/bench_coalesce_chain.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/bench_mor_merge.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/fuzz/bank.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/fuzz/datagen.py": (
        3,
        "parity harness: 3 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/fuzz/generator.py": (
        4,
        "parity harness: 4 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/fuzz/run_fuzz.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/fuzz/runner.py": (
        5,
        "parity harness: 5 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/tpcds/query_worker.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/tpcds/run_tpcds.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/tpcds/runner.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/tpch/check_baseline_ratios.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/tpch/query_worker.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/tpch/run_tpch.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/tpch/runner.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/tpch/sail_engine.py": (
        4,
        "parity harness: 4 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/write/merge_runner.py": (
        5,
        "parity harness: 5 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/write/overwrite_runner.py": (
        4,
        "parity harness: 4 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/write/run_write_bench.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/write/runner.py": (
        4,
        "parity harness: 4 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/bench/write/schemas.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/compat/classify.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/compat/compare_reports.py": (
        2,
        "parity harness: 2 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/compat/redact.py": (
        1,
        "parity harness: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/compat/runner.py": (
        3,
        "parity harness: 3 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark-parity/record_ta_goldens.py": (
        3,
        "parity harness: 3 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark/src/repark/errors.py": (
        6,
        "shipped facade: 6 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark/src/repark/spark/dataframe/writer_readwriter.py": (
        1,
        "shipped facade: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark/src/repark/spark/functions_udf.py": (
        50,
        "shipped facade UDF surface: 50 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a UDF-surface docstring pass",
    ),
    "python/repark/src/repark/spark/merge.py": (
        4,
        "shipped facade: 4 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark/src/repark/spark/polars.py": (
        3,
        "shipped facade: 3 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark/src/repark/spark/session/builder_conf.py": (
        1,
        "shipped facade: 1 undocumented public name. "
        "Add a Google-style docstring; RATCHET: after a docstring pass on this file",
    ),
    "python/repark/src/repark/spark/storage.py": (
        4,
        "shipped facade: 4 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "python/repark/src/repark/spark/types.py": (
        2,
        "shipped facade: 2 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "scripts/check_lib_py.py": (
        2,
        "repo guard: 2 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "scripts/check_lib_rs.py": (
        2,
        "repo guard: 2 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "scripts/check_parity_live_dual_wire.py": (
        3,
        "repo guard: 3 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "scripts/check_python_conventions.py": (
        2,
        "repo guard: 2 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "scripts/check_rust_file_size.py": (
        2,
        "repo guard: 2 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
    "scripts/sync_map_md.py": (
        2,
        "repo guard: 2 undocumented public names. "
        "Add Google-style docstrings; RATCHET: after a docstring pass on this file",
    ),
}


def collect_python_files(repo: Path) -> list[Path]:
    """Return every scanned ``*.py`` under SCAN_ROOTS, tests excluded.

    Raises:
        FileNotFoundError: a named scan root is missing.
    """
    paths: list[Path] = []
    for root in SCAN_ROOTS:
        root_path = repo / root
        if not root_path.is_dir():
            raise FileNotFoundError(root)
        for path in sorted(root_path.rglob("*.py")):
            if not path.is_file():
                continue
            if "__pycache__" in path.parts or "tests" in path.parts:
                continue
            paths.append(path)
    return paths


def parse_ruff_stdout(returncode: int, stdout: str, stderr: str) -> list[dict[str, object]]:
    """Decode Ruff JSON. Empty or non-JSON stdout is fail-closed, never zero findings."""
    if returncode not in (0, 1):
        detail = stderr.strip() or stdout.strip() or "no output"
        raise RuntimeError(f"ruff@{RUFF_PIN} exited {returncode} — refuse to pass closed: {detail}")
    stripped = stdout.strip()
    if not stripped:
        raise RuntimeError("ruff stdout is empty — refuse to pass closed")
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"ruff JSON did not parse ({error}) — refuse to pass closed") from error
    if not isinstance(payload, list):
        raise RuntimeError("ruff JSON is not a list — refuse to pass closed")
    return payload


def run_ruff(repo: Path, paths: list[Path]) -> list[dict[str, object]]:
    """Run pinned Ruff on the collected files and return the JSON diagnostic list.

    Files are passed explicitly so Ruff's default exclude (``build``, ``dist``,
    ``venv``, …) cannot silently omit a path ``collect_python_files`` scanned.
    ``--isolated`` and ``--ignore-noqa`` keep pyproject / ``# noqa`` from
    dropping presence diagnostics the ratchet must see.

    Ruff exits 1 when it reports findings; that is not an environment error.
    Any other non-zero, a missing binary, or stdout that is not JSON is fail-closed.
    """
    uvx_path = shutil.which("uvx")
    if uvx_path is None:
        raise RuntimeError("uvx is not on PATH — refuse to pass closed")
    select = ",".join(PRESENCE_RULES)
    relatives: list[str] = [path.relative_to(repo).as_posix() for path in paths]
    command: list[str] = [
        uvx_path,
        f"ruff@{RUFF_PIN}",
        "check",
        *relatives,
        "--select",
        select,
        "--isolated",
        "--ignore-noqa",
        "--output-format",
        "json",
    ]
    completed = subprocess.run(
        command,
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    return parse_ruff_stdout(completed.returncode, completed.stdout, completed.stderr)


def resolve_diagnostic_path(repo: Path, filename: str) -> str:
    """Map a Ruff ``filename`` to a repo-relative posix path.

    Relative names are resolved against ``repo``, not the parent process cwd,
    because the wrapper may be invoked without first ``cd``-ing to the repo.
    """
    raw_path = Path(filename)
    absolute = raw_path.resolve() if raw_path.is_absolute() else (repo / raw_path).resolve()
    try:
        return absolute.relative_to(repo.resolve()).as_posix()
    except ValueError as error:
        raise RuntimeError(
            f"ruff diagnostic path {filename} is outside the repo — refuse to pass closed"
        ) from error


def counts_by_relative(repo: Path, diagnostics: list[dict[str, object]]) -> dict[str, int]:
    """Group Ruff diagnostics by repo-relative posix path."""
    counts: Counter[str] = Counter()
    for diagnostic in diagnostics:
        filename = diagnostic.get("filename")
        if not isinstance(filename, str) or not filename:
            raise RuntimeError("ruff diagnostic missing filename — refuse to pass closed")
        code = diagnostic.get("code")
        if code not in PRESENCE_RULES:
            raise RuntimeError(
                f"ruff diagnostic code {code!r} is not a presence rule — refuse to pass closed"
            )
        relative = resolve_diagnostic_path(repo, filename)
        counts[relative] += 1
    return dict(counts)


def check_counts(
    scanned_relatives: set[str],
    measured: dict[str, int],
    exceptions: dict[str, tuple[int, str]] | None = None,
) -> list[str]:
    """Compare measured counts to the exceptions table. Returns error strings.

    ``exceptions`` defaults to the module EXCEPTIONS table. Tests pass a
    closed-world fixture so a live 39-row walk cannot satisfy an assertion.
    """
    table = EXCEPTIONS if exceptions is None else exceptions
    errors: list[str] = []
    for relative, count in sorted(measured.items()):
        if relative not in scanned_relatives:
            errors.append(
                f"ERROR: {relative} produced {count} presence finding(s) but is not in the "
                f"scan set (tests are excluded; a leaked path is a gate bug)"
            )
            continue
        ceiling_row = table.get(relative)
        if ceiling_row is None:
            errors.append(
                f"ERROR: {relative} has {count} undocumented public name(s) and no EXCEPTIONS "
                f"row. Sanctioned outs: (1) add a Google-style docstring on each name, or "
                f"(2) add a row to EXCEPTIONS in scripts/check_docstring_presence.py with a "
                f"reason (ceilings ratchet down only)."
            )
            continue
        ceiling, reason = ceiling_row
        if count > ceiling:
            errors.append(
                f"ERROR: {relative} has {count} undocumented public name(s) "
                f"(ceiling {ceiling}). Reason on file: {reason}. "
                f"Sanctioned outs: (1) add a Google-style docstring, or (2) raise the row "
                f"in EXCEPTIONS with a reason (ceilings ratchet down only)."
            )
    for relative, (ceiling, reason) in sorted(table.items()):
        if relative not in scanned_relatives:
            errors.append(
                f"ERROR: EXCEPTIONS key {relative} is not in the scan set "
                f"(tests are excluded; remove the row or restore the path under SCAN_ROOTS)"
            )
            continue
        if relative not in measured:
            errors.append(
                f"ERROR: EXCEPTIONS key {relative} measures 0 "
                f"(ceiling {ceiling}) — delete the row rather than keep it. "
                f"Reason on file: {reason}"
            )
    return errors


def main() -> int:
    """Run the presence ratchet over SCAN_ROOTS. Returns the process exit code."""
    repo = Path(__file__).resolve().parent.parent
    errors: list[str] = []

    for relative in sorted(EXCEPTIONS):
        if not (repo / relative).is_file():
            errors.append(
                f"ERROR: EXCEPTIONS key has no file on disk: {relative} "
                f"(remove the row or restore the path)"
            )

    try:
        paths = collect_python_files(repo)
    except FileNotFoundError as error:
        print(f"ERROR: scan root {error} not found", file=sys.stderr)
        return 2

    if not paths:
        print(
            "ERROR: docstring-presence scan set is empty — refuse to pass closed",
            file=sys.stderr,
        )
        return 2

    scanned_relatives = {path.relative_to(repo).as_posix() for path in paths}

    try:
        diagnostics = run_ruff(repo, paths)
        measured = counts_by_relative(repo, diagnostics)
    except RuntimeError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2

    errors.extend(check_counts(scanned_relatives, measured))

    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        print(
            f"docstring-presence: FAIL — {len(errors)} violation(s) across {len(paths)} files",
            file=sys.stderr,
        )
        return 1

    print(
        f"docstring-presence: {len(paths)} files clean "
        f"(presence rows {len(EXCEPTIONS)}, findings {sum(measured.values())})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
