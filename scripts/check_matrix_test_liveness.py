#!/usr/bin/env python3
"""Fail if a surface-matrix Tested row cites a cargo-test name that --list does not contain.

The in-process audit (`repark_common::surfaces::audit`) cannot enumerate cargo test names.
This harness-level gate closes that hole: every `Row::Tested { test }` in both door
matrices must appear in
`cargo test --locked --workspace --lib --tests --bins -- --list`.

Matching: exact listed name, or the listed name is the cited string's last `::`
component (integration-test binaries list the function name; some matrix rows
prefix `binary::name` or `crate tests/file.rs::name`).

Dual-wired: `make check-matrix-test-liveness` (in `make ci` / `make preflight`) AND
the `matrix test-name liveness` step in ci.yml's rust-test job. Change one, change
the other.

SSOT is this script. Prose points here. Doc-tests are out of scope (`--lib --tests
--bins`): matrix rows cite executable tests, never rustdoc examples.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

MATRICES: tuple[tuple[str, Path], ...] = (
    ("repark-spark", REPO_ROOT / "crates" / "repark-spark" / "src" / "matrix.rs"),
    ("repark-sql", REPO_ROOT / "crates" / "repark-sql" / "src" / "matrix.rs"),
)

# Only the `t("name"` shorthand. A bare `t(` also matches the suffix of `absent(` /
# `audit(` — require a word boundary so those are not treated as cites.
TESTED_CITE_RE = re.compile(r'\bt\(\s*"([^"]*)"')
LIST_LINE_RE = re.compile(r"^\s*(.+): (?:test|bench)\s*$")

CARGO_LIST_CMD: tuple[str, ...] = (
    "cargo",
    "test",
    "--locked",
    "--workspace",
    "--lib",
    "--tests",
    "--bins",
    "--color",
    "never",
    "--",
    "--list",
)


class ParseError(Exception):
    """A required input could not be extracted — fail closed, never skip."""


def _cited_is_live(cited: str, listed: set[str]) -> bool:
    """Return True when `cited` names a test that `--list` actually printed."""
    name = cited.strip()
    if not name:
        return False
    if name in listed:
        return True
    last = name.rsplit("::", 1)[-1]
    if last in listed:
        return True
    suffix = f"::{name}"
    last_suffix = f"::{last}"
    return any(entry.endswith(suffix) or entry.endswith(last_suffix) for entry in listed)


def _extract_tested_cites(path: Path) -> list[str]:
    """Return every Tested `test` string in one matrix.rs, in file order."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ParseError(f"{path} could not be read — {error}") from error
    cites = TESTED_CITE_RE.findall(text)
    if not cites:
        raise ParseError(
            f'{path} yielded zero Tested cites — the extractor missed the `t("` '
            f"shorthand, or the matrix is empty (fail-closed)"
        )
    return cites


def _parse_list_output(output: str) -> set[str]:
    """Parse `cargo test -- --list` stdout into a set of test names."""
    names: set[str] = set()
    for line in output.splitlines():
        match = LIST_LINE_RE.match(line)
        if match:
            names.add(match.group(1))
    if not names:
        raise ParseError("cargo test -- --list printed zero test names — parse miss (fail-closed)")
    return names


def _run_cargo_list() -> set[str]:
    """Run the workspace `--list` and return parsed test names."""
    try:
        completed = subprocess.run(
            CARGO_LIST_CMD,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "CARGO_TERM_COLOR": "never"},
        )
    except OSError as error:
        raise ParseError(f"could not invoke cargo: {error}") from error
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or "(no stderr)"
        raise ParseError(f"cargo test -- --list exited {completed.returncode}\n{stderr}")
    return _parse_list_output(completed.stdout + "\n" + completed.stderr)


def main() -> int:
    """Exit 0 when every Tested cite is live; non-zero on a dead name or parse miss."""
    try:
        listed = _run_cargo_list()
        dead: list[str] = []
        cited_count = 0
        for door, path in MATRICES:
            if not path.is_file():
                raise ParseError(f"matrix file missing: {path}")
            for cited in _extract_tested_cites(path):
                cited_count += 1
                if not _cited_is_live(cited, listed):
                    dead.append(f"  {door}: {cited!r}")
    except ParseError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        print("matrix-test-liveness: FAIL — parse / cargo error", file=sys.stderr)
        return 2

    if dead:
        print(
            "ERROR: cited test name is not in `cargo test -- --list`:",
            file=sys.stderr,
        )
        for line in dead:
            print(line, file=sys.stderr)
        print(
            f"matrix-test-liveness: FAIL — {len(dead)} dead cite(s) "
            f"({cited_count} Tested; --list {len(listed)} names)",
            file=sys.stderr,
        )
        return 1

    print(
        f"matrix-test-liveness: {cited_count} Tested cites live (cargo --list {len(listed)} names)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
