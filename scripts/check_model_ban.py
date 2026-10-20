#!/usr/bin/env python3
"""Fail when a tracked file names a banned Opus 5.0 model id (owner ruling 2026-10-17)."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

BANNED = re.compile(r"claude[-_]opus[-_]5[-_.]0\b|\bopus[-_]5[-_.]0\b", re.IGNORECASE)
ALLOWED_ID = "claude-opus-5-5"


def banned_spellings() -> list[str]:
    """Return the spellings the gate must catch, assembled so this file never contains one."""
    parts = ("claude", "opus", "5", "0")
    return [
        "-".join(parts),
        "_".join(parts),
        "-".join(parts[1:]),
        "-".join(parts[:3]) + "." + parts[3],
        "-".join(parts).upper(),
    ]


def self_test() -> list[str]:
    """Return findings when the pattern misses a banned spelling or hits an allowed one."""
    found = [f"self-test: pattern misses {s}" for s in banned_spellings() if not BANNED.search(s)]
    for allowed in (ALLOWED_ID, "Opus 5.0 is banned", "claude-opus-5", "opus-5-5"):
        if BANNED.search(allowed):
            found.append(f"self-test: pattern wrongly hits {allowed}")
    return found


def tracked_files(repo: Path) -> list[Path]:
    """Return every git-tracked file under the repository."""
    output = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "-z"], check=True, capture_output=True
    ).stdout
    return [repo / name for name in output.decode("utf-8").split("\0") if name]


def findings(repo: Path) -> list[str]:
    """Return one line per tracked-file hit, as path:line: text."""
    found = self_test()
    for path in tracked_files(repo):
        if not path.is_file() or path.is_symlink():
            continue
        data = path.read_bytes()
        if b"\0" in data:
            continue
        for number, line in enumerate(data.decode("utf-8", errors="replace").splitlines(), 1):
            if BANNED.search(line):
                found.append(f"{path.relative_to(repo)}:{number}: {line.strip()[:120]}")
    return found


def main() -> int:
    """Check the repository and return a process exit status."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parent.parent)
    arguments = parser.parse_args()
    found = findings(arguments.repo.resolve())
    if found:
        for finding in found:
            print(finding, file=sys.stderr)
        print(
            f"model-ban: FAIL — {len(found)} finding(s); Opus means {ALLOWED_ID}", file=sys.stderr
        )
        return 1
    print(f"model-ban: no tracked file names an Opus 5.0 id; Opus means {ALLOWED_ID}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
