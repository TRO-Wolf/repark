#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

CEILINGS_PATH = "scripts/comment_ceilings.json"
ROOTS: tuple[str, ...] = ("crates/", "python/", "scripts/")
RUST_COMMENT = re.compile(r"^\s*//")
PYTHON_COMMENT = re.compile(r"^\s*#(?!!)")


def tracked_code_files(repo: Path) -> list[str]:
    command = ["git", "-C", str(repo), "ls-files", "-z", *ROOTS]
    out = subprocess.run(command, capture_output=True, check=True, text=True).stdout
    return sorted(p for p in out.split("\0") if p.endswith((".rs", ".py")))


def comment_lines(path: Path) -> int:
    pattern = RUST_COMMENT if path.suffix == ".rs" else PYTHON_COMMENT
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return 0
    return sum(1 for line in text.splitlines() if pattern.match(line))


def measure(repo: Path) -> dict[str, int]:
    return {f: comment_lines(repo / f) for f in tracked_code_files(repo)}


def load_ceilings(repo: Path) -> dict[str, int]:
    path = repo / CEILINGS_PATH
    if not path.exists():
        return {}
    return {k: int(v) for k, v in json.loads(path.read_text(encoding="utf-8")).items()}


def findings(counts: dict[str, int], ceilings: dict[str, int]) -> list[str]:
    found: list[str] = []
    for f, n in counts.items():
        ceiling = ceilings.get(f, 0)
        if n > ceiling:
            found.append(f"{f}: {n} comment lines exceeds its ceiling of {ceiling}")
    for f in ceilings:
        if f not in counts:
            found.append(f"{f}: has a ceiling but is not a tracked code file — drop the row")
    return found


def reseed(repo: Path, counts: dict[str, int], ceilings: dict[str, int]) -> dict[str, int]:
    lowered = {f: min(n, ceilings.get(f, n)) for f, n in counts.items()}
    lowered = {f: n for f, n in lowered.items() if n > 0}
    text = json.dumps(lowered, indent=0, sort_keys=True) + "\n"
    (repo / CEILINGS_PATH).write_text(text, encoding="utf-8")
    return lowered


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--reseed", action="store_true")
    args = parser.parse_args(argv)
    repo = args.repo.resolve()
    counts = measure(repo)
    ceilings = load_ceilings(repo)
    if args.reseed:
        lowered = reseed(repo, counts, ceilings)
        print(f"comment-density: reseeded {len(lowered)} ceilings (never upward)")
        return 0
    found = findings(counts, ceilings)
    total = sum(counts.values())
    if found:
        print("comment-density: FAIL — comments only ever condense; new files carry none")
        for line in found:
            print(f"  {line}")
        return 1
    summary = f"{total} comment lines under {len(ceilings)} ceilings"
    print(f"comment-density: {len(counts)} files clean ({summary})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
