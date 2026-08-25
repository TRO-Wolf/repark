#!/usr/bin/env python3
"""The live documents carry only live state — the gate (DL-4, 2026-08-25).

Over `STATUS.md` and `briefs/next-sequence.md`, four checks:

(a) no `state=closed` ws block is still in STATUS — a closed campaign has left
    for `docs/history/` (the lifecycle script's `compact` moves it);
(b) no `unit` marker in the slate names a ledger that sits in
    `task/ledgers/completed/` or the archive — a merged unit has left, whole;
(c) every top-level bullet under STATUS "Active workstreams" is inside a ws
    block, so a new campaign cannot arrive unmarked;
(d) neither file exceeds its byte ceiling. CEILINGS is seeded from the
    post-migration measurement and is raised only by an explicit edit in the
    PR that needs it — the PYC-6 ratchet pattern. Markers make compaction
    mechanical; this ceiling is what makes regrowth visible.

Grammar and meanings: `scripts/doc_blocks.py`. Exit 0 clean, 1 findings, 2
environment.
"""

from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType

COMPLETED = "task/ledgers/completed"
ARCHIVE = "task/ledgers/archive"
LEDGER_SUFFIX = "-ledger.md"
# Bytes. Interim values until the DL-4 migration measures the compacted files and seeds them;
# thereafter raised only in the PR that needs it, with the reason.
CEILINGS: dict[str, int] = {
    "STATUS.md": 70_000,
    "briefs/next-sequence.md": 28_000,
}


def _load(name: str) -> ModuleType:
    """A sibling script as a module."""
    location = Path(__file__).resolve().parent / f"{name}.py"
    specification = importlib.util.spec_from_file_location(name, location)
    if specification is None or specification.loader is None:
        raise RuntimeError(f"cannot load {location}")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


def tracked(repo: Path) -> list[str]:
    """Every tracked path, sorted."""
    out = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "-z"], capture_output=True, check=True, text=True
    ).stdout
    return sorted(p for p in out.split("\0") if p)


def findings(repo: Path, ceilings: dict[str, int]) -> list[str]:
    """Every violation of (a)-(d), each naming its file and line where one exists."""
    blocks = _load("doc_blocks")
    paths = tracked(repo)
    found: list[str] = []
    status_path, slate_path = blocks.STATUS_PATH, blocks.SLATE_PATH
    retired = [
        p
        for p in paths
        if p.endswith(LEDGER_SUFFIX)
        and (p.startswith(COMPLETED + "/") or p.startswith(ARCHIVE + "/"))
    ]
    for path in (status_path, slate_path):
        if path not in paths:
            found.append(f"{path}: not tracked")
            continue
        text = (repo / path).read_text(encoding="utf-8")
        parsed = blocks.parse(text, path)
        found.extend(parsed.findings)
        if path == status_path:
            for block in parsed.blocks:
                if block.kind == "ws" and block.attrs.get("state") == "closed":
                    found.append(
                        f"{path}:{block.start + 1}: closed campaign `{block.id}` is still here — "
                        "run `python3 scripts/ledger_lifecycle.py compact`"
                    )
            for line in blocks.uncovered_bullets(text, parsed):
                found.append(f"{path}:{line}: workstream bullet outside any `ws` block")
        else:
            departed = blocks.departed_units(parsed, retired)
            for block in parsed.blocks:
                if block.kind == "unit" and block.id in departed:
                    found.append(
                        f"{path}:{block.start + 1}: unit `{block.id}` merged and is still on the "
                        "slate — run `python3 scripts/ledger_lifecycle.py compact`"
                    )
        size = len(text.encode("utf-8"))
        ceiling = ceilings[path]
        if size > ceiling:
            found.append(
                f"{path}: {size} B exceeds its ceiling of {ceiling} B — compact it, or raise "
                "CEILINGS in scripts/check_docs_compaction.py in this PR with the reason"
            )
    return found


def main() -> int:
    """Run the gate over the repository."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parent.parent, help=argparse.SUPPRESS
    )
    arguments = parser.parse_args()
    try:
        found = findings(arguments.repo.resolve(), CEILINGS)
    except (subprocess.CalledProcessError, OSError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    if found:
        for line in found:
            print(line, file=sys.stderr)
        print(f"docs-compaction: FAIL — {len(found)} finding(s)", file=sys.stderr)
        return 1
    sizes = ", ".join(f"{p} {(arguments.repo / p).stat().st_size} B" for p in CEILINGS)
    print(f"docs-compaction: clean ({sizes})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
