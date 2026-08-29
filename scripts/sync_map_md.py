#!/usr/bin/env python3
"""Keep every `map.md` honest about the tree it navigates.

The repo's hard rule is "`map.md` in every directory, updated in the same change" (AGENTS.md).
`scripts/check_map_md.sh` holds the *lockstep* half: staged code forces a staged map. This
script holds the *content* half — a touched map can still point at a file that moved and stay
silent about a file that arrived.

Two rules, over every tracked `map.md`:

1. **Link validity.** Every relative markdown link in the map resolves to a file or directory
   that exists. External links (`http:`, `https:`, `mailto:`) and bare anchors (`#section`) are
   out of scope: nothing local can check them. An absolute target (`/docs/foo.md`) is a finding
   in its own right — it is neither repo-relative nor resolvable by GitHub. Link-shaped text
   inside an inline code span or a fenced block is documentation of the syntax, not a link, and
   is skipped. Inline `[text](target)` links only: reference-style definitions and bare URLs
   are not link rows in this repo's maps and are not parsed.

2. **Coverage** (`--strict`). Every *mappable* tracked file sitting in the map's own directory
   is mentioned somewhere in the map by name. Mappable = one of MAPPABLE_SUFFIXES, excluding
   `map.md` itself, lockfiles, and dotfiles. Directories below the map belong to their own map.

Coverage is behind `--strict` because the tree carries a large body of pre-existing unmentioned
files; a gate nobody can run green is not a gate. Link validity is armed unconditionally. The
recorded coverage count is a **floor**: a name counts as mentioned wherever it appears as a
whole token in the map.

`--fix` performs only the mechanical half of the repair and never writes prose: a row whose
target does not exist is deleted when the row is a list item whose *only* link is the dead one,
and an unmentioned file gets a stub row ending in `TODO(describe)`. An absolute target is never
deleted — it is a misspelling that may well resolve once repointed. A row carrying a nested
sub-list is never deleted, because the sub-items would be orphaned; it is reported for a hand
edit. Descriptions stay agent-authored: the description is the whole value of a map.

Exit 0 clean, 1 findings, 2 usage/environment error. The tracked set comes from `git ls-files`,
so an untracked build directory is never walked.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

MAP_NAME = "map.md"

# Suffixes a map is expected to account for: source and prose. Data fixtures,
# notebooks and binaries are deliberately absent — a map describes the files a
# reader navigates by, not every byte in the directory.
MAPPABLE_SUFFIXES: frozenset[str] = frozenset({".rs", ".py", ".sh", ".md", ".toml"})

# Lockfiles churn without a map edit by explicit rule (check_map_md.sh excludes
# them from lockstep for the same reason), so they are not coverage subjects.
LOCKFILE_NAMES: frozenset[str] = frozenset({"Cargo.lock", "uv.lock"})

# Inline `[text](target)` links. The target body allows one level of balanced parentheses so
# a filename like `weird(1).md` is read whole instead of truncated at its first `)`.
# Reference-style definitions and bare URLs are not link rows in this repo's maps.
LINK_PATTERN = re.compile(
    r"\[[^\]]*\]\(\s*(<[^<>]*>|(?:[^()\s]|\([^()]*\))*)(?:\s+\"[^\"]*\")?\s*\)"
)

# A markdown list item: `- `, `* `, or `1. `, with optional leading indent.
LIST_ITEM_PATTERN = re.compile(r"^\s*(?:[-*+]|\d+\.)\s")

# Code spans and fenced blocks hold link-shaped TEXT that documents the syntax, not a link —
# a map that documents `[<name>](<name>)` must not be told its own example is broken.
CODE_SPAN_PATTERN = re.compile(r"`[^`]*`")
FENCE_PATTERN = re.compile(r"^\s*(?:```|~~~)")

EXTERNAL_PREFIXES: tuple[str, ...] = ("http://", "https://", "mailto:", "ftp://")

STUB_SUFFIX = "TODO(describe)"

MISSING = "does not exist"
ABSOLUTE = "is an absolute path — map links must be relative to the map"


def tracked_paths(repo: Path) -> list[str]:
    """Every path git tracks, repo-relative posix, sorted."""
    completed = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "-z"],
        capture_output=True,
        check=True,
        text=True,
    )
    return sorted(entry for entry in completed.stdout.split("\0") if entry)


def _is_external(target: str) -> bool:
    """True for a link this repository cannot resolve on disk."""
    lowered = target.strip().lower()
    return lowered.startswith(EXTERNAL_PREFIXES) or lowered.startswith("#")


def _local_target(target: str) -> str:
    """The on-disk part of a link target: angle brackets, title and fragment stripped.

    `<path with space.md>` is the sanctioned CommonMark spelling for a target
    containing a space; the brackets are delimiters, not part of the path.
    """
    cleaned = target.strip()
    if cleaned.startswith("<") and cleaned.endswith(">"):
        cleaned = cleaned[1:-1].strip()
    else:
        cleaned = cleaned.split(" ", 1)[0]
    return cleaned.split("#", 1)[0]


def _line_links(line: str) -> list[str]:
    """Inline link targets on one line, code spans removed first."""
    return LINK_PATTERN.findall(CODE_SPAN_PATTERN.sub("", line))


def find_dead_links(map_path: Path, lines: list[str]) -> list[tuple[int, str, str]]:
    """Unresolvable links, as (1-based line, target, what is wrong with it)."""
    directory = map_path.parent
    dead: list[tuple[int, str, str]] = []
    in_fence = False
    for index, line in enumerate(lines, start=1):
        if FENCE_PATTERN.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        for target in _line_links(line):
            if _is_external(target):
                continue
            resolved = _local_target(target)
            if not resolved:
                continue
            if resolved.startswith("/"):
                dead.append((index, resolved, ABSOLUTE))
                continue
            if not (directory / resolved).exists():
                dead.append((index, resolved, MISSING))
    return dead


def mappable_siblings(map_relative: str, tracked: list[str]) -> list[str]:
    """Tracked mappable file names in the map's own directory (not below it)."""
    parent = str(Path(map_relative).parent)
    prefix = "" if parent == "." else f"{parent}/"
    names: list[str] = []
    for path in tracked:
        if not path.startswith(prefix):
            continue
        remainder = path[len(prefix) :]
        if "/" in remainder or not remainder:
            continue
        if remainder == MAP_NAME or remainder in LOCKFILE_NAMES:
            continue
        if remainder.startswith("."):
            continue
        if Path(remainder).suffix in MAPPABLE_SUFFIXES:
            names.append(remainder)
    return sorted(names)


def find_unmentioned(text: str, names: list[str]) -> list[str]:
    """Mappable names the map never writes down as a whole token.

    Plain substring containment would let `sublib.rs` vouch for `lib.rs`, so the
    name must be bounded by something that cannot continue a filename.
    """
    missing: list[str] = []
    for name in names:
        pattern = re.compile(rf"(?<![\w./-]){re.escape(name)}(?![\w.-])")
        if not pattern.search(text):
            missing.append(name)
    return missing


def _stub_row(name: str) -> str:
    """The mechanical placeholder row for an unmentioned file."""
    return f"- [{name}]({name}) — {STUB_SUFFIX}"


def _item_span(lines: list[str], start: int) -> tuple[int, bool]:
    """Exclusive end index of the list item at `start`, and whether it nests.

    A row is its bullet plus every indented continuation line — deleting only the bullet would
    orphan the wrapped prose. An indented *list item* is a nested sub-list, not a continuation:
    the row cannot be deleted mechanically, so the caller is told to hand it back.
    """
    end = start + 1
    while end < len(lines):
        candidate = lines[end]
        if not candidate.strip() or not candidate.startswith((" ", "\t")):
            break
        if LIST_ITEM_PATTERN.match(candidate):
            return end, True
        end += 1
    return end, False


def drop_dead_rows(
    lines: list[str], dead: list[tuple[int, str, str]]
) -> tuple[list[str], int, int]:
    """Delete each dead link's whole row when the row is a list item with one link.

    Returns the surviving lines, the number of rows dropped, and the number of
    dead rows refused because they carry a nested sub-list.
    """
    doomed: set[int] = set()
    dropped = 0
    refused = 0
    for lineno, _target, problem in dead:
        # An absolute target may resolve once repointed; deleting the row throws away a
        # live description.
        if problem != MISSING:
            continue
        start = lineno - 1
        if start in doomed or not LIST_ITEM_PATTERN.match(lines[start]):
            continue
        end, nested = _item_span(lines, start)
        if nested:
            refused += 1
            continue
        links = [link for line in lines[start:end] for link in _line_links(line)]
        if len(links) != 1:
            continue
        doomed.update(range(start, end))
        dropped += 1
    kept = [line for index, line in enumerate(lines) if index not in doomed]
    return kept, dropped, refused


def contents_insert_index(lines: list[str]) -> int:
    """Line index to append stub rows at: end of `## Contents`, else end of file."""
    start = -1
    for index, line in enumerate(lines):
        if line.strip().lower() == "## contents":
            start = index
            break
    if start == -1:
        return len(lines)
    for index in range(start + 1, len(lines)):
        if lines[index].startswith("## "):
            end = index
            while end > start + 1 and not lines[end - 1].strip():
                end -= 1
            return end
    return len(lines)


def apply_fix(map_path: Path, tracked: list[str], map_relative: str, strict: bool) -> list[str]:
    """Rewrite one map mechanically; returns a description of what changed."""
    text = map_path.read_text(encoding="utf-8")
    lines = text.splitlines()
    changes: list[str] = []

    mutated = False

    lines, dropped, refused = drop_dead_rows(lines, find_dead_links(map_path, lines))
    if dropped:
        changes.append(f"dropped {dropped} dead link row(s)")
        mutated = True
    if refused:
        changes.append(f"left {refused} dead row(s) with a nested list for a hand edit")

    if strict:
        missing = find_unmentioned("\n".join(lines), mappable_siblings(map_relative, tracked))
        if missing:
            insert_at = contents_insert_index(lines)
            stubs = [_stub_row(name) for name in missing]
            lines = lines[:insert_at] + stubs + lines[insert_at:]
            changes.append(f"added {len(stubs)} stub row(s)")
            mutated = True

    if mutated:
        map_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return changes


def check_map(map_path: Path, tracked: list[str], map_relative: str, strict: bool) -> list[str]:
    """Findings for one map, already formatted for the report."""
    text = map_path.read_text(encoding="utf-8")
    lines = text.splitlines()
    findings = [
        f"{map_relative}:{lineno}: dead link — `{target}` {problem}"
        for lineno, target, problem in find_dead_links(map_path, lines)
    ]
    if strict:
        findings.extend(
            f"{map_relative}: unmentioned — `{name}` is in this directory but not in the map"
            for name in find_unmentioned(text, mappable_siblings(map_relative, tracked))
        )
    return findings


def build_parser() -> argparse.ArgumentParser:
    """The CLI surface: `--check` (default), `--fix`, `--strict`."""
    parser = argparse.ArgumentParser(
        prog="sync_map_md.py",
        description="Validate map.md links, and (--strict) that maps mention their own files.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="report violations without editing (the default)",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="mechanical repair only: drop dead link rows, append TODO(describe) stubs",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="also require every mappable tracked file to be mentioned in its directory's map",
    )
    return parser


def run(repo: Path, fix: bool, strict: bool) -> int:
    try:
        tracked = tracked_paths(repo)
    except (subprocess.CalledProcessError, OSError) as error:
        print(f"ERROR: cannot list tracked files ({error})", file=sys.stderr)
        return 2

    maps = [path for path in tracked if Path(path).name == MAP_NAME]
    if not maps:
        print("ERROR: no tracked map.md found — refuse to pass closed", file=sys.stderr)
        return 2

    findings: list[str] = []
    fixed: list[str] = []
    for map_relative in maps:
        map_path = repo / map_relative
        if not map_path.is_file():
            findings.append(f"{map_relative}: tracked but missing on disk")
            continue
        if fix:
            changes = apply_fix(map_path, tracked, map_relative, strict)
            if changes:
                fixed.append(f"{map_relative}: {', '.join(changes)}")
        findings.extend(check_map(map_path, tracked, map_relative, strict))

    for line in fixed:
        print(f"fixed {line}")

    if findings:
        for finding in findings:
            print(finding, file=sys.stderr)
        print(
            f"map-sync: FAIL — {len(findings)} finding(s) across {len(maps)} maps "
            f"(strict={'on' if strict else 'off'})",
            file=sys.stderr,
        )
        return 1

    print(f"map-sync: {len(maps)} maps clean (strict={'on' if strict else 'off'})")
    return 0


def main() -> int:
    arguments = build_parser().parse_args()
    if arguments.check and arguments.fix:
        print("ERROR: --check and --fix are mutually exclusive", file=sys.stderr)
        return 2
    repo = Path(__file__).resolve().parent.parent
    return run(repo, fix=arguments.fix, strict=arguments.strict)


if __name__ == "__main__":
    sys.exit(main())
