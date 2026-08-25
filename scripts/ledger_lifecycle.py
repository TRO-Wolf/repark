#!/usr/bin/env python3
"""Move ledgers between their bins and keep every link to them true.

A unit ledger's state is its directory (AGENTS.md "Markdown document
lifecycle", chartered by DL-1):

    task/ledgers/staging/                    in flight; born on the unit's branch
    task/ledgers/completed/                  finished; the agent's move, in the unit's last commit
    task/ledgers/archive/yyyy-mm/yyyy-mm-dd-<name>.md   immutable; the script's move, at pickup

Moving a markdown file is a repository-wide link rewrite, so the move and the
rewrite are one operation here, and it is resolution-based: a link is rewritten
only when it *resolved* to the moved file, so a same-named file elsewhere is
untouched, and the moved file's own outgoing links are re-expressed from its new
directory. The row for the file in its old directory's `map.md` travels to the
new directory's map, description intact.

Four subcommands:

- `archive [PATH ...]` — every ledger in `completed/` (or the paths given) moves
  to the monthly archive. The date is the author date of the `--first-parent`
  commit on `main` that added the file at its current path, so two machines
  produce the same name. A ledger not on `main` yet is the current unit's own
  (retired in its departure commit): left for the next pickup when unnamed,
  refused when named explicitly.
  `archive/map.md` is regenerated (one row per month, newest first).
- `move PATH BIN` — the agent's `staging` → `completed` step and the roadmap
  promotions (`mid-term`, `epic-term`). `archive` is not a `move` target.
- `check` — the gate: a `*-ledger.md` under `task/` outside the bins; an archive
  name whose `yyyy-mm-dd-` prefix disagrees with its `yyyy-mm/` directory; a
  relative link anywhere in the repository's tracked markdown whose target is a
  `-ledger.md` that does not exist; a `completed/` or `archive/` file changed
  since the base commit beyond a link repair or an errata note prepended at its
  top (the "frozen" and "immutable" rules).
- `compact` — the live documents carry only live state (DL-4): every `unit`
  marker in `briefs/next-sequence.md` whose ledger sits in `completed/` or the
  archive leaves the slate whole (row, prose, no obituary), and every
  `state=closed` ws block in `STATUS.md` moves to its campaign's
  `docs/history/<dir>/status-record.md` (bin and `map.md` created, links
  rewritten, refused on a dangling one) leaving one line in STATUS's
  closed-campaigns list. Grammar and transforms: `scripts/doc_blocks.py`.
  `archive` and a `move` to `completed/` run it themselves, so a pickup and a
  departure never leave the slate or STATUS stale.

Determinism: no wall clock (dates come from git), no network, sorted inputs,
idempotent (`archive` over an empty `completed/` is a no-op). Nothing is
written unless every rewritten link resolves; the moves and the rewritten files
are staged together. Exit 0 clean, 1 findings or refused, 2 usage/environment.
"""

from __future__ import annotations

import argparse
import importlib.util
import posixpath
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType

LEDGERS_DIR = "task/ledgers"
ARCHIVE_DIR = "task/ledgers/archive"
BINS: dict[str, str] = {
    "staging": "task/ledgers/staging",
    "completed": "task/ledgers/completed",
    "mid-term": "task/roadmap/mid-term",
    "epic-term": "task/roadmap/epic-term",
}
LEDGER_BINS: tuple[str, ...] = (BINS["staging"], BINS["completed"])
FROZEN_DIRS: tuple[str, ...] = (BINS["completed"], ARCHIVE_DIR)
LEDGER_SUFFIX = "-ledger.md"
MAP_NAME = "map.md"
# The fetched ref first: on a box where the owner merges remotely, local `main` lags.
MAIN_REFS: tuple[str, ...] = ("origin/main", "main")

ARCHIVE_NAME = re.compile(r"^(\d{4}-\d{2})-\d{2}-.+\.md$")
MONTH_DIR = re.compile(r"^\d{4}-\d{2}$")
# `](target)` — the part of a link the lifecycle is allowed to change. A target with
# whitespace in it is visible prose, not a path, and stays in the comparison.
LINK_TARGET = re.compile(r"\]\([^)\s]*\)")
# A row of a map's `## Contents`: an unindented bullet.
TOP_ROW = re.compile(r"^(?:[-*+]|\d+\.)\s")

PURPOSE: dict[str, str] = {
    BINS["staging"]: "Ledgers of units in flight; a charter stays here until its retirement event.",
    BINS["completed"]: "Ledgers of finished units, frozen; `make ledger-archive` files them.",
    BINS["mid-term"]: "Evaluated intakes awaiting an owner charter.",
    BINS["epic-term"]: "North-star tracks, shaped like PROJECT.md roadmap items.",
}


def _load_sync_map_md() -> ModuleType:
    """The map-content guard, for its link parser and list-item helpers."""
    location = Path(__file__).resolve().parent / "sync_map_md.py"
    specification = importlib.util.spec_from_file_location("sync_map_md", location)
    if specification is None or specification.loader is None:
        raise RuntimeError(f"cannot load {location}")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


MAPS = _load_sync_map_md()


def _load_doc_blocks() -> ModuleType:
    """The block grammar of the two live documents (DL-4)."""
    location = Path(__file__).resolve().parent / "doc_blocks.py"
    specification = importlib.util.spec_from_file_location("doc_blocks", location)
    if specification is None or specification.loader is None:
        raise RuntimeError(f"cannot load {location}")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


BLOCKS = _load_doc_blocks()
HISTORY_DIR = "docs/history"
STATUS_RECORD = "status-record.md"


def git(repo: Path, *arguments: str) -> str:
    """Run one git command in `repo` and return its stdout."""
    completed = subprocess.run(
        ["git", "-C", str(repo), *arguments], capture_output=True, check=True, text=True
    )
    return completed.stdout


def known_paths(tracked: list[str]) -> set[str]:
    """Every tracked file plus every directory above one — what a link may resolve to."""
    known: set[str] = set()
    for path in tracked:
        known.add(path)
        parent = posixpath.dirname(path)
        while parent:
            known.add(parent)
            parent = posixpath.dirname(parent)
    return known


def _masked_lines(text: str) -> list[tuple[str, str]]:
    """Each line paired with a copy whose code spans are blanked, fenced blocks blanked whole."""
    pairs: list[tuple[str, str]] = []
    in_fence = False
    for line in text.splitlines(keepends=True):
        if MAPS.FENCE_PATTERN.match(line):
            in_fence = not in_fence
            pairs.append((line, " " * len(line)))
            continue
        masked = " " * len(line) if in_fence else MAPS.CODE_SPAN_PATTERN.sub(_blank, line)
        pairs.append((line, masked))
    return pairs


def _blank(match: re.Match[str]) -> str:
    """Same-length whitespace, so positions survive masking."""
    return " " * len(match.group(0))


def resolve(base_dir: str, target: str) -> str | None:
    """Repo-relative path a link target resolves to from `base_dir`, or None if not local."""
    if MAPS._is_external(target):
        return None
    local = MAPS._local_target(target)
    if not local or local.startswith("/"):
        return None
    return posixpath.normpath(posixpath.join(base_dir, local))


def rewrite_links(
    text: str, old_dir: str, new_dir: str, moves: dict[str, str], known: set[str]
) -> tuple[str, int]:
    """Re-express every resolvable link in `text` from `new_dir`, following `moves`.

    A link that did not resolve from `old_dir` is left exactly as it was: it was
    already dangling, and rewriting it would only hide that.
    """
    out: list[str] = []
    rewritten = 0
    for line, masked in _masked_lines(text):
        cursor = 0
        pieces: list[str] = []
        for match in MAPS.LINK_PATTERN.finditer(masked):
            target = match.group(1)
            resolved = resolve(old_dir, target)
            if resolved is None or resolved not in known:
                continue
            destination = moves.get(resolved, resolved)
            relative = posixpath.relpath(destination, new_dir or ".")
            local = MAPS._local_target(target)
            if local.endswith("/"):
                relative += "/"
            fragment = target.strip().split("#", 1)
            new_target = relative + ("#" + fragment[1] if len(fragment) == 2 else "")
            if new_target == target:
                continue
            start, end = match.span(1)
            pieces.append(line[cursor:start])
            pieces.append(new_target)
            cursor = end
            rewritten += 1
        pieces.append(line[cursor:])
        out.append("".join(pieces))
    return "".join(out), rewritten


def dangling(text: str, base_dir: str, known: set[str]) -> list[str]:
    """Local link targets in `text` that resolve to nothing in `known`."""
    missing: list[str] = []
    for _line, masked in _masked_lines(text):
        for match in MAPS.LINK_PATTERN.finditer(masked):
            resolved = resolve(base_dir, match.group(1))
            if resolved is not None and resolved not in known:
                missing.append(resolved)
    return missing


def _contents_bounds(lines: list[str]) -> tuple[int, int]:
    """[start, end) line indices of the rows under `## Contents`."""
    end = MAPS.contents_insert_index(lines)
    start = 0
    for index, line in enumerate(lines):
        if line.strip().lower() == "## contents":
            start = index + 1
            break
    return start, end


def _row_spans(lines: list[str], start: int, end: int) -> list[tuple[int, int]]:
    """Every row inside [start, end): a top-level bullet plus every indented line under it.

    Indented lines are the row's own — wrapped description, or a nested sub-list —
    and travel with it. (`sync_map_md`'s span helper stops at an indented bullet
    because *deleting* past one would orphan children; a move carries them.)
    """
    spans: list[tuple[int, int]] = []
    index = start
    while index < end:
        if TOP_ROW.match(lines[index]):
            stop = index + 1
            while stop < end and lines[stop].strip() and lines[stop][0] in " \t":
                stop += 1
            spans.append((index, stop))
            index = stop
        else:
            index += 1
    return spans


def _first_link(line: str) -> str | None:
    """The first link target on a line, or None."""
    links = MAPS._line_links(line)
    return links[0] if links else None


def _collapse_blanks(lines: list[str], start: int, end: int) -> list[str]:
    """Inside [start, end), no blank line under the heading and never two in a row."""
    kept: list[str] = []
    for index, line in enumerate(lines):
        blank = start <= index < end and not line.strip()
        if blank and (index == start or (kept and not kept[-1].strip())):
            continue
        kept.append(line)
    return kept


def cut_row(text: str, map_dir: str, path: str) -> tuple[str, str | None]:
    """Remove the `## Contents` row whose first link is `path`; return (text, row)."""
    lines = text.splitlines(keepends=True)
    start, end = _contents_bounds(lines)
    for row_start, row_end in _row_spans(lines, start, end):
        target = _first_link(lines[row_start])
        if target is not None and resolve(map_dir, target) == path:
            row = "".join(lines[row_start:row_end])
            if not row.endswith("\n"):
                row += "\n"
            remaining = lines[:row_start] + lines[row_end:]
            width = row_end - row_start
            return "".join(_collapse_blanks(remaining, start, end - width)), row
    return text, None


def _condense_row(row: str) -> str:
    """One line: the bullet with its wrapped lines joined, cut at the first sentence.

    Archive month maps are an index off the normal read path (DL-3, owner ruling
    2026-08-23): the record is the ledger and git history keeps the long row, so
    the map carries the link plus the first sentence of the description. A
    description with no `. ` boundary stays whole.
    """
    joined = " ".join(part.strip() for part in row.splitlines() if part.strip())
    marker = " — "
    head, separator, description = joined.partition(marker)
    if separator and ". " in description:
        description = description.split(". ", 1)[0] + "."
    return f"{head}{separator}{description}\n"


def paste_row(text: str, row: str) -> str:
    """Insert `row` among the `## Contents` rows, keeping them sorted by first link."""
    lines = text.splitlines(keepends=True)
    start, end = _contents_bounds(lines)
    spans = _row_spans(lines, start, end)
    rows = ["".join(lines[a:b]) for a, b in spans]
    rows.append(row)
    rows = [r if r.endswith("\n") else r + "\n" for r in rows]
    rows.sort(key=lambda r: _first_link(r.splitlines()[0]) or "")
    head = lines[:start]
    tail = lines[end:]
    if tail and tail[0].strip():
        tail.insert(0, "\n")
    body = "".join(rows)
    return "".join(head) + body + "".join(tail)


def _synthesized_row(path: str, text: str) -> str:
    """A map row built from the file's H1 when its old map carried no row for it."""
    title = Path(path).name
    for line in text.splitlines():
        if line.startswith("# "):
            title = line[2:].strip()
            break
    name = Path(path).name
    return f"- [{name}]({name}) — {title}\n"


def map_template(directory: str, bin_purpose: str) -> str:
    """A fresh `map.md` for a bin directory the script is populating."""
    depth = directory.count("/") + 1
    up = "../" * depth
    return (
        f"# map — {directory}/\n\n## Purpose\n{bin_purpose}\n\n## Contents\n\n"
        f"## Pointers\n- Up: [../map.md](../map.md)\n"
        f'- Policy: [{up}AGENTS.md]({up}AGENTS.md) "Markdown document lifecycle"\n'
    )


def _purpose_for(directory: str) -> str:
    """The one-line purpose of a bin directory, monthly archive folders included."""
    if directory.startswith(ARCHIVE_DIR + "/"):
        month = Path(directory).name
        return (
            f"Ledgers archived in {month}; immutable — corrections are dated errata at the top.\n"
            "One line per ledger, and off the normal read path: grep this directory for a unit; "
            "do not read this file whole."
        )
    return PURPOSE.get(directory, "Ledgers filed by the lifecycle script.")


def plan(
    repo: Path, tracked: list[str], moves: dict[str, str], promised: set[str]
) -> tuple[dict[str, str], int, list[str]]:
    """Every markdown file's new text keyed by its new path, the link count, and new dangling links.

    `promised` names files the caller will write after the plan (the regenerated
    `archive/map.md`), so links to them are not reported as dangling. Pure:
    reads the tree, writes nothing.
    """
    known_before = known_paths(tracked)
    after = [moves.get(path, path) for path in tracked]
    known_after = known_paths(after) | known_paths(sorted(promised))
    texts: dict[str, str] = {}
    originals: dict[str, str] = {}
    rewritten = 0
    for path in tracked:
        if not path.endswith(".md") or not (repo / path).is_file():
            continue
        text = (repo / path).read_text(encoding="utf-8")
        new_path = moves.get(path, path)
        originals[new_path] = text
        new_text, count = rewrite_links(
            text, posixpath.dirname(path), posixpath.dirname(new_path), moves, known_before
        )
        rewritten += count
        texts[new_path] = new_text

    for old, new in sorted(moves.items()):
        source_map = posixpath.join(posixpath.dirname(old), MAP_NAME)
        target_dir = posixpath.dirname(new)
        target_map = posixpath.join(target_dir, MAP_NAME)
        row = None
        if source_map in texts and source_map != target_map:
            texts[source_map], row = cut_row(texts[source_map], posixpath.dirname(old), new)
        if row is None:
            row = _synthesized_row(new, texts[new])
        else:
            row, _count = rewrite_links(row, posixpath.dirname(old), target_dir, {}, known_after)
            old_name, new_name = Path(old).name, Path(new).name
            if old_name != new_name:
                row = row.replace(f"[{old_name}]", f"[{new_name}]", 1)
        if target_map not in texts:
            texts[target_map] = map_template(target_dir, _purpose_for(target_dir))
            originals[target_map] = ""
            known_after.add(target_map)
        if target_dir.startswith(ARCHIVE_DIR + "/"):
            row = _condense_row(row)
        texts[target_map] = paste_row(texts[target_map], row)

    broken: list[str] = []
    for path in sorted(texts):
        base = posixpath.dirname(path)
        before = set(dangling(originals.get(path, ""), base, known_before))
        for target in dangling(texts[path], base, known_after):
            if target not in before:
                broken.append(f"{path}: `{target}` would not resolve")
    return texts, rewritten, broken


def apply(repo: Path, moves: dict[str, str], texts: dict[str, str]) -> int:
    """Perform the moves and write every changed file, staging all of it. Returns files written."""
    for old, new in sorted(moves.items()):
        (repo / new).parent.mkdir(parents=True, exist_ok=True)
        git(repo, "mv", old, new)
    written = 0
    for path in sorted(texts):
        target = repo / path
        before = target.read_text(encoding="utf-8") if target.exists() else None
        if before == texts[path]:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(texts[path], encoding="utf-8")
        git(repo, "add", path)
        written += 1
    return written


def archive_date(repo: Path, path: str) -> str | None:
    """Author date (yyyy-mm-dd) of the first-parent `main` commit that added `path`."""
    for ref in MAIN_REFS:
        try:
            git(repo, "rev-parse", "--verify", "--quiet", ref)
        except subprocess.CalledProcessError:
            continue
        out = git(
            repo, "log", "--diff-filter=A", "--format=%as", "-1", "--first-parent", ref, "--", path
        ).strip()
        return out or None
    return None


def archive_map_text(tracked_after: list[str]) -> str:
    """`task/ledgers/archive/map.md`, regenerated: one row per month, newest first."""
    counts: dict[str, int] = {}
    for path in tracked_after:
        parts = path.split("/")
        if path.startswith(ARCHIVE_DIR + "/") and len(parts) == 5 and parts[4] != MAP_NAME:
            counts[parts[3]] = counts.get(parts[3], 0) + 1
    rows = [
        f"- [{month}/]({month}/map.md) — {count} ledger{'s' if count != 1 else ''}\n"
        for month, count in sorted(counts.items(), reverse=True)
    ]
    return (
        f"# map — {ARCHIVE_DIR}/\n\n## Purpose\nThe ledger archive, one folder per month, "
        "immutable and off the normal read path (grep for a unit; do not read the month maps "
        "whole). Generated by `scripts/ledger_lifecycle.py archive`; do not edit by hand.\n\n"
        "## Contents\n" + "".join(rows) + "\n## Pointers\n- Up: [../map.md](../map.md)\n"
        '- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"\n'
    )


def execute(
    repo: Path, tracked: list[str], moves: dict[str, str], verb: str, promised: set[str]
) -> int:
    """Plan, verify, apply; the shared tail of `archive` and `move`."""
    texts, rewritten, broken = plan(repo, tracked, moves, promised)
    if broken:
        for line in broken:
            print(line, file=sys.stderr)
        print(
            f"ledger-lifecycle: REFUSED — {len(broken)} link(s) would dangle; nothing changed",
            file=sys.stderr,
        )
        return 1
    written = apply(repo, moves, texts)
    print(
        f"ledger-lifecycle: {verb} {len(moves)} file(s), "
        f"rewrote {rewritten} link(s), wrote {written} file(s)"
    )
    return 0


def run_archive(repo: Path, paths: list[str]) -> int:
    """`archive`: file `completed/` (or the given paths) into the monthly archive."""
    tracked = MAPS.tracked_paths(repo)
    named = bool(paths)
    if not named:
        prefix = BINS["completed"] + "/"
        paths = [p for p in tracked if p.startswith(prefix) and Path(p).name != MAP_NAME]
    moves: dict[str, str] = {}
    for path in sorted(paths):
        if path not in tracked:
            print(f"ERROR: {path} is not a tracked file", file=sys.stderr)
            return 2
        date = archive_date(repo, path)
        if date is None:
            if named:
                print(
                    f"ERROR: {path} is not on main yet — archive runs at pickup, after the merge",
                    file=sys.stderr,
                )
                return 1
            # The current unit's own retired ledger: it waits for its merge.
            print(f"ledger-lifecycle: {path} is not on main yet — left for the next pickup")
            continue
        new = f"{ARCHIVE_DIR}/{date[:7]}/{date}-{Path(path).name}"
        if new in tracked or (repo / new).exists():
            print(f"ERROR: {new} already exists", file=sys.stderr)
            return 1
        moves[path] = new
    if not moves:
        print("ledger-lifecycle: nothing to archive")
        return 0
    archive_map = f"{ARCHIVE_DIR}/{MAP_NAME}"
    status = execute(repo, tracked, moves, "archived", {archive_map})
    if status:
        return status
    after = [moves.get(p, p) for p in tracked] + [archive_map]
    (repo / archive_map).write_text(archive_map_text(after), encoding="utf-8")
    git(repo, "add", archive_map)
    return run_compact(repo)


def run_move(repo: Path, path: str, bin_name: str) -> int:
    """`move`: relocate one document into a named bin."""
    tracked = MAPS.tracked_paths(repo)
    if path not in tracked:
        print(f"ERROR: {path} is not a tracked file", file=sys.stderr)
        return 2
    new = f"{BINS[bin_name]}/{Path(path).name}"
    if new == path:
        print(f"ledger-lifecycle: {path} is already in {bin_name}")
        return 0
    if new in tracked or (repo / new).exists():
        print(f"ERROR: {new} already exists", file=sys.stderr)
        return 1
    status = execute(repo, tracked, {path: new}, "moved", set())
    if status or bin_name != "completed":
        return status
    return run_compact(repo)


def retired_ledgers(tracked: list[str]) -> list[str]:
    """Every ledger in `completed/` or the archive — the units that have left."""
    return sorted(
        path
        for path in tracked
        if path.endswith(LEDGER_SUFFIX)
        and (path.startswith(BINS["completed"] + "/") or path.startswith(ARCHIVE_DIR + "/"))
    )


def _history_purpose(directory: str) -> str:
    """The Purpose line of a campaign's history bin the compaction creates."""
    name = Path(directory).name
    return (
        f"Archived record of the {name} campaign, cut from STATUS.md when the owner ruled it "
        "closed. History, not law. Current state: [STATUS.md](../../../STATUS.md)."
    )


def compact_plan(repo: Path, tracked: list[str]) -> tuple[dict[str, str], list[str], list[str]]:
    """The compaction's new texts keyed by path, its log lines, and any dangling link. Pure."""
    texts: dict[str, str] = {}
    log: list[str] = []
    known = known_paths(tracked)
    slate_path, status_path = BLOCKS.SLATE_PATH, BLOCKS.STATUS_PATH
    if slate_path in tracked:
        slate = (repo / slate_path).read_text(encoding="utf-8")
        parsed = BLOCKS.parse(slate, slate_path)
        if parsed.findings:
            return {}, [], [f"{slate_path}: {f}" for f in parsed.findings]
        departed = BLOCKS.departed_units(parsed, retired_ledgers(tracked))
        if departed:
            new_slate, removed = BLOCKS.remove_units(slate, departed)
            texts[slate_path] = new_slate
            log.append(f"{', '.join(departed)} left the slate ({removed} block(s))")
    if status_path in tracked:
        status = (repo / status_path).read_text(encoding="utf-8")
        parsed = BLOCKS.parse(status, status_path)
        if parsed.findings:
            return {}, [], [f"{status_path}: {f}" for f in parsed.findings]
        new_status, cuts = BLOCKS.cut_closed(status)
        history_map = f"{HISTORY_DIR}/{MAP_NAME}"
        for block, content in cuts:
            directory = block.attrs["history"].rstrip("/")
            record = f"{directory}/{STATUS_RECORD}"
            moved, _count = rewrite_links(content, "", directory, {}, known)
            heading = (
                f"## Cut from STATUS.md — closed {block.attrs['closed']} by {block.attrs['by']}\n\n"
            )
            previous = texts.get(record)
            if previous is None and (repo / record).exists():
                previous = (repo / record).read_text(encoding="utf-8")
            if previous is None:
                previous = f"# {Path(directory).name} — STATUS record\n\n"
            texts[record] = previous + heading + moved
            bin_map = f"{directory}/{MAP_NAME}"
            if bin_map not in texts and not (repo / bin_map).exists():
                texts[bin_map] = map_template(directory, _history_purpose(directory))
                if history_map in tracked:
                    parent = texts.get(history_map) or (repo / history_map).read_text(
                        encoding="utf-8"
                    )
                    row = (
                        f"- [{Path(directory).name}/]({Path(directory).name}/{MAP_NAME}) — "
                        f"the {Path(directory).name} campaign's STATUS record, cut "
                        f"{block.attrs['closed']}.\n"
                    )
                    texts[history_map] = paste_row(parent, row)
            if bin_map in texts or not any(
                STATUS_RECORD in line for line in (repo / bin_map).read_text().splitlines()
            ):
                current = texts.get(bin_map) or (repo / bin_map).read_text(encoding="utf-8")
                texts[bin_map] = paste_row(
                    current,
                    f"- [{STATUS_RECORD}]({STATUS_RECORD}) — the workstream bullet as STATUS.md "
                    f"carried it, cut {block.attrs['closed']}.\n",
                )
            new_status = BLOCKS.record_closed(new_status, block, content, record)
            log.append(f"{block.id} left STATUS for {record}")
        if cuts:
            texts[status_path] = new_status
    broken: list[str] = []
    known_after = known | known_paths(sorted(texts))
    for path, text in sorted(texts.items()):
        for target in dangling(text, posixpath.dirname(path), known_after):
            broken.append(f"{path}: `{target}` would not resolve")
    return texts, log, broken


def run_compact(repo: Path) -> int:
    """`compact`: the live documents carry only live state."""
    tracked = MAPS.tracked_paths(repo)
    texts, log, broken = compact_plan(repo, tracked)
    if broken:
        for line in broken:
            print(line, file=sys.stderr)
        print(
            f"docs-compaction: REFUSED — {len(broken)} finding(s); nothing changed",
            file=sys.stderr,
        )
        return 1
    if not texts:
        print("docs-compaction: nothing to do")
        return 0
    written = apply(repo, {}, texts)
    for line in log:
        print(f"docs-compaction: {line}")
    print(f"docs-compaction: wrote {written} file(s)")
    return 0


def _base_commit(repo: Path, base: str | None) -> str | None:
    """The commit the frozen rule diffs against: `--base`, else merge-base with main."""
    candidates = [base] if base else list(MAIN_REFS)
    for ref in candidates:
        try:
            return git(repo, "merge-base", "HEAD", ref).strip()
        except subprocess.CalledProcessError:
            continue
    return None


def _frozen(path: str) -> bool:
    """True for a file the frozen/immutable rule protects."""
    return any(path.startswith(d + "/") for d in FROZEN_DIRS) and Path(path).name != MAP_NAME


def _archived_twins(repo: Path, path: str) -> list[str]:
    """Where a `completed/` ledger may have gone: its dated copies under `archive/`."""
    if not path.startswith(BINS["completed"] + "/"):
        return []
    twins = sorted((repo / ARCHIVE_DIR).glob(f"*/????-??-??-{Path(path).name}"))
    return [twin.relative_to(repo).as_posix() for twin in twins]


def _beyond_repair(repo: Path, base: str, old: str, new: str) -> bool:
    """True if `new` differs from `base:old` by more than link targets or a prepended note."""
    before = LINK_TARGET.sub("]()", git(repo, "show", f"{base}:{old}"))
    after = LINK_TARGET.sub("]()", (repo / new).read_text(encoding="utf-8"))
    return after != before and not after.endswith(before)


def frozen_findings(repo: Path, base: str) -> list[str]:
    """Frozen files changed since `base` beyond a link repair or a prepended errata note.

    No rename heuristics: a `completed/` file may disappear only into its dated
    archive twin, found by name, and the twin must carry the same text.
    """
    findings: list[str] = []
    status = git(repo, "diff", "--no-renames", "--name-status", base, "--", *FROZEN_DIRS)
    for entry in status.splitlines():
        kind, old = entry.split("\t")[:2]
        if not _frozen(old) or kind == "A":
            continue
        homes = [old] if kind == "M" else _archived_twins(repo, old)
        if not homes:
            findings.append(f"{old}: deleted — truth moves, it is never deleted")
        elif all(_beyond_repair(repo, base, old, new) for new in homes):
            findings.append(
                f"{homes[-1]}: frozen ledger edited beyond a link repair or a prepended errata note"
            )
    return findings


def run_check(repo: Path, base: str | None) -> int:
    """`check`: the gate."""
    tracked = MAPS.tracked_paths(repo)
    known = known_paths(tracked)
    findings: list[str] = []
    ledgers = 0
    archived = 0
    for path in tracked:
        directory, name = posixpath.dirname(path), Path(path).name
        if path.startswith("task/") and name.endswith(LEDGER_SUFFIX):
            ledgers += 1
            in_archive = directory.startswith(ARCHIVE_DIR + "/")
            if directory not in LEDGER_BINS and not in_archive:
                bins = ", ".join((*LEDGER_BINS, f"{ARCHIVE_DIR}/yyyy-mm"))
                findings.append(f"{path}: ledger outside the bins ({bins})")
        if path.startswith(ARCHIVE_DIR + "/") and name != MAP_NAME:
            archived += 1
            month = Path(directory).name
            match = ARCHIVE_NAME.match(name)
            if directory == ARCHIVE_DIR or not MONTH_DIR.match(month) or match is None:
                findings.append(
                    f"{path}: archive files are {ARCHIVE_DIR}/yyyy-mm/yyyy-mm-dd-<name>.md"
                )
            elif match.group(1) != month:
                findings.append(f"{path}: prefix {match.group(1)} disagrees with directory {month}")

    links = 0
    for path in tracked:
        if not path.endswith(".md") or not (repo / path).is_file():
            continue
        base_dir = posixpath.dirname(path)
        text = (repo / path).read_text(encoding="utf-8")
        for _line, masked in _masked_lines(text):
            for match in MAPS.LINK_PATTERN.finditer(masked):
                resolved = resolve(base_dir, match.group(1))
                if resolved is None or not resolved.endswith(LEDGER_SUFFIX):
                    continue
                links += 1
                if resolved not in known:
                    findings.append(f"{path}: dead ledger link `{resolved}`")

    base_commit = _base_commit(repo, base)
    if base_commit is None:
        wanted = base or " or ".join(MAIN_REFS)
        print(
            f"ERROR: no base commit — {wanted} does not resolve; refuse to pass closed",
            file=sys.stderr,
        )
        return 2
    findings.extend(frozen_findings(repo, base_commit))

    if findings:
        for finding in findings:
            print(finding, file=sys.stderr)
        print(f"ledger-check: FAIL — {len(findings)} finding(s)", file=sys.stderr)
        return 1
    print(
        f"ledger-check: {ledgers} ledgers in bins ({archived} archived), "
        f"{links} ledger links resolve, frozen rule clean"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    """The CLI surface: `archive`, `move`, `compact`, `check`."""
    parser = argparse.ArgumentParser(
        prog="ledger_lifecycle.py", description=__doc__.split("\n\n")[0]
    )
    default_repo = Path(__file__).resolve().parent.parent
    parser.add_argument("--repo", type=Path, default=default_repo, help=argparse.SUPPRESS)
    commands = parser.add_subparsers(dest="command", required=True)
    archive = commands.add_parser(
        "archive", help="file completed/ (or PATHs) into archive/yyyy-mm/"
    )
    archive.add_argument("paths", nargs="*", help="explicit tracked paths instead of completed/")
    move = commands.add_parser("move", help="relocate one document into a bin")
    move.add_argument("path")
    move.add_argument("bin", choices=sorted(BINS))
    commands.add_parser("compact", help="merged units leave the slate; closed campaigns leave it")
    check = commands.add_parser("check", help="the gate")
    check.add_argument(
        "--base", default=None, help="commit to diff the frozen bins against (default: main)"
    )
    return parser


def main() -> int:
    """Dispatch to the subcommand."""
    arguments = build_parser().parse_args()
    repo: Path = arguments.repo.resolve()
    try:
        if arguments.command == "archive":
            return run_archive(repo, [posixpath.normpath(p) for p in arguments.paths])
        if arguments.command == "move":
            return run_move(repo, posixpath.normpath(arguments.path), arguments.bin)
        if arguments.command == "compact":
            return run_compact(repo)
        return run_check(repo, arguments.base)
    except (subprocess.CalledProcessError, OSError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
