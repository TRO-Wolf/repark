#!/usr/bin/env python3
"""Move `pins: <unit>/C-NNN` citations out of directory maps into per-ledger sidecars.

MAP-GOVERNANCE-1. Every citation the grammar gate's CITATION regex finds in a tracked file under
its CITATION_ROOTS becomes a line of `task/ledgers/<bin>/<unit>-pins.md` (the archive sidecar
takes its ledger's `yyyy-mm-dd-` prefix), in the format rule B of
`scripts/check_ledger_grammar.py` verifies: `C-NNN: <path>[::<name>], ...`.

A citation in a source file binds that file and stays where it is. A citation in a `map.md`
binds the file its row describes: the nearest row at or above it, within its section, whose
first link or first code span names a tracked file under the map's directory, else the map
itself. The names are the backticked identifiers of the citation's stretch of that row that
occur in the bound file as whole-word tokens. The citation is then cut from the map: a line left
with nothing but punctuation is deleted, a line that also carries prose keeps the prose.

A citation is bound but left in the map's text when cutting it would break the sentence around
it: inside a fenced block, inside a code span the line continues past, or followed by an id the
regex does not read (a range, a second unit, a non-clause id).

Idempotent: an existing sidecar's lines are kept and the new entries merged in. `--check`
writes nothing, prints the same report and exits 1 when a citation names no clause in any
ledger. Exit 0 clean, 1 unplaceable citations, 2 usage or environment error.
"""

from __future__ import annotations

import argparse
import bisect
import importlib.util
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import NamedTuple

MAP_NAME = "map.md"
SENTINEL = "\x00"
HEADER = "# {unit} pin binding: one line per clause, read and verified by "
HEADER_TAIL = "scripts/check_ledger_grammar.py rule B."
FENCE = re.compile(r"^\s*(```|~~~)")
LIST_ITEM = re.compile(r"^\s*(?:[-*+]|\d+\.)\s")
TABLE_ROW = re.compile(r"^\s*\|")
HEADING = re.compile(r"^#{1,6}\s")
LINK = re.compile(r"\[[^\]]*\]\(\s*<?([^()<>\s]+)>?(?:\s+\"[^\"]*\")?\s*\)")
CODE_SPAN = re.compile(r"`([^`]+)`")
QUALIFIED = re.compile(r"^(?:[\w./-]+::)?(?:[A-Za-z_]\w*(?:::|\.))*([A-Za-z_]\w*)(?:\(\))?$")
STOP_NAMES: frozenset[str] = frozenset(
    {"true", "false", "None", "True", "False", "null", "self", "Self", "NULL", "pins"}
)
TAIL = re.compile(
    r"^(?:\s*[,;]?\s*[a-z0-9][a-z0-9.-]*/[A-Za-z]"
    r"|\s*,\s*[A-Z][A-Za-z0-9]*-[A-Za-z0-9-]*\d"
    r"|\s*(?:\.\.|…))"
)
EMPTY_REST = re.compile(r"^[\s\-*+.,;:()·]*$")


class Binding(NamedTuple):
    """One citation, resolved: where it was, what it binds, and whether it left the text."""

    source: str
    unit: str
    clause_ids: tuple[str, ...]
    path: str
    names: tuple[str, ...]
    kept: bool


def load_gate() -> ModuleType:
    """The grammar gate, so the citation regex and the bins stay single-homed."""
    location = Path(__file__).resolve().parent / "check_ledger_grammar.py"
    spec = importlib.util.spec_from_file_location("check_ledger_grammar", location)
    if spec is None or spec.loader is None:
        message = f"cannot load {location}"
        raise RuntimeError(message)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GATE = load_gate()


def is_token(text: str, name: str) -> bool:
    """Whether `name` occurs in `text` as a whole-word token."""
    return re.search(rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])", text) is not None


def ledger_homes(repo: Path, tracked: list[str]) -> tuple[dict[str, str], set[tuple[str, str]]]:
    """unit -> the ledger its sidecar sits beside (staging, then completed, then the archive),
    and every (unit, clause id) that exists."""
    homes: dict[str, str] = {}
    known: set[tuple[str, str]] = set()
    order = (GATE.STAGING + "/", GATE.COMPLETED + "/", GATE.ARCHIVE + "/")
    for prefix in order:
        for path in tracked:
            if not path.startswith(prefix) or not path.endswith(GATE.LEDGER_SUFFIX):
                continue
            unit = GATE.unit_of(path)
            homes.setdefault(unit, path)
            text = (repo / path).read_text(encoding="utf-8")
            known.update((unit, clause_id) for _n, clause_id, _c in GATE.clause_rows(text))
    return homes, known


def sidecar_path(ledger: str) -> str:
    """The sidecar beside a ledger, archive date prefix included."""
    return ledger[: -len(GATE.LEDGER_SUFFIX)] + GATE.PINS_SUFFIX


def fence_mask(lines: list[str]) -> list[bool]:
    """True for every line inside a fenced block, the fence lines included."""
    mask: list[bool] = []
    inside = False
    for line in lines:
        if FENCE.match(line):
            mask.append(True)
            inside = not inside
            continue
        mask.append(inside)
    return mask


def row_starts(lines: list[str], fenced: list[bool]) -> list[int]:
    """Line indexes that open a row: a list item, a table row, a heading or a paragraph."""
    starts: list[int] = []
    previous_blank = True
    for index, line in enumerate(lines):
        blank = not line.strip()
        if fenced[index] or blank:
            previous_blank = blank or previous_blank
            continue
        if (
            LIST_ITEM.match(line)
            or TABLE_ROW.match(line)
            or HEADING.match(line)
            or previous_blank
            or (index > 0 and HEADING.match(lines[index - 1]))
        ):
            starts.append(index)
        previous_blank = False
    return starts


def resolve_named_file(map_dir: str, candidate: str, tracked: set[str]) -> str | None:
    """A tracked file under the map's directory that `candidate` names, if any."""
    target = candidate.strip().split("#", 1)[0].split("::", 1)[0].rstrip("/")
    if not target or target.startswith(("http:", "https:", "mailto:", "/")):
        return None
    prefix = f"{map_dir}/" if map_dir else ""
    joined = Path(prefix + target).as_posix() if prefix else Path(target).as_posix()
    parts: list[str] = []
    for part in joined.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                return None
            parts.pop()
            continue
        parts.append(part)
    resolved = "/".join(parts)
    if resolved in tracked and resolved.startswith(prefix):
        return resolved
    if target in tracked and target.startswith(prefix):
        return target
    return None


def row_file(map_dir: str, row_text: str, tracked: set[str]) -> str | None:
    """The file a row describes: its first link's target, else its first code span."""
    without_spans = CODE_SPAN.sub("", row_text)
    link = LINK.search(without_spans)
    if link is not None:
        named = resolve_named_file(map_dir, link.group(1), tracked)
        if named is not None:
            return named
    span = CODE_SPAN.search(row_text)
    if span is not None:
        return resolve_named_file(map_dir, span.group(1), tracked)
    return None


def bound_file(
    map_path: str, lines: list[str], starts: list[int], line_index: int, tracked: set[str]
) -> str:
    """The nearest row at or above `line_index`, inside its section, that names a file."""
    map_dir = str(Path(map_path).parent)
    map_dir = "" if map_dir == "." else map_dir
    earlier = [start for start in starts if start <= line_index]
    ends = dict(zip(starts, [*starts[1:], len(lines)], strict=True))
    for start in reversed(earlier):
        text = "\n".join(lines[start : ends[start]])
        named = row_file(map_dir, text, tracked)
        if named is not None:
            return named
        if HEADING.match(lines[start]):
            break
    return map_path


def defines(path: str, text: str, name: str) -> bool:
    """Whether a Rust or Python file defines `name`: a fn, type, module, constant or class."""
    escaped = re.escape(name)
    if path.endswith(".rs"):
        pattern = rf"\b(?:fn|struct|enum|trait|mod|const|static|type|union)\s+{escaped}\b"
        return re.search(pattern + rf"|macro_rules!\s*{escaped}\b", text) is not None
    if path.endswith(".py"):
        pattern = rf"\b(?:def|class)\s+{escaped}\b|^{escaped}\s*[:=]"
        return re.search(pattern, text, flags=re.MULTILINE) is not None
    return False


def names_in(stretch: str, path: str, file_text: str) -> tuple[str, ...]:
    """Backticked identifiers of `stretch` that the bound file defines and carries as tokens."""
    found: set[str] = set()
    for span in CODE_SPAN.findall(stretch):
        match = QUALIFIED.match(span.strip())
        if match is None:
            continue
        name = match.group(1)
        if len(name) < 3 or name in STOP_NAMES:
            continue
        if not any(char.islower() or char == "_" for char in name):
            continue
        if is_token(file_text, name) and defines(path, file_text, name):
            found.add(name)
    return tuple(sorted(found))


def cut_span(text: str, start: int, end: int) -> tuple[int, int]:
    """Widen a citation to the separators and brackets that go with it."""
    if start > 0 and end < len(text) and text[start - 1] == "`" and text[end] == "`":
        start, end = start - 1, end + 1
    before = start
    while before > 0 and text[before - 1] in " \t":
        before -= 1
    if before > 0 and text[before - 1] == "(" and end < len(text) and text[end] == ")":
        start, end = before - 1, end + 1
        if start > 0 and text[start - 1] in " \t":
            start -= 1
        return start, end
    start = before
    if start > 0 and text[start - 1] in ",;·":
        start -= 1
        while start > 0 and text[start - 1] in " \t":
            start -= 1
    head = text[:start].rstrip(" \t")
    if end < len(text) and text[end] in ".,;" and head and head[-1] in ".:;,!?":
        end += 1
    return start, end


def keeps_text(text: str, start: int, end: int, line_start: int, line_end: int) -> bool:
    """Whether cutting the citation would break the sentence around it."""
    if TAIL.match(text[end:]):
        return True
    wrapped = start > 0 and end < len(text) and text[start - 1] == "`" and text[end] == "`"
    if not wrapped:
        return False
    rest = text[line_start : start - 1] + text[end + 1 : line_end]
    return EMPTY_REST.match(rest) is None


def tidy(text: str) -> tuple[str, int, int]:
    """Drop lines a cut left empty, trim the rest; (text, lines deleted, lines trimmed)."""
    lines = text.split("\n")
    kept: list[str] = []
    deleted = 0
    trimmed = 0
    for index, line in enumerate(lines):
        if SENTINEL not in line:
            kept.append(line)
            continue
        clean = line.replace(SENTINEL, "").rstrip()
        follower = lines[index + 1] if index + 1 < len(lines) else ""
        continued = bool(LIST_ITEM.match(clean)) and follower.startswith((" ", "\t"))
        structural = TABLE_ROW.match(clean) or HEADING.match(clean)
        if EMPTY_REST.match(clean) and not structural and not continued:
            deleted += 1
            if kept and not kept[-1].strip() and not follower.replace(SENTINEL, "").strip():
                kept.pop()
            continue
        trimmed += 1
        kept.append(clean)
    return "\n".join(kept), deleted, trimmed


def migrate_map(
    repo: Path, path: str, tracked: set[str]
) -> tuple[list[Binding], str | None, int, int]:
    """Bind every citation of one map and cut the ones that can go; the new text if changed."""
    text = (repo / path).read_text(encoding="utf-8")
    lines = text.split("\n")
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line) + 1)
    fenced = fence_mask(lines)
    starts = row_starts(lines, fenced)
    matches = list(GATE.CITATION.finditer(text))
    bindings: list[Binding] = []
    cuts: list[tuple[int, int]] = []
    file_texts: dict[str, str] = {}
    previous_end: dict[int, int] = {}
    for match in matches:
        line_index = bisect.bisect_right(offsets, match.start()) - 1
        end_index = bisect.bisect_right(offsets, match.end()) - 1
        row = max((start for start in starts if start <= line_index), default=line_index)
        target = bound_file(path, lines, starts, line_index, tracked)
        if target not in file_texts:
            file_texts[target] = (repo / target).read_text(encoding="utf-8", errors="replace")
        stretch_start = max(offsets[row], previous_end.get(row, 0))
        stretch = text[stretch_start : match.start()] + text[match.end() : offsets[end_index + 1]]
        previous_end[row] = match.end()
        names = names_in(stretch, target, file_texts[target])
        line_end = offsets[end_index + 1] - 1
        kept = fenced[line_index] or keeps_text(
            text, match.start(), match.end(), offsets[line_index], line_end
        )
        clause_ids = tuple(re.findall(r"C-\d{3}", match.group(2)))
        bindings.append(Binding(path, match.group(1), clause_ids, target, names, kept))
        if not kept:
            cuts.append((match.start(), match.end()))
    if not cuts:
        return bindings, None, 0, 0
    new = text
    for start, end in reversed(cuts):
        cut_start, cut_end = cut_span(new, start, end)
        new = new[:cut_start] + SENTINEL + new[cut_end:]
    new, deleted, trimmed = tidy(new)
    return bindings, new, deleted, trimmed


def source_bindings(repo: Path, path: str) -> list[Binding]:
    """Citations in a source file: each binds that file, path only, and stays."""
    try:
        text = (repo / path).read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return []
    return [
        Binding(path, m.group(1), tuple(re.findall(r"C-\d{3}", m.group(2))), path, (), True)
        for m in GATE.CITATION.finditer(text)
    ]


def parse_sidecar(text: str) -> dict[str, set[str]]:
    """Clause id -> entries of an existing sidecar."""
    entries: dict[str, set[str]] = {}
    for line in text.splitlines():
        match = GATE.BINDING_LINE.match(line)
        if match is None:
            continue
        bucket = entries.setdefault(match.group(1), set())
        bucket.update(entry.strip() for entry in match.group(2).split(",") if entry.strip())
    return entries


def normalised(entries: set[str]) -> list[str]:
    """Sorted entries; a bare path is dropped where the same path also carries a name."""
    named = {entry.split("::", 1)[0] for entry in entries if "::" in entry}
    return sorted(entry for entry in entries if "::" in entry or entry not in named)


def render_sidecar(unit: str, entries: dict[str, set[str]]) -> str:
    """The sidecar text: the header line, a blank line, one binding line per clause."""
    body = "".join(
        f"{clause_id}: {', '.join(normalised(entries[clause_id]))}\n"
        for clause_id in sorted(entries)
    )
    return HEADER.format(unit=unit) + HEADER_TAIL + "\n\n" + body


def kind_of(path: str) -> str:
    """map, test, script or other, for the report."""
    name = Path(path).name
    if name == MAP_NAME:
        return "map"
    if "/tests/" in path or name.startswith("test_") or name.endswith(("tests.rs", "_test.py")):
        return "test"
    if path.startswith("scripts/"):
        return "script"
    return "other"


def collect(
    repo: Path, tracked: list[str]
) -> tuple[list[Binding], dict[str, tuple[str, int, int]]]:
    """Every citation under CITATION_ROOTS, bound; and the new text of every map that changes."""
    tracked_set = set(tracked)
    bindings: list[Binding] = []
    rewrites: dict[str, tuple[str, int, int]] = {}
    for path in tracked:
        if not path.startswith(GATE.CITATION_ROOTS) or not (repo / path).is_file():
            continue
        if Path(path).name != MAP_NAME:
            bindings.extend(source_bindings(repo, path))
            continue
        found, new, deleted, trimmed = migrate_map(repo, path, tracked_set)
        bindings.extend(found)
        if new is not None:
            rewrites[path] = (new, deleted, trimmed)
    return bindings, rewrites


def plan_sidecars(
    repo: Path, bindings: list[Binding], homes: dict[str, str], known: set[tuple[str, str]]
) -> tuple[dict[str, str], list[str]]:
    """Sidecar path -> new text for every sidecar that gains an entry, and the unplaceable
    citations. A path the sidecar on disk already binds for a clause is left as it stands."""
    entries: dict[str, dict[str, set[str]]] = {}
    on_disk: dict[str, dict[str, set[str]]] = {}
    units: dict[str, str] = {}
    grown: set[str] = set()
    unplaceable: list[str] = []
    for binding in bindings:
        for clause_id in binding.clause_ids:
            if (binding.unit, clause_id) not in known or binding.unit not in homes:
                unplaceable.append(f"{binding.source}: pins: {binding.unit}/{clause_id}")
                continue
            sidecar = sidecar_path(homes[binding.unit])
            units[sidecar] = binding.unit
            if sidecar not in entries:
                existing = repo / sidecar
                text = existing.read_text(encoding="utf-8") if existing.is_file() else ""
                entries[sidecar] = parse_sidecar(text)
                on_disk[sidecar] = parse_sidecar(text)
            bound = {entry.split("::", 1)[0] for entry in on_disk[sidecar].get(clause_id, set())}
            if binding.path in bound:
                continue
            bucket = entries[sidecar].setdefault(clause_id, set())
            before = len(bucket)
            if binding.names:
                bucket.update(f"{binding.path}::{name}" for name in binding.names)
            else:
                bucket.add(binding.path)
            if len(bucket) != before:
                grown.add(sidecar)
    changed = {sidecar: render_sidecar(units[sidecar], entries[sidecar]) for sidecar in grown}
    return changed, unplaceable


def sidecar_totals(repo: Path, tracked: list[str], changed: dict[str, str]) -> tuple[int, ...]:
    """(sidecars, binding lines, path-only entries, named entries) over the planned tree."""
    texts = dict(changed)
    for path in tracked:
        if path.endswith(GATE.PINS_SUFFIX) and path not in texts and (repo / path).is_file():
            texts[path] = (repo / path).read_text(encoding="utf-8")
    lines = path_only = named = 0
    for text in texts.values():
        for clause_entries in parse_sidecar(text).values():
            lines += 1
            for entry in normalised(clause_entries):
                if "::" in entry:
                    named += 1
                else:
                    path_only += 1
    return len(texts), lines, path_only, named


def report(
    bindings: list[Binding],
    rewrites: dict[str, tuple[str, int, int]],
    totals: tuple[int, ...],
    written: int,
    unplaceable: list[str],
) -> None:
    """The migration report, one count per line."""
    by_kind: dict[str, list[int]] = {}
    for binding in bindings:
        counts = by_kind.setdefault(kind_of(binding.source), [0, 0])
        counts[0] += 1
        counts[1] += len(binding.clause_ids)
    for kind in sorted(by_kind):
        print(f"citations[{kind}]: {by_kind[kind][0]} ({by_kind[kind][1]} clause ids)")
    map_bindings = [b for b in bindings if kind_of(b.source) == "map"]
    print(f"map citations bound to the map itself: {sum(b.path == b.source for b in map_bindings)}")
    print(f"map citations bound with names: {sum(bool(b.names) for b in map_bindings)}")
    print(f"map citations left in the text: {sum(b.kept for b in map_bindings)}")
    print(f"maps rewritten: {len(rewrites)}")
    print(f"map lines removed: {sum(deleted for _t, deleted, _k in rewrites.values())}")
    print(f"map lines trimmed: {sum(trimmed for _t, _d, trimmed in rewrites.values())}")
    sidecars, lines, path_only, named = totals
    print(f"sidecars: {sidecars} ({written} written by this run)")
    print(f"sidecar lines: {lines}")
    print(f"path-only entries: {path_only}")
    print(f"named entries: {named}")
    print(f"unplaceable citations: {len(unplaceable)}")
    for line in unplaceable:
        print(f"  {line}", file=sys.stderr)


def run(repo: Path, check: bool) -> int:
    """Plan, report, and unless `check`, write the sidecars and the cut maps."""
    try:
        tracked = GATE.tracked_paths(repo)
    except (subprocess.CalledProcessError, OSError) as error:
        print(f"ERROR: cannot list tracked files ({error})", file=sys.stderr)
        return 2
    homes, known = ledger_homes(repo, tracked)
    if not homes:
        print("ERROR: no ledgers found under task/ledgers — refuse to pass closed", file=sys.stderr)
        return 2
    bindings, rewrites = collect(repo, tracked)
    changed, unplaceable = plan_sidecars(repo, bindings, homes, known)
    totals = sidecar_totals(repo, tracked, changed)
    if unplaceable:
        report(bindings, rewrites, totals, 0, unplaceable)
        return 1
    if not check:
        for sidecar, text in changed.items():
            (repo / sidecar).write_text(text, encoding="utf-8")
        for path, (text, _deleted, _trimmed) in rewrites.items():
            (repo / path).write_text(text, encoding="utf-8")
    report(bindings, rewrites, totals, 0 if check else len(changed), unplaceable)
    return 0


def main() -> int:
    """CLI: `--check` (write nothing) and the hidden `--repo` for the tests."""
    parser = argparse.ArgumentParser(
        prog="migrate_pins_to_sidecars.py", description=__doc__.split("\n\n")[0]
    )
    parser.add_argument("--check", action="store_true", help="report only; write nothing")
    parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parent.parent, help=argparse.SUPPRESS
    )
    arguments = parser.parse_args()
    return run(arguments.repo.resolve(), arguments.check)


if __name__ == "__main__":
    sys.exit(main())
