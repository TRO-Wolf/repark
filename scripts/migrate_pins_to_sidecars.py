#!/usr/bin/env python3
"""Move `pins: <unit>/C-NNN` citations out of directory maps into per-ledger sidecars.

MAP-GOVERNANCE-1. Every citation the grammar gate's CITATION regex finds in a tracked file under
its CITATION_ROOTS becomes a line of `task/ledgers/<bin>/<unit>-pins.md` beside its ledger (the
archive sidecar takes its ledger's `yyyy-mm-dd-` prefix), in the format rule B of
`scripts/check_ledger_grammar.py` verifies: `C-NNN: <path>[::<name>], ...` or
`C-NNN: none (records)`.

A citation in a source file binds that file when it is a test file or a script, and stays where
it is. A citation in a `map.md` binds the file its own entry names, and never looks past that
entry: the entry is the citation's list row, table row, paragraph or column-0 dated entry, and
its file is its first link under the map's directory, else its first code span. An indented
dated paragraph inside a list item, and a nested sub-bullet whose own entry names no file, stay
with the enclosing item's file, and bind it for a clause only when a backticked name of the
citation's own paragraph, or of that clause's ledger row, is defined there. Every PROVEN clause
also binds each test its own row backticks that exactly one bindable file defines, overriding a
records line; a name two files define is listed as a hand candidate. A bare file name that does
not resolve in the map's directory resolves to the one tracked file of that name under the map's
directory tree; a name found only elsewhere in the tree is a reference, not the entry's file; a
name found nowhere, or more than once here, makes the citation unplaceable, and the run stops
and lists it unless the sidecar on disk already binds that clause by hand. A clause's binding is
the union of its citing entries' files and every bindable `.rs`, `.py` or `.sh` file its own
ledger row names (an explicit path, else by its basename). The names are the backticked
identifiers, of the citation's stretch of its entry and of the clause's own ledger row, that the
bound file defines. Only a `.rs`, `.py` or `.sh` test file or script is bound: an entry whose
file is a fixture or a test-placed helper with no test binds the test its entry names that reads
it, if any; a disclaiming clause ("so the grammar gate counts it") unbinds only the clause ids
it names explicitly; one that names none, and a "proves ... only" sentence, unbind nothing and
are listed by `--report hand-candidates` for a hand records line; and a clause left with no
bindable entry and no citation in a test or script is written as `none (records)`, which a rerun
never replaces except by the row-named pass. The citation is then cut from the map: a line left
with nothing but punctuation is deleted, a line that also carries prose keeps the prose and its
indentation.

A citation is bound but left in the map's text when cutting it would break the sentence around
it: inside a fenced block, inside a code span the line continues past, or followed by an id the
regex does not read (a range, a second unit, a non-clause id).

Idempotent: an existing sidecar's lines are kept and the new entries merged in. `--check`
writes nothing, prints the same report and exits 1 when a citation is unplaceable or a rewrite
is pending. Exit 0 clean, 1 unplaceable or pending, 2 usage or environment error.
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
HEADER = "# {unit} pin binding: one line per clause, read and verified by "
HEADER_TAIL = "scripts/check_ledger_grammar.py rule B."
LINK = re.compile(r"\[[^\]]*\]\(\s*<?([^()<>\s]+)>?(?:\s+\"[^\"]*\")?\s*\)")
FILE_LIKE = re.compile(
    r"^[\w./-]*[\w-]\.(?:rs|py|json|jsonl|toml|sh|md|sql|yaml|yml|txt|csv|parquet|avro|orc|"
    r"ipynb|j2|cfg|ini)$",
    re.ASCII,
)
STOP_NAMES: frozenset[str] = frozenset(
    {"true", "false", "None", "True", "False", "null", "self", "Self", "NULL", "pins"}
)
CODE_SUFFIXES = (".rs", ".py", ".sh")


class Binding(NamedTuple):
    """One citation, resolved: where it was, the bindable file it names, and its fate."""

    source: str
    line: int
    unit: str
    clause_ids: tuple[str, ...]
    path: str | None
    names: tuple[str, ...]
    kept: bool
    unresolved: str | None
    named: str | None
    inherited: bool
    disclaimed: bool


class Target(NamedTuple):
    """What a map entry names: a tracked file, or an unresolvable file name, or nothing."""

    path: str | None
    unresolved: str | None
    by_basename: bool


def load_gate() -> ModuleType:
    """The grammar gate, so the citation regex, the bins and the binding rules stay single-homed."""
    location = Path(__file__).resolve().parent / "check_ledger_grammar.py"
    spec = importlib.util.spec_from_file_location("check_ledger_grammar", location)
    if spec is None or spec.loader is None:
        message = f"cannot load {location}"
        raise RuntimeError(message)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GATE = load_gate()


def load_text() -> ModuleType:
    """The map-text helpers beside this script."""
    location = Path(__file__).resolve().parent / "migrate_pins_text.py"
    spec = importlib.util.spec_from_file_location("migrate_pins_text", location)
    if spec is None or spec.loader is None:
        message = f"cannot load {location}"
        raise RuntimeError(message)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


TEXT = load_text()
SENTINEL = TEXT.SENTINEL
FENCE = TEXT.FENCE
LIST_ITEM = TEXT.LIST_ITEM
TABLE_ROW = TEXT.TABLE_ROW
HEADING = TEXT.HEADING
DATED = TEXT.DATED
DATED_TOP = TEXT.DATED_TOP
CODE_SPAN = TEXT.CODE_SPAN
TAIL = TEXT.TAIL
EMPTY_REST = TEXT.EMPTY_REST
LEADING_STRAY = TEXT.LEADING_STRAY
SENTENCE_END = TEXT.SENTENCE_END
PROVES_ONLY = TEXT.PROVES_ONLY
DISCLAIMER = TEXT.DISCLAIMER
fence_mask = TEXT.fence_mask
entry_starts = TEXT.entry_starts
cut_span = TEXT.cut_span
keeps_text = TEXT.keeps_text
tidy = TEXT.tidy
parent_item = TEXT.parent_item
clauses_of = TEXT.clauses_of
disclaimers = TEXT.disclaimers


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


def ledger_rows(repo: Path, homes: dict[str, str]) -> dict[tuple[str, str], str]:
    """(unit, clause id) -> the clause row's text in the unit's home ledger."""
    rows: dict[tuple[str, str], str] = {}
    for unit, ledger in homes.items():
        text = (repo / ledger).read_text(encoding="utf-8")
        for _number, clause_id, cells in GATE.clause_rows(text):
            rows.setdefault((unit, clause_id), " | ".join(cells))
    return rows


def proven_clauses(repo: Path, homes: dict[str, str]) -> set[tuple[str, str]]:
    """Every (unit, clause id) whose row in the unit's home ledger carries a PROVEN verdict."""
    proven: set[tuple[str, str]] = set()
    for unit, ledger in homes.items():
        text = (repo / ledger).read_text(encoding="utf-8")
        for _number, clause_id, cells in GATE.clause_rows(text):
            verdicts = [GATE.VERDICT_CELL.match(cell) for cell in cells]
            if any(match is not None and match.group(1) == "PROVEN" for match in verdicts):
                proven.add((unit, clause_id))
    return proven


def row_code_files(row: str, tracked: set[str], basenames: dict[str, list[str]]) -> list[str]:
    """The `.rs`, `.py` and `.sh` files a ledger row names, in order: explicit paths first (whole
    or by a unique path suffix), then bare names, which take an explicit path of the same name
    from the row, else the one tracked file of that name."""
    tokens: list[str] = []
    for span in [*CODE_SPAN.findall(row), *LINK.findall(row)]:
        for word in span.split():
            token = word.split("::", 1)[0].split("#", 1)[0].strip(",;()")
            while token.startswith("../"):
                token = token[3:]
            if token.endswith(CODE_SUFFIXES) and FILE_LIKE.match(token):
                tokens.append(token)
    found: list[str] = []
    for token in (token for token in tokens if "/" in token):
        hits = (
            [token]
            if token in tracked
            else [
                path for path in basenames.get(Path(token).name, []) if path.endswith(f"/{token}")
            ]
        )
        if not hits:
            hits = basenames.get(Path(token).name, [])
        if len(hits) == 1 and hits[0] not in found:
            found.append(hits[0])
    for token in (token for token in tokens if "/" not in token):
        same = [path for path in found if Path(path).name == token]
        hits = same or basenames.get(token, [])
        if len(hits) == 1 and hits[0] not in found:
            found.append(hits[0])
    return found


def sidecar_path(ledger: str) -> str:
    """The sidecar beside a ledger, archive date prefix included."""
    return ledger[: -len(GATE.LEDGER_SUFFIX)] + GATE.PINS_SUFFIX


def normalise(joined: str) -> str | None:
    """A posix path with `.` and `..` folded, or None when it climbs above the root."""
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
    return "/".join(parts)


def under(map_dir: str, path: str) -> bool:
    """Whether `path` sits in the map's directory tree."""
    return not map_dir or path.startswith(f"{map_dir}/")


def resolve_link(map_dir: str, target: str, tracked: set[str]) -> str | None:
    """The tracked file under the map's directory a link target names, if any."""
    cleaned = target.strip().split("#", 1)[0].rstrip("/")
    if not cleaned or cleaned.startswith(("http:", "https:", "mailto:", "/")):
        return None
    resolved = normalise(f"{map_dir}/{cleaned}" if map_dir else cleaned)
    if resolved is not None and resolved in tracked and under(map_dir, resolved):
        return resolved
    return None


def resolve_span(
    map_dir: str, span: str, tracked: set[str], basenames: dict[str, list[str]]
) -> Target:
    """What a code span names: a tracked file under the map's directory, found by its relative
    path or by its unique basename; nothing, when the name is a file elsewhere in the tree; or an
    unresolvable name, when the file exists nowhere or its basename is ambiguous here."""
    candidate = span.strip().split("::", 1)[0]
    if not FILE_LIKE.match(candidate):
        return Target(None, None, by_basename=False)
    relative = normalise(f"{map_dir}/{candidate}" if map_dir else candidate)
    if relative is not None and relative in tracked and under(map_dir, relative):
        return Target(relative, None, by_basename=False)
    if candidate in tracked and under(map_dir, candidate):
        return Target(candidate, None, by_basename=False)
    name = candidate.rsplit("/", 1)[-1]
    elsewhere = [
        path
        for path in basenames.get(name, [])
        if path == candidate or path.endswith(f"/{candidate}")
    ]
    if "/" in candidate:
        if elsewhere or (relative is not None and relative in tracked):
            return Target(None, None, by_basename=False)
        return Target(None, f"{candidate} (no such file)", by_basename=False)
    matches = [path for path in elsewhere if under(map_dir, path)]
    if len(matches) == 1:
        return Target(matches[0], None, by_basename=True)
    if not matches and elsewhere:
        return Target(None, None, by_basename=False)
    return Target(None, f"{candidate} ({len(matches)} matches here)", by_basename=False)


def entry_target(
    map_dir: str, entry_text: str, tracked: set[str], basenames: dict[str, list[str]]
) -> Target:
    """The file an entry describes: its first link's target, else its first code span."""
    link = LINK.search(CODE_SPAN.sub("", entry_text))
    if link is not None:
        named = resolve_link(map_dir, link.group(1), tracked)
        if named is not None:
            return Target(named, None, by_basename=False)
    span = CODE_SPAN.search(entry_text)
    if span is None:
        return Target(None, None, by_basename=False)
    return resolve_span(map_dir, span.group(1), tracked, basenames)


def names_in(stretch: str, path: str, file_text: str) -> tuple[str, ...]:
    """Backticked identifiers of `stretch` that the bound file defines and carries as tokens."""
    found: set[str] = set()
    for span in TEXT.code_spans(stretch):
        name = TEXT.span_name(span)
        if name is None:
            continue
        if len(name) < 3 or name in STOP_NAMES:
            continue
        if not any(char.islower() or char == "_" for char in name):
            continue
        if is_token(file_text, name) and GATE.defines(path, file_text, name):
            found.add(name)
    return tuple(sorted(found))


def file_text(repo: Path, path: str, cache: dict[str, str]) -> str:
    """A tracked file's text, cached; undecodable bytes replaced."""
    if path not in cache:
        cache[path] = (repo / path).read_text(encoding="utf-8", errors="replace")
    return cache[path]


def fixture_reader(
    repo: Path,
    map_dir: str,
    entry_text: str,
    fixture: str,
    tracked: set[str],
    basenames: dict[str, list[str]],
    cache: dict[str, str],
) -> str | None:
    """A test file the entry names that reads `fixture`, when the entry's own file is a fixture."""
    candidates = [resolve_link(map_dir, link, tracked) for link in LINK.findall(entry_text)]
    candidates.extend(
        resolve_span(map_dir, span, tracked, basenames).path
        for span in CODE_SPAN.findall(entry_text)
    )
    name = Path(fixture).name
    module = Path(fixture).stem if fixture.endswith(".py") else None
    for candidate in candidates:
        if not candidate or candidate == fixture:
            continue
        body = file_text(repo, candidate, cache)
        if not GATE.bindable(candidate, body):
            continue
        if name in body or (module is not None and is_token(body, module)):
            return candidate
    return None


def migrate_map(
    repo: Path,
    path: str,
    tracked: set[str],
    basenames: dict[str, list[str]],
    candidates: list[tuple[str, str, str, str, str]],
) -> tuple[list[Binding], str | None, int, int]:
    """Bind every citation of one map and cut the ones that can go; the new text if changed.
    Hand candidates (unit, clause, bound file, where, sentence) are appended to `candidates`."""
    text = (repo / path).read_text(encoding="utf-8")
    lines = text.split("\n")
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line) + 1)
    fenced = fence_mask(lines)
    starts = entry_starts(lines, fenced, DATED_TOP)
    own_starts = entry_starts(lines, fenced, DATED)
    ends = dict(zip(starts, [*starts[1:], len(lines)], strict=True))
    map_dir = str(Path(path).parent)
    map_dir = "" if map_dir == "." else map_dir
    bindings: list[Binding] = []
    cuts: list[tuple[int, int]] = []
    cache: dict[str, str] = {}
    previous_end: dict[int, int] = {}
    disclaimed_by_row: dict[int, tuple[set[tuple[int, str]], list[tuple[int, str, str]]]] = {}
    reported: set[tuple[int, str]] = set()
    for match in GATE.CITATION.finditer(text):
        line_index = bisect.bisect_right(offsets, match.start()) - 1
        end_index = bisect.bisect_right(offsets, match.end()) - 1
        row = starts[bisect.bisect_right(starts, line_index) - 1] if starts else line_index
        row_end = ends.get(row, len(lines))
        target = entry_target(map_dir, "\n".join(lines[row:row_end]), tracked, basenames)
        entry_text = "\n".join(lines[row:row_end])
        bound = target.path
        if bound is not None and not GATE.bindable(bound, file_text(repo, bound, cache)):
            bound = None
        if target.path is not None and bound is None:
            bound = fixture_reader(
                repo, map_dir, entry_text, target.path, tracked, basenames, cache
            )
        own = own_starts[bisect.bisect_right(own_starts, line_index) - 1] if own_starts else row
        inherited = own != row
        if bound is None and target.path is None and target.unresolved is None:
            parent = parent_item(lines, fenced, row)
            if parent is not None:
                above = entry_target(map_dir, "\n".join(lines[parent:row]), tracked, basenames).path
                if above is not None and GATE.bindable(above, file_text(repo, above, cache)):
                    bound, inherited = above, True
        stretch_start = max(offsets[own], previous_end.get(row, 0))
        stretch = text[stretch_start : match.start()] + text[match.end() : offsets[end_index + 1]]
        previous_end[row] = match.end()
        names = names_in(stretch, bound, file_text(repo, bound, cache)) if bound else ()
        if row not in disclaimed_by_row:
            disclaimed_by_row[row] = disclaimers(text, lines, offsets, row, row_end)
        disclaimed_ids, row_candidates = disclaimed_by_row[row]
        unresolved = None if inherited else target.unresolved
        line_end = offsets[end_index + 1] - 1
        kept = fenced[line_index] or keeps_text(
            text, match.start(), match.end(), offsets[line_index], line_end
        )
        clause_ids = tuple(re.findall(r"C-\d{3}", match.group(2)))
        named = target.path if target.by_basename else None
        refused = tuple(i for i in clause_ids if (match.start(), i) in disclaimed_ids)
        for at, clause_id, sentence in row_candidates:
            fresh = (at, clause_id) not in reported and clause_id not in refused
            if at == match.start() and clause_id in clause_ids and fresh:
                reported.add((at, clause_id))
                where = f"{path}:{line_index + 1}"
                candidates.append((match.group(1), clause_id, bound or "-", where, sentence))
        accepted = tuple(i for i in clause_ids if i not in refused)
        for ids, path_bound, path_names, disclaimed in (
            (accepted, bound, names, False),
            (refused, None, (), True),
        ):
            if not ids:
                continue
            bindings.append(
                Binding(
                    path,
                    line_index + 1,
                    match.group(1),
                    ids,
                    path_bound,
                    path_names,
                    kept,
                    unresolved,
                    named,
                    inherited,
                    disclaimed,
                )
            )
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
    """Citations in a source file: each binds that file when it is bindable, and stays."""
    try:
        text = (repo / path).read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return []
    bound = path if GATE.bindable(path, text) else None
    return [
        Binding(
            path,
            text.count("\n", 0, m.start()) + 1,
            m.group(1),
            tuple(re.findall(r"C-\d{3}", m.group(2))),
            bound,
            (),
            True,
            None,
            None,
            False,
            False,
        )
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
        bucket.update(entry.strip() for entry in match.group(2).split(", ") if entry.strip())
    return entries


def normalised(entries: set[str]) -> list[str]:
    """Sorted entries: a bare path is dropped where the same path also carries a name, and
    `none (records)` where any real entry exists."""
    real = {entry for entry in entries if entry != GATE.RECORDS}
    if not real:
        return [GATE.RECORDS] if entries else []
    named = {entry.split("::", 1)[0] for entry in real if "::" in entry}
    return sorted(entry for entry in real if "::" in entry or entry not in named)


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
    repo: Path,
    tracked: list[str],
    candidates: list[tuple[str, str, str, str, str]] | None = None,
) -> tuple[list[Binding], dict[str, tuple[str, int, int]]]:
    """Every citation under CITATION_ROOTS, bound; and the new text of every map that changes.
    Hand candidates go to `candidates` when it is given."""
    sink: list[tuple[str, str, str, str, str]] = [] if candidates is None else candidates
    tracked_set = set(tracked)
    basenames: dict[str, list[str]] = {}
    for path in tracked:
        basenames.setdefault(Path(path).name, []).append(path)
    bindings: list[Binding] = []
    rewrites: dict[str, tuple[str, int, int]] = {}
    for path in tracked:
        if not path.startswith(GATE.CITATION_ROOTS) or not (repo / path).is_file():
            continue
        if Path(path).name != MAP_NAME:
            bindings.extend(source_bindings(repo, path))
            continue
        found, new, deleted, trimmed = migrate_map(repo, path, tracked_set, basenames, sink)
        bindings.extend(found)
        if new is not None:
            rewrites[path] = (new, deleted, trimmed)
    return bindings, rewrites


class Plan(NamedTuple):
    """The sidecar side of a run."""

    changed: dict[str, str]
    unplaceable: list[str]
    hand_bound: list[str]
    records: list[str]
    row_added: dict[tuple[str, str], list[str]]
    by_intent: set[tuple[str, str]]
    ambiguous: list[tuple[str, str, str, list[str]]]


def add_entry(
    repo: Path,
    entries: dict[str, set[str]],
    on_disk: dict[str, set[str]],
    clause_id: str,
    target: str,
    row: str,
    own_names: tuple[str, ...],
) -> bool:
    """Add `target` to a clause's bucket with its own names and the row's names the file
    defines, unless the sidecar on disk already binds that file; whether the bucket grew."""
    if target in {entry.split("::", 1)[0] for entry in on_disk.get(clause_id, set())}:
        return False
    bucket = entries.setdefault(clause_id, set())
    before = set(bucket)
    names = sorted(set(own_names) | set(names_in(row, target, GATE.read_tracked(repo, target))))
    if names:
        bucket.update(f"{target}::{name}" for name in names)
    else:
        bucket.add(target)
    return bucket != before


def plan_sidecars(
    repo: Path,
    bindings: list[Binding],
    homes: dict[str, str],
    known: set[tuple[str, str]],
    rows: dict[tuple[str, str], str],
    tracked: set[str],
    proven: set[tuple[str, str]],
) -> Plan:
    """Every sidecar that gains a line, the unplaceable and hand-bound citations, and the
    clauses written as `none (records)`. The row-named pass runs first, so the citation and
    row-file passes see its result and a rerun adds nothing; a records line blocks those two
    passes only while it is still records. A path the sidecar on disk already binds for a
    clause is left as it stands, and a clause the sidecar on disk already carries gets no
    records line."""
    entries: dict[str, dict[str, set[str]]] = {}
    on_disk: dict[str, dict[str, set[str]]] = {}
    units: dict[str, str] = {}
    grown: set[str] = set()
    unplaceable: list[str] = []
    hand_bound: list[str] = []
    row_added: dict[tuple[str, str], list[str]] = {}
    basenames: dict[str, list[str]] = {}
    for path in sorted(tracked):
        basenames.setdefault(Path(path).name, []).append(path)
    by_intent: set[tuple[str, str]] = set()
    ambiguous: list[tuple[str, str, str, list[str]]] = []
    index = GATE.test_index(repo, sorted(tracked))
    for unit, clause_id in sorted(proven):
        sidecar = sidecar_path(homes[unit])
        units[sidecar] = unit
        if sidecar not in entries:
            existing = repo / sidecar
            text = existing.read_text(encoding="utf-8") if existing.is_file() else ""
            entries[sidecar] = parse_sidecar(text)
            on_disk[sidecar] = parse_sidecar(text)
        for name in GATE.backticked_names(rows.get((unit, clause_id), "")):
            files = index.get(name, [])
            if len(files) > 1:
                ambiguous.append((unit, clause_id, name, sorted(files)))
            if len(files) != 1:
                continue
            bucket = entries[sidecar].setdefault(clause_id, set())
            entry = f"{files[0]}::{name}"
            if entry not in bucket:
                bucket.discard(GATE.RECORDS)
                bucket.add(entry)
                grown.add(sidecar)
                by_intent.add((unit, clause_id))
    wanted: dict[tuple[str, str], str] = {}
    source_cited: set[tuple[str, str]] = set()
    for binding in bindings:
        for clause_id in binding.clause_ids:
            key = (binding.unit, clause_id)
            where = f"{binding.source}:{binding.line}: pins: {binding.unit}/{clause_id}"
            if key not in known or binding.unit not in homes:
                unplaceable.append(f"{where} names no clause in any ledger")
                continue
            sidecar = sidecar_path(homes[binding.unit])
            units[sidecar] = binding.unit
            if sidecar not in entries:
                existing = repo / sidecar
                text = existing.read_text(encoding="utf-8") if existing.is_file() else ""
                entries[sidecar] = parse_sidecar(text)
                on_disk[sidecar] = parse_sidecar(text)
            if kind_of(binding.source) != "map" and binding.path is not None:
                source_cited.add(key)
            if binding.unresolved is not None:
                if clause_id in on_disk[sidecar]:
                    hand_bound.append(f"{where} ({binding.unresolved})")
                else:
                    unplaceable.append(f"{where} names {binding.unresolved}")
                continue
            wanted.setdefault(key, sidecar)
            if binding.path is None or entries[sidecar].get(clause_id) == {GATE.RECORDS}:
                continue
            row = rows.get(key, "")
            path_text = GATE.read_tracked(repo, binding.path)
            if (
                binding.inherited
                and not binding.names
                and not names_in(row, binding.path, path_text)
            ):
                continue
            if add_entry(
                repo,
                entries[sidecar],
                on_disk[sidecar],
                clause_id,
                binding.path,
                row,
                binding.names,
            ):
                grown.add(sidecar)
    for key, sidecar in sorted(wanted.items()):
        clause_id = key[1]
        if entries[sidecar].get(clause_id) == {GATE.RECORDS}:
            continue
        for target in row_code_files(rows.get(key, ""), tracked, basenames):
            if not GATE.bindable(target, GATE.read_tracked(repo, target)):
                continue
            already = {entry.split("::", 1)[0] for entry in entries[sidecar].get(clause_id, set())}
            if add_entry(
                repo, entries[sidecar], on_disk[sidecar], clause_id, target, rows.get(key, ""), ()
            ):
                grown.add(sidecar)
                if target not in already:
                    row_added.setdefault(key, []).append(target)
    records: list[str] = []
    for (unit, clause_id), sidecar in sorted(wanted.items()):
        if clause_id in on_disk[sidecar] or (unit, clause_id) in source_cited:
            continue
        bucket = entries[sidecar].setdefault(clause_id, set())
        if bucket - {GATE.RECORDS}:
            continue
        if not bucket:
            bucket.add(GATE.RECORDS)
            grown.add(sidecar)
        records.append(f"{unit}/{clause_id}")
    changed = {sidecar: render_sidecar(units[sidecar], entries[sidecar]) for sidecar in grown}
    return Plan(changed, unplaceable, hand_bound, records, row_added, by_intent, ambiguous)


def sidecar_totals(repo: Path, tracked: list[str], changed: dict[str, str]) -> tuple[int, ...]:
    """(sidecars, binding lines, path-only entries, named entries, records lines, bytes) over
    the planned tree."""
    texts = dict(changed)
    for path in tracked:
        if path.endswith(GATE.PINS_SUFFIX) and path not in texts and (repo / path).is_file():
            texts[path] = (repo / path).read_text(encoding="utf-8")
    lines = path_only = named = records = 0
    for text in texts.values():
        for clause_entries in parse_sidecar(text).values():
            lines += 1
            for entry in normalised(clause_entries):
                if entry == GATE.RECORDS:
                    records += 1
                elif "::" in entry:
                    named += 1
                else:
                    path_only += 1
    size = sum(len(text.encode("utf-8")) for text in texts.values())
    return len(texts), lines, path_only, named, records, size


def report(
    bindings: list[Binding],
    rewrites: dict[str, tuple[str, int, int]],
    totals: tuple[int, ...],
    written: int,
    plan: Plan,
) -> None:
    """The migration report, one count per line."""
    by_kind: dict[str, list[int]] = {}
    for binding in bindings:
        counts = by_kind.setdefault(kind_of(binding.source), [0, 0])
        counts[0] += 1
        counts[1] += len(binding.clause_ids)
    for kind in sorted(by_kind):
        print(f"citations[{kind}]: {by_kind[kind][0]} ({by_kind[kind][1]} clause ids)")
    on_maps = [b for b in bindings if kind_of(b.source) == "map"]
    print(f"map citations bound to a test file or script: {sum(bool(b.path) for b in on_maps)}")
    print(f"map citations bound with names: {sum(bool(b.names) for b in on_maps)}")
    print(f"map citations resolved by basename: {sum(bool(b.named) for b in on_maps)}")
    inherited = [b for b in on_maps if b.inherited]
    print(f"map citations whose own text disclaims a pin: {sum(b.disclaimed for b in on_maps)}")
    print(f"map citations in an indented dated paragraph: {len(inherited)}")
    named_inherited = sum(bool(b.path and b.names) for b in inherited)
    print(f"  bound to their item's file by a defined name: {named_inherited}")
    print(f"map citations with no bindable file: {sum(b.path is None for b in on_maps)}")
    print(f"map citations left in the text: {sum(b.kept for b in on_maps)}")
    print(f"maps rewritten: {len(rewrites)}")
    print(f"map lines removed: {sum(deleted for _t, deleted, _k in rewrites.values())}")
    print(f"map lines trimmed: {sum(trimmed for _t, _d, trimmed in rewrites.values())}")
    sidecars, lines, path_only, named, records, size = totals
    print(f"sidecars: {sidecars} ({written} written by this run)")
    print(f"sidecar lines: {lines}")
    print(f"path-only entries: {path_only}")
    print(f"named entries: {named}")
    print(f"none (records) lines: {records}")
    print(f"sidecar bytes: {size}")
    print(f"clauses that gained a file their own ledger row names: {len(plan.row_added)}")
    print(f"PROVEN clauses bound by a test their row names: {len(plan.by_intent)}")
    print(f"ambiguous row-named tests: {len(plan.ambiguous)}")
    print(f"hand-bound citations: {len(plan.hand_bound)}")
    print(f"unplaceable citations: {len(plan.unplaceable)}")
    for line in plan.unplaceable:
        print(f"  {line}", file=sys.stderr)


def open_candidates(
    repo: Path,
    candidates: list[tuple[str, str, str, str, str]],
    homes: dict[str, str],
    changed: dict[str, str],
) -> list[tuple[str, str, str, str, str]]:
    """The candidates whose clause the planned sidecar does not already set to records."""
    kept: list[tuple[str, str, str, str, str]] = []
    parsed: dict[str, dict[str, set[str]]] = {}
    for candidate in candidates:
        home = homes.get(candidate[0])
        if home is None:
            continue
        sidecar = sidecar_path(home)
        if sidecar not in parsed:
            existing = repo / sidecar
            text = changed.get(sidecar)
            if text is None:
                text = existing.read_text(encoding="utf-8") if existing.is_file() else ""
            parsed[sidecar] = parse_sidecar(text)
        if parsed[sidecar].get(candidate[1]) != {GATE.RECORDS}:
            kept.append(candidate)
    return kept


def print_candidates(candidates: list[tuple[str, str, str, str, str]]) -> None:
    """The hand-candidate list: one tab-separated line per clause a disclaimer with no clause id
    leaves bound, for a hand `none (records)` line."""
    print(f"hand candidates: {len(candidates)}")
    for unit, clause_id, path, where, sentence in sorted(candidates):
        print(f"  {unit}/{clause_id}\t{path}\t{where}\t{sentence[:240]}")


def run(repo: Path, check: bool, report_candidates: bool = False) -> int:
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
    candidates: list[tuple[str, str, str, str, str]] = []
    bindings, rewrites = collect(repo, tracked, candidates)
    rows = ledger_rows(repo, homes)
    proven = proven_clauses(repo, homes)
    plan = plan_sidecars(repo, bindings, homes, known, rows, set(tracked), proven)
    totals = sidecar_totals(repo, tracked, plan.changed)
    if plan.unplaceable:
        report(bindings, rewrites, totals, 0, plan)
        return 1
    if report_candidates:
        print_candidates(open_candidates(repo, candidates, homes, plan.changed))
        for unit, clause_id, name, files in plan.ambiguous:
            print(f"  {unit}/{clause_id}\t{name}\t{', '.join(files)}\tambiguous row-named test")
    if check:
        report(bindings, rewrites, totals, 0, plan)
        pending = len(rewrites) + len(plan.changed)
        if pending:
            print(
                f"pending: {len(rewrites)} map(s) and {len(plan.changed)} sidecar(s) would change",
                file=sys.stderr,
            )
            return 1
        return 0
    for sidecar, text in plan.changed.items():
        (repo / sidecar).write_text(text, encoding="utf-8")
    for path, (text, _deleted, _trimmed) in rewrites.items():
        (repo / path).write_text(text, encoding="utf-8")
    report(bindings, rewrites, totals, len(plan.changed), plan)
    return 0


def main() -> int:
    """CLI: `--check` (write nothing), `--report hand-candidates`, and the hidden `--repo`."""
    parser = argparse.ArgumentParser(
        prog="migrate_pins_to_sidecars.py", description=__doc__.split("\n\n")[0]
    )
    parser.add_argument("--check", action="store_true", help="report only; write nothing")
    parser.add_argument(
        "--report",
        choices=["hand-candidates"],
        help="also list the clauses an id-less disclaimer leaves bound, for hand records lines",
    )
    parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parent.parent, help=argparse.SUPPRESS
    )
    arguments = parser.parse_args()
    return run(arguments.repo.resolve(), arguments.check, arguments.report == "hand-candidates")


if __name__ == "__main__":
    sys.exit(main())
