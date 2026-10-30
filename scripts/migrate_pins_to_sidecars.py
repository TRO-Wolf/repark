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
dated paragraph inside a list item stays with that item's file, and binds it only when a
backticked name of the citation's own paragraph is defined there; otherwise it binds nothing. A
bare file name that does not resolve in the map's directory resolves to the one tracked file of
that name under the map's directory tree; a name found only elsewhere in the tree is a
reference, not the entry's file; a name found nowhere, or more than once here, makes the
citation unplaceable, and the run stops and lists it unless the sidecar on disk already binds
that clause by hand. A clause of a citation shared with other ids, whose own ledger row names
`.rs`, `.py` or `.sh` files but not the citation's file, binds the bindable ones of those
instead, or nothing. The names are the backticked identifiers, of the citation's stretch of its
entry and of the clause's own ledger row, that the bound file defines. Only a `.rs`, `.py` or
`.sh` test file or script is bound: an entry whose file is a fixture or a test-placed helper
with no test binds the test its entry names that reads it, if any; a disclaiming sentence ("so
the grammar gate counts it", "this file pins none") unbinds the clause ids it names, or, naming
none, the citation on its own line; and a clause left with no bindable entry and no citation in
a test or script is written as `none (records)`, which a rerun never replaces. The citation is
then cut from the map: a line left with nothing but punctuation is deleted, a line that also
carries prose keeps the prose and its indentation.

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
import itertools
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
DATED = re.compile(r"^\s*(?:\*\*)?[A-Z][^()\n`]{0,80}?\(\d{4}-\d{2}-\d{2}[^)\n]*\)")
DATED_TOP = re.compile(r"^(?:\*\*)?[A-Z][^()\n`]{0,80}?\(\d{4}-\d{2}-\d{2}[^)\n]*\)")
LINK = re.compile(r"\[[^\]]*\]\(\s*<?([^()<>\s]+)>?(?:\s+\"[^\"]*\")?\s*\)")
CODE_SPAN = re.compile(r"`([^`]+)`")
FILE_LIKE = re.compile(
    r"^[\w./-]*[\w-]\.(?:rs|py|json|jsonl|toml|sh|md|sql|yaml|yml|txt|csv|parquet|avro|orc|"
    r"ipynb|j2|cfg|ini)$",
    re.ASCII,
)
QUALIFIED = re.compile(
    r"^(?:[\w./-]+::)?(?:[A-Za-z_]\w*(?:::|\.))*([A-Za-z_]\w*)(?:\(\))?$", re.ASCII
)
STOP_NAMES: frozenset[str] = frozenset(
    {"true", "false", "None", "True", "False", "null", "self", "Self", "NULL", "pins"}
)
TAIL = re.compile(
    r"^(?:\s*[,;]?\s*[a-z0-9][a-z0-9.-]*/[A-Za-z]"
    r"|\s*,\s*[A-Z][A-Za-z0-9]*-[A-Za-z0-9-]*\d"
    r"|\s*(?:\.\.|…))"
)
EMPTY_REST = re.compile(r"^[\s\-*+.,;:()·]*$")
LEADING_STRAY = ".;)"
CODE_SUFFIXES = (".rs", ".py", ".sh")
SENTENCE_END = re.compile(r"\.(?=\s)")
DISCLAIMER = re.compile(
    r"grammar gate counts|pins? none|pins nothing|not a test pin", re.IGNORECASE
)


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


def entry_starts(lines: list[str], fenced: list[bool], dated: re.Pattern[str]) -> list[int]:
    """Line indexes that open an entry: a list item, a table row, a heading, a dated entry (as
    `dated` reads one) or a paragraph."""
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
            or dated.match(line)
            or previous_blank
            or (index > 0 and HEADING.match(lines[index - 1]))
        ):
            starts.append(index)
        previous_blank = False
    return starts


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
    for span in CODE_SPAN.findall(stretch):
        match = QUALIFIED.match(span.strip())
        if match is None:
            continue
        name = match.group(1)
        if len(name) < 3 or name in STOP_NAMES:
            continue
        if not any(char.islower() or char == "_" for char in name):
            continue
        if is_token(file_text, name) and GATE.defines(path, file_text, name):
            found.add(name)
    return tuple(sorted(found))


def _open_paren(text: str, start: int) -> bool:
    """Whether the paragraph before `start` leaves a parenthesis open, code spans aside."""
    paragraph = text.rfind("\n\n", 0, start)
    before = CODE_SPAN.sub("", text[paragraph + 1 if paragraph != -1 else 0 : start])
    return before.count("(") > before.count(")")


def cut_span(text: str, start: int, end: int) -> tuple[int, int]:
    """Widen a citation to the separators and brackets that go with it; a citation that opens
    its line keeps the indentation and takes the stray punctuation after it, except a `)` that
    closes a parenthesis opened earlier in the paragraph, which joins the line above."""
    line_start = text.rfind("\n", 0, start) + 1
    if start > 0 and end < len(text) and text[start - 1] == "`" and text[end] == "`":
        start, end = start - 1, end + 1
    before = start
    while before > line_start and text[before - 1] in " \t":
        before -= 1
    wrapped = (
        before > line_start and text[before - 1] == "(" and end < len(text) and text[end] == ")"
    )
    if wrapped:
        start, end = before - 1, end + 1
    if not text[line_start:start].strip():
        if line_start > 0 and end < len(text) and text[end] == ")" and _open_paren(text, start):
            return line_start - 1, end
        while end < len(text) and text[end] in LEADING_STRAY:
            end += 1
        while end < len(text) and text[end] in " \t":
            end += 1
        return start, end
    if wrapped:
        if text[start - 1] in " \t":
            start -= 1
        return start, end
    start = before
    if start > line_start and text[start - 1] in ",;·":
        start -= 1
        while start > line_start and text[start - 1] in " \t":
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


def disclaimers(
    text: str, lines: list[str], offsets: list[int], row: int, row_end: int
) -> tuple[set[tuple[int, str]], list[tuple[int, int]], set[int]]:
    """What an entry's disclaiming clauses unbind: the clause ids they name; else the ids of the
    clause before them in the same sentence; else the citations inside their sentence; else the
    citation on their own line. A sentence ends at a full stop, a `pins:` line is a sentence of
    its own, and a semicolon separates clauses. A named id unbinds the first citation after the
    clause that cites it, else the last one before; the result keys are (citation start, id)."""
    start, end = offsets[row], offsets[row_end]
    bounds = {start, end}
    bounds.update(match.end() for match in SENTENCE_END.finditer(text, start, end))
    for index in range(row, row_end):
        if lines[index].lstrip().startswith("pins:"):
            bounds.update({offsets[index], offsets[index + 1]})
    ordered = sorted(bounds)
    named_at: list[tuple[int, set[str]]] = []
    spans: list[tuple[int, int]] = []
    own_lines: set[int] = set()
    for low, high in itertools.pairwise(ordered):
        sentence = text[low:high]
        if DISCLAIMER.search(sentence) is None:
            continue
        previous: set[str] = set()
        clause_start = low
        for clause in sentence.split(";"):
            named = set(re.findall(r"C-\d{3}", GATE.CITATION.sub("", clause)))
            found = DISCLAIMER.search(clause)
            if found is not None:
                if named or previous:
                    named_at.append((clause_start, named or previous))
                elif GATE.CITATION.search(sentence) is not None:
                    spans.append((low, high))
                else:
                    own_lines.add(bisect.bisect_right(offsets, clause_start + found.start()) - 1)
            previous = named
            clause_start += len(clause) + 1
    citations = [
        (match.start(), set(re.findall(r"C-\d{3}", match.group(2))))
        for match in GATE.CITATION.finditer(text, start, end)
    ]
    unbound: set[tuple[int, str]] = set()
    for position, named in named_at:
        for clause_id in named:
            after = [at for at, cited in citations if at >= position and clause_id in cited]
            before = [at for at, cited in citations if at < position and clause_id in cited]
            if after:
                unbound.add((after[0], clause_id))
            elif before:
                unbound.add((before[-1], clause_id))
    return unbound, spans, own_lines


def migrate_map(
    repo: Path, path: str, tracked: set[str], basenames: dict[str, list[str]]
) -> tuple[list[Binding], str | None, int, int]:
    """Bind every citation of one map and cut the ones that can go; the new text if changed."""
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
    disclaimed_by_row: dict[int, tuple[set[tuple[int, str]], list[tuple[int, int]], set[int]]] = {}
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
        stretch_start = max(offsets[own], previous_end.get(row, 0))
        stretch = text[stretch_start : match.start()] + text[match.end() : offsets[end_index + 1]]
        previous_end[row] = match.end()
        names = names_in(stretch, bound, file_text(repo, bound, cache)) if bound else ()
        if inherited and not names:
            bound = None
        if row not in disclaimed_by_row:
            disclaimed_by_row[row] = disclaimers(text, lines, offsets, row, row_end)
        disclaimed_ids, disclaimed_spans, disclaimed_lines = disclaimed_by_row[row]
        unresolved = None if inherited else target.unresolved
        line_end = offsets[end_index + 1] - 1
        kept = fenced[line_index] or keeps_text(
            text, match.start(), match.end(), offsets[line_index], line_end
        )
        clause_ids = tuple(re.findall(r"C-\d{3}", match.group(2)))
        named = target.path if target.by_basename else None
        own_line = any(index in disclaimed_lines for index in range(line_index, end_index + 1))
        own_line = own_line or any(low <= match.start() < high for low, high in disclaimed_spans)
        refused = tuple(i for i in clause_ids if own_line or (match.start(), i) in disclaimed_ids)
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
    repo: Path, tracked: list[str]
) -> tuple[list[Binding], dict[str, tuple[str, int, int]]]:
    """Every citation under CITATION_ROOTS, bound; and the new text of every map that changes."""
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
        found, new, deleted, trimmed = migrate_map(repo, path, tracked_set, basenames)
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
    rerouted: dict[tuple[str, str], list[str]]


def plan_sidecars(
    repo: Path,
    bindings: list[Binding],
    homes: dict[str, str],
    known: set[tuple[str, str]],
    rows: dict[tuple[str, str], str],
    tracked: set[str],
) -> Plan:
    """Every sidecar that gains a line, the unplaceable and hand-bound citations, and the
    clauses written as `none (records)`. A path the sidecar on disk already binds for a clause
    is left as it stands, and a clause the sidecar on disk already carries gets no records line."""
    entries: dict[str, dict[str, set[str]]] = {}
    on_disk: dict[str, dict[str, set[str]]] = {}
    units: dict[str, str] = {}
    grown: set[str] = set()
    unplaceable: list[str] = []
    hand_bound: list[str] = []
    rerouted: dict[tuple[str, str], list[str]] = {}
    basenames: dict[str, list[str]] = {}
    for path in sorted(tracked):
        basenames.setdefault(Path(path).name, []).append(path)
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
            if binding.path is None or on_disk[sidecar].get(clause_id) == {GATE.RECORDS}:
                continue
            targets = [binding.path]
            if kind_of(binding.source) == "map" and len(binding.clause_ids) > 1:
                named = row_code_files(rows.get(key, ""), tracked, basenames)
                if named and binding.path not in named:
                    targets = [
                        path for path in named if GATE.bindable(path, GATE.read_tracked(repo, path))
                    ]
                    rerouted[key] = targets
            bound = {entry.split("::", 1)[0] for entry in on_disk[sidecar].get(clause_id, set())}
            for target in targets:
                if target in bound:
                    continue
                bucket = entries[sidecar].setdefault(clause_id, set())
                before = set(bucket)
                row_names = names_in(rows.get(key, ""), target, GATE.read_tracked(repo, target))
                own = set(binding.names) if target == binding.path else set()
                names = sorted(own | set(row_names))
                if names:
                    bucket.update(f"{target}::{name}" for name in names)
                else:
                    bucket.add(target)
                if bucket != before:
                    grown.add(sidecar)
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
    return Plan(changed, unplaceable, hand_bound, records, rerouted)


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
    print(f"  bound to their item's file by a defined name: {sum(bool(b.path) for b in inherited)}")
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
    print(f"shared-citation clauses rebound by their own ledger row: {len(plan.rerouted)}")
    print(f"  of which nothing bindable remains: {sum(not t for t in plan.rerouted.values())}")
    print(f"hand-bound citations: {len(plan.hand_bound)}")
    print(f"unplaceable citations: {len(plan.unplaceable)}")
    for line in plan.unplaceable:
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
    plan = plan_sidecars(repo, bindings, homes, known, ledger_rows(repo, homes), set(tracked))
    totals = sidecar_totals(repo, tracked, plan.changed)
    if plan.unplaceable:
        report(bindings, rewrites, totals, 0, plan)
        return 1
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
