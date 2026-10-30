#!/usr/bin/env python3
"""The map-text side of `migrate_pins_to_sidecars.py`: entries, cuts and disclaimers.

Entries are a map's list rows, table rows, headings, dated entries and paragraphs; a cut takes a
`pins:` citation out of a map with the separators and brackets that go with it; a disclaimer is a
sentence that says a cited file pins nothing. The migration imports this module; it is not run on
its own.
"""

from __future__ import annotations

import importlib.util
import itertools
import re
from pathlib import Path
from types import ModuleType


def load_gate() -> ModuleType:
    """The grammar gate, for its citation regex; loaded on first use, because the gate itself
    imports this module for its code-span reader."""
    if "gate" not in _LOADED:
        location = Path(__file__).resolve().parent / "check_ledger_grammar.py"
        spec = importlib.util.spec_from_file_location("check_ledger_grammar", location)
        if spec is None or spec.loader is None:
            message = f"cannot load {location}"
            raise RuntimeError(message)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _LOADED["gate"] = module
    return _LOADED["gate"]


def code_spans(text: str) -> list[str]:
    """The contents of every CommonMark code span in `text`: a run of N backticks opens a span
    that only the next run of exactly N backticks closes; runs of another length inside it are
    literal, and an opener with no closer is literal text. One leading and one trailing space
    are dropped when both are present and the content is not all spaces."""
    spans: list[str] = []
    index = 0
    while True:
        opener = BACKTICK_RUN.search(text, index)
        if opener is None:
            return spans
        width = len(opener.group(0))
        closer = BACKTICK_RUN.search(text, opener.end())
        while closer is not None and len(closer.group(0)) != width:
            closer = BACKTICK_RUN.search(text, closer.end())
        if closer is None:
            index = opener.end()
            continue
        content = text[opener.end() : closer.start()].replace("\n", " ")
        if len(content) > 1 and content[0] == " " and content[-1] == " " and content.strip():
            content = content[1:-1]
        spans.append(content)
        index = closer.end()


def span_name(span: str) -> str | None:
    """The identifier a code span names: the last segment of a qualified, `path::name` or
    `::name` span, with a trailing `()` or a pytest parametrize id `[...]` dropped."""
    match = SPAN_NAME.match(span.strip())
    return match.group(1) if match is not None else None


def gate_citation() -> re.Pattern[str]:
    """The grammar gate's citation regex."""
    return load_gate().CITATION


_LOADED: dict[str, ModuleType] = {}
BACKTICK_RUN = re.compile(r"`+")
SPAN_NAME = re.compile(
    r"^(?:[\w./-]*::)?(?:[A-Za-z_]\w*(?:::|\.))*([A-Za-z_]\w*)(?:\(\)|\[[^\]]*\])?$", re.ASCII
)
SENTINEL = "\x00"
FENCE = re.compile(r"^\s*(```|~~~)")
LIST_ITEM = re.compile(r"^\s*(?:[-*+]|\d+\.)\s")
TABLE_ROW = re.compile(r"^\s*\|")
HEADING = re.compile(r"^#{1,6}\s")
DATED = re.compile(r"^\s*(?:\*\*)?[A-Z][^()\n`]{0,80}?\(\d{4}-\d{2}-\d{2}[^)\n]*\)")
DATED_TOP = re.compile(r"^(?:\*\*)?[A-Z][^()\n`]{0,80}?\(\d{4}-\d{2}-\d{2}[^)\n]*\)")
CODE_SPAN = re.compile(r"`([^`]+)`")
TAIL = re.compile(
    r"^(?:\s*[,;]?\s*[a-z0-9][a-z0-9.-]*/[A-Za-z]"
    r"|\s*,\s*[A-Z][A-Za-z0-9]*-[A-Za-z0-9-]*\d"
    r"|\s*(?:\.\.|…))"
)
EMPTY_REST = re.compile(r"^[\s\-*+.,;:()·]*$")
LEADING_STRAY = ".;)"
SENTENCE_END = re.compile(r"\.(?=\s)")
PROVES_ONLY = re.compile(r"\bproves?\b[^.;]*?\bonly\b", re.IGNORECASE)
DISCLAIMER = re.compile(
    r"grammar gate counts|pins? none|pins nothing|not a test pin", re.IGNORECASE
)


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


def parent_item(lines: list[str], fenced: list[bool], row: int) -> int | None:
    """The list item an indented sub-bullet sits under: the nearest earlier list item with less
    indentation, before any heading or unindented non-list line."""
    line = lines[row]
    if not LIST_ITEM.match(line) or not line.startswith((" ", "\t")):
        return None
    depth = len(line) - len(line.lstrip())
    for index in range(row - 1, -1, -1):
        candidate = lines[index]
        if fenced[index] or not candidate.strip():
            continue
        if HEADING.match(candidate) or not candidate.startswith((" ", "\t", "-", "*", "+")):
            return None
        if LIST_ITEM.match(candidate) and len(candidate) - len(candidate.lstrip()) < depth:
            return index
    return None


def clauses_of(sentence: str) -> list[tuple[int, str]]:
    """A sentence split at the semicolons outside parentheses, with each clause's offset."""
    parts: list[tuple[int, str]] = []
    depth = 0
    begin = 0
    for index, char in enumerate(sentence):
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == ";" and depth == 0:
            parts.append((begin, sentence[begin:index]))
            begin = index + 1
    parts.append((begin, sentence[begin:]))
    return parts


def disclaimers(
    text: str, lines: list[str], offsets: list[int], row: int, row_end: int
) -> tuple[set[tuple[int, str]], list[tuple[int, str, str]]]:
    """An entry's disclaimers: a clause naming ids unbinds each at the nearest citation citing
    it (among the units the sentence names); an id-less one, unless it follows a clause naming
    ids, and a `proves ... only` sentence unbind nothing and yield hand candidates
    (citation start, id, sentence). Sentences end at a full stop; a `pins:` line is one."""
    start, end = offsets[row], offsets[row_end]
    bounds = {start, end}
    bounds.update(match.end() for match in SENTENCE_END.finditer(text, start, end))
    for index in range(row, row_end):
        if lines[index].lstrip().startswith("pins:"):
            bounds.update({offsets[index], offsets[index + 1]})
    ordered = sorted(bounds)
    citations = [
        (match.start(), match.group(1), set(re.findall(r"C-\d{3}", match.group(2))))
        for match in gate_citation().finditer(text, start, end)
    ]
    unbound: set[tuple[int, str]] = set()
    candidates: list[tuple[int, str, str]] = []
    for low, high in itertools.pairwise(ordered):
        sentence = text[low:high]
        plain = gate_citation().sub("", sentence)
        quoted = " ".join(sentence.split())
        proves = PROVES_ONLY.search(plain)
        if proves is not None:
            proven = set(re.findall(r"C-\d{3}", proves.group(0)))
            candidates.extend(
                (at, clause_id, quoted)
                for at, _unit, cited in citations
                for clause_id in sorted(cited - proven)
            )
        if DISCLAIMER.search(plain) is None:
            continue
        lowered = sentence.lower()
        units = {unit for _at, unit, _cited in citations if unit.lower() in lowered}
        anaphor = False
        for offset, clause in clauses_of(sentence):
            if DISCLAIMER.search(gate_citation().sub("", clause)) is None:
                continue
            named = set(re.findall(r"C-\d{3}", gate_citation().sub("", clause)))
            position = low + offset
            if not named and anaphor:
                continue
            anaphor = bool(named)
            if not named:
                candidates.extend(
                    (at, clause_id, quoted)
                    for at, _unit, cited in citations
                    if at >= low
                    for clause_id in sorted(cited)
                )
                continue
            eligible = [item for item in citations if not units or item[1] in units]
            for clause_id in named:
                after = [at for at, _u, cited in eligible if at >= position and clause_id in cited]
                before = [at for at, _u, cited in eligible if at < position and clause_id in cited]
                if after:
                    unbound.add((after[0], clause_id))
                elif before:
                    unbound.add((before[-1], clause_id))
    return unbound, candidates
