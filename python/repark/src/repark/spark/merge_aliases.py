"""Alias resolution for ``DataFrame.mergeInto``'s rendered ``MERGE INTO`` statement.

Spark's own condition form qualifies the target by its **short table name** and the source by the
frame's alias (``s.mergeInto(t, expr("t.id = s.id"))``). RePark also accepts the ``target.`` /
``source.`` spellings Spark rejects, and a bare-key equi-join sugar (registry row ``EX-DF-9``,
owner decision 22). Both have to bind, and a SQL relation carries exactly one alias, so the pair
is chosen from the qualifiers the user's own expressions reference.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

LEGACY_TARGET = "target"
LEGACY_SOURCE = "source"

_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")
_QUALIFIER = re.compile(
    r'(?<![\w.`"])(?:`((?:[^`]|``)*)`|"((?:[^"]|"")*)"|([A-Za-z_][A-Za-z0-9_]*))\s*\.'
)


def _qualifiers(fragment: str) -> set[str]:
    """Every relation qualifier a rendered SQL fragment references, string literals removed."""
    stripped = _STRING_LITERAL.sub("''", fragment)
    found: set[str] = set()
    for tick, quoted, bare in _QUALIFIER.findall(stripped):
        if tick:
            found.add(tick.replace("``", "`"))
        elif quoted:
            found.add(quoted.replace('""', '"'))
        elif bare:
            found.add(bare)
    return found


def short_table_name(table: str) -> str:
    """The last dotted component of a table name, with its quoting removed."""
    parts: list[str] = []
    current: list[str] = []
    quote: str | None = None
    for character in table.strip():
        if quote is not None:
            if character == quote:
                quote = None
            else:
                current.append(character)
        elif character in '`"':
            quote = character
        elif character == ".":
            parts.append("".join(current))
            current = []
        else:
            current.append(character)
    parts.append("".join(current))
    return parts[-1].strip()


def resolve_aliases(table: str, fragments: Iterable[str]) -> tuple[str, str]:
    """Pick the ``(target, source)`` aliases the rendered statement declares.

    The target is its short table name when the expressions name it, the legacy ``target`` when
    they name that instead, and the short name otherwise. The source is whatever single other
    qualifier the expressions reference, else the legacy ``source``.
    """
    referenced: set[str] = set()
    for fragment in fragments:
        referenced |= _qualifiers(fragment)
    folded = {name.casefold(): name for name in referenced}
    short = short_table_name(table)
    if short.casefold() in folded:
        target = folded[short.casefold()]
    elif LEGACY_TARGET in folded:
        target = folded[LEGACY_TARGET]
    else:
        target = short
    rest = sorted(name for name in referenced if name.casefold() != target.casefold())
    source = rest[0] if len(rest) == 1 else LEGACY_SOURCE
    return target, source
