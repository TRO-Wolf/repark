"""Unresolved and ambiguous column-name errors for the Column facade."""

from __future__ import annotations

from typing import NoReturn

from repark.errors import AnalysisException


def _suggestion_candidates(name: str, displays: list[str]) -> list[str]:
    lowered = name.lower()
    seen: set[str] = set()
    candidates: list[str] = []
    for display in displays:
        if display.lower() == lowered and display not in seen:
            seen.add(display)
            candidates.append(display)
    return candidates


def _qualified_target(qualifier: list[str] | None, name: str) -> str:
    if qualifier is None:
        return f"`{name}`"
    return ".".join([*(f"`{part}`" for part in qualifier), f"`{name}`"])


def _raise_unresolved_name(qualifier: list[str] | None, name: str, displays: list[str]) -> NoReturn:
    message = (
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter "
        f"with name {_qualified_target(qualifier, name)} cannot be resolved."
    )
    candidates = _suggestion_candidates(name, displays)
    if candidates:
        quoted = ", ".join(f"`{candidate}`" for candidate in candidates)
        message = f"{message} Did you mean one of the following? [{quoted}]."
    raise AnalysisException(f"{message} SQLSTATE: 42703")


def _raise_folded_ambiguous(
    qualifier: list[str] | None, name: str, hits: list[int], displays: list[str]
) -> NoReturn:
    reference = _qualified_target(qualifier, name)
    if qualifier is None:
        echoed = ", ".join(f"`{displays[position]}`" for position in hits)
    else:
        echoed = ", ".join(_qualified_target(qualifier, displays[position]) for position in hits)
    raise AnalysisException(
        f"[AMBIGUOUS_REFERENCE] Reference {reference} is ambiguous, "
        f"could be: [{echoed}]. SQLSTATE: 42704"
    )
