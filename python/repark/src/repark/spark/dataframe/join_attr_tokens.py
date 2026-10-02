"""Join attribute-token siding: exact sides, complement, positional fallback."""

from __future__ import annotations

import functools
import re
from typing import TYPE_CHECKING, Any

from repark.errors import AnalysisException
from repark.spark import column_fields as _column_fields
from repark.spark._idents import quote_ident as _quote_ident_sql
from repark.spark._idents import unescape_attr_token_quals as _unescape_attr_token_quals

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame


_ATTR_TOKEN_RE = re.compile(r"__REPARK_ATTR_([A-Za-z0-9]+)__([\w\\|]*)__")


def _attr_token_exact_side(
    attr_id: str,
    token_quals: frozenset[str],
    *,
    left_ids: list[str | None],
    right_ids: list[str | None],
    left_quals: dict[str, set[str]],
    right_quals: dict[str, set[str]],
) -> str | None:
    """Return the join side a token names exactly, else ``None`` for later siding.

    A token whose id sits on one side binds there. A token whose id sits on both sides
    binds through its qualifier names when they hit one side only. Any other token
    (unknown id, empty or tied qualifiers) resolves in the rewrite pre-scan.
    """
    in_left = attr_id in left_ids
    in_right = attr_id in right_ids
    if in_left and not in_right:
        return "left"
    if in_right and not in_left:
        return "right"
    if not in_left or not in_right:
        return None
    if token_quals:
        left_hit = bool(token_quals & (left_quals.get(attr_id) or set()))
        right_hit = bool(token_quals & (right_quals.get(attr_id) or set()))
        if left_hit and not right_hit:
            return "left"
        if right_hit and not left_hit:
            return "right"
    return None


_ATTR_SIDE_BOUNDARY_RE = re.compile(r"(?i)(<=>|<=|>=|<>|!=|=|<|>|\bAND\b|\bOR\b)")


def _same_object_attr_alternation_safe(join_sql: str) -> bool:
    """Return whether token alternation can preserve sides in a self-join.

    Reject compound arms because alternation could bind a field to the wrong side.
    """
    matches = list(_ATTR_TOKEN_RE.finditer(join_sql))
    if len(matches) < 2:
        return True
    for index in range(len(matches) - 1):
        between = join_sql[matches[index].end() : matches[index + 1].start()]
        if _ATTR_SIDE_BOUNDARY_RE.search(between) is None:
            return False
    return True


def _resolve_join_token_sides(
    join_sql: str,
    *,
    left: DataFrame,
    right: DataFrame,
    left_ids: list[str | None],
    right_ids: list[str | None],
) -> list[str | None]:
    """Precompute the rewrite side per token match (``None`` leaves it unchanged).

    Exact tokens bind their side; each remaining token of an id with exact siblings
    takes the less-claimed side; any other remaining token alternates left/right by
    occurrence, which refuses multi-token arms rather than mis-binding them.
    """
    matches = list(_ATTR_TOKEN_RE.finditer(join_sql))
    left_quals = dict(left._frame_qualifiers or {})
    right_quals = dict(right._frame_qualifiers or {})
    exact: list[str | None] = [
        _attr_token_exact_side(
            match.group(1),
            _unescape_attr_token_quals(match.group(2)),
            left_ids=left_ids,
            right_ids=right_ids,
            left_quals=left_quals,
            right_quals=right_quals,
        )
        for match in matches
    ]
    claimed: dict[str, dict[str, int]] = {}
    for match, side in zip(matches, exact, strict=True):
        if side is not None:
            counts = claimed.setdefault(match.group(1), {"left": 0, "right": 0})
            counts[side] += 1
    same_object = left is right
    sides: list[str | None] = list(exact)
    positional = False
    for index, (match, side) in enumerate(zip(matches, exact, strict=True)):
        if side is not None:
            continue
        counts = claimed.get(match.group(1))
        if not same_object and counts is not None:
            if counts["left"] < counts["right"]:
                sides[index] = "left"
            elif counts["right"] < counts["left"]:
                sides[index] = "right"
            continue
        if match.group(1) in left_ids:
            sides[index] = "positional"
            positional = True
    if positional and not _same_object_attr_alternation_safe(join_sql):
        raise AnalysisException(
            "self-join condition has multi-token comparison arms that cannot "
            "be disambiguated by alternating left/right attribute sides (would silently "
            "mis-bind columns). Rewrite the condition so each comparison arm holds "
            "a single attribute reference."
        )
    take_left = True
    for index, side in enumerate(sides):
        if side == "positional":
            sides[index] = "left" if take_left else "right"
            take_left = not take_left
    return sides


def _replace_local_attr_token(
    match: re.Match[str],
    *,
    frame: DataFrame,
    held: list[str | None],
    engines: list[str],
    spell: Any,
) -> str:
    """Rewrite one ``__REPARK_ATTR_*`` token against a single frame's attribute ids."""
    attr_id = match.group(1)
    frame._raise_if_id_not_emitted(attr_id)
    if attr_id not in held:
        return match.group(0)
    return _quote_ident_sql(spell(engines[held.index(attr_id)]))


def _rewrite_attr_tokens_local(join_sql: str, frame: DataFrame, spell: Any = str) -> str:
    """Rewrite attribute tokens to quoted engine fields, or their ``spell`` names."""
    held, engines = _column_fields._stamped_ids_and_engines(frame)
    return _ATTR_TOKEN_RE.sub(
        functools.partial(
            _replace_local_attr_token, frame=frame, held=held, engines=engines, spell=spell
        ),
        join_sql,
    )


def _rewrite_join_attr_sql(
    join_sql: str,
    *,
    left: DataFrame,
    right: DataFrame,
    left_alias: str,
    right_alias: str,
) -> str:
    """Rewrite join attribute tokens to quoted fields on the matching side.

    Unknown tokens remain unchanged so the engine reports an analysis error.
    """
    left_ids, left_engines = _column_fields._stamped_ids_and_engines(left)
    right_ids, right_engines = _column_fields._stamped_ids_and_engines(right)
    sides = _resolve_join_token_sides(
        join_sql, left=left, right=right, left_ids=left_ids, right_ids=right_ids
    )
    rewriter = _JoinAttrRewriter(
        left_ids=left_ids,
        left_engines=left_engines,
        right_ids=right_ids,
        right_engines=right_engines,
        left_alias=left_alias,
        right_alias=right_alias,
        sides=sides,
    )
    return _ATTR_TOKEN_RE.sub(rewriter, join_sql)


class _JoinAttrRewriter:
    """Rewrite join attribute tokens to their precomputed sides in match order."""

    def __init__(
        self,
        *,
        left_ids: list[str | None],
        left_engines: list[str],
        right_ids: list[str | None],
        right_engines: list[str],
        left_alias: str,
        right_alias: str,
        sides: list[str | None],
    ) -> None:
        self.left_ids = left_ids
        self.left_engines = left_engines
        self.right_ids = right_ids
        self.right_engines = right_engines
        self.left_alias = left_alias
        self.right_alias = right_alias
        self.sides = sides
        self.position = 0

    def __call__(self, match: re.Match[str]) -> str:
        attr_id = match.group(1)
        side = self.sides[self.position] if self.position < len(self.sides) else None
        self.position += 1
        if side == "left":
            engine = self.left_engines[self.left_ids.index(attr_id)]
            return f"{self.left_alias}.{_quote_ident_sql(engine)}"
        if side == "right":
            engine = self.right_engines[self.right_ids.index(attr_id)]
            return f"{self.right_alias}.{_quote_ident_sql(engine)}"
        return match.group(0)
