"""SQL-UDF call discovery."""

from __future__ import annotations

import re

from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from repark.spark.session.sql_relations import (
        _find_matching_paren,
        _sql_mask_strings_and_comments,
    )
    from repark.spark.session.sql_udf_parsing import (
        _split_sql_select_list,
        _sql_strip_comments_preserve_strings,
        _sql_top_level_keyword_index,
    )


def _sql_collect_registry_udf_hits(
    body: str,
    registry: dict[str, dict[str, Any]],
) -> list[tuple[str, str, int]]:
    """Collect ``(canonical_name, matched_text, index)`` for registry UDF calls in ``body``."""

    # === r20 U9: sql-udf-rewrite ===

    body_code = _sql_mask_strings_and_comments(body)

    hits: list[tuple[str, str, int]] = []

    seen_spans: set[tuple[int, int]] = set()

    for registered_name in registry:
        pattern = re.compile(
            rf"(?<![\w.])({re.escape(registered_name)})\s*\(",
            re.IGNORECASE,
        )

        for match in pattern.finditer(body_code):
            span = (match.start(), match.end())

            if span in seen_spans:
                continue

            # Star-only call ``name(*)`` is an engine aggregate form, not a Python UDF

            # invocation (U9-C2-001 — registering ``count`` must not break ``count(*)``).

            paren_open = match.end() - 1

            close = _find_matching_paren(body, paren_open)

            if close is not None:
                args_blob = body[paren_open + 1 : close].strip()

                if args_blob == "*":
                    continue

            seen_spans.add(span)

            matched_raw = body[match.start() : match.end() - 1].rstrip()

            canonical = registered_name

            for key in registry:
                if key.lower() == matched_raw.lower():
                    canonical = key

                    break

            hits.append((canonical, matched_raw, match.start()))

    hits.sort(key=lambda item: item[2])

    return hits


def _sql_find_registry_udf_calls(
    expr_text: str,
    registry: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Find registry UDF calls in a SELECT-list expression (paren-matched args)."""

    # === r20 U9: sql-udf-rewrite ===

    masked = _sql_mask_strings_and_comments(expr_text)

    calls: list[dict[str, Any]] = []

    seen: set[tuple[int, int]] = set()

    for registered_name in registry:
        pattern = re.compile(
            rf"(?<![\w.])({re.escape(registered_name)})\s*\(",
            re.IGNORECASE,
        )

        for match in pattern.finditer(masked):
            name_start = match.start(1)

            paren_open = match.end() - 1  # index of '('

            # Skip whitespace/comments between name and '(' already consumed by \s*.

            close = _find_matching_paren(expr_text, paren_open)

            if close is None:
                continue

            span = (name_start, close + 1)

            if span in seen:
                continue

            args_blob = expr_text[paren_open + 1 : close]

            # Engine aggregate ``name(*)`` is not a Python UDF call (U9-C2-001).

            if args_blob.strip() == "*":
                continue

            seen.add(span)

            args = _split_sql_select_list(args_blob) if args_blob.strip() else []

            # Resolve canonical registry name.

            raw_name = expr_text[name_start:paren_open].strip()

            # Strip trailing comments between name and paren.

            raw_name = _sql_strip_comments_preserve_strings(raw_name).strip()

            canonical = registered_name

            for key in registry:
                if key.lower() == raw_name.lower():
                    canonical = key

                    break

            calls.append(
                {
                    "registered_name": canonical,
                    "start": name_start,
                    "end": close + 1,
                    "args": args,
                }
            )

    return calls


def _sql_udf_arg_is_simple(arg: str) -> bool:
    """True when ``arg`` is a simple col/lit suitable as a UDF input projection."""

    # === r20 U9: sql-udf-rewrite ===

    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", arg):
        return True

    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+", arg):
        return True

    if re.fullmatch(r'"([^"]|"")*"', arg) or re.fullmatch(r"`([^`]|``)*`", arg):
        return True

    if re.fullmatch(
        r'("([^"]|"")*"|`([^`]|``)*`|[A-Za-z_][A-Za-z0-9_]*)'
        r'(\.("([^"]|"")*"|`([^`]|``)*`|[A-Za-z_][A-Za-z0-9_]*))+',
        arg,
    ):
        return True

    if re.fullmatch(r"-?\d+(\.\d+)?([eE][+-]?\d+)?", arg):
        return True

    if re.fullmatch(r"'(?:[^']|'')*'", arg):
        return True

    return arg.upper() in {"NULL", "TRUE", "FALSE"}


def _sql_peel_select_trailing_clauses(rest: str) -> tuple[str, dict[str, str | None]]:
    """Split ``FROM … [WHERE …] [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT …]``.



    Returns ``(core_rest, peeled)`` where ``core_rest`` keeps FROM + JOIN chain (U10

    peels WHERE too so UDF residuals can be applied post-materialization) and

    ``peeled`` holds optional trailing clause SQL fragments.

    """

    # === r20 U9: sql-udf-rewrite ===

    # === r21 T7: census-r6 ===

    peeled: dict[str, str | None] = {
        "where": None,
        "group_by": None,
        "having": None,
        "order_by": None,
        "limit": None,
    }

    # Find top-level clause starts (paren depth 0).

    markers: list[tuple[str, int]] = []

    for keyword in ("WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"):
        index = _sql_top_level_keyword_index(rest, keyword)

        if index is not None:
            markers.append((keyword, index))

    if not markers:
        return rest, peeled

    markers.sort(key=lambda item: item[1])

    core_end = markers[0][1]

    core_rest = rest[:core_end].rstrip()

    # Slice each clause until the next marker (or end).

    for position, (keyword, start) in enumerate(markers):
        end = markers[position + 1][1] if position + 1 < len(markers) else len(rest)

        fragment = rest[start:end].strip()

        key = {
            "WHERE": "where",
            "GROUP BY": "group_by",
            "HAVING": "having",
            "ORDER BY": "order_by",
            "LIMIT": "limit",
        }[keyword]

        peeled[key] = fragment

    return core_rest, peeled


def _sql_udf_call_match_key(call_text: str) -> str:
    """Case- and whitespace-normalized key for SELECT↔GROUP BY/HAVING UDF match (U10 C2)."""

    # === r21 T7: census-r6 ===

    return re.sub(r"\s+", "", call_text.strip()).lower()


def _sql_residual_has_subquery(residual: str) -> bool:
    """True when a WHERE/HAVING residual still embeds a SELECT/EXISTS subquery (U10 C3)."""

    # === r21 T7: census-r6 ===

    masked = _sql_mask_strings_and_comments(residual)

    return bool(
        re.search(r"(?is)\(\s*SELECT\b", masked) or re.search(r"(?is)\bEXISTS\s*\(", masked)
    )
