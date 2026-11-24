"""SQL-UDF lexical parsing."""

from __future__ import annotations

import re

from typing import Any

from repark.spark.filter_quote import (
    _is_string_prefix_at,
    _quoted_span_closed,
    _quoted_span_end,
    _spark_block_comment_end,
    _spark_line_comment_end,
)


def _sql_top_level_keyword_index(query: str, keyword: str) -> int | None:
    """Index of top-level ``keyword`` (paren depth 0), or ``None``."""

    upper = query.upper()

    target = keyword.upper()

    depth = 0

    index = 0

    length = len(query)

    while index < length:
        char = query[index]

        if char == "-" and index + 1 < length and query[index + 1] == "-":
            index = _spark_line_comment_end(query, index)

            continue

        if char == "/" and index + 1 < length and query[index + 1] == "*":
            index = _spark_block_comment_end(query, index)

            continue

        if _is_string_prefix_at(query, index):
            index = _quoted_span_end(query, index + 1, raw=True)

            continue

        if char in {"'", '"', "`"}:
            index = _quoted_span_end(query, index)

            continue

        if char == "(":
            depth += 1

            index += 1

            continue

        if char == ")":
            depth = max(0, depth - 1)

            index += 1

            continue

        if depth == 0 and upper.startswith(target, index):
            prev_ok = index == 0 or not (query[index - 1].isalnum() or query[index - 1] == "_")

            end = index + len(target)

            next_ok = end >= length or not (query[end].isalnum() or query[end] == "_")

            if prev_ok and next_ok:
                return index

        index += 1

    return None


def _sql_udf_in_nested_subquery(query: str, udf_index: int) -> bool:
    """True when ``udf_index`` sits inside a parenthesized ``(SELECT|WITH …)`` subquery.



    U9: expression parens (``CAST(…)``, ``abs(…)``, ``f(g(x))``) are **not** subqueries —

    only regions whose open-paren is followed by SELECT/WITH (after whitespace).

    """

    stack: list[bool] = []

    index = 0

    length = len(query)

    while index < udf_index and index < length:
        char = query[index]

        if char == "-" and index + 1 < length and query[index + 1] == "-":
            index = _spark_line_comment_end(query, index)

            continue

        if char == "/" and index + 1 < length and query[index + 1] == "*":
            index = _spark_block_comment_end(query, index)

            continue

        if _is_string_prefix_at(query, index):
            index = _quoted_span_end(query, index + 1, raw=True)

            continue

        if char in {"'", '"', "`"}:
            index = _quoted_span_end(query, index)

            continue

        if char == "(":
            peek = index + 1

            while peek < length and query[peek].isspace():
                peek += 1

            is_sub = False

            if query[peek : peek + 6].upper() == "SELECT":
                end = peek + 6

                is_sub = end >= length or not (query[end].isalnum() or query[end] == "_")

            elif query[peek : peek + 4].upper() == "WITH":
                end = peek + 4

                is_sub = end >= length or not (query[end].isalnum() or query[end] == "_")

            stack.append(is_sub)

            index += 1

            continue

        if char == ")":
            if stack:
                stack.pop()

            index += 1

            continue

        index += 1

    return any(stack)


def _split_sql_select_list(select_list: str) -> list[str]:
    """Split a SELECT list on top-level commas (respecting parens/quotes)."""

    items: list[str] = []

    depth = 0

    start = 0

    index = 0

    length = len(select_list)

    while index < length:
        char = select_list[index]

        if char == "-" and index + 1 < length and select_list[index + 1] == "-":
            index = _spark_line_comment_end(select_list, index)

            continue

        if char == "/" and index + 1 < length and select_list[index + 1] == "*":
            index = _spark_block_comment_end(select_list, index)

            continue

        if _is_string_prefix_at(select_list, index):
            index = _quoted_span_end(select_list, index + 1, raw=True)

            continue

        if char in {"'", '"', "`"}:
            index = _quoted_span_end(select_list, index)

            continue

        if char == "(":
            depth += 1

        elif char == ")":
            depth = max(0, depth - 1)

        elif char == "," and depth == 0:
            items.append(select_list[start:index].strip())

            start = index + 1

        index += 1

    tail = select_list[start:].strip()

    if tail:
        items.append(tail)

    return items


def _sql_strip_comments_preserve_strings(query: str) -> str:
    """Remove ``--`` / ``/* */`` comments; keep string/ident quotes intact (octo C4-L-001).



    Unlike :func:`_sql_mask_strings_and_comments`, strings are preserved so UDF arg

    literals remain parseable. Used only for SELECT-list item structure matching.

    """

    if not query:
        return query

    out: list[str] = []

    length = len(query)

    index = 0

    while index < length:
        char = query[index]

        if char == "-" and index + 1 < length and query[index + 1] == "-":
            index = _spark_line_comment_end(query, index)

            continue

        if char == "/" and index + 1 < length and query[index + 1] == "*":
            out.append(" ")

            index = _spark_block_comment_end(query, index)

            continue

        if _is_string_prefix_at(query, index):
            end = _quoted_span_end(query, index + 1, raw=True)

            out.append(query[index:end])

            index = end

            continue

        if char in {"'", '"', "`"}:
            end = _quoted_span_end(query, index)

            out.append(query[index:end])

            index = end

            continue

        out.append(char)

        index += 1

    return "".join(out)


def _parse_simple_sql_udf_call(
    item: str,
    registry: dict[str, dict[str, Any]],
) -> tuple[str, list[str], str | None] | None:
    """Parse ``name(arg[, …]) [AS alias]`` with simple args, or return ``None``.



    Args must be bare identifiers, double-quoted idents, or simple numeric/string/NULL

    literals. Nested calls / expressions refuse by returning ``None``.

    """

    # Strip comments so ``udf /*c*/ (a)`` still parses (hit scan already masks them).

    text = _sql_strip_comments_preserve_strings(item).strip()

    # Optional trailing AS alias

    alias: str | None = None

    as_alias_pattern = r"(?i)\s+AS\s+((?:\"[^\"]+\")|(?:`[^`]+`)|(?:[A-Za-z_][A-Za-z0-9_]*))\s*$"

    as_match = re.search(as_alias_pattern, text)

    if as_match:
        alias_raw = as_match.group(1)

        if alias_raw.startswith('"') and alias_raw.endswith('"'):
            alias = alias_raw[1:-1].replace('""', '"')

        elif alias_raw.startswith("`") and alias_raw.endswith("`"):
            alias = alias_raw[1:-1].replace("``", "`")

        else:
            alias = alias_raw

        text = text[: as_match.start()].strip()

    call_match = re.match(
        r"^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)\s*$",
        text,
        re.DOTALL,
    )

    if not call_match:
        return None

    func_name = call_match.group(1)

    # Resolve against registry (case-insensitive key match → canonical registered name).

    registered_name: str | None = None

    for key in registry:
        if key.lower() == func_name.lower():
            registered_name = key

            break

    if registered_name is None:
        return None

    args_blob = call_match.group(2).strip()

    if not args_blob:
        return None  # zero-arg SQL UDF unsupported (same as DF path)

    arg_parts = _split_sql_select_list(args_blob)

    simple_args: list[str] = []

    for part in arg_parts:
        arg = part.strip()

        if not arg:
            return None

        # Bare ident / qualified col (t.a / cat.db.col) / quoted ident / numeric /
        # string / NULL / boolean — simple form only.

        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", arg):
            simple_args.append(arg)

            continue

        if re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+",
            arg,
        ):
            simple_args.append(arg)

            continue

        if re.fullmatch(r'"([^"]|"")*"', arg) or re.fullmatch(r"`([^`]|``)*`", arg):
            simple_args.append(arg)

            continue

        # Quoted multi-part: "t"."a" or `t`.`a`

        if re.fullmatch(
            r'("([^"]|"")*"|`([^`]|``)*`|[A-Za-z_][A-Za-z0-9_]*)'
            r'(\.("([^"]|"")*"|`([^`]|``)*`|[A-Za-z_][A-Za-z0-9_]*))+',
            arg,
        ):
            simple_args.append(arg)

            continue

        if re.fullmatch(r"-?\d+(\.\d+)?([eE][+-]?\d+)?", arg):
            simple_args.append(arg)

            continue

        if arg.startswith("'"):
            span_end, closed = _quoted_span_closed(arg, 0)

            if closed and span_end == len(arg):
                simple_args.append(arg)

                continue

        if _is_string_prefix_at(arg, 0):
            span_end, closed = _quoted_span_closed(arg, 1, raw=True)

            if closed and span_end == len(arg):
                simple_args.append(arg)

                continue

        if arg.upper() in {"NULL", "TRUE", "FALSE"}:
            simple_args.append(arg)

            continue

        return None  # expression arg — not simple form

    return registered_name, simple_args, alias
