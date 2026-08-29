"""SQL-UDF lexical parsing."""

from __future__ import annotations

import re

from typing import Any


def _sql_top_level_keyword_index(query: str, keyword: str) -> int | None:
    """Index of top-level ``keyword`` (paren depth 0), or ``None``."""

    upper = query.upper()

    target = keyword.upper()

    depth = 0

    index = 0

    length = len(query)

    in_single = False

    in_double = False

    while index < length:
        char = query[index]

        if in_single:
            if char == "'":
                if index + 1 < length and query[index + 1] == "'":
                    index += 2

                    continue

                in_single = False

            index += 1

            continue

        if in_double:
            if char == '"':
                if index + 1 < length and query[index + 1] == '"':
                    index += 2

                    continue

                in_double = False

            index += 1

            continue

        if char == "'":
            in_single = True

            index += 1

            continue

        if char == '"':
            in_double = True

            index += 1

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
            # Word boundary: prev not alnum/_, next not alnum/_

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

    in_single = False

    in_double = False

    while index < udf_index and index < length:
        char = query[index]

        if in_single:
            if char == "'":
                if index + 1 < length and query[index + 1] == "'":
                    index += 2

                    continue

                in_single = False

            index += 1

            continue

        if in_double:
            if char == '"':
                if index + 1 < length and query[index + 1] == '"':
                    index += 2

                    continue

                in_double = False

            index += 1

            continue

        if char == "'":
            in_single = True

            index += 1

            continue

        if char == '"':
            in_double = True

            index += 1

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

    in_single = False

    in_double = False

    while index < length:
        char = select_list[index]

        if in_single:
            if char == "'":
                if index + 1 < length and select_list[index + 1] == "'":
                    index += 2

                    continue

                in_single = False

            index += 1

            continue

        if in_double:
            if char == '"':
                if index + 1 < length and select_list[index + 1] == '"':
                    index += 2

                    continue

                in_double = False

            index += 1

            continue

        if char == "'":
            in_single = True

            index += 1

            continue

        if char == '"':
            in_double = True

            index += 1

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
            end = query.find("\n", index)

            if end < 0:
                break

            out.append("\n")

            index = end + 1

            continue

        if char == "/" and index + 1 < length and query[index + 1] == "*":
            end = query.find("*/", index + 2)

            if end < 0:
                break

            out.append(" ")

            index = end + 2

            continue

        if char in {"'", '"', "`"}:
            quote = char

            out.append(char)

            index += 1

            while index < length:
                current = query[index]

                out.append(current)

                index += 1

                if current == quote:
                    if index < length and query[index] == quote:
                        out.append(query[index])

                        index += 1

                        continue

                    break

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

        if re.fullmatch(r"'(?:[^']|'')*'", arg):
            simple_args.append(arg)

            continue

        if arg.upper() in {"NULL", "TRUE", "FALSE"}:
            simple_args.append(arg)

            continue

        return None  # expression arg — not simple form

    return registered_name, simple_args, alias
