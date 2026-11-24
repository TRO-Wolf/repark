from __future__ import annotations

import functools
import re
from typing import Any

from repark import _native
from repark.errors import AnalysisException, PySparkTypeError, PySparkValueError


class _FoldedLambdaFallbackError(Exception):
    pass


def _unqualified_candidates(name: str, displays: list[str]) -> tuple[list[int], list[int]]:
    exact_hits = [index for index, display in enumerate(displays) if display == name]
    folded = name.casefold()
    folded_hits = [
        index
        for index, display in enumerate(displays)
        if display != name and display.casefold() == folded
    ]
    return exact_hits, folded_hits


def _group_candidates(candidates: list[int], held: list[str | None]) -> tuple[str, list[int]]:
    groups: list[str | None] = []
    for index in candidates:
        held_id = held[index]
        if held_id is None or held_id not in groups:
            groups.append(held_id)
    if not candidates:
        return ("missing", [])
    if len(groups) == 1:
        return ("bound", [candidates[0]])
    return ("ambiguous", candidates)


_FILTER_TOKEN_PATTERN = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\b(?!\s*\()"
)

_SQL_LITERAL_KEYWORDS = frozenset({"true", "false", "null"})
_STRING_PREFIX_CHARS = frozenset({"r", "R", "x", "X"})
_BOOL_OPERATOR_WORDS = frozenset({"and", "or", "not"})
_INFIX_AFTER_NOT = frozenset({"in", "like", "ilike", "rlike", "regexp", "between"})
_PREFIX_EXPR_KEYWORDS = frozenset(
    {
        "and",
        "or",
        "not",
        "when",
        "then",
        "else",
        "case",
        "where",
        "on",
        "having",
        "select",
        "between",
        "in",
        "is",
        "like",
        "ilike",
        "rlike",
        "regexp",
        "distinct",
        "from",
        "as",
        "by",
        "escape",
        "over",
        "div",
        "mod",
        "exists",
    }
)
_POSTFIX_OR_INFIX_KEYWORDS = frozenset(
    {
        "is",
        "in",
        "like",
        "ilike",
        "rlike",
        "regexp",
        "between",
        "and",
        "or",
        "then",
        "else",
        "end",
        "as",
        "asc",
        "desc",
        "nulls",
        "from",
        "where",
        "group",
        "order",
        "having",
        "limit",
        "union",
        "intersect",
        "except",
        "when",
    }
)
_CODE_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|\d+(?:\.\d*)?|\S")


def _spark_line_comment_end(sql: str, index: int) -> int:
    cursor = index + 2
    bound = len(sql)
    while cursor < bound and sql[cursor] not in "\n\r":
        cursor += 1
    return cursor


def _spark_block_comment_end(sql: str, index: int) -> int:
    cursor = index + 2
    bound = len(sql)
    depth = 1
    while cursor < bound and depth > 0:
        if sql[cursor] == "/" and cursor + 1 < bound and sql[cursor + 1] == "*":
            depth += 1
            cursor += 2
        elif sql[cursor] == "*" and cursor + 1 < bound and sql[cursor + 1] == "/":
            depth -= 1
            cursor += 2
        else:
            cursor += 1
    return cursor


def _is_string_prefix_at(sql: str, index: int) -> bool:
    if index + 1 >= len(sql):
        return False
    if sql[index] not in _STRING_PREFIX_CHARS:
        return False
    if sql[index + 1] not in "'\"":
        return False
    return not (index > 0 and (sql[index - 1].isalnum() or sql[index - 1] == "_"))


def _quoted_span_closed(sql: str, index: int, *, raw: bool = False) -> tuple[int, bool]:
    quote = sql[index]
    cursor = index + 1
    bound = len(sql)
    if raw:
        while cursor < bound:
            if sql[cursor] == quote:
                return (cursor + 1, True)
            cursor += 1
        return (bound, False)
    while cursor < bound:
        char = sql[cursor]
        if char == "\\" and quote != "`":
            cursor = min(cursor + 2, bound)
            continue
        if char == quote:
            if cursor + 1 < bound and sql[cursor + 1] == quote:
                cursor += 2
                continue
            return (cursor + 1, True)
        cursor += 1
    return (bound, False)


def _quoted_span_end(sql: str, index: int, *, raw: bool = False) -> int:
    return _quoted_span_closed(sql, index, raw=raw)[0]


def _iter_spark_sql_spans(sql: str) -> list[tuple[str, int, int]]:
    spans: list[tuple[str, int, int]] = []
    bound = len(sql)
    index = 0
    code_start = 0
    while index < bound:
        char = sql[index]
        if char == "-" and index + 1 < bound and sql[index + 1] == "-":
            if code_start < index:
                spans.append(("code", code_start, index))
            end = _spark_line_comment_end(sql, index)
            spans.append(("comment", index, end))
            index = end
            code_start = index
            continue
        if char == "/" and index + 1 < bound and sql[index + 1] == "*":
            if code_start < index:
                spans.append(("code", code_start, index))
            end = _spark_block_comment_end(sql, index)
            spans.append(("comment", index, end))
            index = end
            code_start = index
            continue
        if _is_string_prefix_at(sql, index):
            if code_start < index:
                spans.append(("code", code_start, index))
            end = _quoted_span_end(sql, index + 1, raw=True)
            spans.append(("string", index, end))
            index = end
            code_start = index
            continue
        if char in "'\"":
            if code_start < index:
                spans.append(("code", code_start, index))
            end = _quoted_span_end(sql, index)
            spans.append(("string", index, end))
            index = end
            code_start = index
            continue
        if char == "`":
            if code_start < index:
                spans.append(("code", code_start, index))
            end = _quoted_span_end(sql, index)
            spans.append(("backtick", index, end))
            index = end
            code_start = index
            continue
        index += 1
    if code_start < bound:
        spans.append(("code", code_start, bound))
    return spans


def _sql_token_stream(
    sql: str, spans: list[tuple[str, int, int]]
) -> list[tuple[str, str, int, int]]:
    tokens: list[tuple[str, str, int, int]] = []
    for kind, start, end in spans:
        if kind == "comment":
            continue
        if kind in {"string", "backtick"}:
            tokens.append((kind, sql[start:end], start, end))
            continue
        for match in _CODE_TOKEN_RE.finditer(sql, start, end):
            text = match.group(0)
            tok_kind = "word" if (text[0].isalnum() or text[0] == "_") else "punct"
            tokens.append((tok_kind, text, match.start(), match.end()))
    return tokens


def _token_is_binary_or_end(token: tuple[str, str, int, int] | None) -> bool:
    if token is None:
        return True
    kind, text, _, _ = token
    if kind in {"string", "backtick"}:
        return False
    if kind == "punct":
        return text not in {"(", "~"}
    return text.casefold() in _POSTFIX_OR_INFIX_KEYWORDS


def _not_token_is_operator(
    tokens: list[tuple[str, str, int, int]],
    position: int,
    prev_word: str | None,
    prev_ends_value: bool,
) -> bool:
    if prev_word == "is":
        return True
    next_tok = tokens[position + 1] if position + 1 < len(tokens) else None
    if (
        next_tok is not None
        and next_tok[0] == "word"
        and next_tok[1].casefold() in _INFIX_AFTER_NOT
    ):
        return True
    if prev_ends_value:
        return False
    if next_tok is None:
        return False
    if next_tok[0] == "word" and next_tok[1].casefold() == "not":
        after = tokens[position + 2] if position + 2 < len(tokens) else None
        return not (
            after is not None and after[0] == "word" and after[1].casefold() in _INFIX_AFTER_NOT
        )
    if next_tok[0] == "word" and next_tok[1].casefold() in {"and", "or"}:
        after = tokens[position + 2] if position + 2 < len(tokens) else None
        if after is None:
            return True
        if after[0] == "punct":
            return after[1] not in {"(", "~"}
        if after[0] == "word":
            return after[1].casefold() in _POSTFIX_OR_INFIX_KEYWORDS
        return False
    return not _token_is_binary_or_end(next_tok)


def _boolean_operator_spans(
    sql: str, spans: list[tuple[str, int, int]]
) -> frozenset[tuple[int, int]]:
    tokens = _sql_token_stream(sql, spans)
    operators: set[tuple[int, int]] = set()
    prev_ends_value = False
    prev_word: str | None = None
    for position, (kind, text, start, end) in enumerate(tokens):
        if kind in {"string", "backtick"}:
            prev_ends_value = True
            prev_word = None
            continue
        if kind == "punct":
            prev_ends_value = text in {")", "]"}
            prev_word = None
            continue
        folded = text.casefold()
        if folded in {"and", "or"}:
            if prev_ends_value:
                operators.add((start, end))
                prev_ends_value = False
                prev_word = folded
            else:
                prev_ends_value = True
                prev_word = folded
            continue
        if folded == "not":
            if _not_token_is_operator(tokens, position, prev_word, prev_ends_value):
                operators.add((start, end))
                prev_ends_value = False
                prev_word = folded
            else:
                prev_ends_value = True
                prev_word = folded
            continue
        prev_ends_value = folded not in _PREFIX_EXPR_KEYWORDS
        prev_word = folded
    return frozenset(operators)


def _lambda_paren_match(sql: str, arrows: list[int]) -> dict[int, int]:
    opener: list[int] = []
    matched: dict[int, int] = {}
    index = 0
    bound = len(sql)
    while index < bound:
        char = sql[index]
        if char == "-" and index + 1 < bound and sql[index + 1] == "-":
            index = _spark_line_comment_end(sql, index)
        elif char == "/" and index + 1 < bound and sql[index + 1] == "*":
            index = _spark_block_comment_end(sql, index)
        elif _is_string_prefix_at(sql, index):
            index = _quoted_span_end(sql, index + 1, raw=True)
        elif char in "'\"`":
            index = _quoted_span_end(sql, index)
        elif char == "(":
            opener.append(index)
            index += 1
        elif char == ")":
            if opener:
                matched[index] = opener.pop()
            index += 1
        elif sql.startswith("->", index) and not sql.startswith("->>", index):
            arrows.append(index)
            index += 2
        else:
            index += 1
    return matched


def _lambda_piece_spelling(piece: str) -> str | None:
    text = piece.strip(" \t\n\r")
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text):
        return text
    if len(text) >= 2 and text.startswith("`") and text.endswith("`"):
        inner = text[1:-1]
        if "``" in inner:
            inner = inner.replace("``", "`")
        if "`" not in inner:
            return inner
    return None


def _lambda_group_piece(sql: str, piece_start: int, cursor: int) -> tuple[int, int, str] | None:
    piece = sql[piece_start:cursor]
    spelling = _lambda_piece_spelling(piece)
    if spelling is None:
        return None
    stripped = piece.strip(" \t\n\r")
    offset = piece.find(stripped)
    return (piece_start + offset, piece_start + offset + len(stripped), spelling)


def _lambda_group_params(sql: str, opener: int, closer: int) -> list[tuple[int, int, str]] | None:
    params: list[tuple[int, int, str]] = []
    cursor = opener + 1
    piece_start = cursor
    depth = 0
    while cursor <= closer:
        if cursor == closer:
            piece = _lambda_group_piece(sql, piece_start, cursor)
            if piece is None:
                return None
            params.append(piece)
            break
        char = sql[cursor]
        if char == "-" and cursor + 1 < closer and sql[cursor + 1] == "-":
            cursor = min(_spark_line_comment_end(sql, cursor), closer)
            continue
        if char == "/" and cursor + 1 < closer and sql[cursor + 1] == "*":
            cursor = min(_spark_block_comment_end(sql, cursor), closer)
            continue
        if _is_string_prefix_at(sql, cursor):
            cursor = min(_quoted_span_end(sql, cursor + 1, raw=True), closer)
            continue
        if char in "'\"`":
            cursor = min(_quoted_span_end(sql, cursor), closer)
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            piece = _lambda_group_piece(sql, piece_start, cursor)
            if piece is None:
                return None
            params.append(piece)
            piece_start = cursor + 1
        cursor += 1
    if not params:
        return None
    return params


def _lambda_single_param(sql: str, arrow: int) -> tuple[int, int, str] | None:
    index = arrow - 1
    while index >= 0 and sql[index] in " \t\n\r":
        index -= 1
    if index < 0:
        return None
    if sql[index] == "`":
        cursor = index - 1
        while cursor >= 0:
            if sql[cursor] != "`":
                cursor -= 1
                continue
            if cursor > 0 and sql[cursor - 1] == "`":
                cursor -= 2
                continue
            break
        if cursor < 0:
            return None
        spelling = sql[cursor + 1 : index].replace("``", "`")
        if not spelling or (cursor > 0 and (sql[cursor - 1] == "." or sql[cursor - 1] == "`")):
            return None
        return (cursor, index + 1, spelling)
    cursor = index
    while cursor >= 0 and (sql[cursor].isalnum() or sql[cursor] == "_"):
        cursor -= 1
    if cursor == index:
        return None
    if cursor >= 0 and sql[cursor] == ".":
        return None
    spelling = sql[cursor + 1 : index + 1]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", spelling):
        return None
    return (cursor + 1, index + 1, spelling)


def _lambda_params_before(
    sql: str, arrow: int, matched: dict[int, int]
) -> list[tuple[int, int, str]] | None:
    index = arrow - 1
    while index >= 0 and sql[index] in " \t\n\r":
        index -= 1
    if index >= 0 and sql[index] == ")":
        opener = matched.get(index)
        if opener is None:
            return None
        return _lambda_group_params(sql, opener, index)
    found = _lambda_single_param(sql, arrow)
    return [found] if found is not None else None


def _lambda_body_end(sql: str, start: int) -> int:
    index = start
    bound = len(sql)
    nested = 0
    while index < bound:
        char = sql[index]
        if char == "-" and index + 1 < bound and sql[index + 1] == "-":
            index = _spark_line_comment_end(sql, index)
            continue
        if char == "/" and index + 1 < bound and sql[index + 1] == "*":
            index = _spark_block_comment_end(sql, index)
            continue
        if _is_string_prefix_at(sql, index):
            index = _quoted_span_end(sql, index + 1, raw=True)
            continue
        if char in "'\"`":
            index = _quoted_span_end(sql, index)
            continue
        if char == "(":
            nested += 1
        elif char == ")":
            if nested == 0:
                return index
            nested -= 1
        elif char == "," and nested == 0:
            return index
        index += 1
    return bound


def _lambda_scopes(sql: str) -> tuple[list[tuple[int, int, list[str]]], dict[tuple[int, int], str]]:
    arrows: list[int] = []
    matched = _lambda_paren_match(sql, arrows)
    scopes: list[tuple[int, int, list[str]]] = []
    decls: dict[tuple[int, int], str] = {}
    for arrow in arrows:
        params = _lambda_params_before(sql, arrow, matched)
        if params is None:
            continue
        for start, end, spelling in params:
            decls[(start, end)] = spelling
        scopes.append(
            (arrow + 2, _lambda_body_end(sql, arrow + 2), [spelling for _, _, spelling in params])
        )
    return (scopes, decls)


def _scopes_have_folded_collision(scopes: list[tuple[int, int, list[str]]], exact: bool) -> bool:
    if exact:
        return False
    for outer_start, outer_end, outer_params in scopes:
        for inner_start, inner_end, inner_params in scopes:
            if (outer_start, outer_end) == (inner_start, inner_end):
                continue
            if not (outer_start <= inner_start and inner_end <= outer_end):
                continue
            for inner_param in inner_params:
                for outer_param in outer_params:
                    if inner_param == outer_param:
                        continue
                    if inner_param.casefold() == outer_param.casefold():
                        return True
    return False


def _scope_param_for(
    scopes: list[tuple[int, int, list[str]]],
    start: int,
    end: int,
    spelling: str,
    exact: bool,
) -> str | None:
    for body_start, body_end, params in reversed(scopes):
        if body_start <= start and end <= body_end:
            for param in params:
                if param == spelling or (not exact and param.casefold() == spelling.casefold()):
                    return param
    return None


def _main_path_dotted_token(
    parts: list[str], displays: list[str], engine_names: list[str], fold_map: dict[str, list[int]]
) -> str:
    from repark.spark._idents import quote_ident as _quote_ident

    quoted: list[str] = []
    for segment in parts:
        if segment.casefold() in _SQL_LITERAL_KEYWORDS:
            quoted.append(segment)
            continue
        positions = fold_map.get(segment.casefold(), [])
        if not positions:
            quoted.append(segment)
            continue
        if len(positions) > 1:
            echo = ", ".join(f"`{displays[position]}`" for position in positions)
            raise AnalysisException(
                f"[AMBIGUOUS_REFERENCE] Reference `{segment}` is ambiguous, could be: [{echo}]."
            )
        quoted.append(_quote_ident(engine_names[positions[0]]))
    return ".".join(quoted)


def _name_matches_head(head: str, names: list[str], exact: bool) -> bool:
    if exact:
        return head in names
    folded = head.casefold()
    return any(name.casefold() == folded for name in names)


def _frame_qualifiers_for_bind(frame: Any) -> dict[str, list[str]] | None:
    held = frame._frame_qualifiers
    if held is None:
        return None
    return {attr: sorted(names) for attr, names in held.items()}


def _displays_unique(displays: list[str]) -> bool:
    """Whether no two displays match under either case rule."""
    seen_exact = set()
    seen_folded = set()
    for display in displays:
        folded = display.lower()
        if display in seen_exact or folded in seen_folded:
            return False
        seen_exact.add(display)
        seen_folded.add(folded)
    return True


def _refuse_ambiguous_free_names(
    frame: Any,
    *,
    sql: str | None = None,
    column: Any | None = None,
    names: list[str] | None = None,
    select_item: bool = False,
    qualified_only: bool = False,
) -> None:
    """Refuse free names matching two displays that share no engine field."""
    displays = list(frame.columns)
    if _displays_unique(displays):
        return
    _native.refuse_ambiguous_free_names(
        frame._plan(),
        sql=sql,
        column=None if column is None else column._inner,
        names=names,
        select_item=select_item,
        qualified_only=qualified_only,
        displays=displays,
        exact=bool(_native.session_case_sensitive(frame._session)),
        frame_qualifiers=_frame_qualifiers_for_bind(frame),
    )


def _known_qualifiers(
    frame: Any, native: Any, exact: bool
) -> tuple[list[str], dict[str, list[str]] | None]:
    qualifiers = [held for held in _native.logical_column_qualifiers(native) if held]
    payload = _frame_qualifiers_for_bind(frame)
    if payload:
        seen = set(qualifiers) if exact else {name.casefold() for name in qualifiers}
        for extra in sorted({part for quals in payload.values() for part in quals}):
            key = extra if exact else extra.casefold()
            if key not in seen:
                qualifiers.append(extra)
                seen.add(key)
    return (qualifiers, payload)


def _lambda_quoted_span(
    span: str,
    scopes: list[tuple[int, int, list[str]]],
    decls: dict[tuple[int, int], str],
    base: int,
    exact: bool,
) -> str:
    from repark.spark._idents import quote_ident as _quote_ident

    if (base, base + len(span)) in decls:
        return span
    spelling = span[1:-1].replace("``", "`")
    param = _scope_param_for(scopes, base, base + len(span), spelling, exact)
    if param is None:
        return span
    return _quote_ident(param)


def _bind_filter_token(
    match: re.Match[str],
    *,
    native: Any,
    displays: list[str],
    engine_names: list[str],
    held: list[str | None],
    exact: bool,
    qualifiers: list[str],
    fold_map: dict[str, list[int]],
    scopes: list[tuple[int, int, list[str]]],
    decls: dict[tuple[int, int], str],
    base: int,
    collision: bool,
    frame_qualifiers: dict[str, list[str]] | None = None,
    operator_spans: frozenset[tuple[int, int]] = frozenset(),
) -> str:
    from repark.spark._idents import quote_ident as _quote_ident

    token = match.group(1)
    start = base + match.start(1)
    end = base + match.end(1)
    if (start, end) in decls:
        return _quote_ident(decls[(start, end)])
    parts = token.split(".")
    name = parts[-1]
    if len(parts) == 1:
        if name.casefold() in _SQL_LITERAL_KEYWORDS or (start, end) in operator_spans:
            return token
        param = _scope_param_for(scopes, start, end, name, exact)
        if param is not None:
            if collision and param != name:
                raise _FoldedLambdaFallbackError
            return _quote_ident(param)
        exact_hits, folded_hits = _unqualified_candidates(name, displays)
        candidates = exact_hits if exact else exact_hits + folded_hits
        if not candidates:
            return token
        status, hits = _group_candidates(candidates, held)
        if status == "bound":
            engine_field = engine_names[hits[0]]
            if len(candidates) > 1 and _native.join_dup_below_wrappers(native):
                candidates_echo = ", ".join(f"`{displays[position]}`" for position in candidates)
                detail = f"[AMBIGUOUS_REFERENCE] Reference `{token}` is ambiguous, "
                raise AnalysisException(f"{detail}could be: [{candidates_echo}].")
            if _native.engine_field_is_unique(native, engine_field):
                return _quote_ident(engine_field)
            return token
        if status == "missing":
            return token
        candidates_echo = ", ".join(f"`{displays[position]}`" for position in hits)
        detail = f"[AMBIGUOUS_REFERENCE] Reference `{token}` is ambiguous, "
        raise AnalysisException(f"{detail}could be: [{candidates_echo}].")
    head = parts[0]
    param = _scope_param_for(scopes, start, start + len(head), head, exact)
    if param is not None:
        if collision and param != head:
            raise _FoldedLambdaFallbackError
        from repark.spark._idents import escape_sql_single_quotes as _escape_quotes

        subscript = "".join(f"['{_escape_quotes(rest)}']" for rest in parts[1:])
        return _quote_ident(param) + subscript
    if not _name_matches_head(head, qualifiers, exact):
        if exact and not _name_matches_head(head, displays, exact):
            return token
        return _main_path_dotted_token(parts, displays, engine_names, fold_map)
    qualifier = ".".join(parts[:-1])
    status, hits, plan_quals = _native.resolve_display_name(
        native, name, qualifier, displays, exact, frame_qualifiers
    )
    if status == "bound":
        engine_field = engine_names[hits[0]]
        if len(hits) > 1 and _native.join_dup_below_wrappers(native):
            candidates_echo = ", ".join(
                f"`{qualifier}`.`{displays[position]}`" for position in hits
            )
            detail = f"[AMBIGUOUS_REFERENCE] Reference `{token}` is ambiguous, "
            raise AnalysisException(f"{detail}could be: [{candidates_echo}].")
        if _native.engine_field_is_unique(native, engine_field):
            return _quote_ident(engine_field)
        held_parts = plan_quals[0] if plan_quals else None
        if held_parts:
            return ".".join(
                [*(_quote_ident(part) for part in held_parts), _quote_ident(engine_field)]
            )
        return token
    if status == "ambiguous":
        candidates_echo = ", ".join(f"`{token}`" for _ in hits)
        detail = f"[AMBIGUOUS_REFERENCE] Reference `{token}` is ambiguous, "
        raise AnalysisException(f"{detail}could be: [{candidates_echo}].")
    if not exact and _name_matches_head(head, displays, exact):
        return _main_path_dotted_token(parts, displays, engine_names, fold_map)
    return token


def _bind_select_expr_dotted_token(
    match: re.Match[str],
    *,
    native: Any,
    displays: list[str],
    engine_names: list[str],
    exact: bool,
    qualifiers: list[str],
    payload: dict[str, list[str]] | None,
) -> str:
    from repark.spark._idents import quote_ident as _quote_ident

    token = match.group(1)
    parts = token.split(".")
    if not _name_matches_head(parts[0], qualifiers, exact):
        return token
    qualifier = ".".join(parts[:-1])
    status, hits, _plan_quals = _native.resolve_display_name(
        native, parts[-1], qualifier, displays, exact, payload
    )
    if status == "bound":
        engine_field = engine_names[hits[0]]
        if len(hits) > 1 and _native.join_dup_below_wrappers(native):
            candidates_echo = ", ".join(
                f"`{qualifier}`.`{displays[position]}`" for position in hits
            )
            detail = f"[AMBIGUOUS_REFERENCE] Reference `{token}` is ambiguous, "
            raise AnalysisException(f"{detail}could be: [{candidates_echo}].")
        if _native.engine_field_is_unique(native, engine_field):
            return _quote_ident(engine_field)
        return token
    if status == "ambiguous":
        candidates_echo = ", ".join(f"`{token}`" for _ in hits)
        detail = f"[AMBIGUOUS_REFERENCE] Reference `{token}` is ambiguous, "
        raise AnalysisException(f"{detail}could be: [{candidates_echo}].")
    return token


_DOTTED_TOKEN_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+)\b")


def _single_token_select_alias(
    native: Any,
    displays: list[str],
    engine_names: list[str],
    exact: bool,
    qualifiers: list[str],
    payload: dict[str, list[str]] | None,
    token: str,
) -> str | None:
    from repark.spark._idents import quote_ident as _quote_ident

    parts = token.split(".")
    if not _name_matches_head(parts[0], qualifiers, exact):
        return None
    qualifier = ".".join(parts[:-1])
    status, hits, _plan_quals = _native.resolve_display_name(
        native, parts[-1], qualifier, displays, exact, payload
    )
    if status != "bound":
        return None
    engine_field = engine_names[hits[0]]
    if len(hits) > 1 and _native.join_dup_below_wrappers(native):
        return None
    if not _native.engine_field_is_unique(native, engine_field):
        return None
    return f"{_quote_ident(engine_field)} AS {_quote_ident(parts[-1])}"


def _quote_select_expr_dotted(frame: Any, expr: str) -> str:
    native = frame._plan()
    displays = list(frame.columns)
    engine_names = list(_native.logical_column_names(native))
    if not displays or len(displays) != len(engine_names):
        return expr
    if None in list(_native.attribute_ids(native)):
        frame._inner = _native.stamp_attribute_ids(frame._inner)
        native = frame._plan()
        engine_names = list(_native.logical_column_names(native))
    exact = bool(_native.session_case_sensitive(frame._session))
    qualifiers, payload = _known_qualifiers(frame, native, exact)
    _refuse_ambiguous_free_names(frame, sql=expr, select_item=True)
    from repark.spark import qualified_names as _qualified_names

    held_here = list(_native.attribute_ids(native))
    _qualified_names._refuse_using_key_text(
        _qualified_names._using_mark(frame), held_here, expr, exact, False
    )
    single = _DOTTED_TOKEN_PATTERN.fullmatch(expr.strip())
    if single is not None:
        aliased = _single_token_select_alias(
            native, displays, engine_names, exact, qualifiers, payload, single.group(1)
        )
        if aliased is not None:
            return aliased
    binder = functools.partial(
        _bind_select_expr_dotted_token,
        native=native,
        displays=displays,
        engine_names=engine_names,
        exact=exact,
        qualifiers=qualifiers,
        payload=payload,
    )
    rebuilt: list[str] = []
    for kind, start, end in _iter_spark_sql_spans(expr):
        piece = expr[start:end]
        if kind == "code":
            rebuilt.append(_DOTTED_TOKEN_PATTERN.sub(binder, piece))
        else:
            rebuilt.append(piece)
    return "".join(rebuilt)


def _select_expr_frame(frame: Any, expr: tuple[str, ...]) -> Any:
    from repark.spark._temp_views import scratch_view_name

    frame._ensure_alive()
    if not expr:
        raise PySparkValueError("selectExpr requires at least one expression")
    for item in expr:
        if not isinstance(item, str):
            raise PySparkTypeError(f"selectExpr expressions must be str, got {type(item).__name__}")
    if len(expr) == 1 and expr[0].strip() == "*":
        return frame.select("*")
    view = scratch_view_name(frame._session, "__repark_selx_")
    frame._session.create_or_replace_temp_view(view, frame._plan())
    try:
        projection = ",\n".join(_quote_select_expr_dotted(frame, item) for item in expr)
        planned = frame._session.sql(f"SELECT {projection}\nFROM {view}")
        return frame._spawn(planned)
    finally:
        frame._session.drop_temp_view(view)


_MAIN_IDENT_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\()")


def _main_path_filter_token(
    match: re.Match[str],
    *,
    columns_by_fold: dict[str, list[str]],
    base: int = 0,
    operator_spans: frozenset[tuple[int, int]] = frozenset(),
) -> str:
    from repark.spark._idents import quote_ident as _quote_ident

    token = match.group(1)
    start = base + match.start(1)
    end = base + match.end(1)
    if token.casefold() in _SQL_LITERAL_KEYWORDS or (start, end) in operator_spans:
        return token
    matches = columns_by_fold.get(token.casefold())
    if matches is None:
        return token
    if len(matches) > 1:
        candidates = ", ".join(f"`{name}`" for name in matches)
        raise AnalysisException(
            f"[AMBIGUOUS_REFERENCE] Reference `{token}` is ambiguous, could be: [{candidates}]."
        )
    return _quote_ident(matches[0])


def _main_path_filter_sql(sql: str, displays: list[str]) -> str:
    columns_by_fold: dict[str, list[str]] = {}
    for column in displays:
        columns_by_fold.setdefault(column.casefold(), []).append(column)
    spans = _iter_spark_sql_spans(sql)
    has_bool = any(column.casefold() in _BOOL_OPERATOR_WORDS for column in displays)
    operator_spans = _boolean_operator_spans(sql, spans) if has_bool else frozenset()
    rebuilt: list[str] = []
    for kind, start, end in spans:
        piece = sql[start:end]
        if kind == "code":
            rebuilt.append(
                _MAIN_IDENT_PATTERN.sub(
                    functools.partial(
                        _main_path_filter_token,
                        columns_by_fold=columns_by_fold,
                        base=start,
                        operator_spans=operator_spans,
                    ),
                    piece,
                )
            )
        else:
            rebuilt.append(piece)
    return "".join(rebuilt)
