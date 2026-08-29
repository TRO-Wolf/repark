"""SQL relation and table-name parsing."""

from __future__ import annotations

import re

from repark.spark._idents import is_plain_ident as _is_plain_ident

from repark.spark._idents import quote_ident as _quote_ident

from repark.spark._idents import reject_path_escape_segment as _reject_path_escape_segment

from repark.errors import PySparkValueError


_DROP_TABLE_SQL_RE = re.compile(r"(?is)^\s*DROP\s+TABLE\s+(IF\s+EXISTS\s+)?(.+?)\s*;?\s*$")


_INSERT_PREFIX_RE = re.compile(
    r"(?is)^\s*(INSERT\s+(?:OVERWRITE\s+(?:TABLE\s+)?|INTO\s+(?:TABLE\s+)?))"
)


_INSERT_DIRECTORY_HEAD_RE = re.compile(r"(?is)^(?:LOCAL\s+)?DIRECTORY\b")


_CREATE_TABLE_PREFIX_RE = re.compile(
    r"(?is)^\s*(CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)"
)


_CREATE_VIEW_SQL_RE = re.compile(
    r"(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+|TEMP\s+)?VIEW\b"
)


_CREATE_TEMP_TABLE_SQL_RE = re.compile(
    r"(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY|TEMP)\s+TABLE\b"
)


_MERGE_INTO_SQL_RE = re.compile(r"(?is)^\s*MERGE\s+INTO\s+(.+?)\s+USING\s+(.+?)\s+ON\b(.*)\s*$")


_UPDATE_PREFIX_RE = re.compile(r"(?is)^\s*(UPDATE\s+)")


_DELETE_FROM_PREFIX_RE = re.compile(r"(?is)^\s*(DELETE\s+FROM\s+)")


_SELECT_OR_WITH_HEAD_RE = re.compile(r"(?is)^\s*(?:WITH\b|SELECT\b)")


_FROM_JOIN_NON_TABLE = frozenset(
    {
        "LATERAL",
        "ONLY",
        "UNNEST",
        "VALUES",
        "SELECT",
        "WITH",
    }
)


_RELATION_FOLLOW_KEYWORDS = frozenset(
    {
        "WHERE",
        "GROUP",
        "HAVING",
        "ORDER",
        "LIMIT",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "JOIN",
        "ON",
        "USING",
        "LEFT",
        "RIGHT",
        "FULL",
        "INNER",
        "CROSS",
        "NATURAL",
        "OUTER",
        "SEMI",
        "ANTI",
        "WINDOW",
        "QUALIFY",
        "LATERAL",
        "AS",
        "TABLESAMPLE",
        "PIVOT",
        "UNPIVOT",
        "FOR",
        "CLUSTER",
        "DISTRIBUTE",
        "SORT",
        "TABLE",
        "FETCH",
        "OFFSET",
        "SETTINGS",  # engine-specific; never an alias
    }
)


def _sql_table_ref(table_name: str) -> str:
    """Validate a multipart table identifier and return a quoted SQL table reference.

    Accepts ``ident`` or ``ident.ident…`` with unquoted segments matching
    ``[A-Za-z_][A-Za-z0-9_]*``, double-quoted segments (``""`` escapes a quote; dots allowed
    inside quotes), or Spark-style backtick-quoted segments. Rejects SQL fragments so
    ``spark.table`` cannot be used as a FROM-clause injection surface.

    Does **not** apply default-catalog / default-namespace qualification — callers that need
    bare-name expansion must resolve first via :meth:`ReparkSession.resolve_table_name` /
    :meth:`ReparkSession._sql_table_ref_resolved`.
    """

    from repark.errors import AnalysisException

    name = table_name.strip()

    if not name:
        raise AnalysisException("table name must not be empty")

    try:
        segments = _parse_table_identifier_segments(name)

    except ValueError as error:
        raise AnalysisException(
            f"invalid table identifier {name[:128]!r}: {error} "
            "(expected multipart name like catalog.db.table; SQL fragments are not allowed)"
        ) from error

    return ".".join(_quote_ident(segment) for segment in segments)


def _sql_mask_strings_and_comments(query: str) -> str:
    """Return ``query`` with string literals and comments replaced by spaces.

    **Length and indices are preserved** so hit positions from a masked scan remain
    valid against the original body (registry-name scan). Handles
    single quotes (``''`` escape), double quotes (``""`` escape), backticks, ``--``
    line comments, and ``/* … */`` block comments. Does not interpret nested block
    comments (SQL standard single-level).
    """

    if not query:
        return query

    chars = list(query)

    length = len(query)

    index = 0

    while index < length:
        char = query[index]

        if char == "-" and index + 1 < length and query[index + 1] == "-":
            end = query.find("\n", index)

            if end < 0:
                for pos in range(index, length):
                    chars[pos] = " "

                break

            for pos in range(index, end):
                chars[pos] = " "

            index = end

            continue

        if char == "/" and index + 1 < length and query[index + 1] == "*":
            end = query.find("*/", index + 2)

            if end < 0:
                for pos in range(index, length):
                    chars[pos] = " "

                break

            for pos in range(index, end + 2):
                chars[pos] = " "

            index = end + 2

            continue

        if char in {"'", '"', "`"}:
            quote = char

            chars[index] = " "

            index += 1

            while index < length:
                current = query[index]

                chars[index] = " "

                index += 1

                if current == quote:
                    # SQL doubled-quote escape inside the same quote style.

                    if index < length and query[index] == quote:
                        chars[index] = " "

                        index += 1

                        continue

                    break

            continue

        index += 1

    return "".join(chars)


def _split_leading_sql_trivia(query: str) -> tuple[str, str]:
    """Split leading whitespace + SQL comments from ``query``.

    Returns ``(trivia, body)`` so statement-form classifiers see a clean head while the
    original leading trivia is re-prefixed onto the expanded body.
    """

    index = _skip_sql_ws_and_comments(query, 0)

    return query[:index], query[index:]


def _skip_sql_ws_and_comments(query: str, index: int) -> int:
    """Advance ``index`` past whitespace and ``--`` / ``/* */`` comments."""

    length = len(query)

    while index < length:
        char = query[index]

        if char.isspace():
            index += 1

            continue

        if char == "-" and index + 1 < length and query[index + 1] == "-":
            end = query.find("\n", index)

            if end < 0:
                return length

            index = end + 1

            continue

        if char == "/" and index + 1 < length and query[index + 1] == "*":
            end = query.find("*/", index + 2)

            if end < 0:
                return length

            index = end + 2

            continue

        break

    return index


def _find_matching_paren(query: str, open_index: int) -> int | None:
    """Return the index of the ``)`` matching ``query[open_index] == '('``, or None."""

    if open_index >= len(query) or query[open_index] != "(":
        return None

    depth = 0

    index = open_index

    length = len(query)

    while index < length:
        char = query[index]

        if char in {'"', "'", "`"}:
            quote = char

            index += 1

            while index < length:
                current = query[index]

                index += 1

                if current == quote:
                    if quote == '"' and index < length and query[index] == '"':
                        index += 1

                        continue

                    break

            continue

        if char == "(":
            depth += 1

        elif char == ")":
            depth -= 1

            if depth == 0:
                return index

        index += 1

    return None


def _collect_cte_names(query: str) -> set[str]:
    """Collect CTE names from a leading ``WITH name AS (…), …`` list (lowercase).

    Used so ``FROM cte`` is not rewritten to ``catalog.db.cte`` (time-travel CTE pin).
    """

    match = re.match(r"(?is)^\s*WITH\b", query)

    if match is None:
        return set()

    names: set[str] = set()

    index = match.end()

    length = len(query)

    while index < length:
        while index < length and query[index].isspace():
            index += 1

        if index < length and query[index : index + 9].upper() == "RECURSIVE":
            index += 9

            while index < length and query[index].isspace():
                index += 1

        # Optional RECURSIVE already skipped; read CTE name.

        name_end = _scan_sql_table_identifier_end(query, index)

        if name_end is None or name_end == index:
            break

        raw_name = query[index:name_end]

        # Only one-part CTE names are standard; take last segment lowercased.

        segment = raw_name.split(".")[-1].strip().strip('"').strip("`").lower()

        if segment:
            names.add(segment)

        index = name_end

        while index < length and query[index].isspace():
            index += 1

        # Optional column list (name (a, b) AS …)

        if index < length and query[index] == "(":
            close = _find_matching_paren(query, index)

            if close is None:
                break

            index = close + 1

            while index < length and query[index].isspace():
                index += 1

        if index + 2 <= length and query[index : index + 2].upper() == "AS":
            index += 2

        else:
            break

        while index < length and query[index].isspace():
            index += 1

        if index >= length or query[index] != "(":
            break

        close = _find_matching_paren(query, index)

        if close is None:
            break

        index = close + 1

        while index < length and query[index].isspace():
            index += 1

        if index < length and query[index] == ",":
            index += 1

            continue

        break

    return names


def _split_leading_table_ident(blob: str) -> tuple[str | None, str]:
    """Split ``blob`` into a leading table identifier and the remaining suffix (aliases).

    Returns ``(None, blob)`` when no identifier can be scanned (e.g. subquery ``(SELECT …)``
    — callers treat the whole blob as opaque). Used by MERGE INTO target/source expansion.
    """

    stripped = blob.strip()

    if not stripped:
        return None, blob

    if stripped.startswith("("):
        return stripped, ""

    end = _scan_sql_table_identifier_end(stripped, 0)

    if end is None or end == 0:
        return None, blob

    return stripped[:end], stripped[end:]


def _match_from_or_join_keyword(query: str, index: int) -> str | None:
    """If ``query[index:]`` starts with FROM/JOIN as a whole word, return that keyword.

    Word-boundary only: the char before ``index`` (if any) must be non-identifier, and the
    char after the keyword must be non-identifier (space, end, or punctuation). Case
    insensitive. Used by the free-SQL FROM/JOIN expander.
    """

    if index > 0:
        previous = query[index - 1]

        if previous.isalnum() or previous == "_":
            return None

    remaining = query[index:]

    for keyword in ("FROM", "JOIN"):
        if remaining[: len(keyword)].upper() != keyword:
            continue

        after = index + len(keyword)

        if after < len(query):
            next_char = query[after]

            if next_char.isalnum() or next_char == "_":
                continue

        return query[index:after]  # preserve original case

    return None


def _update_rest_has_set_clause(rest: str) -> bool:
    """True when ``rest`` after an UPDATE target still contains a SET keyword.

    Accepts optional alias forms (``AS a`` / bare ``a``) before SET. Used to refuse expanding
    ``UPDATE SET x = 1`` where the identifier scan ate the SET keyword as a table name.
    """

    stripped = rest.lstrip()

    if not stripped:
        return False

    # Optional alias: AS name | bare name, then SET.

    alias_match = re.match(
        r"(?is)^(?:(?:AS\s+)?(?:\"[^\"]+\"|`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)\s+)?SET\b",
        stripped,
    )

    return alias_match is not None


def _scan_sql_table_identifier_end(query: str, start: int) -> int | None:
    """Return the end index of a multipart table identifier starting at ``start``.

    Accepts unquoted ``[A-Za-z_][A-Za-z0-9_]*`` segments and double/backtick-quoted
    segments, joined by ``.``. Returns ``None`` when no identifier is present.
    """

    length = len(query)

    index = start

    if index >= length:
        return None

    saw_segment = False

    while index < length:
        char = query[index]

        if char in {'"', "`"}:
            quote = char

            index += 1

            closed = False

            while index < length:
                current = query[index]

                if current == quote:
                    if quote == '"' and index + 1 < length and query[index + 1] == '"':
                        index += 2

                        continue

                    index += 1

                    closed = True

                    break

                index += 1

            if not closed:
                return None

            saw_segment = True

        elif char.isalpha() or char == "_":
            index += 1

            while index < length and (query[index].isalnum() or query[index] == "_"):
                index += 1

            saw_segment = True

        else:
            break

        if index < length and query[index] == ".":
            index += 1

            # Require another segment after the dot (trailing '.' is not a table name).

            if index >= length:
                return None

            continue

        break

    if not saw_segment:
        return None

    return index


def _split_sql_table_name_list(names_blob: str) -> list[str]:
    """Split a comma-separated list of table identifiers with quote awareness."""

    parts: list[str] = []

    buf: list[str] = []

    quote: str | None = None

    index = 0

    while index < len(names_blob):
        character = names_blob[index]

        if quote is not None:
            buf.append(character)

            if character == quote:
                if quote == '"' and index + 1 < len(names_blob) and names_blob[index + 1] == '"':
                    buf.append(names_blob[index + 1])

                    index += 2

                    continue

                quote = None

            index += 1

            continue

        if character in {'"', "`"}:
            quote = character

            buf.append(character)

            index += 1

            continue

        if character == ",":
            part = "".join(buf).strip()

            if part:
                parts.append(part)

            buf = []

            index += 1

            continue

        buf.append(character)

        index += 1

    tail = "".join(buf).strip()

    if tail:
        parts.append(tail)

    return parts


def _parse_table_identifier_segments(name: str) -> list[str]:
    """Split a multipart table name with quote-awareness (``"…"`` / ```…```).

    Raises :class:`~repark.errors.PySparkValueError` on empty segments, trailing dots,
    unterminated quotes,
    unquoted non-identifier text (spaces, operators, etc.), or path-escape segments
    (``..`` / ``/`` / ``\\``).
    """

    segments: list[str] = []

    index = 0

    length = len(name)

    while index < length:
        char = name[index]

        if char in {'"', "`"}:
            quote = char

            index += 1

            buffer: list[str] = []

            closed = False

            while index < length:
                current = name[index]

                if current == quote:
                    if quote == '"' and index + 1 < length and name[index + 1] == '"':
                        buffer.append('"')

                        index += 2

                        continue

                    index += 1

                    closed = True

                    break

                buffer.append(current)

                index += 1

            if not closed:
                raise PySparkValueError("unterminated quoted identifier")

            inner = "".join(buffer)

            if not inner:
                raise PySparkValueError("empty quoted identifier")

            _reject_path_escape_segment(inner)

            segments.append(inner)

        else:
            start = index

            while index < length and name[index] != ".":
                index += 1

            segment = name[start:index]

            if not segment:
                raise PySparkValueError("empty identifier segment")

            if not _is_plain_ident(segment):
                raise PySparkValueError(f"invalid unquoted segment {segment[:64]!r}")

            _reject_path_escape_segment(segment)

            segments.append(segment)

        if index < length:
            if name[index] != ".":
                raise PySparkValueError("expected '.' between identifier segments")

            index += 1

            if index >= length:
                raise PySparkValueError("trailing '.'")

    if not segments:
        raise PySparkValueError("empty identifier")

    return segments
