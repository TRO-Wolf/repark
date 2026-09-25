"""Keep an ``INSERT INTO t REPLACE WHERE <predicate>`` head out of relation expansion."""

from __future__ import annotations

import re
from collections.abc import Callable

from repark.spark.session.sql_relations import _sql_mask_strings_and_comments

_REPLACE_WHERE_HEAD_RE = re.compile(r"(?is)^\s*REPLACE\s+WHERE\b")
_QUERY_START_RE = re.compile(r"(?i)\b(?:WITH|SELECT|VALUES|TABLE|FROM)\b")


def expand_insert_body(rest: str, expand: Callable[[str], str]) -> str:
    """Expand the relations of an INSERT body after its target name.

    Args:
        rest: The INSERT text that follows the target name.
        expand: The session expansion for a query body.

    Returns:
        ``rest`` with only its query expanded. A ``REPLACE WHERE`` predicate stays as
        written, so the query keeps its own ``WITH`` scope.
    """
    if _REPLACE_WHERE_HEAD_RE.match(rest) is None:
        return expand(rest)
    masked = _sql_mask_strings_and_comments(rest)
    for match in _QUERY_START_RE.finditer(masked):
        prefix = masked[: match.start()]
        if prefix.count("(") == prefix.count(")"):
            return rest[: match.start()] + expand(rest[match.start() :])
    return expand(rest)
