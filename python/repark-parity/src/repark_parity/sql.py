"""Generic SQL text helpers for the standalone parity harness."""

from __future__ import annotations


def escape_sql_single_quotes(value: str) -> str:
    """Escape a Generic SQL string body without adding quotes.

    Args:
        value: Text to escape.

    Returns:
        Text with each single quote doubled and all other characters unchanged.
    """
    return value.replace("'", "''")
