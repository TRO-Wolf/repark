"""Single-source SQL identifier quoting and path-escape needles (r23 QI1 / CQ-006/007).

Quote semantics by call-site class (behavior-preserving vs prior per-module copies):

* **Always-quote (Spark / DataFusion column + alias class)** — :func:`quote_ident`:
  every name becomes ``"…"`` with embedded ``"`` doubled. Used by session, dataframe,
  column, functions, ML, and polars (after bare-name validation).
* **Quote-if-needed (catalog / assign-target class)** — :func:`quote_ident_if_needed`:
  plain ``[A-Za-z_][A-Za-z0-9_]*`` stays unquoted; everything else is always-quoted.
* **Path-escape** — :func:`reject_path_escape_segment` mirrors the Rust shared needles in
  ``repark_write::idents`` (O3-C4-SEC-001 / C2-SEC-003 / C1-SEC-001).

PostgreSQL dialect quoting lives only in ``repark-postgres`` — never import this module
for PG SQL. Security: over-quote is acceptable; under-quote is not.

Live-Spark oracle (Q6): mid-night Zulu/pyspark unavailable → pin existing always-quote vs
quote-if-needed split (DF-dialect evidence for engine-side; no divergence-as-bug fix without
oracle). Probe tables lockstep with ``crates/repark-write/src/idents.rs::probes``.
"""

from __future__ import annotations

import re
from typing import Final

from repark.errors import PySparkValueError

# Plain unquoted SQL identifier (Spark/DF + catalog bare segment).
_PLAIN_IDENT: Final[re.Pattern[str]] = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

# ---------------------------------------------------------------------------
# Shared probe tables (lockstep with repark_write::idents::probes)
# ---------------------------------------------------------------------------

# Hostile / edge identifier strings for the Spark always-quote dialect.
# Harvested from r21 T5 PG probes (adapted) + 2026-08-03 audit.
INJECTION_PROBES: Final[tuple[str, ...]] = (
    r'"; DROP TABLE x; --',
    r'id"; DROP TABLE x; --',
    'na"me',
    "order",  # reserved word — must still quote under always-quote class
    "a b",
    "a.b",
    "",
)

# (segment, kind_tag) — kind_tag is "traversal" | "separator".
PATH_ESCAPE_PROBES: Final[tuple[tuple[str, str], ...]] = (
    (".", "traversal"),
    ("..", "traversal"),
    ("foo..bar", "traversal"),
    ("a/b", "separator"),
    (r"a\b", "separator"),
    ("../etc", "traversal"),
)

PATH_ESCAPE_SAFE: Final[tuple[str, ...]] = ("ok_table", "my_table", "t0", "Order")


def is_plain_ident(name: str) -> bool:
    """Return True when ``name`` is a plain unquoted SQL identifier segment."""
    return _PLAIN_IDENT.fullmatch(name) is not None


def quote_ident(name: str) -> str:
    """Always double-quote a SQL identifier (Spark/DF column + alias class).

    Embedded ``"`` are doubled. Empty names become ``\"\"`` (callers that refuse empty
    do so before quoting).
    """
    return '"' + name.replace('"', '""') + '"'


def quote_ident_if_needed(segment: str) -> str:
    """Quote only when ``segment`` is not a plain unquoted identifier (catalog class).

    Plain ``[A-Za-z_][A-Za-z0-9_]*`` stays unquoted (stable SHOW / catalog SQL form).
    All other segments go through :func:`quote_ident`.
    """
    if is_plain_ident(segment):
        return segment
    return quote_ident(segment)


def quote_column_sql_expr(name: str) -> str:
    """Double-quote a column reference for free-SQL embeds (octo C3-SEC-001).

    Unqualified names become ``\"x\"``. Dotted qualifiers (``source.name`` for MERGE)
    are quoted per segment as ``\"source\".\"name\"`` so a single identifier containing a
    literal dot is never produced.
    """
    if "." not in name:
        return quote_ident(name)
    return ".".join(quote_ident(segment) for segment in name.split("."))


def quote_multipart(parts: list[str] | tuple[str, ...], *, always: bool = False) -> str:
    """Join identifier segments with ``.``, quoting each per ``always``.

    When ``always`` is False (catalog default), uses :func:`quote_ident_if_needed`.
    When True, uses :func:`quote_ident`.
    """
    quoter = quote_ident if always else quote_ident_if_needed
    return ".".join(quoter(part) for part in parts)


def path_escape_kind(segment: str) -> str | None:
    """Classify a path-escape segment; return ``\"traversal\"``, ``\"separator\"``, or None.

    Needles match ``repark_write::idents::path_escape_kind`` byte-for-byte.
    """
    if segment == "." or segment == ".." or ".." in segment:
        return "traversal"
    if "/" in segment or "\\" in segment:
        return "separator"
    return None


def reject_path_escape_segment(segment: str) -> None:
    """Reject identifier segments that could escape a warehouse root (O3-C4-SEC-001).

    Mirrors the engine CTAS ``reject_path_escape_ident`` needles so writer / ``table`` /
    ``table_exists`` fail at the API boundary rather than only at path composition.
    """
    kind = path_escape_kind(segment)
    if kind == "traversal":
        raise PySparkValueError(
            f"identifier segment {segment!r} must not contain path traversal ('..')"
        )
    if kind == "separator":
        raise PySparkValueError(f"identifier segment {segment!r} must not contain path separators")


def assert_spark_injection_probe_is_single_token(probe: str) -> str:
    """Quote ``probe`` and assert it is a single double-quoted token (Spark dialect).

    Returns the quoted form. Used by the injection-probe battery and cross-lang pins.

    Mutation-proof (octo C1-Q-002 / C1-SEC-001): equality to an independent oracle
    (always-quote with ``"`` → ``""``) — a bare starts/ends-``"`` + undouble round-trip
    **false-passes** under-escape (forgetting to double embedded quotes), which leaves a
    mid-token ``"`` that closes the identifier early in SQL.
    """
    quoted = quote_ident(probe)
    # Independent oracle — do not derive expected solely from undoubling the result.
    expected = '"' + probe.replace('"', '""') + '"'
    if quoted != expected:
        raise AssertionError(
            f"under-quote/escape residual for {probe!r}: got {quoted!r}, want {expected!r}"
        )
    # Residual unpaired `"` inside the token (defense-in-depth if oracle formula drifts).
    inner = quoted[1:-1]
    if '"' in inner.replace('""', ""):
        raise AssertionError(f"unpaired quote inside token for {probe!r}: {quoted!r}")
    return quoted


__all__ = [
    "INJECTION_PROBES",
    "PATH_ESCAPE_PROBES",
    "PATH_ESCAPE_SAFE",
    "assert_spark_injection_probe_is_single_token",
    "is_plain_ident",
    "path_escape_kind",
    "quote_column_sql_expr",
    "quote_ident",
    "quote_ident_if_needed",
    "quote_multipart",
    "reject_path_escape_segment",
]
