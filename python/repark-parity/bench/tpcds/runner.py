"""TPC-DS scoreboard runner: repark vs DuckDB over parquet tables.

Statuses: OK | WRONG-RESULT | ERROR | TIMEOUT | DIED (subprocess OOM/signal).
Wall times are median of ``repeats`` (default 3). Default timeout 120s per side;
on TIMEOUT, one 300s retry distinguishes slow from hung. D1 has no Iceberg leg.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import platform
import re
import resource
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from concurrent.futures import TimeoutError as FuturesTimeout
from pathlib import Path
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from repark.spark._idents import escape_sql_single_quotes

from .compare import compare_result_sets
from .datagen import TABLES, default_data_root, ensure_parquet_sf
from .queries import TpcdsQuery, load_queries

LOGGER = logging.getLogger(__name__)

StatusKind = Literal["OK", "WRONG-RESULT", "ERROR", "TIMEOUT", "DIED"]
IsolationKind = Literal["inprocess", "subprocess"]

KNOWN_STATUSES: Final[frozenset[str]] = frozenset(
    {"OK", "WRONG-RESULT", "ERROR", "TIMEOUT", "DIED"}
)

DEFAULT_TIMEOUT_S: Final[float] = 120.0
TIMEOUT_RETRY_S: Final[float] = 300.0
DEFAULT_REPEATS: Final[int] = 3
# SF1 TPC-DS parquet is multi-GB; refuse to start when free disk is too low.
SF1_MIN_FREE_DISK_GIB: Final[float] = 5.0
# Exit codes: 0 OK; 2 usage; 3 WRONG; 4 ERROR; 5 TIMEOUT; 6 DIED.
EXIT_DIED: Final[int] = 6
# Extra seconds beyond greylight budget so a finishing worker is not race-killed.
SUBPROCESS_HARD_TIMEOUT_GRACE_S: Final[float] = 30.0


class QueryResult(BaseModel):
    """Per-query scoreboard row."""

    model_config = ConfigDict(extra="forbid")

    query_nr: int
    # str, not Literal: dataclass stored unknown labels; status_ledger / exit_code
    # are the gates (BANANA must construct, then refuse a green exit).
    status: str
    repark_wall_s: float | None
    duckdb_wall_s: float | None
    ratio: float | None
    repark_rows: int | None
    duckdb_rows: int | None
    error_class: str | None = None
    error_message: str | None = None
    rewrite_note: str | None = None
    missing_feature_hint: str | None = None
    rss_peak_kb: int | None = None
    # Greylight: timeout_first_s = first-pass ceiling (usually 120).
    # timeout_retry_s dual use (disclosed in report tags):
    #   - error_class Slow → measured wall on the 300s retry
    #   - error_class Timeout (hung) → retry budget ceiling (not a measured wall)
    timeout_first_s: float | None = None
    timeout_retry_s: float | None = None
    ordered_compare: bool = False


class Scoreboard(BaseModel):
    """Full SF run matrix + environment disclosure."""

    model_config = ConfigDict(extra="forbid")

    scale_factor: float
    data_dir: str
    environment: dict[str, str]
    queries: list[QueryResult] = Field(default_factory=list)
    rewrites: list[dict[str, str]] = Field(default_factory=list)
    findings: list[str] = Field(default_factory=list)
    skipped: bool = False
    rss_peak_kb_max: int | None = None

    def to_json(self) -> str:
        payload = {
            "scale_factor": self.scale_factor,
            "data_dir": self.data_dir,
            "environment": self.environment,
            "queries": [query.model_dump() for query in self.queries],
            "rewrites": self.rewrites,
            "findings": self.findings,
            "skipped": self.skipped,
            "rss_peak_kb_max": self.rss_peak_kb_max,
        }
        return json.dumps(payload, indent=2, sort_keys=True)


def free_disk_gib(path: Path) -> float:
    """Free disk space in GiB for the filesystem hosting ``path`` (or its parent)."""
    probe = path if path.exists() else path.parent
    if not probe.exists():
        probe = Path("/")
    usage = shutil.disk_usage(probe)
    return usage.free / float(1024**3)


def sf_disk_gate(
    data_root: Path | None = None,
    *,
    min_free_gib: float = SF1_MIN_FREE_DISK_GIB,
) -> tuple[bool, float]:
    """Return ``(ok_to_run, free_gib)``. Free disk < min → skip (FINDING)."""
    root = (data_root if data_root is not None else default_data_root()).expanduser()
    free = free_disk_gib(root)
    return free >= min_free_gib, free


def query_result_to_dict(result: QueryResult) -> dict[str, Any]:
    """Serialize a :class:`QueryResult` for the subprocess worker."""
    return result.model_dump()


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        msg = f"expected float, got bool {value!r}"
        raise TypeError(msg)
    return float(value)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        msg = f"expected int, got bool {value!r}"
        raise TypeError(msg)
    return int(value)


def query_result_from_dict(payload: dict[str, Any]) -> QueryResult:
    """Deserialize a :class:`QueryResult` from the subprocess worker."""
    status_raw = str(payload["status"])
    if status_raw not in KNOWN_STATUSES:
        # Corrupt worker JSON must never look like success — force ERROR.
        return QueryResult(
            query_nr=int(payload.get("query_nr", -1)),
            status="ERROR",
            repark_wall_s=None,
            duckdb_wall_s=None,
            ratio=None,
            repark_rows=None,
            duckdb_rows=None,
            error_class="InvalidStatus",
            error_message=f"unknown status {status_raw!r} in worker payload",
            rewrite_note=payload.get("rewrite_note"),
            ordered_compare=bool(payload.get("ordered_compare", False)),
        )
    try:
        return QueryResult(
            query_nr=int(payload["query_nr"]),
            status=status_raw,  # type: ignore[arg-type]
            repark_wall_s=_optional_float(payload.get("repark_wall_s")),
            duckdb_wall_s=_optional_float(payload.get("duckdb_wall_s")),
            ratio=_optional_float(payload.get("ratio")),
            repark_rows=_optional_int(payload.get("repark_rows")),
            duckdb_rows=_optional_int(payload.get("duckdb_rows")),
            error_class=payload.get("error_class"),
            error_message=payload.get("error_message"),
            rewrite_note=payload.get("rewrite_note"),
            missing_feature_hint=payload.get("missing_feature_hint"),
            rss_peak_kb=_optional_int(payload.get("rss_peak_kb")),
            timeout_first_s=_optional_float(payload.get("timeout_first_s")),
            timeout_retry_s=_optional_float(payload.get("timeout_retry_s")),
            ordered_compare=bool(payload.get("ordered_compare", False)),
        )
    except (TypeError, ValueError) as exc:
        return QueryResult(
            query_nr=int(payload.get("query_nr", -1)),
            status="ERROR",
            repark_wall_s=None,
            duckdb_wall_s=None,
            ratio=None,
            repark_rows=None,
            duckdb_rows=None,
            error_class="InvalidPayload",
            error_message=f"worker payload field error: {exc}",
            rewrite_note=payload.get("rewrite_note"),
            ordered_compare=bool(payload.get("ordered_compare", False)),
        )


def run_scoreboard(
    *,
    scale_factor: float = 1.0,
    data_root: Path | None = None,
    repeats: int = DEFAULT_REPEATS,
    timeout_s: float | None = None,
    timeout_retry_s: float | None = None,
    query_filter: set[int] | None = None,
    isolation: IsolationKind | None = None,
    min_free_disk_gib: float | None = None,
) -> Scoreboard:
    """Run the full (or filtered) TPC-DS matrix for one scale factor.

    D1 behaviour:
    - Parquet temp views only (no Iceberg).
    - Default timeout 120s; on TIMEOUT, one retry at 300s (Slow vs hung).
    - SF >= 1: disk gate (default 5 GiB free) → SKIP FINDING, not hard-fail.
    - SF datagen OOM → SKIP FINDING + partial census.
    """
    resolved_timeout = timeout_s if timeout_s is not None else DEFAULT_TIMEOUT_S
    resolved_retry = timeout_retry_s if timeout_retry_s is not None else TIMEOUT_RETRY_S
    resolved_isolation: IsolationKind = isolation or "inprocess"
    findings: list[str] = []

    # Refuse invalid arguments before ANY datagen work — an empty filter must raise the
    # same ValueError whether or not the parquet cache (or duckdb itself) is present.
    if query_filter is not None and len(query_filter) == 0:
        msg = "query_filter is empty — refusing 0-query scoreboard (would exit 0 silently)"
        raise ValueError(msg)

    disk_min = (
        min_free_disk_gib
        if min_free_disk_gib is not None
        else (SF1_MIN_FREE_DISK_GIB if scale_factor >= 1.0 else 0.0)
    )
    if disk_min > 0:
        ok, free_gib = sf_disk_gate(data_root, min_free_gib=disk_min)
        if not ok:
            finding = (
                f"SF{scale_factor:g} SKIPPED: free disk {free_gib:.1f} GiB "
                f"< {disk_min:g} GiB hard cap (D1 SF1 disk-gate)"
            )
            LOGGER.warning(finding)
            return Scoreboard(
                scale_factor=scale_factor,
                data_dir="",
                environment={
                    "machine": platform.node(),
                    "platform": platform.platform(),
                    "python": platform.python_version(),
                    "scale_factor": str(scale_factor),
                    "skipped": "disk",
                    "free_disk_gib": f"{free_gib:.2f}",
                    "min_free_disk_gib": str(disk_min),
                    "storage": "parquet-not-Iceberg (D1)",
                    "isolation": resolved_isolation,
                    "timeout_s": str(resolved_timeout),
                    "timeout_retry_s": str(resolved_retry),
                    "repeats": str(repeats),
                },
                findings=[finding],
                skipped=True,
            )
        findings.append(f"SF{scale_factor:g} free disk {free_gib:.1f} GiB >= {disk_min:g} GiB")

    try:
        data_dir = ensure_parquet_sf(scale_factor, data_root=data_root)
    except (MemoryError, OSError) as exc:
        finding = (
            f"SF{scale_factor:g} SKIPPED: datagen OOM/disk failure "
            f"({type(exc).__name__}: {exc}) — partial census empty (D1)"
        )
        LOGGER.warning(finding)
        return Scoreboard(
            scale_factor=scale_factor,
            data_dir="",
            environment={
                "machine": platform.node(),
                "platform": platform.platform(),
                "python": platform.python_version(),
                "scale_factor": str(scale_factor),
                "skipped": "datagen",
                "storage": "parquet-not-Iceberg (D1)",
                "isolation": resolved_isolation,
                "timeout_s": str(resolved_timeout),
                "timeout_retry_s": str(resolved_retry),
                "repeats": str(repeats),
            },
            findings=[finding],
            skipped=True,
        )
    except Exception as exc:
        # DuckDB OOM often surfaces as RuntimeError / duckdb.OutOfMemoryException.
        message = f"{type(exc).__name__}: {exc}"
        lower = message.lower()
        if "out of memory" in lower or "oom" in lower or "cannot allocate" in lower:
            finding = (
                f"SF{scale_factor:g} SKIPPED: datagen OOM ({message}) — partial census empty (D1)"
            )
            LOGGER.warning(finding)
            return Scoreboard(
                scale_factor=scale_factor,
                data_dir="",
                environment={
                    "machine": platform.node(),
                    "platform": platform.platform(),
                    "python": platform.python_version(),
                    "scale_factor": str(scale_factor),
                    "skipped": "datagen_oom",
                    "storage": "parquet-not-Iceberg (D1)",
                    "isolation": resolved_isolation,
                    "timeout_s": str(resolved_timeout),
                    "timeout_retry_s": str(resolved_retry),
                    "repeats": str(repeats),
                },
                findings=[finding],
                skipped=True,
            )
        raise

    queries = load_queries()
    if query_filter is not None:
        if len(query_filter) == 0:
            msg = "query_filter is empty — refusing 0-query scoreboard (would exit 0 silently)"
            raise ValueError(msg)
        queries = [query for query in queries if query.query_nr in query_filter]
        if not queries:
            msg = (
                f"query_filter {sorted(query_filter)!r} matched no TPC-DS queries "
                "(valid range 1..99)"
            )
            raise ValueError(msg)

    storage_label = "parquet-not-Iceberg (D1; no Iceberg leg)"
    engine_label = (
        "single-node repark + DuckDB (subprocess per query)"
        if resolved_isolation == "subprocess"
        else "single-node repark + DuckDB same process"
    )

    if query_filter is not None:
        filter_label = ",".join(str(number) for number in sorted(query_filter))
        query_count_label = str(len(queries))
    else:
        filter_label = "all"
        query_count_label = str(len(queries))

    environment = {
        "machine": platform.node(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "scale_factor": str(scale_factor),
        "engine": engine_label,
        "storage": storage_label,
        "storage_kind": "parquet",
        "isolation": resolved_isolation,
        "timeout_s": str(resolved_timeout),
        "timeout_retry_s": str(resolved_retry),
        "repeats": str(repeats),
        "query_filter": filter_label,
        "query_count": query_count_label,
        "numeric_tolerance": (
            "1e-6 relative (non-integral floats only); ints and integral decimals exact"
        ),
        "query_provenance": "DuckDB tpcds extension tpcds_queries() (not TPC-spec vendored)",
        "compare_order": "ordered when ORDER BY present; else multiset (sort)",
    }

    board = Scoreboard(
        scale_factor=scale_factor,
        data_dir=str(data_dir),
        environment=environment,
        rewrites=[
            {
                "query_nr": str(query.query_nr),
                "note": query.rewrite_note or "",
            }
            for query in queries
            if query.rewrite_note
        ],
        findings=findings,
    )

    if resolved_isolation == "subprocess":
        for query in queries:
            LOGGER.info("TPC-DS Q%02d (subprocess) …", query.query_nr)
            result = _run_one_query_subprocess(
                data_dir=data_dir,
                query=query,
                repeats=repeats,
                timeout_s=resolved_timeout,
                timeout_retry_s=resolved_retry,
            )
            board.queries.append(result)
            _track_rss_peak(board, result)
        return board

    duckdb_conn = _open_duckdb_over_parquet(data_dir)
    spark = _open_repark_over_parquet(data_dir)
    try:
        for query in queries:
            LOGGER.info("TPC-DS Q%02d …", query.query_nr)
            result = _run_one_query(
                spark=spark,
                duckdb_conn=duckdb_conn,
                query=query,
                repeats=repeats,
                timeout_s=resolved_timeout,
                timeout_retry_s=resolved_retry,
            )
            result.rss_peak_kb = _max_rss_kb()
            board.queries.append(result)
            _track_rss_peak(board, result)
    finally:
        try:
            spark.stop()
        except Exception:
            LOGGER.exception("repark session stop failed")
        try:
            duckdb_conn.close()
        except Exception:
            LOGGER.exception("duckdb close failed")

    return board


def _track_rss_peak(board: Scoreboard, result: QueryResult) -> None:
    if result.rss_peak_kb is None:
        return
    if board.rss_peak_kb_max is None or result.rss_peak_kb > board.rss_peak_kb_max:
        board.rss_peak_kb_max = result.rss_peak_kb


def _max_rss_kb() -> int:
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def _error_needle_matches(needle: str, lower_message: str) -> bool:
    """Match error-class needles without false-prefix hits.

    Bare alphanumeric tokens use word boundaries so ``except`` does **not** match
    ``exception`` / ``PySparkException`` (census-critical). Needles that already
    contain spaces or punctuation keep substring match (e.g. ``over (``, ``like ``).
    """
    if re.search(r"[^a-z0-9_]", needle):
        return needle in lower_message
    return re.search(rf"(?<![a-z0-9_]){re.escape(needle)}(?![a-z0-9_])", lower_message) is not None


def classify_error(message: str) -> tuple[str, str | None]:
    """Map an exception string to (error_class, missing_feature_hint)."""
    lower = message.lower()
    # Order matters — more specific first. TPC-DS-heavy constructs first.
    patterns: list[tuple[str, str, str | None]] = [
        ("unsupportedoperationexception", "UnsupportedOperationException", None),
        ("not implemented", "NotImplemented", None),
        ("rollup", "Rollup", "ROLLUP"),
        ("cube", "Cube", "CUBE"),
        ("grouping sets", "GroupingSets", "GROUPING SETS"),
        ("grouping(", "GroupingSets", "GROUPING SETS"),
        ("grouping_id", "GroupingSets", "GROUPING SETS"),
        ("interval", "IntervalLiteral", "interval literal syntax"),
        ("extract(", "Extract", "EXTRACT / date_part"),
        ("date_part", "Extract", "EXTRACT / date_part"),
        ("date_trunc", "DateTrunc", "date_trunc"),
        ("substring", "Substring", "substring"),
        ("like ", "Like", "LIKE / ILIKE"),
        ("rlike", "RLike", "RLIKE / regexp"),
        ("regexp", "Regexp", "regexp"),
        ("exists", "ExistsSubquery", "EXISTS correlated subquery"),
        ("correlated", "CorrelatedSubquery", "correlated subquery"),
        ("lateral", "Lateral", "LATERAL join / lateral view"),
        ("in (", "InListOrSubquery", "IN subquery"),
        ("case when", "CaseExpr", "CASE expression"),
        ("window", "Window", "window function"),
        ("over (", "Window", "window function"),
        ("rank(", "Window", "window function"),
        ("dense_rank", "Window", "window function"),
        ("row_number", "Window", "window function"),
        ("percentile", "Percentile", "percentile / approx_percentile"),
        ("stddev", "Stddev", "stddev aggregate"),
        ("variance", "Variance", "variance aggregate"),
        ("covar", "Covariance", "covariance aggregate"),
        ("corr(", "Correlation", "corr aggregate"),
        # Set ops: word-boundary via _error_needle_matches (except ⊄ exception).
        ("intersect", "SetOp", "INTERSECT"),
        ("except", "SetOp", "EXCEPT"),
        ("union", "SetOp", "UNION"),
        ("with ", "Cte", "WITH / CTE"),
        ("recursive", "RecursiveCte", "recursive CTE"),
        ("view or table", "MissingRelation", "temp view / table registration"),
        ("schema", "Schema", "schema / type"),
        ("type", "TypeError", "type coercion"),
        ("syntax", "Syntax", "SQL dialect / parser"),
        ("parse", "Parse", "SQL parser"),
        ("analyzer", "Analyzer", "query analyzer"),
        ("planner", "Planner", "query planner"),
        ("timeout", "Timeout", None),
    ]
    for needle, error_class, hint in patterns:
        if _error_needle_matches(needle, lower):
            return error_class, hint
    # Truncate noisy messages for the class label
    short = message.strip().split("\n", maxsplit=1)[0][:80]
    return f"Other({short})", None


def gap_census(board: Scoreboard) -> list[tuple[str, int, list[int]]]:
    """Rank missing features / error classes by how many queries they block."""
    buckets: dict[str, list[int]] = {}
    for query in board.queries:
        if query.status == "OK":
            continue
        key = query.missing_feature_hint or query.error_class or query.status
        buckets.setdefault(key, []).append(query.query_nr)
    ranked = sorted(buckets.items(), key=lambda item: (-len(item[1]), item[0]))
    return [(name, len(numbers), numbers) for name, numbers in ranked]


def exit_code_for_board(board: Scoreboard) -> int:
    """Map scoreboard statuses to CLI exit codes.

    0 = all OK (or skipped-disk/datagen with no queries); 3 = any WRONG-RESULT;
    4 = any ERROR (no WRONG); 6 = any DIED (outranks TIMEOUT — process death);
    5 = any TIMEOUT only (includes Slow class). Skipped boards return 0
    (measurement FINDING, never hard-fail the datagen gate).
    Unknown statuses are ERROR-class (exit 4) — never silent 0.
    """
    if board.skipped and not board.queries:
        return 0
    if any(query.status not in KNOWN_STATUSES for query in board.queries):
        return 4
    if any(query.status == "WRONG-RESULT" for query in board.queries):
        return 3
    if any(query.status == "ERROR" for query in board.queries):
        return 4
    # DIED outranks TIMEOUT: a killed worker is worse than a soft wall overrun.
    if any(query.status == "DIED" for query in board.queries):
        return EXIT_DIED
    if any(query.status == "TIMEOUT" for query in board.queries):
        return 5
    return 0


def render_markdown_report(board: Scoreboard, *, title: str | None = None) -> str:
    """Render the scoreboard + gap census as Markdown (V1 report format)."""
    header = (
        title
        or f"TPC-DS scoreboard SF{board.scale_factor} — {board.environment.get('machine', '')}"
    )
    lines: list[str] = [
        f"# {header}",
        "",
        "## Environment",
        "",
    ]
    for key, value in board.environment.items():
        lines.append(f"- **{key}**: {value}")
    lines.append(f"- **data_dir**: `{board.data_dir}`")
    if board.rss_peak_kb_max is not None:
        lines.append(f"- **rss_peak_kb_max**: {board.rss_peak_kb_max}")
    if board.findings:
        lines.extend(["", "## Findings", ""])
        for finding in board.findings:
            lines.append(f"- {finding}")
    if board.skipped:
        lines.extend(
            [
                "",
                "## Skipped",
                "",
                "Scoreboard did not run queries (see Findings).",
                "",
            ]
        )
        return "\n".join(lines)

    lines.extend(["", "## Per-query matrix", ""])
    lines.append(
        "| Q | status | repark_s | duckdb_s | ratio | rows_r | rows_d | "
        "rss_kb | ordered | error_class / hint |"
    )
    lines.append("|---:|---|---:|---:|---:|---:|---:|---:|---|---|")
    for query in board.queries:
        ratio = f"{query.ratio:.2f}" if query.ratio is not None else "—"
        repark_s = f"{query.repark_wall_s:.3f}" if query.repark_wall_s is not None else "—"
        duck_s = f"{query.duckdb_wall_s:.3f}" if query.duckdb_wall_s is not None else "—"
        rows_r = str(query.repark_rows) if query.repark_rows is not None else "—"
        rows_d = str(query.duckdb_rows) if query.duckdb_rows is not None else "—"
        rss = str(query.rss_peak_kb) if query.rss_peak_kb is not None else "—"
        ordered = "Y" if query.ordered_compare else "N"
        hint = query.missing_feature_hint or query.error_class or ""
        if query.error_message and query.status != "OK":
            short_err = query.error_message.split("\n", maxsplit=1)[0][:60]
            if short_err not in hint:
                hint = f"{hint}: {short_err}".strip(": ")
        if query.timeout_first_s is not None or query.timeout_retry_s is not None:
            first = f"{query.timeout_first_s:.1f}" if query.timeout_first_s is not None else "—"
            retry = f"{query.timeout_retry_s:.1f}" if query.timeout_retry_s is not None else "—"
            # Dual storage: Slow records measured retry wall; hung records budget ceiling.
            retry_tag = "t300_wall" if (query.error_class or "") == "Slow" else "t300_budget"
            hint = f"{hint} [t120={first}s {retry_tag}={retry}s]".strip()
        lines.append(
            f"| {query.query_nr} | {query.status} | {repark_s} | {duck_s} | {ratio} | "
            f"{rows_r} | {rows_d} | {rss} | {ordered} | {hint} |"
        )

    lines.extend(["", "## Gap census (ranked by queries blocked)", ""])
    census = gap_census(board)
    if not census:
        lines.append("_No gaps — all queries OK (or empty matrix)._")
    else:
        lines.append("| feature / class | queries blocked | query numbers |")
        lines.append("|---|---:|---|")
        for name, count, numbers in census:
            nums = ", ".join(str(n) for n in numbers)
            lines.append(f"| {name} | {count} | {nums} |")

    ok = sum(1 for query in board.queries if query.status == "OK")
    wrong = sum(1 for query in board.queries if query.status == "WRONG-RESULT")
    err = sum(1 for query in board.queries if query.status == "ERROR")
    timeout = sum(1 for query in board.queries if query.status == "TIMEOUT")
    slow = sum(
        1
        for query in board.queries
        if query.status == "TIMEOUT" and (query.error_class or "") == "Slow"
    )
    hung = timeout - slow
    died = sum(1 for query in board.queries if query.status == "DIED")
    lines.extend(
        [
            "",
            "## Summary counts",
            "",
            f"- OK: **{ok}**",
            f"- WRONG-RESULT: **{wrong}**",
            f"- ERROR: **{err}**",
            f"- TIMEOUT: **{timeout}** (Slow={slow}, hung={hung})",
            f"- DIED: **{died}**",
            f"- Total: **{len(board.queries)}**",
            "",
        ]
    )

    if board.rewrites:
        lines.extend(["## Dialect rewrites (disclosed)", ""])
        for rewrite in board.rewrites:
            lines.append(f"- Q{rewrite['query_nr']}: {rewrite['note']}")
        lines.append("")

    lines.extend(
        [
            "## Disclosure",
            "",
            "- Oracle = DuckDB result set via `tpcds` extension (`tpcds_queries()` provenance).",
            "- Non-integral floats: relative tolerance 1e-6; "
            "ints and integral-valued decimals **exact**.",
            "- Row order: multiset (sort) unless query has ORDER BY (then ordered).",
            "- Parquet temp views only — **no** Iceberg leg in D1.",
            "- Single-node; wall = median of configured repeats; "
            "timeout via SIGALRM (Unix; best-effort under native code).",
            "- TIMEOUT at 120s → one retry at 300s: **Slow** if completes, hung if not "
            "(both numbers recorded).",
            "- SF1 disk/OOM datagen gate → SKIP FINDING (exit 0), never silent narrowing.",
            "- RSS peaks recorded (Linux ru_maxrss KiB); **no** RSS auto-abort.",
            "",
        ]
    )
    return "\n".join(lines)


def status_ledger(
    board: Scoreboard,
    *,
    expect_query_count: int | None = None,
) -> dict[str, Any]:
    """Status map for the SF1 smoke pin file (includes provenance metadata).

    Raises ``ValueError`` when any status is outside :data:`KNOWN_STATUSES` so a
    partial/corrupt board cannot overwrite the smoke pin file with garbage labels.
    When ``expect_query_count`` is set (CLI full SF1 ledger writes use 99), also
    refuses the wrong cardinality so a ``--queries`` subset cannot clobber the pin.
    """
    queries: dict[str, dict[str, str]] = {}
    for query in board.queries:
        if query.status not in KNOWN_STATUSES:
            msg = (
                f"status_ledger: Q{query.query_nr} has unknown status "
                f"{query.status!r} (allowed: {sorted(KNOWN_STATUSES)})"
            )
            raise ValueError(msg)
        entry: dict[str, str] = {"status": query.status}
        if query.error_class:
            entry["error_class"] = query.error_class
        if query.missing_feature_hint:
            entry["missing_feature_hint"] = query.missing_feature_hint
        if query.ordered_compare:
            entry["ordered_compare"] = "true"
        queries[str(query.query_nr)] = entry
    if expect_query_count is not None and len(queries) != expect_query_count:
        msg = (
            f"status_ledger: expected {expect_query_count} queries, got {len(queries)}; "
            "refusing to write a partial/over-full smoke pin"
        )
        raise ValueError(msg)
    return {
        "scale_factor": board.scale_factor,
        "numeric_tolerance": "1e-6 relative (floats/decimals only; ints exact)",
        "query_provenance": "DuckDB tpcds extension tpcds_queries()",
        "query_count": len(queries),
        "queries": queries,
    }


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _open_duckdb_over_parquet(data_dir: Path) -> Any:
    import duckdb

    connection = duckdb.connect(database=":memory:")
    for table_name in TABLES:
        path = data_dir / f"{table_name}.parquet"
        path_sql = escape_sql_single_quotes(str(path))
        connection.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{path_sql}')"
        )
    return connection


def _open_repark_over_parquet(data_dir: Path) -> Any:
    """Open repark with TPC-DS tables as short-name temp views."""
    from repark import ReparkSession

    spark = ReparkSession.builder.appName(f"tpcds-{data_dir.name}").getOrCreate()
    for table_name in TABLES:
        path = data_dir / f"{table_name}.parquet"
        spark.read.parquet(str(path)).createOrReplaceTempView(table_name)
    return spark


def subprocess_hard_timeout_s(
    timeout_s: float,
    timeout_retry_s: float,
    repeats: int,
    *,
    setup_budget_s: float = 90.0,
    grace_s: float = SUBPROCESS_HARD_TIMEOUT_GRACE_S,
) -> float:
    """Parent hard wall for one subprocess worker.

    Must cover the greylight path on **both** sides (DuckDB + repark): each side
    may spend ``repeats * timeout_s`` then one ``timeout_retry_s`` retry. Clamping
    below this budget misclassifies Slow/TIMEOUT as DIED WorkerTimeout. A small
    ``grace_s`` avoids racing a worker that finishes at the greylight ceiling.
    """
    per_side = float(timeout_s) * float(repeats) + float(timeout_retry_s)
    return per_side * 2.0 + float(setup_budget_s) + float(grace_s)


def _kill_worker_process_group(process: subprocess.Popen[str]) -> None:
    """SIGKILL the worker session (start_new_session) so native children do not orphan."""
    pid = process.pid
    if pid is None:
        return
    with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
        os.killpg(pid, signal.SIGKILL)
    with contextlib.suppress(ProcessLookupError):
        process.kill()


def _run_one_query_subprocess(
    *,
    data_dir: Path,
    query: TpcdsQuery,
    repeats: int,
    timeout_s: float,
    timeout_retry_s: float,
) -> QueryResult:
    """Run one query in a child process; map signal deaths to DIED."""
    worker = Path(__file__).resolve().parent / "query_worker.py"
    # Hard ceiling so a wedged child cannot block the scoreboard forever.
    # Native DataFusion can ignore SIGALRM; parent kills the worker → DIED.
    # Budget must include greylight (120 * repeats + 300) * both engines + setup.
    hard_timeout_s = subprocess_hard_timeout_s(timeout_s, timeout_retry_s, repeats)

    with tempfile.TemporaryDirectory(prefix="tpcds-qworker-") as tmp:
        tmp_path = Path(tmp)
        config_path = tmp_path / "config.json"
        result_path = tmp_path / "result.json"
        config: dict[str, Any] = {
            "data_dir": str(data_dir),
            "query_nr": query.query_nr,
            "original_sql": query.original_sql,
            "sql_for_repark": query.sql_for_repark,
            "rewrite_note": query.rewrite_note,
            "repeats": repeats,
            "timeout_s": timeout_s,
            "timeout_retry_s": timeout_retry_s,
            "result_path": str(result_path),
        }
        config_path.write_text(json.dumps(config), encoding="utf-8")
        process = subprocess.Popen(
            [sys.executable, str(worker), str(config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            stdout, stderr = process.communicate(timeout=hard_timeout_s)
        except subprocess.TimeoutExpired:
            _kill_worker_process_group(process)
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.communicate(timeout=10.0)
            return QueryResult(
                query_nr=query.query_nr,
                status="DIED",
                repark_wall_s=None,
                duckdb_wall_s=None,
                ratio=None,
                repark_rows=None,
                duckdb_rows=None,
                error_class="WorkerTimeout",
                error_message=f"query worker exceeded hard wall {hard_timeout_s:.0f}s",
                rewrite_note=query.rewrite_note,
                ordered_compare=query.is_ordered,
            )

        returncode = process.returncode if process.returncode is not None else -1
        if returncode < 0:
            sig = -returncode
            return QueryResult(
                query_nr=query.query_nr,
                status="DIED",
                repark_wall_s=None,
                duckdb_wall_s=None,
                ratio=None,
                repark_rows=None,
                duckdb_rows=None,
                error_class="Signal",
                error_message=f"query worker killed by signal {sig}",
                rewrite_note=query.rewrite_note,
                ordered_compare=query.is_ordered,
            )
        # Linux OOM / shell sometimes surfaces as 137 (128+SIGKILL).
        if returncode in (137, 9):
            return QueryResult(
                query_nr=query.query_nr,
                status="DIED",
                repark_wall_s=None,
                duckdb_wall_s=None,
                ratio=None,
                repark_rows=None,
                duckdb_rows=None,
                error_class="OOMOrKill",
                error_message=f"query worker exit {returncode}",
                rewrite_note=query.rewrite_note,
                ordered_compare=query.is_ordered,
            )
        if returncode != 0:
            stderr_tail = (stderr or stdout or "")[-500:]
            return QueryResult(
                query_nr=query.query_nr,
                status="ERROR",
                repark_wall_s=None,
                duckdb_wall_s=None,
                ratio=None,
                repark_rows=None,
                duckdb_rows=None,
                error_class="WorkerError",
                error_message=f"worker exit {returncode}: {stderr_tail}",
                rewrite_note=query.rewrite_note,
                ordered_compare=query.is_ordered,
            )
        if not result_path.is_file():
            return QueryResult(
                query_nr=query.query_nr,
                status="ERROR",
                repark_wall_s=None,
                duckdb_wall_s=None,
                ratio=None,
                repark_rows=None,
                duckdb_rows=None,
                error_class="WorkerError",
                error_message="worker exited 0 but wrote no result.json",
                rewrite_note=query.rewrite_note,
                ordered_compare=query.is_ordered,
            )
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        return query_result_from_dict(payload)


def _run_one_query(
    *,
    spark: Any,
    duckdb_conn: Any,
    query: TpcdsQuery,
    repeats: int,
    timeout_s: float,
    timeout_retry_s: float,
) -> QueryResult:
    ordered = query.is_ordered

    # DuckDB first (oracle) — uses original SQL.
    duck_times: list[float] = []
    duck_payloads: list[list[tuple[Any, ...]]] = []
    duck_error: str | None = None
    duck_timed_out = False
    for _ in range(repeats):
        try:
            wall, rows = _timed_call(
                lambda: _duckdb_collect(duckdb_conn, query.original_sql),
                timeout_s=timeout_s,
            )
            duck_times.append(wall)
            duck_payloads.append(rows)
        except FuturesTimeout:
            if duck_payloads:
                break
            duck_timed_out = True
            continue
        except Exception as exc:
            if duck_payloads:
                break
            duck_error = f"{type(exc).__name__}: {exc}"
            break

    if not duck_payloads:
        if duck_error is not None:
            return QueryResult(
                query_nr=query.query_nr,
                status="ERROR",
                repark_wall_s=None,
                duckdb_wall_s=None,
                ratio=None,
                repark_rows=None,
                duckdb_rows=None,
                error_class="DuckDBOracleError",
                error_message=duck_error,
                rewrite_note=query.rewrite_note,
                ordered_compare=ordered,
            )
        if duck_timed_out:
            # One 300s retry for the oracle so hung-oracle is distinguished from Slow.
            try:
                wall, rows = _timed_call(
                    lambda: _duckdb_collect(duckdb_conn, query.original_sql),
                    timeout_s=timeout_retry_s,
                )
                duck_times.append(wall)
                duck_payloads.append(rows)
            except FuturesTimeout:
                return QueryResult(
                    query_nr=query.query_nr,
                    status="TIMEOUT",
                    repark_wall_s=None,
                    duckdb_wall_s=None,
                    ratio=None,
                    repark_rows=None,
                    duckdb_rows=None,
                    error_class="Timeout",
                    error_message=(f"DuckDB exceeded {timeout_s}s and {timeout_retry_s}s retry"),
                    rewrite_note=query.rewrite_note,
                    timeout_first_s=timeout_s,
                    timeout_retry_s=timeout_retry_s,
                    ordered_compare=ordered,
                )
            except Exception as exc:
                return QueryResult(
                    query_nr=query.query_nr,
                    status="ERROR",
                    repark_wall_s=None,
                    duckdb_wall_s=None,
                    ratio=None,
                    repark_rows=None,
                    duckdb_rows=None,
                    error_class="DuckDBOracleError",
                    error_message=f"{type(exc).__name__}: {exc}",
                    rewrite_note=query.rewrite_note,
                    ordered_compare=ordered,
                )
        if not duck_payloads:
            return QueryResult(
                query_nr=query.query_nr,
                status="ERROR",
                repark_wall_s=None,
                duckdb_wall_s=None,
                ratio=None,
                repark_rows=None,
                duckdb_rows=None,
                error_class="DuckDBOracleError",
                error_message="no duckdb rows",
                rewrite_note=query.rewrite_note,
                ordered_compare=ordered,
            )

    duck_rows = duck_payloads[-1]
    duck_median = statistics.median(duck_times)

    # repark — uses possibly rewritten SQL.
    repark_times: list[float] = []
    repark_payloads: list[list[tuple[Any, ...]]] = []
    repark_error: str | None = None
    repark_timed_out = False
    for _ in range(repeats):
        try:
            wall, rows = _timed_call(
                lambda: _repark_collect(spark, query.sql_for_repark),
                timeout_s=timeout_s,
            )
            repark_times.append(wall)
            repark_payloads.append(rows)
        except FuturesTimeout:
            if repark_payloads:
                break
            repark_timed_out = True
            continue
        except Exception as exc:
            if repark_payloads:
                break
            repark_error = f"{type(exc).__name__}: {exc}"
            break

    timeout_first_s: float | None = None
    timeout_retry_wall: float | None = None

    if not repark_payloads and repark_timed_out and repark_error is None:
        # Greylight refinement: one retry at 300s after 120s TIMEOUT.
        timeout_first_s = timeout_s
        try:
            wall, rows = _timed_call(
                lambda: _repark_collect(spark, query.sql_for_repark),
                timeout_s=timeout_retry_s,
            )
            repark_times.append(wall)
            repark_payloads.append(rows)
            timeout_retry_wall = wall
            # Completed on retry → Slow (TIMEOUT status, class Slow) after compare.
        except FuturesTimeout:
            return QueryResult(
                query_nr=query.query_nr,
                status="TIMEOUT",
                repark_wall_s=None,
                duckdb_wall_s=duck_median,
                ratio=None,
                repark_rows=None,
                duckdb_rows=len(duck_rows),
                error_class="Timeout",
                error_message=(f"repark exceeded {timeout_s}s and {timeout_retry_s}s retry (hung)"),
                rewrite_note=query.rewrite_note,
                timeout_first_s=timeout_first_s,
                timeout_retry_s=timeout_retry_s,
                ordered_compare=ordered,
            )
        except Exception as exc:
            repark_error = f"{type(exc).__name__}: {exc}"

    if not repark_payloads:
        if repark_error is not None:
            error_class, hint = classify_error(repark_error)
            return QueryResult(
                query_nr=query.query_nr,
                status="ERROR",
                repark_wall_s=None,
                duckdb_wall_s=duck_median,
                ratio=None,
                repark_rows=None,
                duckdb_rows=len(duck_rows),
                error_class=error_class,
                error_message=repark_error,
                rewrite_note=query.rewrite_note,
                missing_feature_hint=hint,
                timeout_first_s=timeout_first_s,
                timeout_retry_s=timeout_retry_wall,
                ordered_compare=ordered,
            )
        if repark_timed_out:
            return QueryResult(
                query_nr=query.query_nr,
                status="TIMEOUT",
                repark_wall_s=None,
                duckdb_wall_s=duck_median,
                ratio=None,
                repark_rows=None,
                duckdb_rows=len(duck_rows),
                error_class="Timeout",
                error_message=f"repark exceeded {timeout_s}s on all attempts",
                rewrite_note=query.rewrite_note,
                timeout_first_s=timeout_first_s,
                ordered_compare=ordered,
            )
        error_class, hint = classify_error("unknown")
        return QueryResult(
            query_nr=query.query_nr,
            status="ERROR",
            repark_wall_s=None,
            duckdb_wall_s=duck_median,
            ratio=None,
            repark_rows=None,
            duckdb_rows=len(duck_rows),
            error_class=error_class,
            error_message="no repark rows",
            rewrite_note=query.rewrite_note,
            missing_feature_hint=hint,
            ordered_compare=ordered,
        )

    repark_median = statistics.median(repark_times)
    # Any successful repark payload that disagrees with DuckDB is WRONG-RESULT,
    # even if a later payload matches. Prefer reporting the first mismatch.
    comparison = None
    for payload in repark_payloads:
        candidate = compare_result_sets(payload, duck_rows, ordered=ordered)
        if not candidate.equal:
            comparison = candidate
            break
        comparison = candidate
    if comparison is None:
        return QueryResult(
            query_nr=query.query_nr,
            status="ERROR",
            repark_wall_s=repark_median,
            duckdb_wall_s=duck_median,
            ratio=None,
            repark_rows=None,
            duckdb_rows=len(duck_rows),
            error_class="InternalCompare",
            error_message="no comparison produced from repark payloads",
            rewrite_note=query.rewrite_note,
            timeout_first_s=timeout_first_s,
            timeout_retry_s=timeout_retry_wall,
            ordered_compare=ordered,
        )
    ratio = repark_median / duck_median if duck_median > 0 else None

    if not comparison.equal:
        return QueryResult(
            query_nr=query.query_nr,
            status="WRONG-RESULT",
            repark_wall_s=repark_median,
            duckdb_wall_s=duck_median,
            ratio=ratio,
            repark_rows=comparison.repark_rows,
            duckdb_rows=comparison.duckdb_rows,
            error_class="WrongResult",
            error_message=comparison.message,
            rewrite_note=query.rewrite_note,
            timeout_first_s=timeout_first_s,
            timeout_retry_s=timeout_retry_wall,
            ordered_compare=ordered,
        )

    # Completed only on the 300s retry → Slow TIMEOUT (correctness OK, wall over 120s).
    if timeout_first_s is not None and timeout_retry_wall is not None:
        return QueryResult(
            query_nr=query.query_nr,
            status="TIMEOUT",
            repark_wall_s=repark_median,
            duckdb_wall_s=duck_median,
            ratio=ratio,
            repark_rows=comparison.repark_rows,
            duckdb_rows=comparison.duckdb_rows,
            error_class="Slow",
            error_message=(
                f"exceeded {timeout_first_s:g}s; completed in "
                f"{timeout_retry_wall:.3f}s on {timeout_retry_s:g}s retry"
            ),
            rewrite_note=query.rewrite_note,
            timeout_first_s=timeout_first_s,
            timeout_retry_s=timeout_retry_wall,
            ordered_compare=ordered,
        )

    return QueryResult(
        query_nr=query.query_nr,
        status="OK",
        repark_wall_s=repark_median,
        duckdb_wall_s=duck_median,
        ratio=ratio,
        repark_rows=comparison.repark_rows,
        duckdb_rows=comparison.duckdb_rows,
        rewrite_note=query.rewrite_note,
        ordered_compare=ordered,
    )


def _duckdb_collect(connection: Any, sql: str) -> list[tuple[Any, ...]]:
    return list(connection.execute(sql).fetchall())


def _repark_collect(spark: Any, sql: str) -> list[tuple[Any, ...]]:
    frame = spark.sql(sql)
    # Prefer Arrow path for value fidelity; fall back to collect().
    # Schema-name order (not dict.values()) so projection renames stay stable.
    if hasattr(frame, "to_arrow"):
        table = frame.to_arrow()
        names = list(table.column_names)
        return [tuple(row[name] for name in names) for row in table.to_pylist()]
    rows = frame.collect()
    return [tuple(row) for row in rows]


def _timed_call(
    function: Callable[[], list[tuple[Any, ...]]],
    *,
    timeout_s: float,
) -> tuple[float, list[tuple[Any, ...]]]:
    """Run ``function`` with a wall-clock timeout on the main thread.

    Uses ``signal.setitimer`` (Unix) so a wall overrun raises ``TimeoutError`` without
    a ThreadPoolExecutor that cannot cancel a running worker.
    """
    if timeout_s <= 0:
        msg = f"timeout_s must be positive, got {timeout_s}"
        raise ValueError(msg)

    def _alarm_handler(  # nested-def: SIGALRM handler closes over the per-query timeout
        _signum: int, _frame: object
    ) -> None:
        raise TimeoutError(f"TPC-DS query exceeded {timeout_s}s wall")

    # SIGALRM only works on the main thread of the main interpreter.
    if hasattr(signal, "setitimer") and hasattr(signal, "SIGALRM"):
        previous_handler = signal.getsignal(signal.SIGALRM)
        signal.signal(signal.SIGALRM, _alarm_handler)
        signal.setitimer(signal.ITIMER_REAL, timeout_s)
        started = time.perf_counter()
        # Mutable box so a SIGALRM after function() returns but before the
        # assignment target is written still keeps completed rows.
        box: dict[str, list[tuple[Any, ...]] | None] = {"result": None}
        try:

            def _store() -> None:
                box["result"] = function()

            _store()
        except TimeoutError:
            if box["result"] is None:
                raise FuturesTimeout from None
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0.0)
            signal.signal(signal.SIGALRM, previous_handler)
        elapsed = time.perf_counter() - started
        stored = box["result"]
        if stored is None:
            raise FuturesTimeout
        return elapsed, stored

    # Fallback platforms without setitimer: soft wall after completion only.
    started = time.perf_counter()
    result = function()
    elapsed = time.perf_counter() - started
    return elapsed, result
