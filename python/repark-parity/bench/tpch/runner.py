"""TPC-H scoreboard runner: repark (and optional Sail) vs DuckDB over parquet/Iceberg.

Statuses: OK | WRONG-RESULT | ERROR | TIMEOUT | DIED (subprocess OOM/signal, V3).
Wall times are median of ``repeats`` (default 3). Default timeout 120s per side;
on TIMEOUT, **one** retry at 300s (Slow vs hung; B1 / TPC-DS refinement, both engines).
SF10 defaults 300s + subprocess isolation (R-TPCH-V3 greylight B3).

``--engine sail`` / ``both`` (B1): Sail via Spark Connect loopback is measurement
prior-art only — never a RePark product dependency.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import platform
import resource
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from concurrent.futures import TimeoutError as FuturesTimeout
from pathlib import Path
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from .compare import compare_result_sets
from .datagen import TABLES, default_data_root, ensure_parquet_sf
from .queries import TpchQuery, load_queries

LOGGER = logging.getLogger(__name__)

StatusKind = Literal["OK", "WRONG-RESULT", "ERROR", "TIMEOUT", "DIED"]
IsolationKind = Literal["inprocess", "subprocess"]
StorageKind = Literal["parquet", "iceberg"]
EngineKind = Literal["repark", "sail", "both"]

DEFAULT_TIMEOUT_S: Final[float] = 120.0
# B1 / TPC-DS greylight: one retry after 120s TIMEOUT at this ceiling.
TIMEOUT_RETRY_S: Final[float] = 300.0
SF10_DEFAULT_TIMEOUT_S: Final[float] = 300.0
DEFAULT_REPEATS: Final[int] = 3
SF10_MIN_FREE_DISK_GIB: Final[float] = 30.0
# Exit codes: 0 OK; 2 usage; 3 WRONG; 4 ERROR; 5 TIMEOUT; 6 DIED (V3).
EXIT_DIED: Final[int] = 6
VALID_STATUSES: Final[frozenset[str]] = frozenset(
    {"OK", "WRONG-RESULT", "ERROR", "TIMEOUT", "DIED"}
)
STATUS_RANK: Final[dict[str, int]] = {
    "OK": 0,
    "TIMEOUT": 1,
    "ERROR": 2,
    "DIED": 3,
    "WRONG-RESULT": 4,
}
# Unknown/hostile status strings rank as ERROR for merge + exit (C4-Q-001 / C4-Q-002).
_UNKNOWN_STATUS_RANK: Final[int] = STATUS_RANK["ERROR"]


class QueryResult(BaseModel):
    """Per-query scoreboard row.

    Single-engine runs store the subject wall in ``repark_wall_s`` (historical name;
    report renames the column for ``engine=sail`` / Iceberg). Three-way boards fill
    both ``repark_*`` and ``sail_*`` fields.
    """

    model_config = ConfigDict(extra="forbid")

    query_nr: int
    status: StatusKind
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
    # B1 greylight: first-pass ceiling (usually 120) + retry wall or budget.
    timeout_first_s: float | None = None
    timeout_retry_s: float | None = None
    # Three-way Sail columns (engine=both); unused for single-engine runs.
    sail_wall_s: float | None = None
    sail_rows: int | None = None
    sail_status: StatusKind | None = None
    sail_ratio: float | None = None
    sail_error_class: str | None = None
    sail_error_message: str | None = None
    repark_status: StatusKind | None = None


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


def sf10_disk_gate(
    data_root: Path | None = None,
    *,
    min_free_gib: float = SF10_MIN_FREE_DISK_GIB,
) -> tuple[bool, float]:
    """Return ``(ok_to_run, free_gib)``. Free disk < min → skip SF10 (FINDING)."""
    root = (data_root if data_root is not None else default_data_root()).expanduser()
    free = free_disk_gib(root)
    return free >= min_free_gib, free


def query_result_to_dict(result: QueryResult) -> dict[str, Any]:
    """Serialize a :class:`QueryResult` for the subprocess worker."""
    return result.model_dump()


def _coerce_status(raw: object, *, field_name: str = "status") -> StatusKind:
    """Map a status string to a known :data:`StatusKind` (C4-Q-001).

    Unknown values become ERROR so hostile/malformed Sail board JSON cannot
    green-exit the scoreboard.
    """
    if isinstance(raw, str) and raw in VALID_STATUSES:
        return raw  # type: ignore[return-value]
    return "ERROR"


def query_result_from_dict(payload: dict[str, Any]) -> QueryResult:
    """Deserialize a :class:`QueryResult` from the subprocess worker."""
    raw_status = payload["status"]
    status = _coerce_status(raw_status)
    error_class = payload.get("error_class")
    error_message = payload.get("error_message")
    if status == "ERROR" and (not isinstance(raw_status, str) or raw_status not in VALID_STATUSES):
        error_class = error_class or "InvalidStatus"
        error_message = error_message or f"invalid status {raw_status!r} coerced to ERROR"
    sail_raw = payload.get("sail_status")
    sail_status: StatusKind | None = (
        None if sail_raw is None else _coerce_status(sail_raw, field_name="sail_status")
    )
    repark_raw = payload.get("repark_status")
    repark_status: StatusKind | None = (
        None if repark_raw is None else _coerce_status(repark_raw, field_name="repark_status")
    )
    return QueryResult(
        query_nr=int(payload["query_nr"]),
        status=status,
        repark_wall_s=payload.get("repark_wall_s"),
        duckdb_wall_s=payload.get("duckdb_wall_s"),
        ratio=payload.get("ratio"),
        repark_rows=payload.get("repark_rows"),
        duckdb_rows=payload.get("duckdb_rows"),
        error_class=error_class,
        error_message=error_message,
        rewrite_note=payload.get("rewrite_note"),
        missing_feature_hint=payload.get("missing_feature_hint"),
        rss_peak_kb=payload.get("rss_peak_kb"),
        timeout_first_s=payload.get("timeout_first_s"),
        timeout_retry_s=payload.get("timeout_retry_s"),
        sail_wall_s=payload.get("sail_wall_s"),
        sail_rows=payload.get("sail_rows"),
        sail_status=sail_status,
        sail_ratio=payload.get("sail_ratio"),
        sail_error_class=payload.get("sail_error_class"),
        sail_error_message=payload.get("sail_error_message"),
        repark_status=repark_status,
    )


def worse_status(left: StatusKind, right: StatusKind) -> StatusKind:
    """Return the worse of two statuses (WRONG-RESULT > DIED > ERROR > TIMEOUT > OK)."""
    left_rank = STATUS_RANK.get(left, _UNKNOWN_STATUS_RANK)
    right_rank = STATUS_RANK.get(right, _UNKNOWN_STATUS_RANK)
    if left_rank >= right_rank:
        if left in VALID_STATUSES:
            return left
        return "ERROR"
    if right in VALID_STATUSES:
        return right
    return "ERROR"


def merge_three_way(repark_board: Scoreboard, sail_board: Scoreboard) -> Scoreboard:
    """Merge repark and Sail single-engine boards into one three-wall scoreboard.

    DuckDB walls prefer the repark board (same oracle, same machine session). Sail
    correctness remains DuckDB-oracle via the sail board's own compare.
    """
    sail_by_nr = {query.query_nr: query for query in sail_board.queries}
    merged: list[QueryResult] = []
    for repark_query in repark_board.queries:
        sail_query = sail_by_nr.get(repark_query.query_nr)
        if sail_query is None:
            # Board-level skip (empty sail matrix) must not look like per-query ERROR (C2-H-002).
            if sail_board.skipped and not sail_board.queries:
                sail_status: StatusKind | None = None
                sail_error_class = "SailBoardSkipped"
                sail_error_message = "Sail board skipped (see findings); no per-query Sail row"
            else:
                sail_status = "ERROR"
                sail_error_class = "MissingSailRow"
                sail_error_message = "Sail board missing this query number"
            merged.append(
                QueryResult(
                    query_nr=repark_query.query_nr,
                    status=repark_query.status,
                    repark_wall_s=repark_query.repark_wall_s,
                    duckdb_wall_s=repark_query.duckdb_wall_s,
                    ratio=repark_query.ratio,
                    repark_rows=repark_query.repark_rows,
                    duckdb_rows=repark_query.duckdb_rows,
                    error_class=repark_query.error_class,
                    error_message=repark_query.error_message,
                    rewrite_note=repark_query.rewrite_note,
                    missing_feature_hint=repark_query.missing_feature_hint,
                    rss_peak_kb=repark_query.rss_peak_kb,
                    timeout_first_s=repark_query.timeout_first_s,
                    timeout_retry_s=repark_query.timeout_retry_s,
                    repark_status=repark_query.status,
                    sail_status=sail_status,
                    sail_error_class=sail_error_class,
                    sail_error_message=sail_error_message,
                )
            )
            continue
        # Sail single-engine stores its wall in repark_wall_s (subject column).
        overall = worse_status(repark_query.status, sail_query.status)
        # Prefer repark Slow/hung metadata; fall back to Sail so three-way keeps walls.
        timeout_first = repark_query.timeout_first_s
        timeout_retry = repark_query.timeout_retry_s
        if timeout_first is None and sail_query.timeout_first_s is not None:
            timeout_first = sail_query.timeout_first_s
            timeout_retry = sail_query.timeout_retry_s
        merged.append(
            QueryResult(
                query_nr=repark_query.query_nr,
                status=overall,
                repark_wall_s=repark_query.repark_wall_s,
                duckdb_wall_s=repark_query.duckdb_wall_s or sail_query.duckdb_wall_s,
                ratio=repark_query.ratio,
                repark_rows=repark_query.repark_rows,
                duckdb_rows=repark_query.duckdb_rows or sail_query.duckdb_rows,
                error_class=repark_query.error_class,
                error_message=repark_query.error_message,
                rewrite_note=repark_query.rewrite_note or sail_query.rewrite_note,
                missing_feature_hint=repark_query.missing_feature_hint,
                rss_peak_kb=_max_optional_int(repark_query.rss_peak_kb, sail_query.rss_peak_kb),
                timeout_first_s=timeout_first,
                timeout_retry_s=timeout_retry,
                repark_status=repark_query.status,
                sail_wall_s=sail_query.repark_wall_s,
                sail_rows=sail_query.repark_rows,
                sail_status=sail_query.status,
                sail_ratio=sail_query.ratio,
                sail_error_class=sail_query.error_class,
                sail_error_message=sail_query.error_message,
            )
        )
    environment = dict(repark_board.environment)
    environment["engine"] = "three-way repark + Sail + DuckDB (B1)"
    environment["subject_engine"] = "both"
    environment["sail_engine"] = sail_board.environment.get("engine", "sail")
    for key, value in sail_board.environment.items():
        if key.startswith("sail_") or key in {"pysail_version", "pyspark_client_version"}:
            environment[key] = value
    findings = list(repark_board.findings) + [
        f"[sail] {finding}" for finding in sail_board.findings
    ]
    # Boilerplate only when Sail actually ran (C1-H-003 / C6-H-001). A fully-skipped
    # Sail board must not invent gRPC transport cost in Findings.
    sail_actually_ran = bool(sail_board.queries) and not (
        sail_board.skipped and not sail_board.queries
    )
    if sail_actually_ran:
        for boilerplate in (
            "Sail pays gRPC loopback transport cost repark in-process does not — "
            "recorded, never corrected silently.",
            "Sail is measurement prior-art only; not a RePark product dependency.",
        ):
            if boilerplate not in sail_board.findings and boilerplate not in repark_board.findings:
                findings.append(boilerplate)
    return Scoreboard(
        scale_factor=repark_board.scale_factor,
        data_dir=repark_board.data_dir or sail_board.data_dir,
        environment=environment,
        queries=merged,
        rewrites=repark_board.rewrites or sail_board.rewrites,
        findings=findings,
        skipped=repark_board.skipped and sail_board.skipped,
        rss_peak_kb_max=_max_optional_int(repark_board.rss_peak_kb_max, sail_board.rss_peak_kb_max),
    )


def _max_optional_int(left: int | None, right: int | None) -> int | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def _subprocess_run_kill_group(
    command: Sequence[str],
    *,
    timeout_s: float,
) -> subprocess.CompletedProcess[str]:
    """Run ``command`` in a new session; on timeout SIGKILL the whole process group.

    Plain ``subprocess.run(..., timeout=)`` only kills the direct child. Sail's
    SparkConnectServer is often a grandchild — without ``killpg`` it orphans (C1-L-001).
    """
    process = subprocess.Popen(
        list(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired as exc:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            with contextlib.suppress(ProcessLookupError):
                process.kill()
        try:
            process.communicate(timeout=5.0)
        except Exception:
            LOGGER.exception("drain after killpg failed for %s", command[:2])
        raise subprocess.TimeoutExpired(
            cmd=list(command),
            timeout=timeout_s,
            output=exc.output,
            stderr=exc.stderr,
        ) from None
    return subprocess.CompletedProcess(
        args=list(command),
        returncode=process.returncode if process.returncode is not None else -1,
        stdout=stdout,
        stderr=stderr,
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
    storage: StorageKind = "parquet",
    warehouse: Path | None = None,
    min_free_disk_gib: float = SF10_MIN_FREE_DISK_GIB,
    engine: EngineKind = "repark",
    sail_python: Path | None = None,
) -> Scoreboard:
    """Run the full (or filtered) TPC-H matrix for one scale factor.

    V3 (W1) behaviour:
    - SF >= 10: default timeout 300s, default isolation=subprocess, disk gate.
    - storage=iceberg: CTAS SF tables into local memory-catalog Iceberg, then query.

    B1: ``engine=sail`` uses Sail Spark Connect; ``engine=both`` runs repark then
    Sail (Sail via ``sail_python`` subprocess when needed) and merges three walls.
    """
    if engine == "both":
        repark_board = run_scoreboard(
            scale_factor=scale_factor,
            data_root=data_root,
            repeats=repeats,
            timeout_s=timeout_s,
            timeout_retry_s=timeout_retry_s,
            query_filter=query_filter,
            isolation=isolation,
            storage=storage,
            warehouse=warehouse,
            min_free_disk_gib=min_free_disk_gib,
            engine="repark",
        )
        sail_board = _run_sail_scoreboard(
            scale_factor=scale_factor,
            data_root=data_root,
            repeats=repeats,
            timeout_s=timeout_s,
            timeout_retry_s=timeout_retry_s,
            query_filter=query_filter,
            isolation=isolation,
            min_free_disk_gib=min_free_disk_gib,
            sail_python=sail_python,
        )
        return merge_three_way(repark_board, sail_board)

    is_sf10_or_above = scale_factor >= 10.0
    resolved_timeout = (
        timeout_s
        if timeout_s is not None
        else (SF10_DEFAULT_TIMEOUT_S if is_sf10_or_above else DEFAULT_TIMEOUT_S)
    )
    resolved_retry = timeout_retry_s if timeout_retry_s is not None else TIMEOUT_RETRY_S
    resolved_isolation: IsolationKind = isolation or (
        "subprocess" if is_sf10_or_above else "inprocess"
    )
    findings: list[str] = []
    # Iceberg CTAS is session-local; subprocess-per-query would re-CTAS 8 tables each
    # time. Iceberg leg runs in-process (SF1 measurement); SF10 isolation stays parquet.
    if storage == "iceberg" and resolved_isolation == "subprocess":
        findings.append(
            "isolation coerced inprocess for storage=iceberg "
            "(CTAS once per scoreboard; subprocess would re-seed every query)"
        )
        resolved_isolation = "inprocess"
    if engine == "sail" and storage == "iceberg":
        findings.append("storage=iceberg ignored for engine=sail (Sail leg is parquet temp views)")
        storage = "parquet"
    # Sail server is process-global; subprocess-per-query would restart Connect each Q.
    if engine == "sail" and resolved_isolation == "subprocess":
        findings.append(
            "isolation coerced inprocess for engine=sail "
            "(Spark Connect server lives for the scoreboard)"
        )
        resolved_isolation = "inprocess"

    if is_sf10_or_above:
        ok, free_gib = sf10_disk_gate(data_root, min_free_gib=min_free_disk_gib)
        if not ok:
            finding = (
                f"SF{scale_factor:g} SKIPPED: free disk {free_gib:.1f} GiB "
                f"< {min_free_disk_gib:g} GiB hard cap (greylight B3)"
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
                    "min_free_disk_gib": str(min_free_disk_gib),
                    "storage": storage,
                    "isolation": resolved_isolation,
                    "timeout_s": str(resolved_timeout),
                    "timeout_retry_s": str(resolved_retry),
                    "repeats": str(repeats),
                    "subject_engine": engine,
                },
                findings=[finding],
                skipped=True,
            )
        findings.append(
            f"SF{scale_factor:g} free disk {free_gib:.1f} GiB >= {min_free_disk_gib:g} GiB"
        )

    data_dir = ensure_parquet_sf(scale_factor, data_root=data_root)
    queries = load_queries()
    if query_filter is not None:
        queries = [query for query in queries if query.query_nr in query_filter]

    if storage == "iceberg":
        storage_label = "Iceberg memory-catalog (local warehouse; V3 leg)"
    else:
        storage_label = "parquet-not-Iceberg (fast seed path)"

    if engine == "sail":
        engine_label = "single-node Sail Spark Connect + DuckDB (B1 measurement)"
    elif resolved_isolation == "subprocess":
        engine_label = "single-node repark + DuckDB (subprocess per query)"
    else:
        engine_label = "single-node repark + DuckDB same process"

    environment = {
        "machine": platform.node(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "scale_factor": str(scale_factor),
        "engine": engine_label,
        "subject_engine": engine,
        "storage": storage_label,
        "storage_kind": storage,
        "isolation": resolved_isolation,
        "timeout_s": str(resolved_timeout),
        "timeout_retry_s": str(resolved_retry),
        "repeats": str(repeats),
        "numeric_tolerance": (
            "1e-6 relative (non-integral floats only); ints and integral decimals exact"
        ),
    }
    if warehouse is not None:
        environment["iceberg_warehouse"] = str(warehouse)
    # gRPC / prior-art disclosure is appended only after Sail actually opens (C1-H-004).

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
            LOGGER.info("TPC-H Q%02d (subprocess) …", query.query_nr)
            result = _run_one_query_subprocess(
                data_dir=data_dir,
                query=query,
                repeats=repeats,
                timeout_s=resolved_timeout,
                timeout_retry_s=resolved_retry,
                storage=storage,
                warehouse=warehouse,
                engine=engine,
            )
            board.queries.append(result)
            _track_rss_peak(board, result)
        return board

    duckdb_conn = _open_duckdb_over_parquet(data_dir)
    subject: Any
    sail_session: Any = None
    if engine == "sail":
        from .sail_engine import SailUnavailableError, open_sail_over_parquet, sail_package_versions

        try:
            sail_session = open_sail_over_parquet(data_dir)
        except SailUnavailableError as exc:
            board.findings.append(f"Sail unavailable: {exc}")
            board.environment["sail_status"] = "unavailable"
            board.skipped = True  # C1-Q-001: empty matrix is a skip FINDING, not success
            duckdb_conn.close()
            return board
        except Exception as exc:
            board.findings.append(f"Sail open failed: {type(exc).__name__}: {exc}")
            board.environment["sail_status"] = "open_failed"
            board.skipped = True
            duckdb_conn.close()
            return board
        subject = sail_session.spark
        board.environment["sail_version"] = sail_session.version
        board.environment["sail_port"] = str(sail_session.port)
        versions = sail_package_versions()
        board.environment["pysail_version"] = versions.get("pysail") or ""
        board.environment["pyspark_client_version"] = versions.get("pyspark") or ""
        board.findings.append(
            "Sail pays gRPC loopback transport cost repark in-process does not — "
            "recorded, never corrected silently."
        )
        board.findings.append(
            "Sail is measurement prior-art only; not a RePark product dependency."
        )
        subject_label = "sail"
    else:
        subject = _open_repark(data_dir, storage=storage, warehouse=warehouse)
        subject_label = "repark"
    try:
        for query in queries:
            LOGGER.info("TPC-H Q%02d (%s) …", query.query_nr, subject_label)
            result = _run_one_query(
                spark=subject,
                duckdb_conn=duckdb_conn,
                query=query,
                repeats=repeats,
                timeout_s=resolved_timeout,
                timeout_retry_s=resolved_retry,
                subject_label=subject_label,
            )
            result.rss_peak_kb = _max_rss_kb()
            board.queries.append(result)
            _track_rss_peak(board, result)
    finally:
        if sail_session is not None:
            try:
                sail_session.stop()
            except Exception:
                LOGGER.exception("Sail session stop failed")
        else:
            try:
                subject.stop()
            except Exception:
                LOGGER.exception("repark session stop failed")
        try:
            duckdb_conn.close()
        except Exception:
            LOGGER.exception("duckdb close failed")

    return board


def _run_sail_scoreboard(
    *,
    scale_factor: float,
    data_root: Path | None,
    repeats: int,
    timeout_s: float | None,
    timeout_retry_s: float | None,
    query_filter: set[int] | None,
    isolation: IsolationKind | None,
    min_free_disk_gib: float,
    sail_python: Path | None,
) -> Scoreboard:
    """Run the Sail leg, preferring an in-process open when pysail is importable.

    Only :class:`SailUnavailableError` falls through to ``sail_python`` subprocess.
    Real scoreboard failures must not be swallowed and re-run (C1-Q-002).
    """
    in_process_error: str | None = None
    from .sail_engine import SailUnavailableError, require_sail_imports

    try:
        require_sail_imports()
    except SailUnavailableError as exc:
        in_process_error = f"{type(exc).__name__}: {exc}"
        LOGGER.info(
            "In-process Sail unavailable (%s); trying sail_python subprocess",
            in_process_error,
        )
    else:
        # Imports OK — run in-process; do not catch scoreboard exceptions.
        return run_scoreboard(
            scale_factor=scale_factor,
            data_root=data_root,
            repeats=repeats,
            timeout_s=timeout_s,
            timeout_retry_s=timeout_retry_s,
            query_filter=query_filter,
            isolation=isolation,
            storage="parquet",
            min_free_disk_gib=min_free_disk_gib,
            engine="sail",
        )

    python_path = sail_python
    if python_path is None:
        env_path = os.environ.get("REPARK_SAIL_PYTHON")
        if env_path:
            python_path = Path(env_path)
    if python_path is None or not python_path.is_file() or not os.access(python_path, os.X_OK):
        finding = (
            "Sail leg SKIPPED: pysail not importable here and --sail-python / "
            "REPARK_SAIL_PYTHON not set to an executable Sail venv interpreter "
            f"(in-process error: {in_process_error or 'unknown'})"
        )
        return Scoreboard(
            scale_factor=scale_factor,
            data_dir="",
            environment={
                "machine": platform.node(),
                "subject_engine": "sail",
                "sail_status": "skipped",
            },
            findings=[finding],
            skipped=True,
        )
    return _run_sail_scoreboard_subprocess(
        python_path=python_path,
        scale_factor=scale_factor,
        data_root=data_root,
        repeats=repeats,
        timeout_s=timeout_s,
        timeout_retry_s=timeout_retry_s,
        query_filter=query_filter,
        isolation=isolation,
        min_free_disk_gib=min_free_disk_gib,
    )


def _run_sail_scoreboard_subprocess(
    *,
    python_path: Path,
    scale_factor: float,
    data_root: Path | None,
    repeats: int,
    timeout_s: float | None,
    timeout_retry_s: float | None,
    query_filter: set[int] | None,
    isolation: IsolationKind | None,
    min_free_disk_gib: float,
) -> Scoreboard:
    """Spawn Sail-venv python to run ``run_tpch.py --engine sail`` and load JSON."""
    run_tpch = Path(__file__).resolve().parent / "run_tpch.py"
    with tempfile.TemporaryDirectory(prefix="tpch-sail-board-") as tmp:
        out_path = Path(tmp) / "sail-board.json"
        command: list[str] = [
            str(python_path),
            str(run_tpch),
            "--engine",
            "sail",
            "--sf",
            str(scale_factor),
            "--repeats",
            str(repeats),
            "--min-free-gib",
            str(min_free_disk_gib),
            "--out",
            str(out_path),
        ]
        if data_root is not None:
            command.extend(["--data-root", str(data_root)])
        if timeout_s is not None:
            command.extend(["--timeout", str(timeout_s)])
        if timeout_retry_s is not None:
            command.extend(["--timeout-retry", str(timeout_retry_s)])
        if isolation is not None:
            command.extend(["--isolation", isolation])
        if query_filter is not None:
            command.extend(["--queries", ",".join(str(number) for number in sorted(query_filter))])
        # Hard wall: 22 queries * (repeats * timeout + retry) * 2 sides + setup.
        # Match SF≥10 default timeout (300s) when caller left timeout_s unset (C1-Q-004).
        is_sf10_or_above = scale_factor >= 10.0
        resolved_timeout = (
            timeout_s
            if timeout_s is not None
            else (SF10_DEFAULT_TIMEOUT_S if is_sf10_or_above else DEFAULT_TIMEOUT_S)
        )
        resolved_retry = timeout_retry_s if timeout_retry_s is not None else TIMEOUT_RETRY_S
        query_count = float(len(query_filter) if query_filter is not None else 22)
        # At least one query budget even if filter is somehow empty.
        query_count = max(query_count, 1.0)
        hard_s = max(
            600.0,
            query_count * (float(repeats) * resolved_timeout + resolved_retry) * 2.0 + 600.0,
        )
        LOGGER.info("Sail board subprocess: %s (hard wall %.0fs)", python_path, hard_s)
        try:
            completed = _subprocess_run_kill_group(command, timeout_s=hard_s)
        except subprocess.TimeoutExpired:
            return Scoreboard(
                scale_factor=scale_factor,
                data_dir="",
                environment={"subject_engine": "sail", "sail_status": "board_timeout"},
                findings=[f"Sail scoreboard subprocess exceeded hard wall {hard_s:.0f}s"],
                skipped=True,
            )
        if not out_path.is_file():
            tail = (completed.stderr or completed.stdout or "")[-800:]
            return Scoreboard(
                scale_factor=scale_factor,
                data_dir="",
                environment={"subject_engine": "sail", "sail_status": "board_error"},
                findings=[f"Sail scoreboard wrote no JSON (exit {completed.returncode}): {tail}"],
                skipped=True,
            )
        try:
            payload = json.loads(out_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return Scoreboard(
                scale_factor=scale_factor,
                data_dir="",
                environment={"subject_engine": "sail", "sail_status": "board_error"},
                findings=[f"Sail scoreboard JSON invalid: {exc}"],
                skipped=True,
            )
        if not isinstance(payload, dict):
            return Scoreboard(
                scale_factor=scale_factor,
                data_dir="",
                environment={"subject_engine": "sail", "sail_status": "board_error"},
                findings=["Sail scoreboard JSON root must be an object"],
                skipped=True,
            )
        raw_queries = payload.get("queries", [])
        if not isinstance(raw_queries, list):
            return Scoreboard(
                scale_factor=scale_factor,
                data_dir="",
                environment={"subject_engine": "sail", "sail_status": "board_error"},
                findings=["Sail scoreboard JSON queries must be a list"],
                skipped=True,
            )
        try:
            queries = [query_result_from_dict(item) for item in raw_queries]
        except (KeyError, TypeError, ValueError) as exc:
            return Scoreboard(
                scale_factor=scale_factor,
                data_dir="",
                environment={"subject_engine": "sail", "sail_status": "board_error"},
                findings=[f"Sail scoreboard query row invalid: {type(exc).__name__}: {exc}"],
                skipped=True,
            )
        return Scoreboard(
            scale_factor=float(payload.get("scale_factor", scale_factor)),
            data_dir=str(payload.get("data_dir", "")),
            environment=dict(payload.get("environment") or {}),
            queries=queries,
            rewrites=list(payload.get("rewrites") or []),
            findings=list(payload.get("findings") or []),
            skipped=bool(payload.get("skipped", False)),
            rss_peak_kb_max=payload.get("rss_peak_kb_max"),
        )


def _track_rss_peak(board: Scoreboard, result: QueryResult) -> None:
    if result.rss_peak_kb is None:
        return
    if board.rss_peak_kb_max is None or result.rss_peak_kb > board.rss_peak_kb_max:
        board.rss_peak_kb_max = result.rss_peak_kb


def _max_rss_kb() -> int:
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def classify_error(message: str) -> tuple[str, str | None]:
    """Map an exception string to (error_class, missing_feature_hint)."""
    lower = message.lower()
    # Order matters — more specific first.
    patterns: list[tuple[str, str, str | None]] = [
        ("unsupportedoperationexception", "UnsupportedOperationException", None),
        ("not implemented", "NotImplemented", None),
        # Sail / Spark Connect transport (B1 third engine) — before generic "type".
        ("statusruntimeexception", "SailGrpc", "Spark Connect gRPC transport"),
        ("grpc", "SailGrpc", "Spark Connect gRPC transport"),
        ("connection refused", "SailConnect", "Spark Connect server unreachable"),
        ("interval", "IntervalLiteral", "interval literal syntax"),
        ("extract(", "Extract", "EXTRACT / date_part"),
        ("date_part", "Extract", "EXTRACT / date_part"),
        ("date_trunc", "DateTrunc", "date_trunc"),
        ("substring", "Substring", "substring"),
        ("like ", "Like", "LIKE / ILIKE"),
        ("exists", "ExistsSubquery", "EXISTS correlated subquery"),
        ("correlated", "CorrelatedSubquery", "correlated subquery"),
        ("in (", "InListOrSubquery", "IN subquery"),
        ("case when", "CaseExpr", "CASE expression"),
        ("rollup", "Rollup", "ROLLUP"),
        ("cube", "Cube", "CUBE"),
        ("grouping", "GroupingSets", "GROUPING SETS"),
        ("window", "Window", "window function"),
        ("over (", "Window", "window function"),
        ("rank(", "Window", "window function"),
        ("dense_rank", "Window", "window function"),
        ("row_number", "Window", "window function"),
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
        if needle in lower:
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
    """Map scoreboard statuses to CLI exit codes (E1-L-005 / E2-L-002 / V3 DIED).

    0 = all OK (or skipped-disk with no queries); 3 = any WRONG-RESULT;
    4 = any ERROR (no WRONG); 6 = any DIED (outranks TIMEOUT — process death);
    5 = any TIMEOUT only. Skipped-disk boards return 0 (measurement FINDING).
    """
    if board.skipped and not board.queries:
        return 0
    if any(query.status == "WRONG-RESULT" for query in board.queries):
        return 3
    if any(query.status == "ERROR" for query in board.queries):
        return 4
    # DIED outranks TIMEOUT: a killed worker is worse than a soft wall overrun (C1-Q-001).
    if any(query.status == "DIED" for query in board.queries):
        return EXIT_DIED
    if any(query.status == "TIMEOUT" for query in board.queries):
        return 5
    # Unknown statuses should already be coerced to ERROR; belt-and-suspenders (C4-Q-001).
    if any(query.status not in VALID_STATUSES for query in board.queries):
        return 4
    return 0


def render_markdown_report(board: Scoreboard, *, title: str | None = None) -> str:
    """Render the scoreboard + gap census as Markdown."""
    header = (
        title or f"TPC-H scoreboard SF{board.scale_factor} — {board.environment.get('machine', '')}"
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

    storage = board.environment.get("storage", "")
    # Prefer explicit kind key (set by run_scoreboard); fall back carefully —
    # "parquet-not-Iceberg" must NOT match a naive "Iceberg" substring check.
    storage_kind = board.environment.get("storage_kind", "")
    iceberg_mode = storage_kind == "iceberg" or storage.startswith("Iceberg ")
    subject_engine = board.environment.get("subject_engine", "repark")
    three_way = subject_engine == "both" or any(
        query.sail_status is not None for query in board.queries
    )

    lines.extend(["", "## Per-query matrix", ""])
    if three_way:
        lines.append(
            "| Q | repark_st | sail_st | repark_s | sail_s | duckdb_s | "
            "r_ratio | s_ratio | rows_r | rows_s | rows_d | rss_kb | hint |"
        )
        lines.append("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
        for query in board.queries:
            repark_st = query.repark_status or query.status
            sail_st = query.sail_status or "—"
            repark_s = f"{query.repark_wall_s:.3f}" if query.repark_wall_s is not None else "—"
            sail_s = f"{query.sail_wall_s:.3f}" if query.sail_wall_s is not None else "—"
            duck_s = f"{query.duckdb_wall_s:.3f}" if query.duckdb_wall_s is not None else "—"
            r_ratio = f"{query.ratio:.2f}" if query.ratio is not None else "—"
            s_ratio = f"{query.sail_ratio:.2f}" if query.sail_ratio is not None else "—"
            rows_r = str(query.repark_rows) if query.repark_rows is not None else "—"
            rows_s = str(query.sail_rows) if query.sail_rows is not None else "—"
            rows_d = str(query.duckdb_rows) if query.duckdb_rows is not None else "—"
            rss = str(query.rss_peak_kb) if query.rss_peak_kb is not None else "—"
            hint_parts: list[str] = []
            if query.error_class or query.missing_feature_hint:
                hint_parts.append(f"repark:{query.missing_feature_hint or query.error_class}")
            if query.sail_error_class:
                hint_parts.append(f"sail:{query.sail_error_class}")
            if query.error_message and (query.repark_status or query.status) != "OK":
                short_err = query.error_message.split("\n", maxsplit=1)[0][:40]
                hint_parts.append(short_err)
            if query.sail_error_message and query.sail_status not in (None, "OK"):
                short_sail = query.sail_error_message.split("\n", maxsplit=1)[0][:40]
                hint_parts.append(short_sail)
            # Surface Slow/hung timeout walls on three-way rows (C1-H-002).
            if query.timeout_first_s is not None or query.timeout_retry_s is not None:
                first = f"{query.timeout_first_s:.1f}" if query.timeout_first_s is not None else "—"
                retry = f"{query.timeout_retry_s:.1f}" if query.timeout_retry_s is not None else "—"
                slowish = (query.error_class or "") == "Slow" or (
                    query.sail_error_class or ""
                ) == "Slow"
                retry_tag = "t300_wall" if slowish else "t300_budget"
                hint_parts.append(f"[t120={first}s {retry_tag}={retry}s]")
            hint = "; ".join(hint_parts)
            lines.append(
                f"| {query.query_nr} | {repark_st} | {sail_st} | {repark_s} | {sail_s} | "
                f"{duck_s} | {r_ratio} | {s_ratio} | {rows_r} | {rows_s} | {rows_d} | "
                f"{rss} | {hint} |"
            )
    else:
        if iceberg_mode:
            wall_header = "iceberg_wall"
        elif subject_engine == "sail":
            wall_header = "sail_s"
        else:
            wall_header = "repark_s"
        lines.append(
            f"| Q | status | {wall_header} | duckdb_s | ratio | rows_r | rows_d | "
            "rss_kb | error_class / hint |"
        )
        lines.append("|---:|---|---:|---:|---:|---:|---:|---:|---|")
        for query in board.queries:
            ratio = f"{query.ratio:.2f}" if query.ratio is not None else "—"
            repark_s = f"{query.repark_wall_s:.3f}" if query.repark_wall_s is not None else "—"
            duck_s = f"{query.duckdb_wall_s:.3f}" if query.duckdb_wall_s is not None else "—"
            rows_r = str(query.repark_rows) if query.repark_rows is not None else "—"
            rows_d = str(query.duckdb_rows) if query.duckdb_rows is not None else "—"
            rss = str(query.rss_peak_kb) if query.rss_peak_kb is not None else "—"
            hint = query.missing_feature_hint or query.error_class or ""
            if query.error_message and query.status != "OK":
                # keep table cells short
                short_err = query.error_message.split("\n", maxsplit=1)[0][:60]
                if short_err not in hint:
                    hint = f"{hint}: {short_err}".strip(": ")
            if query.timeout_first_s is not None or query.timeout_retry_s is not None:
                first = f"{query.timeout_first_s:.1f}" if query.timeout_first_s is not None else "—"
                retry = f"{query.timeout_retry_s:.1f}" if query.timeout_retry_s is not None else "—"
                retry_tag = "t300_wall" if (query.error_class or "") == "Slow" else "t300_budget"
                hint = f"{hint} [t120={first}s {retry_tag}={retry}s]".strip()
            lines.append(
                f"| {query.query_nr} | {query.status} | {repark_s} | {duck_s} | {ratio} | "
                f"{rows_r} | {rows_d} | {rss} | {hint} |"
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
    died = sum(1 for query in board.queries if query.status == "DIED")
    lines.extend(
        [
            "",
            "## Summary counts",
            "",
            f"- OK: **{ok}**",
            f"- WRONG-RESULT: **{wrong}**",
            f"- ERROR: **{err}**",
            f"- TIMEOUT: **{timeout}**",
            f"- DIED: **{died}**",
            f"- Total: **{len(board.queries)}**",
            "",
        ]
    )
    if three_way:
        repark_ok = sum(1 for query in board.queries if query.repark_status == "OK")
        sail_ok = sum(1 for query in board.queries if query.sail_status == "OK")
        repark_wrong = sum(1 for query in board.queries if query.repark_status == "WRONG-RESULT")
        sail_wrong = sum(1 for query in board.queries if query.sail_status == "WRONG-RESULT")
        repark_err = sum(1 for query in board.queries if query.repark_status == "ERROR")
        sail_err = sum(1 for query in board.queries if query.sail_status == "ERROR")
        repark_to = sum(1 for query in board.queries if query.repark_status == "TIMEOUT")
        sail_to = sum(1 for query in board.queries if query.sail_status == "TIMEOUT")
        repark_died = sum(1 for query in board.queries if query.repark_status == "DIED")
        sail_died = sum(1 for query in board.queries if query.sail_status == "DIED")
        lines.extend(
            [
                "### Per-engine census (non-DuckDB)",
                "",
                f"- repark: OK={repark_ok} WRONG-RESULT={repark_wrong} "
                f"ERROR={repark_err} TIMEOUT={repark_to} DIED={repark_died}",
                f"- Sail: OK={sail_ok} WRONG-RESULT={sail_wrong} "
                f"ERROR={sail_err} TIMEOUT={sail_to} DIED={sail_died}",
                "",
            ]
        )

    if board.rewrites:
        lines.extend(["## Dialect rewrites (disclosed)", ""])
        for rewrite in board.rewrites:
            lines.append(f"- Q{rewrite['query_nr']}: {rewrite['note']}")
        lines.append("")

    storage_disclose = (
        "- Iceberg leg: local memory-catalog warehouse; oracle still DuckDB parquet."
        if iceberg_mode
        else "- Parquet temp views — Iceberg leg is a separate V3 run (`--storage iceberg`)."
    )
    isolation = board.environment.get("isolation", "inprocess")
    isolation_disclose = (
        "- Isolation: **subprocess per query** (OOM/signal → DIED; scoreboard continues)."
        if isolation == "subprocess"
        else "- Isolation: in-process (SF1 default); SF10 uses subprocess."
    )
    sail_disclose: list[str] = []
    # Only claim gRPC / Sail version when Sail actually produced rows or env (C6-H-001).
    # engine=both with a fully-skipped Sail board must not invent transport cost.
    sail_rows_present = any(query.sail_status is not None for query in board.queries)
    sail_env_present = bool(
        board.environment.get("pysail_version")
        or board.environment.get("sail_version")
        or board.environment.get("sail_port")
    )
    sail_ran = subject_engine == "sail" or sail_rows_present or sail_env_present
    if sail_ran and (subject_engine in {"sail", "both"} or three_way):
        sail_disclose = [
            "- Sail pays gRPC loopback transport cost repark in-process does not — "
            "recorded, never corrected silently.",
            "- Single-node local mode both engines.",
            "- Sail is measurement prior-art only; not a RePark product dependency.",
            "- TIMEOUT at 120s → one retry at 300s (Slow if completes, hung if not); "
            "identical treatment for repark and Sail.",
            "- Any Sail fail/error is a FINDING (own census), not retry-until-green "
            "beyond the one 300s TIMEOUT retry.",
        ]
        pysail_v = board.environment.get("pysail_version") or board.environment.get(
            "sail_version", ""
        )
        if pysail_v:
            sail_disclose.append(f"- Sail version / config: {pysail_v}")
    elif subject_engine == "both" and not sail_ran:
        sail_disclose = [
            "- Sail leg did not run (skipped/unavailable); three-way walls incomplete — "
            "see Findings. Sail is measurement prior-art only.",
        ]
    lines.extend(
        [
            "## Disclosure",
            "",
            "- Oracle = DuckDB result set; sorted-row compare.",
            "- Non-integral floats: relative tolerance 1e-6; "
            "ints and integral-valued decimals **exact**.",
            storage_disclose,
            isolation_disclose,
            "- Single-node; wall = median of configured repeats; "
            "timeout via SIGALRM (Unix; best-effort under native code).",
            "- RSS peaks recorded (Linux ru_maxrss KiB); **no** RSS auto-abort (B3).",
            *sail_disclose,
            "",
        ]
    )
    return "\n".join(lines)


def status_ledger(board: Scoreboard) -> dict[str, Any]:
    """Status map for the SF0.01 smoke pin file (includes provenance metadata)."""
    queries: dict[str, dict[str, str]] = {}
    for query in board.queries:
        entry: dict[str, str] = {"status": query.status}
        if query.error_class:
            entry["error_class"] = query.error_class
        if query.missing_feature_hint:
            entry["missing_feature_hint"] = query.missing_feature_hint
        # Three-way boards: keep per-engine status for ledger honesty (C2-H-003).
        if query.repark_status is not None:
            entry["repark_status"] = query.repark_status
        if query.sail_status is not None:
            entry["sail_status"] = query.sail_status
        if query.sail_error_class:
            entry["sail_error_class"] = query.sail_error_class
        queries[str(query.query_nr)] = entry
    return {
        "scale_factor": board.scale_factor,
        "numeric_tolerance": "1e-6 relative (floats/decimals only; ints exact)",
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
        path_sql = str(path).replace("'", "''")
        connection.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{path_sql}')"
        )
    return connection


def _open_repark(
    data_dir: Path,
    *,
    storage: StorageKind = "parquet",
    warehouse: Path | None = None,
) -> Any:
    """Open repark with TPC-H tables as short-name temp views."""
    if storage == "iceberg":
        return _open_repark_over_iceberg(data_dir, warehouse=warehouse)
    return _open_repark_over_parquet(data_dir)


def _open_repark_over_parquet(data_dir: Path) -> Any:
    from repark import ReparkSession

    spark = ReparkSession.builder.appName(f"tpch-{data_dir.name}").getOrCreate()
    for table_name in TABLES:
        path = data_dir / f"{table_name}.parquet"
        spark.read.parquet(str(path)).createOrReplaceTempView(table_name)
    return spark


def _open_repark_over_iceberg(
    data_dir: Path,
    *,
    warehouse: Path | None = None,
) -> Any:
    """CTAS the eight parquet tables into a local memory-catalog Iceberg warehouse.

    Temp views keep short TPC-H names so the 22 query texts stay unchanged.
    Never touches AWS — memory catalog + local filesystem warehouse only.
    """
    from repark import ReparkSession

    if warehouse is None:
        warehouse = default_data_root() / f"iceberg-warehouse-{data_dir.name}"
    warehouse = warehouse.expanduser()
    warehouse.mkdir(parents=True, exist_ok=True)
    warehouse_str = str(warehouse)

    spark = (
        ReparkSession.builder.appName(f"tpch-iceberg-{data_dir.name}")
        .config("spark.sql.catalog.tpch_ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.tpch_ice.type", "memory")
        .config("spark.sql.catalog.tpch_ice.warehouse", warehouse_str)
        .getOrCreate()
    )
    spark.sql("CREATE NAMESPACE IF NOT EXISTS tpch_ice.tpch")
    for table_name in TABLES:
        path = data_dir / f"{table_name}.parquet"
        spark.read.parquet(str(path)).createOrReplaceTempView(f"_parquet_{table_name}")
        fq_table = f"tpch_ice.tpch.{table_name}"
        # Idempotent seed: drop + CTAS so a re-run never mixes schemas.
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fq_table}")
        except Exception as exc:
            LOGGER.warning("DROP TABLE %s failed (continuing to CTAS): %s", fq_table, exc)
        spark.sql(f"CREATE TABLE {fq_table} USING iceberg AS SELECT * FROM _parquet_{table_name}")
        spark.table(fq_table).createOrReplaceTempView(table_name)
        LOGGER.info("Iceberg CTAS ready: %s", fq_table)
    return spark


def _run_one_query_subprocess(
    *,
    data_dir: Path,
    query: TpchQuery,
    repeats: int,
    timeout_s: float,
    timeout_retry_s: float = TIMEOUT_RETRY_S,
    storage: StorageKind,
    warehouse: Path | None,
    engine: EngineKind = "repark",
) -> QueryResult:
    """Run one query in a child process; map signal deaths to DIED (B3)."""
    worker = Path(__file__).resolve().parent / "query_worker.py"
    # Hard ceiling so a wedged child cannot block the scoreboard forever.
    # (timeout * repeats + retry) * 2 sides + CTAS/setup budget.
    setup_budget_s = 600.0 if storage == "iceberg" else 120.0
    per_side = float(timeout_s) * float(repeats) + float(timeout_retry_s)
    hard_timeout_s = per_side * 2.0 + setup_budget_s

    with tempfile.TemporaryDirectory(prefix="tpch-qworker-") as tmp:
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
            "storage": storage,
            "warehouse": str(warehouse) if warehouse is not None else None,
            "result_path": str(result_path),
            "engine": engine,
        }
        config_path.write_text(json.dumps(config), encoding="utf-8")
        try:
            # Kill process group so Sail Connect grandchildren cannot orphan (C1-L-001).
            completed = _subprocess_run_kill_group(
                [sys.executable, str(worker), str(config_path)],
                timeout_s=hard_timeout_s,
            )
        except subprocess.TimeoutExpired:
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
            )

        if completed.returncode < 0:
            sig = -completed.returncode
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
            )
        # Linux OOM / shell sometimes surfaces as 137 (128+SIGKILL).
        if completed.returncode in (137, 9):
            return QueryResult(
                query_nr=query.query_nr,
                status="DIED",
                repark_wall_s=None,
                duckdb_wall_s=None,
                ratio=None,
                repark_rows=None,
                duckdb_rows=None,
                error_class="OOMOrKill",
                error_message=f"query worker exit {completed.returncode}",
                rewrite_note=query.rewrite_note,
            )
        if completed.returncode != 0:
            stderr_tail = (completed.stderr or completed.stdout or "")[-500:]
            return QueryResult(
                query_nr=query.query_nr,
                status="ERROR",
                repark_wall_s=None,
                duckdb_wall_s=None,
                ratio=None,
                repark_rows=None,
                duckdb_rows=None,
                error_class="WorkerError",
                error_message=f"worker exit {completed.returncode}: {stderr_tail}",
                rewrite_note=query.rewrite_note,
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
            )
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        return query_result_from_dict(payload)


def _run_one_query(
    *,
    spark: Any,
    duckdb_conn: Any,
    query: TpchQuery,
    repeats: int,
    timeout_s: float,
    timeout_retry_s: float = 0.0,
    subject_label: str = "repark",
) -> QueryResult:
    """Run DuckDB oracle + subject engine for one query.

    ``timeout_retry_s``: when > 0, one extra attempt at this ceiling after a full
    first-pass TIMEOUT (B1 / TPC-DS Slow-vs-hung). Default ``0.0`` preserves unit
    tests that sequence only first-pass timeouts; production scoreboard passes 300.
    """
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
            # Drain remaining attempts when no rows yet (E1-L-001); keep prior (C4-L-002).
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
        # Prefer real ERROR over TIMEOUT when a later attempt raised (E2-L-001).
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
            )
        if duck_timed_out and timeout_retry_s > 0:
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
                )
        if not duck_payloads:
            if duck_timed_out:
                return QueryResult(
                    query_nr=query.query_nr,
                    status="TIMEOUT",
                    repark_wall_s=None,
                    duckdb_wall_s=None,
                    ratio=None,
                    repark_rows=None,
                    duckdb_rows=None,
                    error_class="Timeout",
                    error_message=f"DuckDB exceeded {timeout_s}s on all attempts",
                    rewrite_note=query.rewrite_note,
                )
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
            )

    duck_rows = duck_payloads[-1]
    duck_median = statistics.median(duck_times)

    # Subject engine SQL: repark may apply dialect rewrites; Sail must use the
    # canonical DuckDB text so repark-only rewrites never reach the third engine (C2-Q-001).
    subject_sql = query.original_sql if subject_label == "sail" else query.sql_for_repark
    subject_times: list[float] = []
    subject_payloads: list[list[tuple[Any, ...]]] = []
    subject_error: str | None = None
    subject_timed_out = False
    for _ in range(repeats):
        try:
            wall, rows = _timed_call(
                lambda: _subject_collect(spark, subject_sql, subject_label=subject_label),
                timeout_s=timeout_s,
            )
            subject_times.append(wall)
            subject_payloads.append(rows)
        except FuturesTimeout:
            # Keep prior successes for compare; else drain remaining (E1-L-001).
            if subject_payloads:
                break
            subject_timed_out = True
            continue
        except Exception as exc:
            if subject_payloads:
                break
            subject_error = f"{type(exc).__name__}: {exc}"
            break

    timeout_first_s: float | None = None
    timeout_retry_wall: float | None = None

    if not subject_payloads and subject_timed_out and subject_error is None and timeout_retry_s > 0:
        # Greylight refinement: one retry at 300s after 120s TIMEOUT (B1).
        timeout_first_s = timeout_s
        try:
            wall, rows = _timed_call(
                lambda: _subject_collect(spark, subject_sql, subject_label=subject_label),
                timeout_s=timeout_retry_s,
            )
            subject_times.append(wall)
            subject_payloads.append(rows)
            timeout_retry_wall = wall
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
                error_message=(
                    f"{subject_label} exceeded {timeout_s}s and {timeout_retry_s}s retry (hung)"
                ),
                rewrite_note=query.rewrite_note,
                timeout_first_s=timeout_first_s,
                timeout_retry_s=timeout_retry_s,
            )
        except Exception as exc:
            subject_error = f"{type(exc).__name__}: {exc}"

    if not subject_payloads:
        # Prefer ERROR over TIMEOUT when a later attempt raised (E2-L-001).
        if subject_error is not None:
            error_class, hint = classify_error(subject_error)
            return QueryResult(
                query_nr=query.query_nr,
                status="ERROR",
                repark_wall_s=None,
                duckdb_wall_s=duck_median,
                ratio=None,
                repark_rows=None,
                duckdb_rows=len(duck_rows),
                error_class=error_class,
                error_message=subject_error,
                rewrite_note=query.rewrite_note,
                missing_feature_hint=hint,
                timeout_first_s=timeout_first_s,
                timeout_retry_s=timeout_retry_wall,
            )
        if subject_timed_out:
            return QueryResult(
                query_nr=query.query_nr,
                status="TIMEOUT",
                repark_wall_s=None,
                duckdb_wall_s=duck_median,
                ratio=None,
                repark_rows=None,
                duckdb_rows=len(duck_rows),
                error_class="Timeout",
                error_message=f"{subject_label} exceeded {timeout_s}s on all attempts",
                rewrite_note=query.rewrite_note,
                timeout_first_s=timeout_first_s,
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
            error_message=f"no {subject_label} rows",
            rewrite_note=query.rewrite_note,
            missing_feature_hint=hint,
        )

    subject_median = statistics.median(subject_times)
    # Any successful subject payload that disagrees with DuckDB is WRONG-RESULT,
    # even if a later payload matches (E1-L-002). Prefer reporting the first mismatch.
    comparison = None
    for payload in subject_payloads:
        candidate = compare_result_sets(
            payload,
            duck_rows,
            subject_label=subject_label,
        )
        if not candidate.equal:
            comparison = candidate
            break
        comparison = candidate
    if comparison is None:
        msg = f"internal: no subject payloads to compare for {subject_label}"
        raise RuntimeError(msg)
    ratio = subject_median / duck_median if duck_median > 0 else None

    if not comparison.equal:
        return QueryResult(
            query_nr=query.query_nr,
            status="WRONG-RESULT",
            repark_wall_s=subject_median,
            duckdb_wall_s=duck_median,
            ratio=ratio,
            repark_rows=comparison.repark_rows,
            duckdb_rows=comparison.duckdb_rows,
            error_class="WrongResult",
            error_message=comparison.message,
            rewrite_note=query.rewrite_note,
            timeout_first_s=timeout_first_s,
            timeout_retry_s=timeout_retry_wall,
        )

    # Completed only on the 300s retry → Slow TIMEOUT (correctness OK, wall over 120s).
    if timeout_first_s is not None and timeout_retry_wall is not None:
        return QueryResult(
            query_nr=query.query_nr,
            status="TIMEOUT",
            repark_wall_s=subject_median,
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
        )

    return QueryResult(
        query_nr=query.query_nr,
        status="OK",
        repark_wall_s=subject_median,
        duckdb_wall_s=duck_median,
        ratio=ratio,
        repark_rows=comparison.repark_rows,
        duckdb_rows=comparison.duckdb_rows,
        rewrite_note=query.rewrite_note,
    )


def _duckdb_collect(connection: Any, sql: str) -> list[tuple[Any, ...]]:
    return list(connection.execute(sql).fetchall())


def _subject_collect(
    spark: Any,
    sql: str,
    *,
    subject_label: str = "repark",
) -> list[tuple[Any, ...]]:
    """Collect subject-engine rows (repark Arrow path or Sail collect)."""
    if subject_label == "sail":
        from .sail_engine import collect_rows

        return collect_rows(spark, sql)
    return _repark_collect(spark, sql)


def _repark_collect(spark: Any, sql: str) -> list[tuple[Any, ...]]:
    frame = spark.sql(sql)
    # Prefer Arrow path for value fidelity; fall back to collect().
    # Schema-name order (not dict.values()) so projection renames stay stable (C3-L-006).
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
    a ThreadPoolExecutor that cannot cancel a running worker (octo C1-Q-002 / C3-L-002).
    Native code that ignores signals may still overshoot; the timeout is best-effort
    for interruptible Python / many C extensions, and hard-bounds *observed* wall when
    the timer fires.
    """
    if timeout_s <= 0:
        msg = f"timeout_s must be positive, got {timeout_s}"
        raise ValueError(msg)

    def _alarm_handler(  # nested-def: SIGALRM handler closes over the per-query timeout
        _signum: int, _frame: object
    ) -> None:
        raise TimeoutError(f"TPC-H query exceeded {timeout_s}s wall")

    # SIGALRM only works on the main thread of the main interpreter.
    if hasattr(signal, "setitimer") and hasattr(signal, "SIGALRM"):
        previous_handler = signal.getsignal(signal.SIGALRM)
        signal.signal(signal.SIGALRM, _alarm_handler)
        signal.setitimer(signal.ITIMER_REAL, timeout_s)
        started = time.perf_counter()
        # Mutable box so a SIGALRM after function() returns but before the
        # assignment target is written still keeps completed rows (C5-Q-001).
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
    # If the call returns, keep the result (do not map slow-OK → TIMEOUT).
    started = time.perf_counter()
    result = function()
    elapsed = time.perf_counter() - started
    return elapsed, result
