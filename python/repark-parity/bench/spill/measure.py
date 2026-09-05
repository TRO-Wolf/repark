"""Drive the H3-SPILL-1 matrix: one subprocess per cell, peak RSS polled from /proc."""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from spill.models import CellRecord, MatrixReport, NodeMetrics  # noqa: E402
from spill.plan_metrics import COUNTERS  # noqa: E402
from spill.roster import POOLS, ROSTER, SCALES  # noqa: E402

DEFAULT_AS_CAP = 12 * 1024 * 1024 * 1024
NONDETERMINISTIC: frozenset[str] = frozenset(
    {"spilled", "degraded", "clean_error", "abort", "abort_at_cap", "internal_error", "error"}
)


def read_vmhwm(pid: int) -> int:
    """Peak resident set of `pid` in bytes, or 0 once the process is gone."""
    try:
        text = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
    except OSError:
        return 0
    for line in text.splitlines():
        if line.startswith("VmHWM:"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                return int(parts[1]) * 1024
    return 0


def host_header() -> dict[str, Any]:
    """The machine facts a baseline number is meaningless without."""
    return {
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "load_1m": os.getloadavg()[0],
        "python": platform.python_version(),
    }


def _worker_command(
    cell: tuple[str, str, int],
    *,
    as_cap: int,
    partitions: int,
    out: Path,
    warehouse: Path,
    digest: bool,
) -> list[str]:
    """Build the argv for one cell worker."""
    operator, pool, scale = cell
    argv = [
        sys.executable,
        "-m",
        "spill.cell_worker",
        "--operator",
        operator,
        "--pool",
        pool,
        "--scale",
        str(scale),
        "--partitions",
        str(partitions),
        "--as-cap-bytes",
        str(as_cap),
        "--warehouse",
        str(warehouse),
        "--json-out",
        str(out),
    ]
    if digest:
        argv.append("--digest")
    return argv


def _outcome_from_signal(returncode: int, stderr: str) -> str:
    """A killed worker is an abort; a cap-driven allocation failure is abort_at_cap."""
    markers = ("memory allocation of", "Cannot allocate memory", "MemoryError", "std::bad_alloc")
    if any(marker in stderr for marker in markers):
        return "abort_at_cap"
    return "abort"


def _nodes_from(payload: dict[str, Any]) -> list[NodeMetrics]:
    """Turn the worker's per-class counter map into typed rows."""
    nodes: list[NodeMetrics] = []
    for name, bucket in sorted(payload.get("nodes", {}).items()):
        fields = {counter: int(bucket.get(counter, 0)) for counter in COUNTERS}
        nodes.append(NodeMetrics(node=name, instances=int(bucket.get("instances", 0)), **fields))
    return nodes


def run_cell(
    cell: tuple[str, str, int],
    *,
    as_cap: int,
    partitions: int,
    scratch: Path,
    timeout_s: float,
    digest: bool,
) -> CellRecord:
    """Run one cell in a fresh subprocess and record outcome, RSS, wall and load."""
    operator, pool, scale = cell
    tag = f"{operator}-{pool}-{scale}"
    out = scratch / f"{tag}.json"
    warehouse = scratch / "wh" / tag
    warehouse.mkdir(parents=True, exist_ok=True)
    out.unlink(missing_ok=True)
    argv = _worker_command(
        cell, as_cap=as_cap, partitions=partitions, out=out, warehouse=warehouse, digest=digest
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(_BENCH_DIR)
    env["TMPDIR"] = str(scratch / "tmp")
    (scratch / "tmp").mkdir(parents=True, exist_ok=True)
    load_start = os.getloadavg()[0]
    started = time.perf_counter()
    proc = subprocess.Popen(
        argv, cwd=str(_BENCH_DIR), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    peak = _drain_with_timeout(proc, timeout_s)
    stderr = proc.stderr.read().decode("utf-8", "replace") if proc.stderr else ""
    wall_ms = (time.perf_counter() - started) * 1000.0
    load_end = os.getloadavg()[0]
    record = CellRecord(
        operator=operator,
        pool=pool,
        scale=scale,
        outcome="error",
        peak_rss_bytes=peak,
        wall_ms=wall_ms,
        load_start=load_start,
        load_end=load_end,
        as_cap_bytes=as_cap,
        returncode=proc.returncode,
    )
    if out.is_file():
        payload = json.loads(out.read_text(encoding="utf-8"))
        record.outcome = str(payload.get("outcome", "error"))
        record.message = payload.get("message")
        record.answer_digest = payload.get("answer_digest")
        record.digest_error = payload.get("digest_error")
        record.rows_out = payload.get("rows_out")
        record.nodes = _nodes_from(payload)
        record.spill_count = sum(node.spill_count for node in record.nodes)
        record.spilled_bytes = sum(node.spilled_bytes for node in record.nodes)
        record.degraded_rows = sum(node.skipped_aggregation_rows for node in record.nodes)
        worker_rss = payload.get("peak_rss_bytes")
        if isinstance(worker_rss, int):
            record.peak_rss_bytes = max(peak, worker_rss)
        if payload.get("wall_ms") is not None:
            record.wall_ms = float(payload["wall_ms"])
    elif proc.returncode is not None and proc.returncode < 0:
        record.outcome = _outcome_from_signal(proc.returncode, stderr)
        record.message = stderr[-600:]
    else:
        record.message = stderr[-600:] or "worker produced no result"
    return record


def _drain_with_timeout(proc: subprocess.Popen[bytes], timeout_s: float) -> int:
    """Poll VmHWM and kill the worker if it outlives `timeout_s`."""
    peak = 0
    deadline = time.perf_counter() + timeout_s
    while proc.poll() is None:
        peak = max(peak, read_vmhwm(proc.pid))
        if time.perf_counter() > deadline:
            proc.kill()
            break
        time.sleep(0.05)
    proc.wait()
    return max(peak, read_vmhwm(proc.pid))


def _plan(operators: list[str], pools: list[str], scales: list[int]) -> list[tuple[str, str, int]]:
    """Every (operator, pool, scale) cell, unbounded pool first so digests exist."""
    ordered = sorted(pools, key=lambda pool: 0 if pool == "none" else 1)
    return [
        (operator, pool, scale) for operator in operators for scale in scales for pool in ordered
    ]


def _apply_answer_check(records: list[CellRecord]) -> None:
    """Mark a bounded cell `wrong` when its digest differs from the unbounded run."""
    baseline: dict[tuple[str, int], str] = {}
    for record in records:
        if record.pool == "none" and record.answer_digest:
            baseline[(record.operator, record.scale)] = record.answer_digest
    for record in records:
        if record.pool == "none" or not record.answer_digest:
            continue
        expected = baseline.get((record.operator, record.scale))
        if expected is not None and expected != record.answer_digest:
            record.outcome = "wrong"


def run_matrix(args: argparse.Namespace) -> MatrixReport:
    """Run every planned cell, writing the report after each one."""
    scratch = Path(args.scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    operators = args.operators or [spec.operator for spec in ROSTER]
    pools = args.pools or list(POOLS)
    scales = args.scales or list(SCALES)
    report = MatrixReport(host=host_header())
    out_path = Path(args.json_out)
    for cell in _plan(operators, pools, scales):
        record = run_cell(
            cell,
            as_cap=args.as_cap_bytes,
            partitions=args.partitions,
            scratch=scratch,
            timeout_s=args.cell_timeout_s,
            digest=not args.no_digest,
        )
        record.runs = [record.outcome]
        if record.outcome in NONDETERMINISTIC and args.repeats > 1:
            for _ in range(args.repeats - 1):
                repeat = run_cell(
                    cell,
                    as_cap=args.as_cap_bytes,
                    partitions=args.partitions,
                    scratch=scratch,
                    timeout_s=args.cell_timeout_s,
                    digest=not args.no_digest,
                )
                record.runs.append(repeat.outcome)
        report.cells.append(record)
        _apply_answer_check(report.cells)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report.model_dump_json(indent=1), encoding="utf-8")
        print(
            f"{cell[0]:22s} {cell[1]:5s} {cell[2]:>9d}  {record.outcome:14s} "
            f"rss={record.peak_rss_bytes} wall={record.wall_ms:.0f}ms "
            f"spill={record.spill_count}",
            flush=True,
        )
    return report


def main(argv: list[str] | None = None) -> int:
    """CLI for the whole matrix or any slice of it."""
    parser = argparse.ArgumentParser(description="H3-SPILL-1 matrix driver")
    parser.add_argument("--operators", nargs="*", default=None)
    parser.add_argument("--pools", nargs="*", default=None)
    parser.add_argument("--scales", nargs="*", type=int, default=None)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--as-cap-bytes", type=int, default=DEFAULT_AS_CAP)
    parser.add_argument("--cell-timeout-s", type=float, default=900.0)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--no-digest", action="store_true")
    parser.add_argument("--scratch", required=True)
    parser.add_argument("--json-out", required=True)
    args = parser.parse_args(argv)
    run_matrix(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
