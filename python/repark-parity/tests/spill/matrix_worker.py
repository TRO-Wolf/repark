"""One spill-matrix cell in its own subprocess: session, address-space cap, JSON out."""

from __future__ import annotations

import argparse
import json
import resource
import sys
import time
from pathlib import Path
from typing import Any

from matrix_generators import (
    GENERATOR_ROWS,
    input_select_expr,
    memory_limit_string,
    payload_extra,
)
from matrix_harness import REFUSED, is_loud_refusal


def read_vm_size() -> int:
    """Read this process's current virtual memory size in bytes."""
    for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
        if line.startswith("VmSize:"):
            return int(line.split()[1]) * 1024
    raise OSError("/proc/self/status has no VmSize line")


def apply_cell_address_space_cap(limit_bytes: int) -> tuple[int, int]:
    """Cap RLIMIT_AS at the measured baseline plus 3x the cell limit; return cap and baseline."""
    baseline = read_vm_size()
    cap = baseline + 3 * limit_bytes
    resource.setrlimit(resource.RLIMIT_AS, (cap, cap))
    verified = resource.getrlimit(resource.RLIMIT_AS)[0]
    if verified != cap:
        raise OSError(f"RLIMIT_AS verification failed: set {cap}, read {verified}")
    return cap, baseline


def build_session(limit_bytes: int, partitions: int) -> Any:
    """Build the facade session whose FairSpillPool is the cell limit."""
    from repark import ReparkSession

    builder = ReparkSession.builder.appName("neveroom-1-cell")
    builder = builder.config("datafusion.runtime.memory_limit", memory_limit_string(limit_bytes))
    builder = builder.config("datafusion.execution.target_partitions", str(partitions))
    return builder.getOrCreate()


def register_input(session: Any, target_bytes: int) -> None:
    """Register the in-engine range input whose Arrow bytes hit `target_bytes`."""
    session.range(GENERATOR_ROWS).selectExpr(
        *input_select_expr(payload_extra(target_bytes))
    ).createOrReplaceTempView("base")


def cell_metrics(session: Any, sql: str) -> tuple[int, int]:
    """Execute the cell once through EXPLAIN ANALYZE and total its spill metrics."""
    from spill.plan_metrics import parse_nodes, plan_text_from_rows, total_counter

    rows = session.sql(f"EXPLAIN ANALYZE {sql}").collect()
    totals = parse_nodes(plan_text_from_rows(rows))
    return total_counter(totals, "spilled_bytes"), total_counter(totals, "spill_count")


def write_payload(json_out: Path, payload: dict[str, Any]) -> None:
    """Write the worker's result JSON."""
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse one cell's argv."""
    parser = argparse.ArgumentParser(description="neveroom-1 one-cell worker")
    parser.add_argument("--operator", required=True)
    parser.add_argument("--multiplier", type=int, required=True)
    parser.add_argument("--sql", required=True)
    parser.add_argument("--refusal-names", required=True)
    parser.add_argument("--limit-bytes", type=int, required=True)
    parser.add_argument("--partitions", type=int, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> int:
    """Run one cell and write its JSON payload; exit non-zero on anything unclassifiable."""
    session = build_session(args.limit_bytes, args.partitions)
    register_input(session, args.multiplier * args.limit_bytes)
    cap, baseline = apply_cell_address_space_cap(args.limit_bytes)
    started = time.perf_counter()
    try:
        spill_bytes, spill_count = cell_metrics(session, args.sql)
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        if is_loud_refusal(error, tuple(args.refusal_names.split(","))):
            write_payload(
                args.json_out,
                {
                    "outcome": REFUSED,
                    "operator": args.operator,
                    "multiplier": args.multiplier,
                    "error_type": type(error).__name__,
                    "message": str(error)[:2000],
                    "limit_bytes": args.limit_bytes,
                    "rlimit_as_bytes": cap,
                    "vm_size_at_cap": baseline,
                },
            )
            return 0
        print(
            f"unclassifiable worker failure: {type(error).__name__}: {error}",
            file=sys.stderr,
        )
        return 1
    wall_ms = (time.perf_counter() - started) * 1000.0
    write_payload(
        args.json_out,
        {
            "outcome": "completed",
            "operator": args.operator,
            "multiplier": args.multiplier,
            "spill_bytes": spill_bytes,
            "spill_count": spill_count,
            "wall_ms": wall_ms,
            "limit_bytes": args.limit_bytes,
            "rlimit_as_bytes": cap,
            "vm_size_at_cap": baseline,
        },
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI for one isolated spill-matrix cell."""
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
