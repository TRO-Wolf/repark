"""One spill-matrix cell in its own subprocess: session, address-space cap, JSON out."""

from __future__ import annotations

import argparse
import json
import re
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

_EXEC_NAME = re.compile(r"\b([A-Za-z][A-Za-z0-9_]*Exec)\b")

_NESTED_SELECT: tuple[str, ...] = (
    "id",
    "named_struct('a', id, 'b', payload) AS s",
    "array(payload) AS l",
)


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


def build_session(limit_bytes: int, partitions: int, conf: dict[str, str]) -> Any:
    """Build the facade session whose FairSpillPool is the cell limit."""
    from repark import ReparkSession

    builder = ReparkSession.builder.appName("neveroom-1-cell")
    builder = builder.config("datafusion.runtime.memory_limit", memory_limit_string(limit_bytes))
    builder = builder.config("datafusion.execution.target_partitions", str(partitions))
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def register_input(session: Any, target_bytes: int, view: str = "base") -> None:
    """Register the in-engine range input whose Arrow bytes hit `target_bytes`."""
    session.range(GENERATOR_ROWS).selectExpr(
        *input_select_expr(payload_extra(target_bytes))
    ).createOrReplaceTempView(view)


def register_right_input(session: Any, right: str, target_bytes: int) -> None:
    """Register the join cell's second input under `other`."""
    if right == "sized":
        register_input(session, target_bytes, view="other")
        return
    if right == "key64":
        session.range(64).selectExpr(
            "id",
            "cast(id as double) * 1.5 AS v",
        ).createOrReplaceTempView("other")
        return
    raise ValueError(f"unknown right input {right!r}")


def plan_text(session: Any, analyze: bool, sql: str) -> str:
    """Return the plan text for `sql`, analyzed or not."""
    from spill.plan_metrics import plan_text_from_rows

    verb = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
    return plan_text_from_rows(session.sql(f"{verb} {sql}").collect())


def cell_metrics(session: Any, sql: str) -> tuple[int, int, str]:
    """Execute the cell once through EXPLAIN ANALYZE; return spill totals and plan text."""
    from spill.plan_metrics import parse_nodes, total_counter

    text = plan_text(session, True, sql)
    totals = parse_nodes(text)
    return (
        total_counter(totals, "spilled_bytes"),
        total_counter(totals, "spill_count"),
        text,
    )


def run_cell_body(session: Any, args: argparse.Namespace, sidecar: Path) -> tuple[int, int]:
    """Run the cell's measured operation; return spill bytes and spill count."""
    if args.kind == "flatten":
        flat = session.table("base").selectExpr(*_NESTED_SELECT).dynamicFlatten()
        flat.createOrReplaceTempView("flat")
    sidecar.write_text(plan_text(session, False, args.sql), encoding="utf-8")
    if args.kind == "collect":
        session.sql(args.sql).collect()
        return 0, 0
    if args.kind in ("sql", "flatten"):
        spill_bytes, spill_count, analyzed = cell_metrics(session, args.sql)
        sidecar.write_text(analyzed, encoding="utf-8")
        return spill_bytes, spill_count
    raise ValueError(f"unknown cell kind {args.kind!r}")


def plan_operator_names(text: str) -> str:
    """Return the sorted distinct physical-operator names a plan text carries."""
    return ",".join(sorted(set(_EXEC_NAME.findall(text))))


def write_payload(json_out: Path, payload: dict[str, Any]) -> None:
    """Write the worker's result JSON."""
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse one cell's argv."""
    parser = argparse.ArgumentParser(description="neveroom-1 one-cell worker")
    parser.add_argument("--operator", required=True)
    parser.add_argument("--multiplier", type=int, required=True)
    parser.add_argument("--kind", default="sql")
    parser.add_argument("--sql", required=True)
    parser.add_argument("--refusal-names", required=True)
    parser.add_argument("--conf", default="{}")
    parser.add_argument("--right", default="")
    parser.add_argument("--limit-bytes", type=int, required=True)
    parser.add_argument("--partitions", type=int, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> int:
    """Run one cell and write its JSON payload; exit non-zero on anything unclassifiable."""
    conf = json.loads(args.conf)
    session = build_session(args.limit_bytes, args.partitions, conf)
    target_bytes = args.multiplier * args.limit_bytes
    register_input(session, target_bytes)
    if args.right:
        register_right_input(session, args.right, target_bytes)
    cap, baseline = apply_cell_address_space_cap(args.limit_bytes)
    refusal_names = tuple(name for name in args.refusal_names.split(",") if name)
    sidecar = args.json_out.with_suffix(".plan.txt")
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    try:
        spill_bytes, spill_count = run_cell_body(session, args, sidecar)
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        if is_loud_refusal(error, refusal_names):
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
                    "plan_operators": plan_operator_names(
                        sidecar.read_text(encoding="utf-8") if sidecar.is_file() else ""
                    ),
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
            "plan_operators": plan_operator_names(
                sidecar.read_text(encoding="utf-8") if sidecar.is_file() else ""
            ),
        },
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI for one isolated spill-matrix cell."""
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
