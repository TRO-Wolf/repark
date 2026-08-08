#!/usr/bin/env python3
"""CLI entry for the seeded SQL differential fuzzer (R-SQL-FUZZER / D3).

Usage::

    # Smoke (default seed 42, 200 queries):
    python python/repark-parity/bench/fuzz/run_fuzz.py

    # Long pass:
    REPARK_FUZZ_N=5000 python python/repark-parity/bench/fuzz/run_fuzz.py \\
        --out /tmp/fuzz-long.json --bank

    # Explicit seed:
    python …/run_fuzz.py --seed 7 --n 100

Determinism: seed is CLI ``--seed`` / env ``REPARK_FUZZ_SEED`` / default ``42``.
Never time-based. Never touches AWS.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="RePark vs DuckDB seeded SQL fuzzer")
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="integer seed (default: env REPARK_FUZZ_SEED or 42)",
    )
    parser.add_argument(
        "--n",
        type=int,
        default=None,
        help="query count (default: env REPARK_FUZZ_N or 200)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="write JSON run result",
    )
    parser.add_argument(
        "--bank",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="bank minimized WRONG-RESULT repros (default: on)",
    )
    parser.add_argument(
        "--repros-dir",
        type=Path,
        default=None,
        help="directory for banked repros (default: bench/fuzz/repros)",
    )
    parser.add_argument(
        "--minimize",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="run greedy minimizer on WRONG-RESULT (default: on)",
    )
    parser.add_argument(
        "--stop-on-first-wrong",
        action="store_true",
        help="stop after the first WRONG-RESULT (debug)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="DEBUG logging",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # Allow `python path/to/run_fuzz.py` without installing the package (C8-Q-001).
    fuzz_dir = Path(__file__).resolve().parent
    if str(fuzz_dir) not in sys.path:
        sys.path.insert(0, str(fuzz_dir))

    from bench.fuzz.bank import default_repros_dir
    from bench.fuzz.runner import run_fuzzer

    repros_dir = args.repros_dir if args.repros_dir is not None else default_repros_dir()
    result = run_fuzzer(
        seed=args.seed,
        count=args.n,
        bank=args.bank,
        repros_dir=repros_dir if args.bank else None,
        minimize=args.minimize,
        stop_on_first_wrong=args.stop_on_first_wrong,
    )

    census = result.census()
    print(
        f"seed={census['seed']} n={census['query_count']} "
        f"OK={census['ok']} WRONG={census['wrong_result']} ERROR={census['error']} "
        f"banked={census['banked_repros']} wall_s={census['wall_s']}"
    )
    if census["error_classes"]:
        print("error_classes:")
        for name, count in census["error_classes"].items():
            print(f"  {count:4d}  {name}")
    if census["wrong_indices"]:
        print(f"wrong_indices: {census['wrong_indices'][:50]}")

    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(
            json.dumps(result.to_json_obj(), indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        print(f"wrote {args.out}")

    # Exit codes: 0 all OK (errors are census, not gate-fail for long runs);
    # smoke tests assert their own bar. CLI returns 0 for completed run, 2 on usage.
    # WRONG-RESULT does not flip the process red here — banking is the deliverable;
    # the smoke test decides the CI pin policy.
    return 0


if __name__ == "__main__":
    # Script invocation: put the repark-parity root (the `bench` package parent) on the
    # path so `bench.fuzz.*` package imports resolve — bank/runner use package-relative
    # imports internally, so the fuzz DIR itself must never be the import root.
    _parity_root = Path(__file__).resolve().parents[2]
    if str(_parity_root) not in sys.path:
        sys.path.insert(0, str(_parity_root))
    raise SystemExit(main())
