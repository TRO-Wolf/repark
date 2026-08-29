#!/usr/bin/env python3
"""Compare a TPC-H scoreboard JSON against committed ratio ceilings.

Exit codes:
  0 — all OK queries are within ceiling (or baseline missing a query → skip that query)
  1 — one or more OK queries exceeded ceiling, or scoreboard has WRONG-RESULT/ERROR/DIED
  2 — usage / IO error

Skip-clean for missing fixture is the workflow's job (before this script runs).
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("check_baseline_ratios")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def compare(scoreboard: dict[str, Any], baseline: dict[str, Any]) -> int:
    """Return process exit code after printing a per-query report.

    Fail-closed: empty scoreboard, zero ceiling checks, or scoreboard
    missing any baseline query number is a gate failure — never green-lie "OK (0 queries)".
    """
    ceilings: dict[str, float] = {}
    for query_nr, entry in baseline.get("queries", {}).items():
        if isinstance(entry, dict) and "ceiling" in entry:
            ceilings[str(query_nr)] = float(entry["ceiling"])
        elif isinstance(entry, (int, float)):
            ceilings[str(query_nr)] = float(entry)

    failures: list[str] = []
    queries = scoreboard.get("queries") or []
    if not queries:
        failures.append("scoreboard has no queries (empty or missing)")
    if not ceilings:
        failures.append("baseline has no query ceilings")

    scoreboard_nrs = {str(query.get("query_nr")) for query in queries}
    missing_from_board = sorted(
        (query_nr for query_nr in ceilings if query_nr not in scoreboard_nrs),
        key=lambda value: int(value) if value.isdigit() else 0,
    )
    if missing_from_board:
        failures.append(
            "scoreboard missing baseline queries: " + ", ".join(f"Q{n}" for n in missing_from_board)
        )

    ok_checked = 0
    for query in queries:
        query_nr = str(query.get("query_nr"))
        status = query.get("status")
        ratio = query.get("ratio")
        if status in {"WRONG-RESULT", "ERROR", "DIED"}:
            failures.append(f"Q{query_nr}: status={status} (not OK)")
            continue
        if status in {"TIMEOUT"}:
            failures.append(f"Q{query_nr}: status=TIMEOUT")
            continue
        if status != "OK":
            failures.append(f"Q{query_nr}: unexpected status={status!r}")
            continue
        if query_nr not in ceilings:
            LOGGER.warning("Q%s: no baseline ceiling — skip ratio check", query_nr)
            continue
        if ratio is None:
            failures.append(f"Q{query_nr}: OK but ratio is null")
            continue
        try:
            ratio_value = float(ratio)
        except (TypeError, ValueError):
            failures.append(f"Q{query_nr}: OK but ratio is not a number ({ratio!r})")
            continue
        # NaN is not > ceiling in IEEE/Python — refuse non-finite.
        if not math.isfinite(ratio_value):
            failures.append(f"Q{query_nr}: OK but ratio is non-finite ({ratio_value})")
            continue
        ceiling = ceilings[query_nr]
        ok_checked += 1
        if ratio_value > ceiling:
            failures.append(
                f"Q{query_nr}: ratio {ratio_value:.3f} > PROVISIONAL ceiling {ceiling:.3f}"
            )
        else:
            LOGGER.info(
                "Q%s: ratio %.3f <= ceiling %.3f",
                query_nr,
                ratio_value,
                ceiling,
            )

    if ok_checked == 0 and not any("no queries" in item for item in failures):
        failures.append("no OK queries checked against ceilings (ok_checked=0) — refuse green-lie")

    if failures:
        print("TPC-H SF1 ratio gate FAILED:", file=sys.stderr)
        for line in failures:
            print(f"  - {line}", file=sys.stderr)
        return 1
    print(
        f"TPC-H SF1 ratio gate OK ({ok_checked} queries within PROVISIONAL ceilings; "
        f"baseline provisional={baseline.get('provisional')})"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scoreboard", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )
    if not args.scoreboard.is_file():
        print(f"scoreboard not found: {args.scoreboard}", file=sys.stderr)
        return 2
    if not args.baseline.is_file():
        print(f"baseline not found: {args.baseline}", file=sys.stderr)
        return 2
    return compare(_load_json(args.scoreboard), _load_json(args.baseline))


if __name__ == "__main__":
    raise SystemExit(main())
