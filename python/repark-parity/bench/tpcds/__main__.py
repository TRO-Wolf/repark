"""CLI: ``python -m repark_parity.bench.tpcds`` is not wired (bench is not a package root).

Use::

    python python/repark-parity/bench/tpcds/run_tpcds.py --sf 1
"""

from __future__ import annotations

from .run_tpcds import main

if __name__ == "__main__":
    raise SystemExit(main())
