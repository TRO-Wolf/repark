"""CLI: ``python -m repark_parity.bench.tpch`` is not wired (bench is not a package root).

Use::

    python python/repark-parity/bench/tpch/run_tpch.py --sf 1
"""

from __future__ import annotations

from .run_tpch import main

if __name__ == "__main__":
    raise SystemExit(main())
