"""Package entry: ``python -m`` is not wired for bench roots; use run_write_bench.py.

Delegates to :func:`run_write_bench.main` when invoked as a package module after
sys.path includes the parent of ``write/``.
"""

from __future__ import annotations

from .run_write_bench import main

if __name__ == "__main__":
    raise SystemExit(main())
