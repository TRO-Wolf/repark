"""``python -m`` entry shim for the SQL fuzzer package."""

from __future__ import annotations

# ``python -m bench.fuzz`` runs in real package context; import package-relative.
# Never inject the fuzz dir itself: bank/runner relative imports need the package root.
from bench.fuzz.run_fuzz import main

if __name__ == "__main__":
    raise SystemExit(main())
