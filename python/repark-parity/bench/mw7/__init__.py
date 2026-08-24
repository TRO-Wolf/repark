"""MW-7 scale-measurement harness (measure only; no engine code).

The package is loaded two ways. ``run_mw7.py`` is the CLI for a full-scale run. The CI
smoke pin (``python/repark/tests/test_mw7_scale_smoke.py``) imports :mod:`measure` as a
synthetic package and runs the same code at a tiny scale, so the numbers in the ledger
come from machinery that a gate exercises on every run.
"""

from __future__ import annotations
