"""W-0 window-shape measurement harness (measure only; no engine code).

Loaded two ways: ``run_w0.py`` for a timed run; the CI pin
(``python/repark-parity/tests/test_w0_window_bench.py``) imports the
engine-free modules so ``make py-test`` exercises the roster, generator,
and result model without the native module.
"""

from __future__ import annotations
