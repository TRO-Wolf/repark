"""MW7 scale-measurement harness (measure only; no engine code).

Loaded two ways: ``run_mw7.py`` for a full-scale run; the CI smoke pin
(``python/repark/tests/test_mw7_scale_smoke.py``) imports :mod:`measure` as a synthetic
package at tiny scale, so a gate exercises the same machinery on every run.
"""

from __future__ import annotations
