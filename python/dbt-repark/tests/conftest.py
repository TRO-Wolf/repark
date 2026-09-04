"""Path wiring: the uninstalled adapter source and the facade test fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_ADAPTER_SOURCE = _REPO_ROOT / "python" / "dbt-repark" / "src"
_FACADE_TESTS = _REPO_ROOT / "python" / "repark" / "tests"

for _path in (_ADAPTER_SOURCE, _FACADE_TESTS):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
