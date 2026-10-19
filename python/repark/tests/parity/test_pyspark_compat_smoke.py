"""C2 compat smoke — subprocess wrapper.

The real suite (``python/repark-parity/compat/smoke_suite.py``) must run in a pristine
interpreter: the redirect permanently patches the ``pyspark`` namespace, and the ML oracles
boot a JVM when Java is present — in-process coexistence is order-dependent in both directions.
The subprocess gives the redirect a clean ``sys.modules`` and shields this suite from any live
JVM in the outer process.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

# The inner suite importorskips pyspark; without it pytest exits 5, which the rc==0 assert
# would misread as a failure (wheel-smoke CI has no pyspark). Skip at wrapper level too.
pytest.importorskip("pyspark")

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SUITE = _REPO_ROOT / "python" / "repark-parity" / "compat" / "smoke_suite.py"


def test_compat_smoke_suite_in_subprocess() -> None:
    """Run the full C2 smoke suite (meta-pins + 25 pinned Apache tests) isolated."""
    env = os.environ.copy()
    env.pop("SPARK_HOME", None)
    src_paths = [
        str(_REPO_ROOT / "python" / "repark" / "src"),
        str(_REPO_ROOT / "python" / "repark-parity" / "src"),
        str(_REPO_ROOT / "python" / "repark-parity"),
    ]
    env["PYTHONPATH"] = os.pathsep.join(
        src_paths + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])
    )
    result = subprocess.run(
        [sys.executable, "-m", "pytest", str(_SUITE), "-q", "--no-header"],
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
        check=False,
        cwd=str(_REPO_ROOT),
    )
    tail = "\n".join((result.stdout + result.stderr).splitlines()[-15:])
    assert result.returncode == 0, f"compat smoke suite failed in subprocess:\n{tail}"
    assert " passed" in result.stdout, tail
