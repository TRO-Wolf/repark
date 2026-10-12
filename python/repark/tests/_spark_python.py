from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path


def resolve_live_spark_python() -> str:
    override = os.environ.get("REPARK_SPARKENV_PYTHON")
    if override:
        return override
    boxed = Path(tempfile.gettempdir()) / "sparkenv" / "bin" / "python"
    if boxed.exists():
        return str(boxed)
    return sys.executable
