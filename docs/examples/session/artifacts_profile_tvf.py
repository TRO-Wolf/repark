"""Driver-local pyfile artifacts, the empty ``profile`` collector, and ``tvf.range``.

pins: session-surface-1/C-005, C-006, C-007
"""

from __future__ import annotations

import importlib
import sys
import tempfile
from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.addArtifact",
    "SparkSession.addArtifacts",
    "SparkSession.profile",
    "SparkSession.tvf",
]


def main() -> None:
    """Land a pyfile on ``sys.path``, no-op the empty profile, read ``tvf.range``."""
    repark = ReparkSession.builder.appName("ex-ses-surface").master("local[1]").getOrCreate()
    try:
        with tempfile.TemporaryDirectory() as staging:
            module_path = Path(staging) / "ses_surface_example_mod.py"
            module_path.write_text("MARKER = 5\n", encoding="utf-8")
            repark.addArtifact(str(module_path), pyfile=True)
            repark.addArtifacts(str(module_path), pyfile=True)
            importlib.invalidate_caches()
            loaded = importlib.import_module("ses_surface_example_mod")
            if loaded.MARKER != 5:
                raise SystemExit(f"artifact MARKER {loaded.MARKER!r} != 5")
            sys.modules.pop("ses_surface_example_mod", None)
        repark.profile.show()
        repark.profile.clear()
        rows = [tuple(row) for row in repark.tvf.range(3).collect()]
        if rows != [(0,), (1,), (2,)]:
            raise SystemExit(f"tvf.range rows {rows!r} != [(0,), (1,), (2,)]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
