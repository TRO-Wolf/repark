"""Build the session from a repark.toml file and read the file values back.

pins: cfg-1/C-026
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.Builder.config_file",
    "SparkSession.Builder.configFile",
]


def check_file_values(repark: ReparkSession) -> None:
    """Assert the conf value and the display style the file sets."""
    probe = repark.conf.get("example.probe.key")
    if probe != "from-file":
        raise SystemExit(f"example.probe.key {probe!r} != 'from-file'")

    style = repark.display_style
    if style != "spark":
        raise SystemExit(f"display_style {style!r} != 'spark'")


def main() -> None:
    """Build once per spelling and read the file values back each time."""
    with tempfile.TemporaryDirectory() as directory:
        config_path = str(Path(directory) / "repark.toml")
        Path(config_path).write_text(
            '[default.conf]\nexample.probe.key = "from-file"\n[default.display]\nstyle = "spark"\n',
            encoding="utf-8",
        )
        repark = ReparkSession.builder.config_file(config_path).getOrCreate()
        try:
            check_file_values(repark)
        finally:
            repark.stop()

        twin = ReparkSession.builder.configFile(config_path).getOrCreate()
        try:
            check_file_values(twin)
        finally:
            twin.stop()


if __name__ == "__main__":
    main()
