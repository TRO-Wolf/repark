"""Write hive-partitioned CSV files: directory layout and per-partition content."""

from __future__ import annotations

import tempfile
from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameWriter.partitionBy",
    "DataFrameWriter.partition_by",
]


def data_bytes(root: Path, suffix: str) -> list[str]:
    return sorted(
        path.read_text(encoding="utf-8") for path in root.rglob(f"*{suffix}") if path.is_file()
    )


def main() -> None:
    """Run the measured partitioned write arms and assert layout plus bytes."""
    repark = ReparkSession.builder.appName("ex26-partition").master("local[1]").getOrCreate()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            root = Path(workdir)
            frame = repark.createDataFrame([(1, "a"), (2, "b")], "id INT, name STRING")
            camel = root / "ex26_camel_part"
            frame.write.mode("overwrite").partitionBy("name").option("header", "false").csv(
                str(camel)
            )
            camel_dirs = sorted(path.name for path in camel.iterdir() if path.is_dir())
            if camel_dirs != ["name=a", "name=b"]:
                raise SystemExit(f"partitionBy dirs {camel_dirs!r} != ['name=a', 'name=b']")
            camel_bytes = data_bytes(camel, ".csv")
            camel_expected = ["1\n", "2\n"]
            if camel_bytes != camel_expected:
                raise SystemExit(f"partitionBy bytes {camel_bytes!r} != {camel_expected!r}")
            snake = root / "ex26_snake_part"
            frame.write.mode("overwrite").partition_by("name").option("header", "false").csv(
                str(snake)
            )
            snake_dirs = sorted(path.name for path in snake.iterdir() if path.is_dir())
            if snake_dirs != ["name=a", "name=b"]:
                raise SystemExit(f"partition_by dirs {snake_dirs!r} != ['name=a', 'name=b']")
            snake_bytes = data_bytes(snake, ".csv")
            if snake_bytes != camel_expected:
                raise SystemExit(f"partition_by bytes {snake_bytes!r} != {camel_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
