"""Print one frame's schema tree to stdout and assert the captured tree lines.

pins: ex-16-dataframe-b/C-001
"""

from __future__ import annotations

import contextlib
import io

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.printSchema",
    "DataFrame.print_schema",
]

TREE_LINES = [
    "root",
    " |-- g: string (nullable = true)",
    " |-- k: long (nullable = true)",
    " |-- v: double (nullable = true)",
]


def main() -> None:
    """Run the measured schema-tree print under both spellings."""
    repark = ReparkSession.builder.appName("ex-df-b-print-schema").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 1, 10.0),
                ("a", 2, 20.0),
                ("a", 2, 30.0),
                ("a", 3, 40.0),
                ("b", 1, 50.0),
                ("b", 2, None),
            ],
            ["g", "k", "v"],
        )
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            frame.printSchema()
        printed_lines = printed.getvalue().rstrip("\n").splitlines()
        if printed_lines != TREE_LINES:
            raise SystemExit(f"DataFrame.printSchema lines {printed_lines!r} != {TREE_LINES!r}")

        snake_printed = io.StringIO()
        with contextlib.redirect_stdout(snake_printed):
            frame.print_schema()
        snake_lines = snake_printed.getvalue().rstrip("\n").splitlines()
        if snake_lines != TREE_LINES:
            raise SystemExit(f"DataFrame.print_schema lines {snake_lines!r} != {TREE_LINES!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
