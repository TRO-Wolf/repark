"""Print a frame's rows and order them with sort and sortWithinPartitions.

pins: ex-18-dataframe-c/C-001
"""

from __future__ import annotations

import contextlib
import io

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.show",
    "DataFrame.sort",
    "DataFrame.sortWithinPartitions",
    "DataFrame.sort_within_partitions",
]


def main() -> None:
    """Run the measured show, sort, and single-partition sortWithinPartitions answers."""
    repark = ReparkSession.builder.appName("ex-df-show-sort").master("local[1]").getOrCreate()
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
            frame.show(3)
        shown = printed.getvalue()
        if shown.strip() == "":
            raise SystemExit("DataFrame.show printed nothing")
        table_lines = [line for line in shown.splitlines() if line.startswith("|")]
        if len(table_lines) != 4:
            raise SystemExit(f"DataFrame.show rows {table_lines!r} != 4 lines")
        for cell in ("a", "1", "10.0", "2", "20.0", "30.0"):
            if cell not in shown:
                raise SystemExit(f"DataFrame.show cell {cell!r} missing from {shown!r}")
        full = io.StringIO()
        with contextlib.redirect_stdout(full):
            frame.show(20)
        full_text = full.getvalue()
        full_lines = [line for line in full_text.splitlines() if line.startswith("|")]
        if len(full_lines) != 7:
            raise SystemExit(f"DataFrame.show full table {full_lines!r} != 7 lines")
        if full_text.count("NULL") != 1:
            raise SystemExit(f"DataFrame.show NULL cell missing from {full_text!r}")

        sorted_rows = frame.sort("k", "v").collect()
        sorted_expected = [
            ("a", 1, 10.0),
            ("b", 1, 50.0),
            ("b", 2, None),
            ("a", 2, 20.0),
            ("a", 2, 30.0),
            ("a", 3, 40.0),
        ]
        if sorted_rows != sorted_expected:
            raise SystemExit(f"DataFrame.sort rows {sorted_rows!r} != {sorted_expected!r}")
        desc_rows = frame.sort("k", ascending=False).collect()
        desc_expected = [
            ("a", 3, 40.0),
            ("a", 2, 20.0),
            ("a", 2, 30.0),
            ("b", 2, None),
            ("a", 1, 10.0),
            ("b", 1, 50.0),
        ]
        if desc_rows != desc_expected:
            raise SystemExit(f"DataFrame.sort desc rows {desc_rows!r} != {desc_expected!r}")
        col_desc_rows = frame.sort(F.col("k").desc(), "v").collect()
        col_desc_expected = [
            ("a", 3, 40.0),
            ("b", 2, None),
            ("a", 2, 20.0),
            ("a", 2, 30.0),
            ("a", 1, 10.0),
            ("b", 1, 50.0),
        ]
        if col_desc_rows != col_desc_expected:
            raise SystemExit(f"DataFrame.sort rows {col_desc_rows!r} != {col_desc_expected!r}")

        single = frame.coalesce(1)
        within_rows = single.sortWithinPartitions("k", "v").collect()
        if within_rows != sorted_expected:
            raise SystemExit(
                f"DataFrame.sortWithinPartitions rows {within_rows!r} != {sorted_expected!r}"
            )
        snake_rows = single.sort_within_partitions("k", "v").collect()
        if snake_rows != sorted_expected:
            raise SystemExit(
                f"DataFrame.sort_within_partitions rows {snake_rows!r} != {sorted_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
