"""Batch answers for the streaming-named surface: watermarks and watermark dedup.

pins: df-stream-batch-1/C-002, C-003
"""

from __future__ import annotations

from repark.errors import AnalysisException
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.dropDuplicatesWithinWatermark",
    "DataFrame.drop_duplicates_within_watermark",
    "DataFrame.withWatermark",
    "DataFrame.with_watermark",
]


def main() -> None:
    """Run the measured batch answers for the four streaming-named members."""
    repark = (
        ReparkSession.builder.appName("ex-df-batch-streaming-names")
        .master("local[1]")
        .getOrCreate()
    )
    try:
        frame = repark.createDataFrame([(1, 2), (3, 4)], ["k", "v"])
        stamped = frame.withWatermark("k", "1 minute")
        if stamped.columns != ["k", "v"]:
            raise SystemExit(f"DataFrame.withWatermark columns {stamped.columns!r} != ['k', 'v']")
        stamped_rows = [tuple(row) for row in stamped.collect()]
        stamped_expected = [(1, 2), (3, 4)]
        if stamped_rows != stamped_expected:
            raise SystemExit(
                f"DataFrame.withWatermark rows {stamped_rows!r} != {stamped_expected!r}"
            )
        stamped_snake = frame.with_watermark("k", "0 seconds")
        if stamped_snake.columns != ["k", "v"]:
            raise SystemExit(
                f"DataFrame.with_watermark columns {stamped_snake.columns!r} != ['k', 'v']"
            )
        refused_camel = None
        try:
            frame.dropDuplicatesWithinWatermark(["k"])
        except AnalysisException as error:
            refused_camel = error
        first_line = "" if refused_camel is None else str(refused_camel).splitlines()[0]
        expected_line = (
            "dropDuplicatesWithinWatermark is not supported with batch DataFrames/DataSets;"
        )
        if first_line != expected_line:
            raise SystemExit(
                f"DataFrame.dropDuplicatesWithinWatermark message {first_line!r} != "
                f"{expected_line!r}"
            )
        refused_snake = None
        try:
            frame.drop_duplicates_within_watermark()
        except AnalysisException as error:
            refused_snake = error
        snake_message = "" if refused_snake is None else str(refused_snake)
        if not snake_message.startswith("dropDuplicatesWithinWatermark is not"):
            raise SystemExit(
                "DataFrame.drop_duplicates_within_watermark message "
                f"{snake_message!r} lost the batch refusal"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
