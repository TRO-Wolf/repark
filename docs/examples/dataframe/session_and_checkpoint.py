"""Owning-session identity, the classic-only executionInfo refusal, and checkpoint.

pins: df-surface-a-1/C-003, C-004, C-005
"""

from __future__ import annotations

from repark.errors import PySparkValueError
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.isLocal",
    "DataFrame.executionInfo",
    "DataFrame.sparkSession",
    "DataFrame.checkpoint",
]


def main() -> None:
    """Run the measured isLocal / executionInfo / sparkSession / checkpoint answers."""
    repark = (
        ReparkSession.builder.appName("ex-df-session-checkpoint").master("local[1]").getOrCreate()
    )
    try:
        frame = repark.createDataFrame([("x", 1, 2), ("y", 3, 4)], "key string, a int, b int")
        if frame.sparkSession is not repark:
            raise SystemExit("sparkSession is not the owning session")
        if frame.isLocal() is not False:
            raise SystemExit("isLocal did not answer False")
        try:
            _ = frame.executionInfo
        except PySparkValueError as error:
            if error.getCondition() != "CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF":
                raise SystemExit(f"executionInfo condition {error.getCondition()!r}") from error
        else:
            raise SystemExit("executionInfo did not raise")
        checkpointed = frame.checkpoint()
        if checkpointed is frame:
            raise SystemExit("checkpoint returned the same frame")
        if [row.asDict() for row in checkpointed.collect()] != [
            {"key": "x", "a": 1, "b": 2},
            {"key": "y", "a": 3, "b": 4},
        ]:
            raise SystemExit("checkpoint rows differ")
        lazy = frame.checkpoint(eager=False)
        if [row.asDict() for row in lazy.collect()] != [
            {"key": "x", "a": 1, "b": 2},
            {"key": "y", "a": 3, "b": 4},
        ]:
            raise SystemExit("lazy checkpoint rows differ")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
