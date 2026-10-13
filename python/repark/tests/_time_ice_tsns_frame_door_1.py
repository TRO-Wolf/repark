"""ICE-TSNS-FRAME-DOOR-1: time ``createDataFrame`` + ``count()`` of a pandas timestamp frame."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from repark import ReparkSession

SEED_NANOS = 1_767_323_045_123_456_789
STEP_NANOS = 1_000_003
DECLARED = "a timestamp, b timestamp"


def frames(rows: int) -> dict[str, pd.DataFrame]:
    """Return the frame with one nanosecond column and its twin with none."""
    nanos = np.arange(rows, dtype="int64") * STEP_NANOS + SEED_NANOS
    micros = pd.Series(nanos // 1_000).astype("datetime64[us]")
    return {
        "no_nanosecond_column": pd.DataFrame({"a": micros, "b": micros}),
        "one_nanosecond_column": pd.DataFrame(
            {"a": micros, "b": pd.Series(nanos).astype("datetime64[ns]")}
        ),
    }


def median_milliseconds(spark: Any, frame: pd.DataFrame, schema: str | None, repeats: int) -> float:
    """Return the median wall time of ``createDataFrame`` + ``count()`` after one warm run."""
    taken: list[float] = []
    for _ in range(repeats + 1):
        start = time.perf_counter()
        made = (
            spark.createDataFrame(frame)
            if schema is None
            else spark.createDataFrame(frame, schema=schema)
        )
        made.count()
        taken.append(time.perf_counter() - start)
    return round(sorted(taken[1:])[repeats // 2] * 1_000, 3)


def main() -> None:
    """Print one JSON object of medians in milliseconds."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--declared-repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    spark = (
        ReparkSession.builder.appName("ice-tsns-frame-door-1-cost")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    made = frames(args.rows)
    result: dict[str, Any] = {"rows": args.rows}
    for name, frame in made.items():
        result[name] = median_milliseconds(spark, frame, None, args.repeats)
    result["one_nanosecond_column_declared_timestamp"] = median_milliseconds(
        spark, made["one_nanosecond_column"], DECLARED, args.declared_repeats
    )
    spark.stop()
    text = json.dumps(result)
    if args.output is not None:
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
