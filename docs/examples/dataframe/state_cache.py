"""Materialization and frame state: emptiness, streaming flags, cache, checkpoint.

pins: ex-16-dataframe-b/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.isEmpty",
    "DataFrame.is_empty",
    "DataFrame.isStreaming",
    "DataFrame.is_streaming",
    "DataFrame.is_cached",
    "DataFrame.persist",
    "DataFrame.localCheckpoint",
]


def main() -> None:
    """Run the measured state answers: emptiness, streaming flags, and the cache arc."""
    repark = ReparkSession.builder.appName("ex-df-b-state").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 3)],
            ["g", "k"],
        )
        empty = repark.createDataFrame([], "n long")
        empty_flag = empty.isEmpty()
        if empty_flag is not True:
            raise SystemExit(f"DataFrame.isEmpty flag {empty_flag!r} != True")
        snake_empty_flag = empty.is_empty()
        if snake_empty_flag is not True:
            raise SystemExit(f"DataFrame.is_empty flag {snake_empty_flag!r} != True")
        full_flag = frame.isEmpty()
        if full_flag is not False:
            raise SystemExit(f"DataFrame.isEmpty flag {full_flag!r} != False")

        streaming = frame.isStreaming
        if streaming is not False:
            raise SystemExit(f"DataFrame.isStreaming flag {streaming!r} != False")
        snake_streaming = frame.is_streaming
        if snake_streaming is not False:
            raise SystemExit(f"DataFrame.is_streaming flag {snake_streaming!r} != False")

        uncached = repark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 3)],
            ["g", "k"],
        )
        before_flag = uncached.is_cached
        if before_flag is not False:
            raise SystemExit(f"DataFrame.is_cached flag {before_flag!r} != False")
        uncached.cache()
        during_flag = uncached.is_cached
        if during_flag is not True:
            raise SystemExit(f"DataFrame.is_cached flag {during_flag!r} != True")
        cached_total = uncached.count()
        if cached_total != 3:
            raise SystemExit(f"DataFrame.is_cached count {cached_total!r} != 3")
        uncached.unpersist()
        after_flag = uncached.is_cached
        if after_flag is not False:
            raise SystemExit(f"DataFrame.is_cached flag {after_flag!r} != False")

        persisted = repark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 3)],
            ["g", "k"],
        ).persist()
        persisted_flag = persisted.is_cached
        if persisted_flag is not True:
            raise SystemExit(f"DataFrame.persist flag {persisted_flag!r} != True")
        persisted_total = persisted.count()
        if persisted_total != 3:
            raise SystemExit(f"DataFrame.persist count {persisted_total!r} != 3")
        persisted_rows = set(persisted.collect())
        persisted_expected = {("a", 1), ("a", 2), ("b", 3)}
        if persisted_rows != persisted_expected:
            raise SystemExit(f"DataFrame.persist rows {persisted_rows!r} != {persisted_expected!r}")

        checkpointed = repark.createDataFrame(
            [
                ("a", 1, 10.0),
                ("a", 2, 20.0),
                ("a", 2, 30.0),
                ("a", 3, 40.0),
                ("b", 1, 50.0),
                ("b", 2, None),
            ],
            ["g", "k", "v"],
        ).localCheckpoint()
        checkpoint_total = checkpointed.count()
        if checkpoint_total != 6:
            raise SystemExit(f"DataFrame.localCheckpoint count {checkpoint_total!r} != 6")
        checkpoint_flag = checkpointed.is_cached
        if checkpoint_flag is not False:
            raise SystemExit(f"DataFrame.localCheckpoint flag {checkpoint_flag!r} != False")
        checkpoint_names = checkpointed.columns
        if checkpoint_names != ["g", "k", "v"]:
            raise SystemExit(
                f"DataFrame.localCheckpoint columns {checkpoint_names!r} != ['g', 'k', 'v']"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
