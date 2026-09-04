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
        empty_flag_expected = True
        if empty_flag != empty_flag_expected:
            raise SystemExit(f"DataFrame.isEmpty flag {empty_flag!r} != {empty_flag_expected!r}")
        snake_empty_flag = empty.is_empty()
        snake_empty_flag_expected = True
        if snake_empty_flag != snake_empty_flag_expected:
            raise SystemExit(
                f"DataFrame.is_empty flag {snake_empty_flag!r} != {snake_empty_flag_expected!r}"
            )
        full_flag = frame.isEmpty()
        full_flag_expected = False
        if full_flag != full_flag_expected:
            raise SystemExit(f"DataFrame.isEmpty flag {full_flag!r} != {full_flag_expected!r}")

        streaming = frame.isStreaming
        streaming_expected = False
        if streaming != streaming_expected:
            raise SystemExit(f"DataFrame.isStreaming flag {streaming!r} != {streaming_expected!r}")
        snake_streaming = frame.is_streaming
        snake_streaming_expected = False
        if snake_streaming != snake_streaming_expected:
            raise SystemExit(
                f"DataFrame.is_streaming flag {snake_streaming!r} != {snake_streaming_expected!r}"
            )

        uncached = repark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 3)],
            ["g", "k"],
        )
        before_flag = uncached.is_cached
        before_flag_expected = False
        if before_flag != before_flag_expected:
            raise SystemExit(
                f"DataFrame.is_cached flag {before_flag!r} != {before_flag_expected!r}"
            )
        uncached.cache()
        during_flag = uncached.is_cached
        during_flag_expected = True
        if during_flag != during_flag_expected:
            raise SystemExit(
                f"DataFrame.is_cached flag {during_flag!r} != {during_flag_expected!r}"
            )
        cached_total = uncached.count()
        cached_total_expected = 3
        if cached_total != cached_total_expected:
            raise SystemExit(
                f"DataFrame.is_cached count {cached_total!r} != {cached_total_expected!r}"
            )
        uncached.unpersist()
        after_flag = uncached.is_cached
        after_flag_expected = False
        if after_flag != after_flag_expected:
            raise SystemExit(f"DataFrame.is_cached flag {after_flag!r} != {after_flag_expected!r}")

        persisted = repark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 3)],
            ["g", "k"],
        ).persist()
        persisted_flag = persisted.is_cached
        persisted_flag_expected = True
        if persisted_flag != persisted_flag_expected:
            raise SystemExit(
                f"DataFrame.persist flag {persisted_flag!r} != {persisted_flag_expected!r}"
            )
        persisted_total = persisted.count()
        persisted_total_expected = 3
        if persisted_total != persisted_total_expected:
            raise SystemExit(
                f"DataFrame.persist count {persisted_total!r} != {persisted_total_expected!r}"
            )
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
        checkpoint_total_expected = 6
        if checkpoint_total != checkpoint_total_expected:
            raise SystemExit(
                f"DataFrame.localCheckpoint count {checkpoint_total!r}"
                f" != {checkpoint_total_expected!r}"
            )
        checkpoint_flag = checkpointed.is_cached
        checkpoint_flag_expected = False
        if checkpoint_flag != checkpoint_flag_expected:
            raise SystemExit(
                f"DataFrame.localCheckpoint flag {checkpoint_flag!r}"
                f" != {checkpoint_flag_expected!r}"
            )
        checkpoint_names = checkpointed.columns
        checkpoint_names_expected = ["g", "k", "v"]
        if checkpoint_names != checkpoint_names_expected:
            raise SystemExit(
                f"DataFrame.localCheckpoint columns {checkpoint_names!r}"
                f" != {checkpoint_names_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
