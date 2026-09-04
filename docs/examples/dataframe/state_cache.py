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
        assert empty.isEmpty() is True
        assert empty.is_empty() is True
        assert frame.isEmpty() is False

        assert frame.isStreaming is False
        assert frame.is_streaming is False

        uncached = repark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 3)],
            ["g", "k"],
        )
        assert uncached.is_cached is False
        uncached.cache()
        assert uncached.is_cached is True
        assert uncached.count() == 3
        uncached.unpersist()
        assert uncached.is_cached is False

        persisted = repark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 3)],
            ["g", "k"],
        ).persist()
        assert persisted.is_cached is True
        assert persisted.count() == 3
        assert set(persisted.collect()) == {("a", 1), ("a", 2), ("b", 3)}

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
        assert checkpointed.count() == 6
        assert checkpointed.is_cached is False
        assert checkpointed.columns == ["g", "k", "v"]
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
