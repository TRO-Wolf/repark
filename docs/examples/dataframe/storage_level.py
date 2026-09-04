"""Report a frame's storage level across the cache lifecycle.

pins: ex-18-dataframe-c/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession
from repark.spark.storage import StorageLevel

COVERS: list[str] = ["DataFrame.storageLevel", "DataFrame.storage_level"]


def main() -> None:
    """Run the measured storageLevel answers: NONE, MEMORY_AND_DISK_DESER, NONE again."""
    repark = ReparkSession.builder.appName("ex-df-storage-level").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([("a", 1), ("b", 2)], ["g", "k"])
        before = frame.storageLevel
        if before != StorageLevel.NONE:
            raise SystemExit(f"DataFrame.storageLevel {before!r} != StorageLevel.NONE")
        snake_before = frame.storage_level
        if snake_before != StorageLevel.NONE:
            raise SystemExit(f"DataFrame.storage_level {snake_before!r} != StorageLevel.NONE")

        cached = frame.cache()
        after = cached.storageLevel
        if after != StorageLevel.MEMORY_AND_DISK_DESER:
            raise SystemExit(
                f"DataFrame.storageLevel {after!r} != StorageLevel.MEMORY_AND_DISK_DESER"
            )

        cleared = frame.unpersist()
        cleared_level = cleared.storageLevel
        if cleared_level != StorageLevel.NONE:
            raise SystemExit(f"DataFrame.storageLevel {cleared_level!r} != StorageLevel.NONE")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
