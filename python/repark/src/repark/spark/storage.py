"""StorageLevel markers for DataFrame persistence and caching."""

from __future__ import annotations


class StorageLevel:
    """PySpark-compatible storage level flags (oracle: live 4.1.2 ``pyspark.storagelevel``).

    Flags are accepted and recorded; repark's materialize path is always an in-process
    MemTable. Cosmetic disk / off-heap / replication claims trigger a session-once
    :class:`UserWarning` on ``persist`` (see ``dataframe.persist``).
    """

    def __init__(
        self,
        useDisk: bool,  # noqa: N803 — PySpark arg name
        useMemory: bool,  # noqa: N803
        useOffHeap: bool,  # noqa: N803
        deserialized: bool,
        replication: int = 1,
    ) -> None:
        self.useDisk = bool(useDisk)
        self.useMemory = bool(useMemory)
        self.useOffHeap = bool(useOffHeap)
        self.deserialized = bool(deserialized)
        self.replication = int(replication)

    def __repr__(self) -> str:
        # Live PySpark 4.1.2: "Disk Memory Deserialized 1x Replicated" style (space-separated).
        parts: list[str] = []
        if self.useDisk:
            parts.append("Disk")
        if self.useMemory:
            parts.append("Memory")
        if self.useOffHeap:
            parts.append("Off Heap")
        if self.deserialized:
            parts.append("Deserialized")
        else:
            parts.append("Serialized")
        parts.append(f"{self.replication}x Replicated")
        return " ".join(parts)

    def __eq__(self, other: object) -> bool:
        # Duck-type compare so Apache suite imports of ``pyspark.storagelevel.StorageLevel``
        try:
            return (
                bool(other.useDisk) == self.useDisk  # type: ignore[attr-defined]
                and bool(other.useMemory) == self.useMemory  # type: ignore[attr-defined]
                and bool(other.useOffHeap) == self.useOffHeap  # type: ignore[attr-defined]
                and bool(other.deserialized) == self.deserialized  # type: ignore[attr-defined]
                and int(other.replication) == self.replication  # type: ignore[attr-defined]
            )
        except (AttributeError, TypeError, ValueError):
            return NotImplemented

    def __hash__(self) -> int:
        return hash(
            (
                self.useDisk,
                self.useMemory,
                self.useOffHeap,
                self.deserialized,
                self.replication,
            )
        )


# Class-level constants (same constructor order as PySpark).
StorageLevel.NONE = StorageLevel(False, False, False, False, 1)  # type: ignore[attr-defined]
StorageLevel.DISK_ONLY = StorageLevel(True, False, False, False, 1)  # type: ignore[attr-defined]
StorageLevel.DISK_ONLY_2 = StorageLevel(True, False, False, False, 2)  # type: ignore[attr-defined]
StorageLevel.MEMORY_ONLY = StorageLevel(False, True, False, False, 1)  # type: ignore[attr-defined]
StorageLevel.MEMORY_ONLY_2 = StorageLevel(False, True, False, False, 2)  # type: ignore[attr-defined]
StorageLevel.MEMORY_AND_DISK = StorageLevel(True, True, False, False, 1)  # type: ignore[attr-defined]
StorageLevel.MEMORY_AND_DISK_2 = StorageLevel(True, True, False, False, 2)  # type: ignore[attr-defined]
StorageLevel.MEMORY_AND_DISK_DESER = StorageLevel(  # type: ignore[attr-defined]
    True, True, False, True, 1
)
StorageLevel.OFF_HEAP = StorageLevel(True, True, True, False, 1)  # type: ignore[attr-defined]
