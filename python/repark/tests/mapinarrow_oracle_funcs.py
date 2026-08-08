"""Picklable helpers for the live PySpark mapInArrow oracle (must be a real importable module)."""

from __future__ import annotations

from collections.abc import Iterator


def double_int_batch(batches: Iterator[object]) -> Iterator[object]:
    import pyarrow as pa

    for batch in batches:
        col = batch.column(0)
        doubled = pa.array(
            [None if value is None else int(value) * 2 for value in col.to_pylist()],
            type=pa.int32(),
        )
        yield pa.record_batch([doubled], names=["x"])


def drop_all_batches(batches: Iterator[object]) -> Iterator[object]:
    for _ in batches:
        pass
    return iter(())


def wrong_type_batches(batches: Iterator[object]) -> Iterator[object]:
    """Yield string column named x — type mismatch vs declared INT."""
    import pyarrow as pa

    for batch in batches:
        as_str = pa.array(
            [str(value) for value in batch.column(0).to_pylist()],
            type=pa.string(),
        )
        yield pa.record_batch([as_str], names=["x"])
