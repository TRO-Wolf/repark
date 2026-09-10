"""Materialize a lazy plan with eager, compute, and lazy.

pins: df-eager-1/C-001, C-002, C-003, C-004
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.compute",
    "DataFrame.eager",
    "DataFrame.lazy",
]


def main() -> None:
    """Run the measured eager shape, compute identity, and lazy round-trip."""
    repark = ReparkSession.builder.appName("ex-df-lazy-eager").master("local[1]").getOrCreate()
    try:
        frame = repark.sql("SELECT 1 AS id UNION ALL SELECT 2 AS id UNION ALL SELECT 3 AS id")
        eager = frame.eager()
        if eager is frame:
            raise SystemExit("DataFrame.eager must answer a new frame")
        if (eager.count(), len(eager.columns)) != (3, 1):
            raise SystemExit(
                f"DataFrame.eager shape {(eager.count(), len(eager.columns))!r} != (3, 1)"
            )
        if frame.is_cached:
            raise SystemExit("DataFrame.eager must leave the source frame uncached")

        via_compute = frame.compute()
        if (via_compute.count(), len(via_compute.columns)) != (3, 1):
            raise SystemExit("DataFrame.compute must answer the same shape as eager")
        if type(frame).compute is not type(frame).eager:
            raise SystemExit("DataFrame.compute is not DataFrame.eager")

        if frame.lazy() is not frame:
            raise SystemExit("DataFrame.lazy on a lazy frame must return self")
        back = eager.lazy()
        if back is eager:
            raise SystemExit("DataFrame.lazy on an eager frame must answer a copy")
        if back.count() != 3 or eager.count() != 3:
            raise SystemExit("DataFrame.lazy copy must answer the same rows")
        if sorted(row.id for row in back.collect()) != [1, 2, 3]:
            raise SystemExit("DataFrame.lazy copy answers the wrong rows")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
