"""Release the cache and create a V2 table through the write door.

pins: ex-19-dataframe-d-window/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.unpersist",
    "DataFrame.writeTo",
    "DataFrame.write_to",
]


def main() -> None:
    """Run the measured unpersist return and the V2 create and read-back answers."""
    repark = ReparkSession.builder.appName("ex-df-d-cache-write").master("local[1]").getOrCreate()
    try:
        base = repark.createDataFrame(
            [
                ("a", 1, 10.0),
                ("a", 2, 20.0),
                ("a", 2, 30.0),
                ("a", 3, 40.0),
                ("b", 1, 50.0),
                ("b", 2, None),
            ],
            ["g", "k", "v"],
        )
        base.cache()
        unpersisted = base.unpersist()
        unpersisted_type = type(unpersisted).__name__
        unpersisted_type_expected = "DataFrame"
        if unpersisted_type != unpersisted_type_expected:
            raise SystemExit(
                f"DataFrame.unpersist type {unpersisted_type!r} != {unpersisted_type_expected!r}"
            )
        recount = base.count()
        recount_expected = 6
        if recount != recount_expected:
            raise SystemExit(f"DataFrame.unpersist count {recount!r} != {recount_expected!r}")

        left = repark.createDataFrame([("a", 1), ("b", 2)], ["g", "k"])
        left.writeTo("ex19_d_v2_target").create()
        read_back = repark.sql("SELECT g, k FROM ex19_d_v2_target")
        read_back_rows = sorted(tuple(row) for row in read_back.collect())
        read_back_expected = [("a", 1), ("b", 2)]
        if read_back_rows != read_back_expected:
            raise SystemExit(f"DataFrame.writeTo rows {read_back_rows!r} != {read_back_expected!r}")

        left.write_to("ex19_d_v2_target_two").create()
        read_two = repark.sql("SELECT g, k FROM ex19_d_v2_target_two")
        read_two_rows = sorted(tuple(row) for row in read_two.collect())
        if read_two_rows != read_back_expected:
            raise SystemExit(f"DataFrame.write_to rows {read_two_rows!r} != {read_back_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
