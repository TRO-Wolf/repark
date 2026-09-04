"""V2 append by name, dynamic partition overwrite, and writer options on a local catalog.

pins: ex-22-types-writerv2/C-001
"""

from __future__ import annotations

from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameWriterV2.append",
    "DataFrameWriterV2.overwritePartitions",
    "DataFrameWriterV2.overwrite_partitions",
    "DataFrameWriterV2.option",
    "DataFrameWriterV2.options",
]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Run the measured append, overwritePartitions, and option answers."""
    repark = ReparkSession.builder.appName("ex-w2-append").master("local[1]").getOrCreate()
    repark.register_memory_catalog("local", Path.cwd() / "ex22_w2_append_wh")
    repark.sql("CREATE NAMESPACE local.ns")
    try:
        seed = repark.createDataFrame([(1, 10)], ["a", "b"])
        seed.writeTo("local.ns.t_append").create()
        reordered = repark.createDataFrame([(20, 2)], ["b", "a"])
        reordered.writeTo("local.ns.t_append").append()
        expect(
            "append by-name rows",
            sorted(
                tuple(row)
                for row in repark.sql("SELECT a, b FROM local.ns.t_append ORDER BY a").collect()
            ),
            [(1, 10), (2, 20)],
        )

        partitioned = repark.sql("SELECT * FROM (VALUES (1,'a'),(2,'b')) AS t(id, cat)")
        partitioned.writeTo("local.ns.t_owp").partitionedBy("cat").create()
        source = repark.sql("SELECT * FROM (VALUES (9,'a')) AS t(id, cat)")
        source.writeTo("local.ns.t_owp").overwritePartitions()
        expect(
            "overwritePartitions rows",
            sorted(
                tuple(row)
                for row in repark.sql("SELECT id, cat FROM local.ns.t_owp ORDER BY id").collect()
            ),
            [(2, "b"), (9, "a")],
        )
        snake_source = repark.sql("SELECT * FROM (VALUES (8,'a')) AS t(id, cat)")
        snake_source.writeTo("local.ns.t_owp").overwrite_partitions()
        expect(
            "overwrite_partitions rows",
            sorted(
                tuple(row)
                for row in repark.sql("SELECT id, cat FROM local.ns.t_owp ORDER BY id").collect()
            ),
            [(2, "b"), (8, "a")],
        )

        option_frame = repark.sql("SELECT * FROM (VALUES (1,'a')) AS t(id, name)")
        option_writer = option_frame.writeTo("local.ns.t_opt")
        option_writer.option("compression", "zstd").create()
        expect(
            "option rows",
            sorted(
                tuple(row)
                for row in repark.sql("SELECT id, name FROM local.ns.t_opt ORDER BY id").collect()
            ),
            [(1, "a")],
        )
        options_frame = repark.sql("SELECT * FROM (VALUES (2,'b')) AS t(id, name)")
        options_writer = options_frame.writeTo("local.ns.t_opts")
        options_writer.options(compression="snappy").create()
        expect(
            "options rows",
            sorted(
                tuple(row)
                for row in repark.sql("SELECT id, name FROM local.ns.t_opts ORDER BY id").collect()
            ),
            [(2, "b")],
        )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
