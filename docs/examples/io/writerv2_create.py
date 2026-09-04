"""V2 create on a local memory-catalog Iceberg table: using, properties, identity partitions.

pins: ex-22-types-writerv2/C-001
"""

from __future__ import annotations

from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameWriterV2.using",
    "DataFrameWriterV2.tableProperty",
    "DataFrameWriterV2.table_property",
    "DataFrameWriterV2.partitionedBy",
    "DataFrameWriterV2.partitioned_by",
    "DataFrameWriterV2.create",
]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Create two local Iceberg tables through the V2 writer and read both back."""
    repark = ReparkSession.builder.appName("ex-w2-create").master("local[1]").getOrCreate()
    repark.register_memory_catalog("local", Path.cwd() / "ex22_w2_create_wh")
    repark.sql("CREATE NAMESPACE local.ns")
    try:
        frame = repark.sql("SELECT * FROM (VALUES (1,'a'),(2,'b')) AS t(id, cat)")
        writer = frame.writeTo("local.ns.t_props")
        writer.tableProperty("write.format.default", "parquet").tableProperty(
            "format-version", "2"
        ).partitionedBy("cat").create()
        expect(
            "tableProperty rows",
            sorted(
                tuple(row)
                for row in repark.sql("SELECT id, cat FROM local.ns.t_props ORDER BY id").collect()
            ),
            [(1, "a"), (2, "b")],
        )
        expect(
            "tableProperty partition filter",
            [
                tuple(row)
                for row in repark.sql("SELECT id FROM local.ns.t_props WHERE cat = 'a'").collect()
            ],
            [(1,)],
        )

        snake_frame = repark.sql("SELECT * FROM (VALUES (1,'x')) AS t(id, p)")
        snake_writer = snake_frame.writeTo("local.ns.t_snake")
        snake_writer.using("iceberg").table_property("repark.example", "v1").partitioned_by(
            "p"
        ).create()
        expect(
            "table_property rows",
            sorted(
                tuple(row)
                for row in repark.sql("SELECT id, p FROM local.ns.t_snake ORDER BY id").collect()
            ),
            [(1, "x")],
        )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
