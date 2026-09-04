"""V2 createOrReplace and replace: full-table rebuilds on a local memory catalog.

pins: ex-22-types-writerv2/C-001
"""

from __future__ import annotations

from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameWriterV2.createOrReplace",
    "DataFrameWriterV2.create_or_replace",
    "DataFrameWriterV2.replace",
]


def expect(label: str, got: object, wanted: object) -> None:
    """Raise SystemExit when the measured answer differs."""
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Run the measured createOrReplace and replace answers, reading back after each."""
    repark = ReparkSession.builder.appName("ex-w2-replace").master("local[1]").getOrCreate()
    repark.register_memory_catalog("local", Path.cwd() / "ex22_w2_replace_wh")
    repark.sql("CREATE NAMESPACE local.ns")
    try:
        first = repark.sql("SELECT * FROM (VALUES (1,'a')) AS t(id, name)")
        writer = first.writeTo("local.ns.t_cor")
        writer.createOrReplace()
        second = repark.sql("SELECT * FROM (VALUES (9,'z')) AS t(id, name)")
        second.writeTo("local.ns.t_cor").createOrReplace()
        expect(
            "createOrReplace rows",
            sorted(
                tuple(row)
                for row in repark.sql("SELECT id, name FROM local.ns.t_cor ORDER BY id").collect()
            ),
            [(9, "z")],
        )

        snake = repark.sql("SELECT * FROM (VALUES (11,'q')) AS t(id, name)")
        snake.writeTo("local.ns.t_snake").create_or_replace()
        expect(
            "create_or_replace rows",
            sorted(
                tuple(row)
                for row in repark.sql("SELECT id, name FROM local.ns.t_snake ORDER BY id").collect()
            ),
            [(11, "q")],
        )

        seed = repark.sql("SELECT * FROM (VALUES (1,'a')) AS t(id, name)")
        seed.writeTo("local.ns.t_rep").create()
        rebuild = repark.sql("SELECT * FROM (VALUES (2,'b')) AS t(id, name)")
        rebuild.writeTo("local.ns.t_rep").replace()
        expect(
            "replace rows",
            sorted(
                tuple(row)
                for row in repark.sql("SELECT id, name FROM local.ns.t_rep ORDER BY id").collect()
            ),
            [(2, "b")],
        )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
