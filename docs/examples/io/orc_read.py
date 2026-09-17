"""ORC read: Spark-typed columns from the committed Spark-written fixtures.

pins: io-orc-1/C-010
"""

from __future__ import annotations

from pathlib import Path

from repark import ReparkSession

COVERS: list[str] = [
    "DataFrameReader.orc",
]

FIXTURES = (
    Path(__file__).resolve().parent.parent.parent.parent
    / "python"
    / "repark"
    / "tests"
    / "fixtures"
    / "orc"
)


def main() -> None:
    """Read one ORC directory plus its format spelling and assert rows and schema."""
    repark = ReparkSession.builder.appName("ex-io-orc").master("local[1]").getOrCreate()
    try:
        back = repark.read.orc(str(FIXTURES / "m1")).orderBy("id")
        if [repr(row) for row in back.collect()] != ["Row(id=0)", "Row(id=1)"]:
            raise SystemExit("orc read does not answer the fixture rows")
        if back.schema.simpleString() != "struct<id:bigint>":
            raise SystemExit(f"orc schema {back.schema.simpleString()!r} is not struct<id:bigint>")
        again = repark.read.format("orc").load(str(FIXTURES / "m1"))
        if again.count() != 2:
            raise SystemExit("format(orc).load does not answer the fixture rows")
        try:
            repark.read.orc(str(FIXTURES / "does_not_exist"))
        except Exception as error:
            if getattr(error, "getCondition", lambda: None)() != "PATH_NOT_FOUND":
                raise SystemExit(f"missing orc path condition {error!r}") from error
        else:
            raise SystemExit("missing orc path did not refuse")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
