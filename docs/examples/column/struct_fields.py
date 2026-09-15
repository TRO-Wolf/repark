"""Edit struct fields and probe values on the repark ``Column`` surface.

PySpark 4.1.2 spellings measured live: ``withField`` / ``dropFields`` rebuild a
struct column (NULL parents stay NULL), ``isin`` is SQL ``IN`` (NULL input stays
NULL), ``isNaN`` coerces through DOUBLE, ``astype`` is ``cast``, ``name`` is
``alias`` with ``metadata=``, and ``outer`` marks a correlated reference that a
plain select answers unchanged.

pins: column-parity-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "Column.astype",
    "Column.dropFields",
    "Column.isNaN",
    "Column.isin",
    "Column.name",
    "Column.outer",
    "Column.withField",
]


def main() -> None:
    """Assert the seven struct/value answers against the live-oracle pins."""
    repark = ReparkSession.builder.appName("ex-col-struct-fields").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [(1, 1.0, (1, "x")), (2, float("nan"), None), (None, None, (3, None))],
            "i int, d double, st struct<a:int,b:string>",
        )
        members = frame.select(frame.i.isin(1, 3).alias("m"))
        member_columns = ["m"]
        if members.columns != member_columns:
            raise SystemExit(f"Column.isin columns {members.columns!r} != {member_columns!r}")
        member_rows = set(members.collect())
        member_expected = {(True,), (False,), (None,)}
        if member_rows != member_expected:
            raise SystemExit(f"Column.isin rows {member_rows!r} != {member_expected!r}")

        nans = frame.select(frame.d.isNaN().alias("n"))
        nan_rows = set(nans.collect())
        nan_expected = {(False,), (True,), (False,)}
        if nan_rows != nan_expected:
            raise SystemExit(f"Column.isNaN rows {nan_rows!r} != {nan_expected!r}")

        casted = frame.select(frame.i.astype("string"))
        cast_rows = set(casted.collect())
        cast_expected = {("1",), ("2",), (None,)}
        if cast_rows != cast_expected:
            raise SystemExit(f"Column.astype rows {cast_rows!r} != {cast_expected!r}")

        renamed = frame.select(frame.i.name("ii", metadata={"tag": "pk"}))
        renamed_field = renamed.schema["ii"]
        if renamed_field.metadata.get("tag") != "pk":
            raise SystemExit(f"Column.name metadata {renamed_field.metadata!r} missing tag")

        outer_rows = frame.select(frame.i.outer()).collect()
        if [row[0] for row in outer_rows] != [1, 2, None]:
            raise SystemExit(f"Column.outer rows {outer_rows!r} != [1, 2, None]")

        grown = frame.select(frame.st.withField("c", F.lit(7)).alias("r"))
        grown_rows = [row[0] for row in grown.collect()]
        grown_expected = [{"a": 1, "b": "x", "c": 7}, None, {"a": 3, "b": None, "c": 7}]
        if grown_rows != grown_expected:
            raise SystemExit(f"Column.withField rows {grown_rows!r} != {grown_expected!r}")

        shrunk = frame.select(frame.st.dropFields("b").alias("r"))
        shrunk_rows = [row[0] for row in shrunk.collect()]
        shrunk_expected = [{"a": 1}, None, {"a": 3}]
        if shrunk_rows != shrunk_expected:
            raise SystemExit(f"Column.dropFields rows {shrunk_rows!r} != {shrunk_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
