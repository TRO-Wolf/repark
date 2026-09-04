"""Rename a column with ``alias`` and reshape it through ``transform``.

pins: ex-17-column-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["Column.alias", "Column.transform"]


def main() -> None:
    """Run the measured rename and reshape answers on two local frames."""
    repark = ReparkSession.builder.appName("ex-col-naming").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
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
        renamed = frame.select(frame.k.alias("kk"), frame.g)
        alias_columns = ["kk", "g"]
        if renamed.columns != alias_columns:
            raise SystemExit(f"Column.alias columns {renamed.columns!r} != {alias_columns!r}")
        alias_rows = sorted(renamed.collect(), key=tuple)
        alias_expected = [(1, "a"), (1, "b"), (2, "a"), (2, "a"), (2, "b"), (3, "a")]
        if alias_rows != alias_expected:
            raise SystemExit(f"Column.alias rows {alias_rows!r} != {alias_expected!r}")

        words = repark.createDataFrame([("apple",), ("mango",), ("cherry",), ("apple pie",)], ["s"])
        shouts = words.select(words.s.transform(F.upper))
        transform_columns = ["upper(s)"]
        if shouts.columns != transform_columns:
            raise SystemExit(
                f"Column.transform columns {shouts.columns!r} != {transform_columns!r}"
            )
        shout_rows = sorted(shouts.collect(), key=tuple)
        shout_expected = [("APPLE",), ("APPLE PIE",), ("CHERRY",), ("MANGO",)]
        if shout_rows != shout_expected:
            raise SystemExit(f"Column.transform rows {shout_rows!r} != {shout_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
