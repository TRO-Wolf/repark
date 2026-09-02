"""Round a column with ``F.ceil``, ``F.ceiling``, ``F.floor``, and half-up ``F.round``.

pins: ex-2-functions-math-bitwise/C-002
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.ceil", "F.ceiling", "F.floor", "F.round", "F.col"]


def main() -> None:
    """Check the ceiling and floor integers, round's halfway rule, and NULL."""
    repark = ReparkSession.builder.appName("ex-rounding").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [(2.3,), (-2.3,), (2.7,), (-2.7,), (2.5,), (-2.5,), (0.0,), (None,)], ["v"]
        )
        rows = frame.select(
            F.col("v"),
            F.ceil(F.col("v")).alias("ceil"),
            F.ceiling(F.col("v")).alias("ceiling"),
            F.floor(F.col("v")).alias("floor"),
            F.round(F.col("v")).alias("round"),
        ).collect()
        for row in rows:
            print(
                f"v={row['v']!r:>6}  ceil={row['ceil']!r:>4}  ceiling={row['ceiling']!r:>4}  "
                f"floor={row['floor']!r:>4}  round={row['round']!r:>4}"
            )
        ceilings = [row["ceil"] for row in rows]
        if ceilings != [3, -2, 3, -2, 3, -2, 0, None]:
            raise SystemExit(f"F.ceil gave {ceilings!r}")
        if ceilings != [row["ceiling"] for row in rows]:
            raise SystemExit("F.ceil and F.ceiling are aliases and must agree row for row")
        floors = [row["floor"] for row in rows]
        if floors != [2, -3, 2, -3, 2, -3, 0, None]:
            raise SystemExit(f"F.floor gave {floors!r}")
        rounded = [row["round"] for row in rows]
        if rounded != [2.0, -2.0, 3.0, -3.0, 3.0, -3.0, 0.0, None]:
            raise SystemExit(f"F.round gave {rounded!r}; halfway cases go away from zero")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
