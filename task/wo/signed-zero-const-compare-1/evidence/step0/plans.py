from __future__ import annotations

import sys
from pathlib import Path

import repark
from repark import ReparkSession
from repark import functions as fn

CELLS = (
    "CAST('0.0' AS DOUBLE) = CAST('-0.0' AS DOUBLE)",
    "CAST(CAST(0 AS DECIMAL(10,2)) AS DOUBLE) = CAST('-0.0' AS DOUBLE)",
    "(CAST(0 AS DOUBLE) * -1) = (-CAST(0 AS DOUBLE) * -1)",
    "CAST('-0.0' AS DOUBLE) = d",
)
SOURCE = " FROM (VALUES (CAST(1.5 AS DOUBLE))) AS t(d)"


def changed(rows: list) -> list[str]:
    lines = []
    for row in rows:
        values = list(row.values()) if isinstance(row, dict) else list(row)
        if "SAME TEXT" in str(values[1]) or "physical_plan" in str(values[0]):
            continue
        lines.append(f"  {values[0]}: {' | '.join(str(values[1]).splitlines())}")
    return lines


def main() -> None:
    session = ReparkSession.builder.appName("plans").getOrCreate()
    out = []
    for cell in CELLS:
        text = f"SELECT {cell} AS r" + (SOURCE if cell.endswith("d") else "")
        out.append(f"## {text}")
        out.append("spark-sql door:")
        out.extend(changed([tuple(r) for r in session.sql("EXPLAIN VERBOSE " + text).collect()]))
        out.append("native door:")
        out.extend(changed(repark.sql("EXPLAIN VERBOSE " + text).to_arrow().to_pylist()))
        out.append("")
    frame = session.sql("SELECT 1 AS k").select(
        (fn.lit("0.0").cast("double") == fn.lit("-0.0").cast("double")).alias("r")
    )
    out.append("## dataframe: fn.lit('0.0').cast('double') == fn.lit('-0.0').cast('double')")
    out.append(f"  answer: {[tuple(r) for r in frame.collect()]}")
    Path(sys.argv[1]).write_text("\n".join(out) + "\n")


if __name__ == "__main__":
    main()
