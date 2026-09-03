"""Demonstrate the ``F.*`` case-mapping names on a small local frame.

pins: ex-4-functions-strings-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.lcase", "F.lower", "F.ucase", "F.upper", "F.initcap", "F.col", "F.lit"]


def main() -> None:
    """Check each case mapping on mixed-case words, an empty string, and NULL."""
    repark = ReparkSession.builder.appName("ex-case").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("Spark",), ("Apache",), ("aPACHE",), ("",), (None,)], ["s"]
        )
        rows = frame.select(
            F.lcase(F.col("s")).alias("lcase"),
            F.lower(F.col("s")).alias("lower"),
            F.ucase(F.col("s")).alias("ucase"),
            F.upper(F.col("s")).alias("upper"),
            F.initcap(F.col("s")).alias("initcap"),
            F.initcap(F.lit("aPACHE sPark")).alias("initcap_words"),
        ).collect()
        checked = (
            ("lcase", ["spark", "apache", "apache", "", None]),
            ("lower", ["spark", "apache", "apache", "", None]),
            ("ucase", ["SPARK", "APACHE", "APACHE", "", None]),
            ("upper", ["SPARK", "APACHE", "APACHE", "", None]),
            ("initcap", ["Spark", "Apache", "Apache", "", None]),
            ("initcap_words", ["Apache Spark"] * 5),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
        if [row["lcase"] for row in rows] != [row["lower"] for row in rows]:
            raise SystemExit("F.lcase is F.lower and must agree exactly")
        if [row["ucase"] for row in rows] != [row["upper"] for row in rows]:
            raise SystemExit("F.ucase is F.upper and must agree exactly")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
