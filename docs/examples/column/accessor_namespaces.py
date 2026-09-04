"""Call the repark-only ``Column.str`` / ``Column.dt`` accessor namespaces.

Polars-style namespaces with no PySpark analog: PySpark spells the same rows
``F.upper`` / ``F.trim`` / ``F.year``, measured Spark-equal on live 4.1.2 and
asserted beside the namespace calls on repark below.

pins: ex-17-column-a/C-001
"""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["Column.str", "Column.dt"]


def main() -> None:
    """Run the measured namespace answers beside their PySpark-spelled twins."""
    repark = ReparkSession.builder.appName("ex-col-accessor-ns").master("local[1]").getOrCreate()
    try:
        words = repark.createDataFrame(
            [("apple",), ("  padded  ",), (None,), ("Äpfel",)],
            ["s"],
        )
        shouted = words.select(words.s.str.to_uppercase())
        upper_columns = ["upper(s)"]
        if shouted.columns != upper_columns:
            raise SystemExit(f"Column.str columns {shouted.columns!r} != {upper_columns!r}")
        shout_rows = set(shouted.collect())
        shout_expected = {("APPLE",), ("  PADDED  ",), ("ÄPFEL",), (None,)}
        if shout_rows != shout_expected:
            raise SystemExit(f"Column.str rows {shout_rows!r} != {shout_expected!r}")
        spelled_upper = words.select(F.upper(words.s))
        spelled_upper_rows = set(spelled_upper.collect())
        if spelled_upper_rows != shout_expected:
            raise SystemExit(f"F.upper rows {spelled_upper_rows!r} != {shout_expected!r}")

        stripped = words.select(words.s.str.strip_chars())
        trim_columns = ["trim(s)"]
        if stripped.columns != trim_columns:
            raise SystemExit(f"Column.str columns {stripped.columns!r} != {trim_columns!r}")
        strip_rows = set(stripped.collect())
        strip_expected = {("apple",), ("padded",), ("Äpfel",), (None,)}
        if strip_rows != strip_expected:
            raise SystemExit(f"Column.str rows {strip_rows!r} != {strip_expected!r}")
        spelled_trim = words.select(F.trim(words.s))
        spelled_trim_rows = set(spelled_trim.collect())
        if spelled_trim_rows != strip_expected:
            raise SystemExit(f"F.trim rows {spelled_trim_rows!r} != {strip_expected!r}")

        dates = repark.createDataFrame(
            [(datetime.date(2024, 3, 15),), (datetime.date(2025, 12, 31),), (None,)],
            ["d"],
        )
        years = dates.select(dates.d.dt.year())
        year_columns = ["year(d)"]
        if years.columns != year_columns:
            raise SystemExit(f"Column.dt columns {years.columns!r} != {year_columns!r}")
        year_rows = set(years.collect())
        year_expected = {(2024,), (2025,), (None,)}
        if year_rows != year_expected:
            raise SystemExit(f"Column.dt rows {year_rows!r} != {year_expected!r}")
        spelled_years = dates.select(F.year(dates.d))
        spelled_year_rows = set(spelled_years.collect())
        if spelled_year_rows != year_expected:
            raise SystemExit(f"F.year rows {spelled_year_rows!r} != {year_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
