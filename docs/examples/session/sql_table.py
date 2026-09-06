"""Run Spark SQL and load a temp view by name, with the missing-name refusal."""

from __future__ import annotations

from repark.errors import AnalysisException
from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.sql",
    "SparkSession.table",
]


def main() -> None:
    """Run the measured sql select and table-by-name arms."""
    repark = ReparkSession.builder.appName("ex26-sql-table").master("local[1]").getOrCreate()
    try:
        numbers = repark.createDataFrame([(1, 10.0), (2, 20.0), (3, 30.0)], "k INT, v DOUBLE")
        numbers.createOrReplaceTempView("tv_ex26n")
        picked = repark.sql("SELECT k, v FROM tv_ex26n WHERE k > 1 ORDER BY k LIMIT 2")
        picked_dtypes = [("k", "int"), ("v", "double")]
        if picked.dtypes != picked_dtypes:
            raise SystemExit(f"sql dtypes {picked.dtypes!r} != {picked_dtypes!r}")
        picked_rows = [tuple(row) for row in picked.collect()]
        picked_expected = [(2, 20.0), (3, 30.0)]
        if picked_rows != picked_expected:
            raise SystemExit(f"sql rows {picked_rows!r} != {picked_expected!r}")
        named = repark.table("tv_ex26n")
        named_rows = sorted(tuple(row) for row in named.collect())
        named_expected = [(1, 10.0), (2, 20.0), (3, 30.0)]
        if named_rows != named_expected:
            raise SystemExit(f"table rows {named_rows!r} != {named_expected!r}")
        did_raise = False
        try:
            repark.table("missing_tv_ex26").collect()
        except AnalysisException:
            did_raise = True
        if not did_raise:
            raise SystemExit("table on a missing name did not raise AnalysisException")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
