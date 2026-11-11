from __future__ import annotations

import sys
from typing import Any

KINDS = ("ListView(Int64)", "LargeListView(Int64)", "List(Int64)")
DOORS = ("df_key_name", "df_on", "sql_using", "sql_on")


def row(kind: str, ident: str, value: int, items: tuple[int, int]) -> str:
    array = f"array(CAST({items[0]} AS BIGINT), CAST({items[1]} AS BIGINT))"
    return f"SELECT {value} AS {ident}, arrow_cast({array}, '{kind}') AS s"


def views(kind: str) -> list[str]:
    left = f"{row(kind, 'lid', 1, (1, 2))} UNION ALL {row(kind, 'lid', 2, (2, 1))}"
    right = f"{row(kind, 'rid', 10, (2, 1))} UNION ALL {row(kind, 'rid', 30, (7, 7))}"
    return [
        f"CREATE OR REPLACE TEMPORARY VIEW lv_l AS {left}",
        f"CREATE OR REPLACE TEMPORARY VIEW lv_r AS {right}",
    ]


def frame(spark: Any, functions: Any, door: str) -> Any:
    left, right = spark.table("lv_l"), spark.table("lv_r")
    if door == "df_key_name":
        return left.join(right, "s", "inner").select("lid", "rid")
    if door == "df_on":
        on = functions.col("x.s") == functions.col("y.s")
        return left.alias("x").join(right.alias("y"), on, "inner").select("lid", "rid")
    if door == "sql_using":
        return spark.sql("SELECT lid, rid FROM lv_l JOIN lv_r USING (s)")
    return spark.sql("SELECT lid, rid FROM lv_l JOIN lv_r ON lv_l.s = lv_r.s")


def outcome(spark: Any, functions: Any, door: str) -> str:
    try:
        return str(sorted(list(row) for row in frame(spark, functions, door).collect()))
    except BaseException as error:
        return f"{type(error).__name__}: {str(error)[:90]}"


def main() -> None:
    from repark import ReparkSession
    from repark.spark import functions

    spark = ReparkSession.builder.appName("listview-key").getOrCreate()
    for kind in KINDS:
        for statement in views(kind):
            spark.sql(statement).collect()
        for door in DOORS:
            sys.stdout.write(f"{kind} {door}: {outcome(spark, functions, door)}\n")
    spark.stop()


if __name__ == "__main__":
    main()
