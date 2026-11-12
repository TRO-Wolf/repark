import argparse
import json
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

TABLE = "ice.ns.t_split_cow"
COLUMNS = "id BIGINT, i INT, d DOUBLE, tf FLOAT, e DOUBLE, s STRING, flag STRING"
ROWS = (
    "(1, 1, CAST('1.5' AS DOUBLE), CAST('1.5' AS FLOAT), NULL, 'a', 'f1')",
    "(2, NULL, NULL, NULL, NULL, NULL, 'f2')",
    "(3, NULL, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT), NULL, '', 'f3')",
    "(4, NULL, CAST('-0.0' AS DOUBLE), CAST('-0.0' AS FLOAT), NULL, 'a ', 'f4')",
    "(5, 2147483647, CAST('Infinity' AS DOUBLE), CAST('Infinity' AS FLOAT), NULL, NULL, 'f5')",
    "(6, -1, CAST('0.0' AS DOUBLE), CAST('0.0' AS FLOAT), NULL, 'A', 'f6')",
    "(7, NULL, NULL, NULL, NULL, 'bb', 'f7')",
    "(8, 1, CAST('1.5' AS DOUBLE), CAST('1.5' AS FLOAT), NULL, 'a', 'f8')",
)
GROUPING = (
    ("V-B1-rollup", "SELECT d = 0.0 AS z, count(*) AS c FROM {T} GROUP BY ROLLUP(d = 0.0)"),
    ("V-B1-cube", "SELECT d >= 0.0 AS z, count(*) AS c FROM {T} GROUP BY CUBE(d >= 0.0)"),
    (
        "V-B1-gsets",
        "SELECT d <=> -0.0 AS z, count(*) AS c FROM {T} GROUP BY GROUPING SETS ((d <=> -0.0), ())",
    ),
)
NAMES = (
    ("V-A3-sx", "frame.selectExpr('id', 'd <=> 0.0')"),
    ("V-A3-fx", "frame.select('id', F.expr('d <=> 0.0'))"),
    ("V-A3-sx-int", "frame.selectExpr('id', 'id <=> 1')"),
    ("V-A3-fx-int", "frame.select('id', F.expr('id <=> 1'))"),
    ("V-A3-sx-isnd", "frame.selectExpr('id', 'd IS NOT DISTINCT FROM 0.0')"),
    ("V-A3-sx-isd", "frame.selectExpr('id', 'd IS DISTINCT FROM 0.0')"),
    ("V-A3-sx-and", "frame.selectExpr('id', '(d <=> 0.0) AND (id > 1)')"),
    ("V-A3-sx-not", "frame.selectExpr('id', 'NOT (d <=> 0.0)')"),
    ("V-A3-sx-noteq", "frame.selectExpr('id', 'NOT (d = 0.0)')"),
    ("V-A3-fx-not", "frame.select('id', F.expr('NOT (d <=> 0.0)'))"),
    ("V-A3-fx-and", "frame.select('id', F.expr('(d <=> 0.0) AND (id > 1)'))"),
)
DML = (
    ("V-A1-abs-eq", "abs(d) = 0.0"),
    ("V-A1-abs-ne", "abs(d) <> 0.0"),
    ("V-A1-plus", "d + 0.0 = 0.0"),
    ("V-A1-square", "d * d = 0.0"),
    ("V-A1-twice", "d * 2 = 0.0"),
    ("V-A1-tf", "tf = 0.0"),
    ("V-A1-tf-neg", "tf = -0.0"),
    ("V-A1-tf-cast", "CAST(tf AS DOUBLE) = 0.0"),
    ("V-A1-ctrl-range", "abs(d) < 2.0 AND abs(d) > 1.0"),
    ("V-A1-ctrl-abs", "abs(d) = 1.5"),
    ("V-A1-ctrl-tf", "tf > 1.0"),
)
STATEMENTS = (
    ("delete", "DELETE FROM {T} WHERE {P}"),
    ("update", "UPDATE {T} SET flag = 'x' WHERE {P}"),
)


def norm(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value in (float("inf"), float("-inf")):
            return "Infinity" if value > 0 else "-Infinity"
        if value == 0.0 and str(value) == "-0.0":
            return "-0.0"
        return repr(value)
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, (list, tuple)):
        return [norm(item) for item in value]
    return value


def spark_rows(frame: Any) -> tuple[list[str], list[list[Any]]]:
    return list(frame.columns), [[norm(value) for value in row] for row in frame.collect()]


def repark_rows(frame: Any) -> tuple[list[str], list[list[Any]]]:
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    columns = [table.column(index).to_pylist() for index in range(table.num_columns)]
    rows = [list(values) for values in zip(*columns, strict=True)] if columns else []
    return list(table.column_names), [[norm(value) for value in row] for row in rows]


def cell(cid: str, route: str, text: str, run: Callable[[], Any]) -> dict[str, Any]:
    entry: dict[str, Any] = {"record": "cell", "cid": cid, "route": route, "text": text}
    try:
        names, rows = run()
    except BaseException as error:
        if isinstance(error, KeyboardInterrupt):
            raise
        entry.update(outcome="REFUSED", error=f"{type(error).__name__}: {str(error)[:300]}")
        return entry
    entry.update(outcome="ANSWERED", names=names, rows=rows)
    return entry


def native_text(text: str) -> str:
    return text.replace("<=>", "IS NOT DISTINCT FROM")


def spark_legs() -> list[dict[str, Any]]:
    from pyspark.sql import SparkSession, functions

    spark = (
        SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    )
    values = ", ".join(ROWS)
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW t AS SELECT * FROM VALUES {values} "
        "AS t(id, i, d, tf, e, s, flag)"
    )
    frame = spark.table("t")
    records = [
        cell(
            cid,
            "spark-sql",
            text,
            lambda text=text: spark_rows(spark.sql(text.replace("{T}", "t"))),
        )
        for cid, text in GROUPING
    ]
    records += [
        cell(
            cid,
            "typed",
            text,
            lambda text=text: spark_rows(eval(text, {"F": functions, "frame": frame})),
        )
        for cid, text in NAMES
    ]
    records += [
        cell(
            cid,
            "spark-sql",
            where,
            lambda where=where: spark_rows(spark.sql(f"SELECT id FROM t WHERE {where}")),
        )
        for cid, where in DML
    ]
    return records


def dml_cell(
    repark: Any, cid: str, seat: str, statement: str, where: str, number: int
) -> dict[str, Any]:
    table = f"ice.ns.dml_{number}"
    repark_rows(repark.sql(f"CREATE TABLE {table} ({COLUMNS})"))
    repark_rows(repark.sql(f"INSERT INTO {table} VALUES {', '.join(ROWS)}"))
    text = statement.replace("{T}", table).replace("{P}", where)
    entry = cell(f"{cid}-{seat}", "native", text, lambda: repark_rows(repark.sql(text)))
    _, post = repark_rows(repark.sql(f"SELECT id, flag FROM {table} ORDER BY id"))
    if seat == "delete":
        entry["touched"] = sorted(set(range(1, 9)) - {row[0] for row in post})
    else:
        entry["touched"] = sorted(row[0] for row in post if row[1] == "x")
    return entry


def repark_legs(warehouse: Path) -> list[dict[str, Any]]:
    import repark
    from repark import ReparkSession, functions
    from repark._native import PyReparkSession

    native = PyReparkSession.native(config={"repark.sql.allowCreateFormatVersion3": "true"})
    native.register_memory_catalog("ice", str(warehouse / "native"))
    native.create_namespace("ice", "ns", str(warehouse / "native" / "ns"))
    repark._ANSI_NATIVE = native
    repark_rows(repark.sql(f"CREATE TABLE {TABLE} ({COLUMNS})"))
    for row in ROWS:
        repark_rows(repark.sql(f"INSERT INTO {TABLE} VALUES {row}"))
    records = []
    for cid, text in GROUPING:
        sql = native_text(text.replace("{T}", TABLE))
        records.append(cell(cid, "native", sql, lambda sql=sql: repark_rows(repark.sql(sql))))
    number = 0
    for cid, where in DML:
        for seat, statement in STATEMENTS:
            number += 1
            records.append(dml_cell(repark, cid, seat, statement, where, number))
    facade = ReparkSession.builder.appName("delta_probe").getOrCreate()
    facade.register_memory_catalog("ice", str(warehouse / "facade"))
    facade.sql("CREATE NAMESPACE ice.ns").collect()
    facade.sql(f"CREATE TABLE {TABLE} ({COLUMNS}) USING iceberg").collect()
    for row in ROWS:
        facade.sql(f"INSERT INTO {TABLE} VALUES {row}").collect()
    for cid, text in GROUPING:
        sql = text.replace("{T}", TABLE)
        records.append(cell(cid, "spark-sql", sql, lambda sql=sql: repark_rows(facade.sql(sql))))
    frame = facade.table(TABLE)
    for cid, text in NAMES:
        records.append(
            cell(
                cid,
                "typed",
                text,
                lambda text=text: repark_rows(eval(text, {"F": functions, "frame": frame})),
            )
        )
    facade.stop()
    return records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("spark", "repark"), required=True)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    arguments = parser.parse_args()
    records = spark_legs() if arguments.engine == "spark" else repark_legs(arguments.warehouse)
    arguments.out.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records), encoding="utf-8"
    )
    for record in records:
        print(
            record["cid"],
            record["route"],
            record["outcome"],
            record.get("names"),
            record.get("touched"),
        )


if __name__ == "__main__":
    main()
