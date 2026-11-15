import json
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"preimage-{engine}-{ansi}-", dir=wh_parent)
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
PROPS = {
    "cow": (
        "'format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
        "'write.update.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write'"
    ),
    "mor": (
        "'format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
        "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read'"
    ),
}
if engine == "spark":
    import pyspark
    from pyspark.sql import SparkSession as Builder
    from pyspark.sql import functions as sf

    builder = (
        Builder.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config("spark.sql.extensions", EXT)
        .config("spark.sql.shuffle.partitions", "1")
    )
    prov = {"engine": "spark", "version": pyspark.__version__}
else:
    import repark._native as native
    from repark import ReparkSession as Builder
    from repark import functions as sf

    builder = Builder.builder.appName("ddu-preimage")
    prov = {
        "engine": "repark",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    }
s = (
    builder.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()

TOP38 = "99999999999999999999.999999999999999999"
ROWS = [
    (1, "0", "0"),
    (2, "0.01", "0.000000000000000001"),
    (3, "-0.01", "-0.000000000000000001"),
    (4, "0.50", "0.5"),
    (5, "1.47", "1.000000000000000100"),
    (6, "99999999.99", TOP38),
    (7, "-99999999.99", "-" + TOP38),
    (8, None, None),
    (9, "4.50", "4.5"),
    (10, "-1.47", "1.0000000000000004"),
]
CONSTANTS = {
    "half": "0.5",
    "exact": {"d10": "1.47", "d38": "1.0000000000000004"},
    "between": {"d10": "0.005", "d38": "2.5E-19"},
    "between_neg": {"d10": "-0.005", "d38": "-2.5E-19"},
    "pzero": "0.0",
    "nzero": "-0.0",
    "nan": "NaN",
    "pinf": "Infinity",
    "ninf": "-Infinity",
    "top": {"d10": "99999999.99", "d38": "1.0E20"},
    "above": {"d10": "1.0E9", "d38": "1.0E21"},
    "below": {"d10": "-1.0E9", "d38": "-1.0E21"},
}
OPS = {
    "eq": "=",
    "ne": "<>",
    "lt": "<",
    "le": "<=",
    "gt": ">",
    "ge": ">=",
    "dist": "IS DISTINCT FROM",
    "ns": "<=>",
}


def text(value: str, column: str) -> str:
    return f"CAST('{value}' AS DECIMAL({'10,2' if column == 'd10' else '38,18'}))"


def values_sql() -> str:
    cells = []
    for row_id, d10, d38 in ROWS:
        a = "NULL" if d10 is None else text(d10, "d10")
        b = "NULL" if d38 is None else text(d38, "d38")
        cells.append(f"({row_id}, {a}, {b})")
    return ", ".join(cells)


def constant_of(name: str, column: str) -> str:
    raw = CONSTANTS[name]
    return raw[column] if isinstance(raw, dict) else raw


def make_table(name: str, mode: str) -> str:
    table = f"lc.ns.{name}"
    s.sql(f"DROP TABLE IF EXISTS {table}").collect()
    s.sql(
        f"CREATE TABLE {table} (id INT, d10 DECIMAL(10,2), d38 DECIMAL(38,18)) USING iceberg "
        f"TBLPROPERTIES ({PROPS[mode]})"
    ).collect()
    s.sql(f"INSERT INTO {table} VALUES {values_sql()}").collect()
    return table


def answer(run: Callable[[], Any]) -> Any:
    try:
        return run()
    except Exception as error:
        return "ERR:" + type(error).__name__ + ":" + str(error).splitlines()[0][:160]


def ids(sql: str) -> list[int]:
    return sorted(int(r[0]) for r in s.sql(sql).collect())


results: dict[str, Any] = {}
base = make_table("e", "cow")
keys = []
for column in ("d10", "d38"):
    for cname in CONSTANTS:
        keys.append((column, cname))
s.sql("DROP TABLE IF EXISTS lc.ns.k").collect()
s.sql("CREATE TABLE lc.ns.k (name STRING, c DOUBLE) USING iceberg").collect()
k_rows = ", ".join(
    f"('{column}/{cname}', CAST('{constant_of(cname, column)}' AS DOUBLE))"
    for column, cname in keys
)
s.sql(f"INSERT INTO lc.ns.k VALUES {k_rows}").collect()
for column, cname in keys:
    lit = f"CAST('{constant_of(cname, column)}' AS DOUBLE)"
    for oname, op in OPS.items():
        for side in ("r", "l"):
            pred = f"{column} {op} {lit}" if side == "r" else f"{lit} {op} {column}"
            held = f"e.{column} {op} k.c" if side == "r" else f"k.c {op} e.{column}"
            key = f"{column}/{cname}/{oname}/{side}"
            results[f"where/{key}"] = answer(lambda p=pred: ids(f"SELECT id FROM {base} WHERE {p}"))
            results[f"held/{key}"] = answer(
                lambda h=held, n=f"{column}/{cname}": ids(
                    f"SELECT e.id FROM {base} e JOIN lc.ns.k k ON k.name = '{n}' WHERE {h}"
                )
            )
            results[f"value/{key}"] = answer(
                lambda p=pred: [
                    [int(r[0]), None if r[1] is None else bool(r[1])]
                    for r in sorted(
                        s.sql(f"SELECT id, {p} FROM {base}").collect(), key=lambda r: r[0]
                    )
                ]
            )

frame = s.table(base)
for name, build in {
    "df_gt": lambda: frame.filter(sf.col("d10") > 4.5),
    "df_eq": lambda: frame.filter(sf.col("d10") == 4.5),
    "df_lt_half": lambda: frame.filter(sf.col("d10") < 0.5),
    "df_d38_ge": lambda: frame.filter(sf.col("d38") >= 0.5),
    "df_d38_eq_nzero": lambda: frame.filter(sf.col("d38") == -0.0),
}.items():
    results[f"frame/{name}"] = answer(lambda b=build: sorted(int(r[0]) for r in b().collect()))

LISTS = {
    "in3": "{c} IN (CAST(0.5 AS DOUBLE), CAST(4.5 AS DOUBLE), CAST(-0.01 AS DOUBLE))",
    "in4": (
        "{c} IN (CAST(0.5 AS DOUBLE), CAST(4.5 AS DOUBLE), CAST(-0.01 AS DOUBLE), "
        "CAST('0.005' AS DOUBLE))"
    ),
    "in3_zero": "{c} IN (CAST('-0.0' AS DOUBLE), CAST(4.5 AS DOUBLE), CAST(0.005 AS DOUBLE))",
    "notin3": "{c} NOT IN (CAST(0.5 AS DOUBLE), CAST(4.5 AS DOUBLE), CAST(-0.01 AS DOUBLE))",
    "notin4": (
        "{c} NOT IN (CAST(0.5 AS DOUBLE), CAST(4.5 AS DOUBLE), CAST(-0.01 AS DOUBLE), "
        "CAST('0.005' AS DOUBLE))"
    ),
    "between": "{c} BETWEEN CAST(0.005 AS DOUBLE) AND CAST(4.5 AS DOUBLE)",
    "between_zero": "{c} BETWEEN CAST('-0.0' AS DOUBLE) AND CAST(0.5 AS DOUBLE)",
    "not_between": "{c} NOT BETWEEN CAST(-0.005 AS DOUBLE) AND CAST(0.5 AS DOUBLE)",
}
for column in ("d10", "d38"):
    for lname, shape in LISTS.items():
        pred = shape.format(c=column)
        results[f"list/{column}/{lname}"] = answer(
            lambda p=pred: ids(f"SELECT id FROM {base} WHERE {p}")
        )

DML = {
    "delete": "DELETE FROM {t} WHERE {c} = {k}",
    "update": "UPDATE {t} SET id = id + 100 WHERE {c} = {k}",
    "merge_on": (
        "MERGE INTO {t} t USING (SELECT 1 AS sid) s ON t.{c} = {k} "
        "WHEN MATCHED THEN UPDATE SET id = t.id + 100"
    ),
}
DML_CONSTANTS = {
    "half": "CAST(0.5 AS DOUBLE)",
    "nzero": "CAST('-0.0' AS DOUBLE)",
    "between": "CAST(0.005 AS DOUBLE)",
}
counter = 0
for mode in ("cow", "mor"):
    for column in ("d10", "d38"):
        for dname, shape in DML.items():
            for kname, k in DML_CONSTANTS.items():
                counter += 1
                table = make_table(f"m{counter}", mode)
                statement = shape.format(t=table, c=column, k=k)

                def run(st: str = statement, tb: str = table) -> list[int]:
                    s.sql(st).collect()
                    return ids(f"SELECT id FROM {tb}")

                results[f"dml/{mode}/{column}/{dname}/{kname}"] = answer(run)

with Path(out).open("w") as fh:
    fh.write(json.dumps({"prov": prov, "ansi": ansi, "warehouse": wh}) + "\n")
    for key, value in results.items():
        fh.write(json.dumps({"key": key, "answer": value}) + "\n")
s.stop()
