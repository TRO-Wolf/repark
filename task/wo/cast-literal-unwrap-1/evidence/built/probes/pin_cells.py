import json
import sys
from pathlib import Path

engine, ansi, wh, out = sys.argv[1:5]
if engine == "spark":
    from pyspark.sql import SparkSession as B

    jar = sys.argv[5]
    b = (
        B.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", wh)
    )
else:
    import repark._native as n

    print("native", n.__file__)
    from repark import ReparkSession as B

    b = B.builder.appName("pin-cells")
s = b.config("spark.sql.ansi.enabled", ansi).getOrCreate()
DEC = [
    "0.50",
    "1.00",
    "1.25",
    "1.50",
    "1.99",
    "-0.50",
    "-1.25",
    "-1.50",
    "2.50",
    "0.00",
    "99999999.99",
    "-99999999.99",
    None,
]
BIG = [
    "1",
    "2",
    "-1",
    "0",
    "2147483647",
    "2147483648",
    "4294967297",
    "-2147483649",
    "-9223372036854775808",
    None,
]


def lit(v: str | None, t: str) -> str:
    return "NULL" if v is None else f"CAST('{v}' AS {t})"


def view(name: str, items: list, t: str, col: str = "c") -> None:
    rows = ", ".join(f"({i}, {lit(v, t)})" for i, v in enumerate(items, start=1))
    s.sql(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM VALUES {rows} AS x(id, `{col}`)")


view("vd", DEC, "DECIMAL(10,2)")
view("vb", BIG, "BIGINT")
view("vb5", BIG[:5], "BIGINT")
view("vcafe", DEC, "DECIMAL(10,2)", "café")
view("vdbl", ["0.5", "1.0", "1.25", "1.99", "-1.5", "2.5"], "DOUBLE")
view("vflt", ["0.5", "1.0", "1.25", "1.99", "-1.5", "2.5"], "FLOAT")
view("vstr", ["1", "2", "10"], "STRING")
view("vts", ["2024-01-01 00:00:00", "2024-01-01 23:59:59", "2024-01-02 00:00:00"], "TIMESTAMP")
view("vtop", ["1.00", "127.00", "127.99", "128.00", "-129.00", None], "DECIMAL(10,2)")
cells = {
    "between": "SELECT id FROM vd WHERE CAST(c AS INT) BETWEEN 1 AND 2",
    "not_in": "SELECT id FROM vd WHERE CAST(c AS INT) NOT IN (1, 0)",
    "decimal_constant": "SELECT id FROM vd WHERE CAST(c AS INT) = 1.0",
    "double_constant": "SELECT id FROM vd WHERE CAST(c AS DECIMAL(10,1)) = CAST(1.3 AS DOUBLE)",
    "double_in": "SELECT id FROM vd WHERE CAST(c AS DECIMAL(10,1)) "
    "IN (CAST(1.3 AS DOUBLE), CAST(2.5 AS DOUBLE))",
    "constant_left": "SELECT id FROM vd WHERE 1 < CAST(c AS INT)",
    "null_safe_zero": "SELECT id FROM vd WHERE CAST(c AS INT) <=> 0",
    "control_arith": "SELECT id FROM vd WHERE CAST(c AS INT) + 1 = 2",
    "control_abs": "SELECT id FROM vd WHERE abs(CAST(c AS INT)) = 1",
    "int_in": "SELECT id FROM vd WHERE CAST(c AS INT) IN (1, 5)",
    "in_past_cap": "SELECT id FROM vd WHERE CAST(c AS INT) IN ("
    + ", ".join(str(k) for k in range(2, 42))
    + ")",
    "non_ascii": "SELECT id FROM vcafe WHERE CAST(`café` AS INT) = 1",
    "double_int": "SELECT id FROM vdbl WHERE CAST(c AS INT) = 1",
    "float_int": "SELECT id FROM vflt WHERE CAST(c AS INT) = 1",
    "string_int": "SELECT id FROM vstr WHERE CAST(c AS INT) = 1",
    "ts_date": "SELECT id FROM vts WHERE CAST(c AS DATE) = DATE'2024-01-01'",
    "big_eq1": "SELECT id FROM vb WHERE CAST(c AS INT) = 1",
    "big_eq5": "SELECT id FROM vb WHERE CAST(c AS INT) = 5",
    "big5_eq1": "SELECT id FROM vb5 WHERE CAST(c AS INT) = 1",
    "big_try": "SELECT id FROM vb WHERE TRY_CAST(c AS INT) = 1",
    "top_gt": "SELECT id FROM vtop WHERE CAST(c AS TINYINT) > 127",
    "top_lt": "SELECT id FROM vtop WHERE 127 < CAST(c AS TINYINT)",
    "bottom_lt": "SELECT id FROM vtop WHERE CAST(c AS TINYINT) < -128",
    "sel_true": "SELECT id FROM (SELECT id, CAST(c AS INT) = 1 AS b FROM vd) WHERE b",
    "tiny_eq1": "SELECT id FROM vd WHERE CAST(c AS TINYINT) = 1",
}
res = {}
for k, q in cells.items():
    try:
        res[k] = sorted(int(r[0]) for r in s.sql(q).collect())
    except Exception as e:
        res[k] = "ERR " + " ".join(str(e).split())[:140]
with Path(out).open("w") as fh:
    json.dump(res, fh, indent=1, sort_keys=True)
print(engine, ansi, len(res))
