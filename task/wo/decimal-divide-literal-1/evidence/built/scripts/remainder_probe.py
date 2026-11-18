import importlib
import json
import sys
import tempfile
from pathlib import Path

engine, ansi, probes, wh_parent, jar, out = sys.argv[1:7]
sys.path.insert(0, probes)
lib = importlib.import_module("lib")

COLUMNS = {
    "id": "INT",
    "d10_2": "DECIMAL(10,2)",
    "d28_2": "DECIMAL(28,2)",
    "d38_10": "DECIMAL(38,10)",
    "i": "INT",
    "zd": "DECIMAL(5,2)",
    "zi": "INT",
}
ROWS = [
    ["1", "7.25", "7.25", "7.25", "7", "0", "0"],
    ["2", "-7.25", "-7.25", "-7.25", "-7", "0", "0"],
    ["3", None, None, None, None, "0", "0"],
    ["4", "6.00", "6.00", "6.00", "6", "0", "0"],
    ["5", "-6.00", "-6.00", "-6.00", "-6", "0", "0"],
    [
        "6",
        "12345678.91",
        "12345678901234567890123456.78",
        "1234567890123456789012345678.1234567890",
        "2147483647",
        "0",
        "0",
    ],
    [
        "7",
        "-12345678.91",
        "-12345678901234567890123456.78",
        "-1234567890123456789012345678.1234567890",
        "-2147483648",
        "0",
        "0",
    ],
]
PRECISIONS = ("d10_2", "d28_2", "d38_10")
FABLE = [
    "mod(d, 3)",
    "d % 3",
    "pmod(d, 3)",
    "mod(d, 0)",
    "d % 0",
    "pmod(d, 0)",
    "mod(d, -3)",
    "d % -3",
    "pmod(d, -3)",
    "mod(d, 3L)",
    "d % 3L",
    "mod(d, CAST(0 AS DECIMAL(5,2)))",
    "d % CAST(0 AS DECIMAL(5,2))",
    "mod(3, d)",
    "3 % d",
    "pmod(3, d)",
]


def sql_cells() -> dict[str, str]:
    found: dict[str, str] = {}
    for e in FABLE:
        found[f"fable/{e}"] = f"SELECT {e} AS q FROM f"
    for c in PRECISIONS:
        for name, e in {
            "zero/mod": f"mod({c}, 0)",
            "zero/pct": f"{c} % 0",
            "zero/pmod": f"pmod({c}, 0)",
            "zero/mod_dec": f"mod({c}, CAST(0 AS DECIMAL(5,2)))",
            "zero/pct_dec": f"{c} % CAST(0 AS DECIMAL(5,2))",
            "zero/pmod_dec": f"pmod({c}, CAST(0 AS DECIMAL(5,2)))",
            "zero/mod_col": f"mod({c}, zd)",
            "zero/pct_col": f"{c} % zd",
            "zero/pmod_col": f"pmod({c}, zd)",
            "neg/mod_pos": f"mod({c}, 3)",
            "neg/mod_neg": f"mod({c}, -3)",
            "neg/pct_pos": f"{c} % 3",
            "neg/pct_neg": f"{c} % -3",
            "neg/pmod_pos": f"pmod({c}, 3)",
            "neg/pmod_neg": f"pmod({c}, -3)",
            "neg/mod_dec_neg": f"mod({c}, -0.7)",
            "neg/pmod_dec_neg": f"pmod({c}, -0.7)",
            "neg/mod_left_neg": f"mod(-3, {c})",
            "neg/pmod_left_neg": f"pmod(-3, {c})",
            "neg/mod_3L": f"mod({c}, 3L)",
            "neg/pmod_3L": f"pmod({c}, 3L)",
        }.items():
            found[f"sql/{name}/{c}"] = f"SELECT id, {e} AS q FROM z"
    for name, e in {
        "int/mod_zero": "mod(i, 0)",
        "int/pct_zero": "i % 0",
        "int/pmod_zero": "pmod(i, 0)",
        "int/mod_zero_col": "mod(i, zi)",
        "int/pmod_zero_col": "pmod(i, zi)",
        "int/mod_neg": "mod(i, -3)",
        "int/pmod_neg": "pmod(i, -3)",
        "int/pmod_pos": "pmod(i, 3)",
    }.items():
        found[f"sql/{name}"] = f"SELECT id, {e} AS q FROM z"
    return found


def frame_cells(sf: object) -> dict:
    found = {}
    for c in PRECISIONS:
        d = sf.col(c)
        found.update(
            {
                f"df/expr_mod_pos/{c}": lambda f, c=c: f.select(
                    "id", sf.expr(f"mod({c}, 3)").alias("q")
                ),
                f"df/expr_mod_neg/{c}": lambda f, c=c: f.select(
                    "id", sf.expr(f"mod({c}, -3)").alias("q")
                ),
                f"df/expr_mod_zero/{c}": lambda f, c=c: f.select(
                    "id", sf.expr(f"mod({c}, 0)").alias("q")
                ),
                f"df/expr_pmod_pos/{c}": lambda f, c=c: f.select(
                    "id", sf.expr(f"pmod({c}, 3)").alias("q")
                ),
                f"df/expr_pmod_neg/{c}": lambda f, c=c: f.select(
                    "id", sf.expr(f"pmod({c}, -3)").alias("q")
                ),
                f"df/expr_pmod_zero/{c}": lambda f, c=c: f.select(
                    "id", sf.expr(f"pmod({c}, 0)").alias("q")
                ),
                f"df/F.pmod_pos/{c}": lambda f, d=d: f.select(
                    "id", sf.pmod(d, sf.lit(3)).alias("q")
                ),
                f"df/F.pmod_neg/{c}": lambda f, d=d: f.select(
                    "id", sf.pmod(d, sf.lit(-3)).alias("q")
                ),
                f"df/F.pmod_zero/{c}": lambda f, d=d: f.select(
                    "id", sf.pmod(d, sf.lit(0)).alias("q")
                ),
                f"df/pct_pos/{c}": lambda f, d=d: f.select("id", (d % 3).alias("q")),
                f"df/pct_neg/{c}": lambda f, d=d: f.select("id", (d % -3).alias("q")),
                f"df/pct_zero/{c}": lambda f, d=d: f.select("id", (d % 0).alias("q")),
                f"df/selectExpr_mod_zero/{c}": lambda f, c=c: f.selectExpr(
                    "id", f"mod({c}, 0) AS q"
                ),
                f"df/selectExpr_pmod_neg/{c}": lambda f, c=c: f.selectExpr(
                    "id", f"pmod({c}, -3) AS q"
                ),
            }
        )
    return found


wh = tempfile.mkdtemp(prefix=f"rem-{engine}-{ansi}-", dir=wh_parent)
s, sf, _, prov = lib.session(engine, ansi, wh, jar, {})
names = list(COLUMNS)
raw = s.createDataFrame(ROWS, ", ".join(f"{n} STRING" for n in names))
raw.selectExpr(*[f"CAST({n} AS {COLUMNS[n]}) AS {n}" for n in names]).write.mode(
    "overwrite"
).parquet(f"{wh}/pq_z")
s.read.parquet(f"{wh}/pq_z").createOrReplaceTempView("z")
s.sql("SELECT d10_2 AS d FROM z WHERE id <= 3").createOrReplaceTempView("f")
records = [dict(key="provenance", ansi=ansi, **prov)]
for key, sql in sql_cells().items():
    records.append(
        {"key": key, "sql": sql, "result": lib.attempt(lambda q=sql: lib.answer_of(s.sql(q)))}
    )
frame = s.table("z")
for key, build in frame_cells(sf).items():
    records.append({"key": key, "result": lib.attempt(lambda b=build: lib.answer_of(b(frame)))})
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records), flush=True)
