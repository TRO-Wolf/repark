import json
import re
import sys
from collections import Counter
from pathlib import Path

from common import error_record, write_jsonl

import repark._native as native
from repark import ReparkSession

table, out = sys.argv[1], sys.argv[2]

FLOAT_CAST = re.compile(r"CAST\(\s*-?[0-9.eE+-]+\s+AS\s+(DOUBLE|FLOAT|REAL)\s*\)", re.I)
DEC_TOKEN = re.compile(r"DECIMAL\(|(?<![\w.'])\d+\.\d+(?![\w.'])", re.I)
FLOAT_TOKEN = re.compile(r"AS\s+(DOUBLE|FLOAT|REAL)\b", re.I)


def root(sql: str) -> str:
    has_float = bool(FLOAT_TOKEN.search(sql))
    rest = FLOAT_CAST.sub("F", sql)
    has_dec = bool(DEC_TOKEN.search(rest))
    return "decimal-double" if has_float and has_dec else "other"


data = json.loads(Path(table).read_text())
records = [
    {
        "key": "provenance",
        "engine": "repark",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
        "ansi": data["banner"]["ansi"],
        "table": "#888 test_nvl_type_coercion_1_spark.json at 54bc9ac2",
    }
]
for section, zone in (("select_utc", "UTC"), ("select_ny", "America/New_York")):
    s = ReparkSession.builder.appName("r888").config("spark.sql.ansi.enabled", "true").getOrCreate()
    s.conf.set("spark.sql.session.timeZone", zone)
    for cell in data[section]:
        exp = cell["expect"]
        rec = {"key": cell["key"], "section": section, "root": root(cell["sql"]), "expect": exp}
        try:
            row = s.sql(cell["sql"]).collect()[0]
            got = {"t": row[0], "v": None if row[1] is None else str(row[1])}
            rec["got"] = got
            if "error" in exp:
                rec["verdict"] = "spark-raises-repark-answers"
            else:
                rec["verdict"] = (
                    "equal"
                    if got == {"t": exp["t"], "v": exp["v"]}
                    else ("type-diff" if got["t"] != exp["t"] else "value-diff")
                )
        except Exception as e:
            rec["got"] = error_record(e)
            rec["verdict"] = "both-raise" if "error" in exp else "repark-raises"
        records.append(rec)
write_jsonl(out, records)
print(Counter((r.get("root"), r.get("verdict")) for r in records[1:]))
