import json
import sys
import tempfile
from pathlib import Path

import repark._native as native
from repark import ReparkSession

QUERIES = [
    "SELECT hour(c) FROM lc.db.t",
    "SELECT minute(c) FROM lc.db.t",
    "SELECT second(c) FROM lc.db.t",
    "SELECT year(c) FROM lc.db.t",
    "SELECT month(c) FROM lc.db.t",
    "SELECT dayofmonth(c) FROM lc.db.t",
    "SELECT extract(HOUR FROM c) FROM lc.db.t",
    "SELECT hour(CAST(c AS TIMESTAMP)) FROM lc.db.t",
    "SELECT hour(ts) FROM lc.db.t",
    "SELECT hour(d) FROM lc.db.t",
    "SELECT date_trunc('HOUR', c) FROM lc.db.t",
    "SELECT date_format(c, 'HH') FROM lc.db.t",
    "SELECT unix_timestamp(c) FROM lc.db.t",
    "SELECT CAST(c AS DATE) FROM lc.db.t",
    "SELECT to_timestamp(c) FROM lc.db.t",
]


def main() -> None:
    parent, out = sys.argv[1], sys.argv[2]
    print("NATIVE", native.__file__, flush=True)
    wh = tempfile.mkdtemp(prefix="explain-", dir=parent)
    s = (
        ReparkSession.builder.appName("shms-explain")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", wh + "/lc")
        .config("spark.sql.session.timeZone", "America/Los_Angeles")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
    s.sql("CREATE TABLE lc.db.t (c STRING, ts TIMESTAMP, d DATE) USING iceberg").collect()
    s.sql(
        "INSERT INTO lc.db.t VALUES ('2024-01-01 03:04:05', "
        "TIMESTAMP '2024-01-01 03:04:05', DATE '2024-01-01')"
    ).collect()
    with Path(out).open("w") as handle:
        for query in QUERIES:
            record = {"query": query}
            try:
                rows = s.sql("EXPLAIN " + query).collect()
                record["plan"] = "\n".join(str(v) for row in rows for v in row)
                record["answer"] = [str(v) for v in s.sql(query).collect()[0]]
            except Exception as error:
                record["err"] = " ".join(str(error).split())[:200]
            handle.write(json.dumps(record) + "\n")


main()
