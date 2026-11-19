import json
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import repark._native as native
from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

TYPES = {"d32": (pa.decimal32(9, 2), 9), "d64": (pa.decimal64(18, 0), 18)}
QUERIES = {
    "g": "SELECT g, avg(c) FROM {v} GROUP BY g",
    "u": "SELECT avg(c) FROM {v}",
    "type": "SELECT typeof(c) FROM {v} LIMIT 1",
}


def write(root: Path) -> None:
    for name, (kind, p) in TYPES.items():
        m = Decimal(10**p - 1).scaleb(-kind.scale)
        files = [[m, m, m], [-m, -m, -m], [Decimal(5)], [Decimal(0)]]
        for k, values in enumerate(files):
            path = root / name
            path.mkdir(parents=True, exist_ok=True)
            table = pa.table({"g": pa.array([1] * len(values)), "c": pa.array(values, kind)})
            pq.write_table(table, path / f"part-{k}.parquet")


def main() -> None:
    root, out = Path(sys.argv[1]), Path(sys.argv[2])
    print("native", native.__file__, flush=True)
    write(root)
    rows = []
    for ansi in ("false", "true"):
        for tp in (1, 2, 4):
            _reset_active_session_for_tests()
            s = (
                ReparkSession.builder.appName("narrow")
                .config("spark.sql.ansi.enabled", ansi)
                .getOrCreate()
            )
            s.sql(f"SET datafusion.execution.target_partitions = {tp}").collect()
            for name in TYPES:
                s.read.parquet(str(root / name)).createOrReplaceTempView(name)
                for key, sql in QUERIES.items():
                    seen: Counter[str] = Counter()
                    for _ in range(20):
                        try:
                            got = [[str(v) for v in r] for r in s.sql(sql.format(v=name)).collect()]
                        except Exception as error:
                            got = "ERR " + " ".join(str(error).split())[:80]
                        seen[json.dumps(got)] += 1
                    rows.append({"ansi": ansi, "tp": tp, "table": name, "q": key, "a": dict(seen)})
            s.stop()
    out.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
