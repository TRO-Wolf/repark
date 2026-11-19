import json
import os
import sys
from pathlib import Path

import r1_cells

import repark._native as native
from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests


def main() -> None:
    out_path, tag, set_name = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
    tp, ansi, runs = int(sys.argv[4]), sys.argv[5], int(sys.argv[6])
    step0, mine = Path(sys.argv[7]), Path(sys.argv[8])
    extra = json.loads(sys.argv[9]) if len(sys.argv) > 9 else {}
    cpus = len(os.sched_getaffinity(0))
    build = native.__file__.split("/")[-3]
    print("native", native.__file__, "cpus", cpus, "tag", tag, "tp", tp, "ansi", ansi, flush=True)
    _reset_active_session_for_tests()
    builder = ReparkSession.builder.appName("avg-r1")
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    builder = builder.config("spark.sql.ansi.enabled", ansi)
    for key, value in extra.get("builder", {}).items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    if extra.get("steal") == "off":
        session.sql("SET datafusion.execution.enable_file_stream_work_stealing = false").collect()
    if tp:
        session.sql(f"SET datafusion.execution.target_partitions = {tp}").collect()
    for statement in extra.get("sets", []):
        session.sql(statement).collect()
    refused = r1_cells.register(session, step0, mine)
    base = {"engine": "repark", "build": build, "cpus": cpus, "tag": tag, "tp": tp, "ansi": ansi}
    with out_path.open("a") as out:
        for name, why in refused.items():
            row = base | {"query": f"view/{name}", "answers": {json.dumps(why): 1}}
            out.write(json.dumps(row) + "\n")
        for key, sql in r1_cells.SETS[set_name]().items():
            answers = r1_cells.count(lambda sql=sql: session.sql(sql).collect(), runs)
            out.write(json.dumps(base | {"query": key, "answers": answers}) + "\n")
            out.flush()
    session.stop()
    _reset_active_session_for_tests()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
