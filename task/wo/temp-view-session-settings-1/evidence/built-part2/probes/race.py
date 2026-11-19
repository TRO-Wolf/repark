import json
import sys
import tempfile
import threading
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

UNRELATED = (
    ("spark.sql.caseSensitive", "false"),
    ("spark.sql.legacy.decimal.retainFractionDigitsOnTruncate", "false"),
)
CAPTURED = (
    ("spark.sql.ansi.enabled", "false"),
    ("spark.sql.session.timeZone", "America/Los_Angeles"),
    ("spark.sql.ansi.enabled", "true"),
    ("spark.sql.session.timeZone", "UTC"),
)
SLOW = 0.5
DOORS = [
    "writeTo",
    "insertInto",
    "writeTo_view_other",
    "insertInto_view_same",
    "sqlInsert_view_other",
]


def build(warehouse: str) -> object:
    return (
        ReparkSession.builder.appName("race")
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", warehouse + "/sc")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


def write(session: object, door: str, step: int) -> None:
    if door == "writeTo":
        session.sql(f"SELECT {step} AS id, -NULL AS c").writeTo("sc.ns.td").append()
    elif door == "insertInto":
        session.sql(f"SELECT {step} AS id, -NULL AS c").write.insertInto("sc.ns.tb")
    elif door == "writeTo_view_other":
        session.table("other_mode").writeTo("sc.ns.td").append()
    elif door == "insertInto_view_same":
        session.table("same_mode").write.insertInto("sc.ns.tb")
    else:
        session.sql("INSERT INTO sc.ns.td SELECT id, c FROM other_mode").collect()


def main() -> None:
    warehouse_root, out = sys.argv[1], sys.argv[2]
    per_door = int(sys.argv[3]) if len(sys.argv) > 3 else 200
    seconds = float(sys.argv[4]) if len(sys.argv) > 4 else 0.0
    settings = (
        UNRELATED + CAPTURED if len(sys.argv) > 5 and sys.argv[5] == "captured" else UNRELATED
    )
    print("NATIVE", native.__file__, flush=True)
    session = build(tempfile.mkdtemp(prefix="race-", dir=warehouse_root))
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns").collect()
    session.sql("CREATE TABLE sc.ns.td (id INT, c DATE) USING iceberg").collect()
    session.sql("CREATE TABLE sc.ns.tb (id INT, c BOOLEAN) USING iceberg").collect()
    session.conf.set("spark.sql.ansi.enabled", "false")
    session.sql("SELECT 7 AS id, -NULL AS c").createOrReplaceTempView("other_mode")
    session.conf.set("spark.sql.ansi.enabled", "true")
    session.sql("SELECT 5 AS id, -NULL AS c").createOrReplaceTempView("same_mode")
    done = threading.Event()
    guard = threading.Lock()
    result: dict = {
        "landed": {},
        "refused": {},
        "other": {},
        "texts": {},
        "conf_sets": 0,
        "slowest_write_s": 0.0,
        "slow_writes": [],
    }
    deadline = time.monotonic() + seconds

    def note(door: str, kind: str, text: str, took: float, step: int) -> None:
        with guard:
            result[kind][door] = result[kind].get(door, 0) + 1
            result["slowest_write_s"] = max(result["slowest_write_s"], took)
            if took > SLOW:
                result["slow_writes"].append([door, step, round(took, 3)])
            if kind == "other":
                result["texts"].setdefault(door, text[:200])

    def writer(door: str) -> None:
        step = 0
        while (step < per_door) if seconds <= 0 else (time.monotonic() < deadline):
            began = time.monotonic()
            try:
                write(session, door, step)
            except Exception as error:
                text = " ".join(str(error).split())
                refused = "CANNOT_SAFELY_CAST" in text or "INCOMPATIBLE_DATA" in text
                note(door, "refused" if refused else "other", text, time.monotonic() - began, step)
            else:
                note(door, "landed", "", time.monotonic() - began, step)
            step += 1

    def setter() -> None:
        made = 0
        while not done.is_set():
            for key, value in settings:
                session.conf.set(key, value)
                made += 1
        with guard:
            result["conf_sets"] += made

    started = time.monotonic()
    setters = [threading.Thread(target=setter) for _ in range(2)]
    writers = [threading.Thread(target=writer, args=(door,)) for door in DOORS]
    for thread in setters + writers:
        thread.start()
    for thread in writers:
        thread.join()
    done.set()
    for thread in setters:
        thread.join()
    result["elapsed_s"] = round(time.monotonic() - started, 2)
    result["writes"] = sum(sum(result[kind].values()) for kind in ("landed", "refused", "other"))
    result["settings"] = [key for key, _ in settings]
    session.conf.set("spark.sql.ansi.enabled", "true")
    result["rows_td"] = session.sql("SELECT count(*) FROM sc.ns.td").collect()[0][0]
    result["rows_tb"] = session.sql("SELECT count(*) FROM sc.ns.tb").collect()[0][0]
    Path(out).write_text(json.dumps(result, indent=1, sort_keys=True))
    print(json.dumps(result, sort_keys=True), flush=True)
    session.stop()


if __name__ == "__main__":
    main()
