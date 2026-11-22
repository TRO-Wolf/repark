import contextlib
import json
import os
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from common import condition, rows, session

engine, parent, out, ansi = sys.argv[1:5]
ONLY = set(filter(None, os.environ.get("ONLY", "").split(",")))
s, wh, F = session(engine, parent, ansi)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
counter = [0]


def fresh_name(prefix: str) -> str:
    counter[0] += 1
    return prefix + str(counter[0])


def run(statement: str) -> None:
    s.sql(statement).collect()


def make(seed: bool = True) -> str:
    name = "lc.db." + fresh_name("t")
    run("CREATE TABLE " + name + " (n INT, c STRING) USING iceberg")
    if seed:
        run("INSERT INTO " + name + " VALUES (1, 'a'), (2, 'b')")
    return name


def one(n: int, c: str) -> Any:
    return s.createDataFrame([(n, c)], "n INT, c STRING")


def insert(t: str) -> None:
    run("INSERT INTO " + t + " VALUES (9, 'z')")


def external(t: str, how: str) -> str:
    statement = "INSERT INTO " + t + " VALUES (9, 'z')"
    if how == "session":
        s.newSession().sql(statement).collect()
        return "session"
    command = os.environ["XWRITER"].split("|")
    environment = dict(os.environ)
    environment.update(json.loads(os.environ.get("XWRITER_ENV", "{}")))
    done = subprocess.run(
        [command[0], str(Path(__file__).with_name("writer.py")), command[1], wh, statement],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    tail = (done.stdout.strip().splitlines() or [done.stderr.strip()[-80:]])[-1]
    return "rc=" + str(done.returncode) + " " + tail[:40]


MERGE = (
    "MERGE INTO {t} AS d USING (SELECT 2 AS n, 'y' AS c UNION ALL SELECT 9 AS n, 'z' AS c) AS u"
    " ON d.n = u.n WHEN MATCHED THEN UPDATE SET c = u.c WHEN NOT MATCHED THEN INSERT *"
)
WRITES: dict[str, Callable[[str], None]] = {
    "insert": insert,
    "delete": lambda t: run("DELETE FROM " + t + " WHERE n = 1"),
    "update": lambda t: run("UPDATE " + t + " SET c = 'u' WHERE n = 2"),
    "merge": lambda t: run(MERGE.format(t=t)),
    "ins_ow": lambda t: run("INSERT OVERWRITE " + t + " VALUES (7, 'o')"),
    "ins_select": lambda t: run("INSERT INTO " + t + " SELECT 9 AS n, 'z' AS c"),
    "wt_append": lambda t: one(9, "z").writeTo(t).append(),
    "wt_overwrite": lambda t: one(7, "o").writeTo(t).overwrite(F.expr("n > 0")),
    "insert_into": lambda t: one(9, "z").write.insertInto(t),
    "save_append": lambda t: one(9, "z").write.mode("append").saveAsTable(t),
}


def cached(kind: str) -> list[Any]:
    t = make()
    held = s.table(t)
    got: list[Any] = []
    if kind == "cache_table":
        run("CACHE TABLE " + t)
    else:
        held.cache()
        if kind != "cache_lazy":
            got.append(held.count())
    if kind == "cache_append":
        one(9, "z").writeTo(t).append()
    else:
        insert(t)
    got.extend([held.count(), s.table(t).count()])
    s.catalog.clearCache()
    return got


def k_cell(kind: str) -> Any:
    t = make()
    held = s.table(t)
    if kind == "persist_del":
        held.persist()
        first = rows(held)
        run("DELETE FROM " + t + " WHERE n = 1")
        found = [first, rows(held)]
    elif kind == "cache_fresh":
        held.cache()
        first = held.count()
        insert(t)
        found = [first, s.table(t).count()]
    else:
        held.cache()
        first = held.count()
        insert(t)
        found = [first, held.count()]
    held.unpersist()
    return found


def write_cell(write: str) -> list[Any]:
    t = make()
    held = s.table(t)
    held.cache()
    got: list[Any] = [held.count()]
    WRITES[write](t)
    got.extend([held.count(), rows(held.select("n", "c")), s.table(t).count()])
    s.catalog.clearCache()
    return got


def ext_cell(how: str, refresh: str) -> list[Any]:
    t = make()
    held = s.table(t)
    held.cache()
    got: list[Any] = [held.count(), external(t, how), held.count(), s.table(t).count()]
    if refresh == "sql":
        run("REFRESH TABLE " + t)
    else:
        s.catalog.refreshTable(t)
    got.extend([held.count(), s.table(t).count()])
    s.catalog.clearCache()
    return got


def stamp(frame: Any) -> Any:
    return frame.agg(F.max("ts")).collect()[0][0]


def ts_cell(event: str) -> list[Any]:
    t = make()
    other = make()
    held = s.table(t).withColumn("ts", F.current_timestamp())
    held.cache()
    first = stamp(held)
    time.sleep(0.05)
    note: Any = None
    if event == "insert_t":
        insert(t)
    elif event == "insert_other":
        insert(other)
    elif event == "refresh_t":
        run("REFRESH TABLE " + t)
    elif event == "refresh_api_t":
        s.catalog.refreshTable(t)
    elif event == "refresh_other":
        run("REFRESH TABLE " + other)
    elif event == "failed_write_t":
        try:
            run("INSERT INTO " + t + " (n, zz) VALUES (1, 'q')")
            note = "wrote"
        except Exception as error:
            note = condition(error)
    elif event == "fresh_read_t":
        note = s.table(t).count()
    elif event == "ext_session_t":
        note = external(t, "session")
    elif event == "ext_process_t":
        note = external(t, "process")
    elif event == "alter_add_t":
        run("ALTER TABLE " + t + " ADD COLUMN x INT")
    elif event == "set_props_t":
        run("ALTER TABLE " + t + " SET TBLPROPERTIES ('k1' = 'v1')")
    elif event == "branch_t":
        run("ALTER TABLE " + t + " CREATE BRANCH b1")
    elif event == "expire_t":
        insert(t)
        run("CALL lc.system.expire_snapshots(table => '" + t[3:] + "', retain_last => 1)")
    elif event == "rewrite_t":
        insert(t)
        run("CALL lc.system.rewrite_data_files(table => '" + t[3:] + "')")
    elif event == "rollback_t":
        snaps = s.sql("SELECT snapshot_id FROM " + t + ".snapshots ORDER BY committed_at").collect()
        insert(t)
        run("CALL lc.system.rollback_to_snapshot('" + t[3:] + "', " + str(snaps[-1][0]) + ")")
    second = stamp(held)
    got = [event, first == second, held.count(), note]
    s.catalog.clearCache()
    return got


def u_cell(kind: str) -> list[Any]:
    t = make()
    held = s.table(t)
    got: list[Any] = []
    if kind == "unpersist_after":
        held.cache()
        got.append(held.count())
        insert(t)
        held.unpersist()
        got.extend([held.count(), held.is_cached])
    elif kind == "uncache_sql":
        run("CACHE TABLE " + t)
        insert(t)
        run("UNCACHE TABLE " + t)
        got.extend([held.count(), s.table(t).count(), s.catalog.isCached(t)])
    elif kind == "uncache_api":
        s.catalog.cacheTable(t)
        insert(t)
        s.catalog.uncacheTable(t)
        got.extend([s.table(t).count(), s.catalog.isCached(t)])
    elif kind == "is_cached_after_write":
        s.catalog.cacheTable(t)
        insert(t)
        got.append(s.catalog.isCached(t))
        got.append(s.table(t).count())
        got.append(s.catalog.isCached(t))
    elif kind == "frame_cached_after_write":
        held.cache()
        got.append(held.count())
        insert(t)
        got.append(s.table(t).count())
        got.append(held.is_cached)
        got.append(held.count())
    elif kind == "cache_lazy_table":
        run("CACHE LAZY TABLE " + t)
        insert(t)
        got.extend([s.table(t).count(), held.count()])
    elif kind == "two_inserts":
        held.cache()
        got.append(held.count())
        insert(t)
        got.append(held.count())
        run("INSERT INTO " + t + " VALUES (8, 'q')")
        got.append(held.count())
    s.catalog.clearCache()
    return got


def v_cell(kind: str) -> list[Any]:
    t = make()
    held = s.table(t)
    got: list[Any] = []
    if kind == "child_cache":
        child = held.filter("n > 0")
        child.cache()
        got.append(child.count())
        insert(t)
        got.extend([child.count(), held.count()])
    elif kind == "child_of_cached":
        held.cache()
        got.append(held.count())
        child = held.filter("n > 0")
        got.append(child.count())
        insert(t)
        got.append(child.count())
    elif kind == "child_built_after":
        held.cache()
        got.append(held.count())
        insert(t)
        got.append(held.filter("n > 0").count())
    elif kind == "cached_dfview":
        held.cache()
        got.append(held.count())
        view = fresh_name("v")
        held.createOrReplaceTempView(view)
        insert(t)
        got.append(s.sql("SELECT count(*) FROM " + view).collect()[0][0])
        got.append(held.count())
    elif kind == "sqlview_cache_table":
        view = fresh_name("v")
        run("CREATE TEMPORARY VIEW " + view + " AS SELECT * FROM " + t)
        run("CACHE TABLE " + view)
        got.append(s.sql("SELECT count(*) FROM " + view).collect()[0][0])
        insert(t)
        got.append(s.sql("SELECT count(*) FROM " + view).collect()[0][0])
    elif kind == "cache_sqlview_frame":
        view = fresh_name("v")
        run("CREATE TEMPORARY VIEW " + view + " AS SELECT * FROM " + t)
        framed = s.sql("SELECT * FROM " + view)
        framed.cache()
        got.append(framed.count())
        insert(t)
        got.append(framed.count())
    elif kind == "nested_cache":
        held.cache()
        got.append(held.count())
        child = held.filter("n > 0")
        child.cache()
        got.append(child.count())
        insert(t)
        got.extend([child.count(), held.count()])
    elif kind == "nested_cache_child_first":
        held.cache()
        child = held.filter("n > 0")
        child.cache()
        got.append(child.count())
        insert(t)
        got.extend([child.count(), held.count()])
    elif kind == "join_other":
        u = make()
        joined = s.table(t).join(s.table(u).withColumnRenamed("c", "d"), "n")
        joined.cache()
        got.append(joined.count())
        run("INSERT INTO " + u + " VALUES (1, 'q')")
        got.append(joined.count())
    elif kind == "join_self_table":
        u = make()
        joined = s.table(t).join(s.table(u).withColumnRenamed("c", "d"), "n")
        joined.cache()
        got.append(joined.count())
        run("INSERT INTO " + t + " VALUES (2, 'q')")
        got.append(joined.count())
    s.catalog.clearCache()
    return got


def o_cell(kind: str) -> list[Any]:
    t = make()
    held = s.table(t)
    got: list[Any] = []
    if kind == "own_collect_collect":
        held.cache()
        got.append(rows(held))
        insert(t)
        got.append(rows(held))
    elif kind == "own_count_collect":
        held.cache()
        got.append(held.count())
        insert(t)
        got.append(rows(held))
    elif kind == "own_collect_count":
        held.cache()
        got.append(rows(held))
        insert(t)
        got.append(held.count())
    elif kind == "cache_insert_collect":
        held.cache()
        insert(t)
        got.append(rows(held))
        got.append(held.count())
    elif kind == "cache_insert_count_collect":
        held.cache()
        insert(t)
        got.append(held.count())
        got.append(rows(held))
    elif kind == "child_collect_twice":
        held.cache()
        got.append(held.count())
        child = held.filter("n > 0")
        got.append(rows(child))
        insert(t)
        got.append(rows(child))
        got.append(held.count())
    elif kind == "child_count_collect":
        held.cache()
        child = held.filter("n > 0")
        got.append(child.count())
        insert(t)
        got.append(rows(child))
    elif kind == "collect_then_cache":
        got.append(rows(held))
        held.cache()
        got.append(held.count())
        insert(t)
        got.extend([held.count(), rows(held)])
    s.catalog.clearCache()
    return got


def d_cell(kind: str) -> list[Any]:
    t = make()
    held = s.table(t)
    held.cache()
    got: list[Any] = [held.count()]
    if kind == "alter_add":
        run("ALTER TABLE " + t + " ADD COLUMN x INT")
        run("INSERT INTO " + t + " VALUES (9, 'z', 1)")
    elif kind == "alter_drop":
        run("ALTER TABLE " + t + " DROP COLUMN c")
    elif kind == "recreate":
        run("DROP TABLE " + t)
        run("CREATE TABLE " + t + " (n INT, c STRING) USING iceberg")
        run("INSERT INTO " + t + " VALUES (5, 'r')")
    elif kind == "alter_drop_insert":
        run("ALTER TABLE " + t + " DROP COLUMN c")
        run("INSERT INTO " + t + " VALUES (9)")
    elif kind == "rename_col":
        run("ALTER TABLE " + t + " RENAME COLUMN c TO d")
    elif kind == "rename_col_insert":
        run("ALTER TABLE " + t + " RENAME COLUMN c TO d")
        run("INSERT INTO " + t + " VALUES (9, 'z')")
    elif kind == "drop_table":
        run("DROP TABLE " + t)
    elif kind == "alter_add_only":
        run("ALTER TABLE " + t + " ADD COLUMN x INT")
    for action in (lambda: held.count(), lambda: rows(held.select("n")), lambda: held.is_cached):
        try:
            got.append(action())
        except Exception as error:
            got.append("ERROR " + condition(error))
    s.catalog.clearCache()
    return got


def plan() -> list[tuple[str, Callable[[], Any]]]:
    todo: list[tuple[str, Callable[[], Any]]] = []
    for kind in ("cache_count", "cache_lazy", "cache_table", "cache_append"):
        todo.append(("C|" + kind, lambda k=kind: cached(k)))
    for kind in ("cache_held", "cache_fresh", "persist_del"):
        todo.append(("K|" + kind, lambda k=kind: k_cell(k)))
    for write in WRITES:
        todo.append(("W|" + write, lambda w=write: write_cell(w)))
    for how in ("session", "process"):
        for refresh in ("sql", "api"):
            todo.append(("E|" + how + "_" + refresh, lambda h=how, r=refresh: ext_cell(h, r)))
    for event in (
        "none",
        "insert_t",
        "insert_other",
        "refresh_t",
        "refresh_api_t",
        "refresh_other",
        "failed_write_t",
        "fresh_read_t",
        "ext_session_t",
        "ext_process_t",
        "alter_add_t",
        "set_props_t",
        "branch_t",
        "expire_t",
        "rewrite_t",
        "rollback_t",
    ):
        todo.append(("T|" + event, lambda e=event: ts_cell(e)))
    for kind in (
        "unpersist_after",
        "uncache_sql",
        "uncache_api",
        "is_cached_after_write",
        "frame_cached_after_write",
        "cache_lazy_table",
        "two_inserts",
    ):
        todo.append(("U|" + kind, lambda k=kind: u_cell(k)))
    for kind in (
        "child_cache",
        "child_of_cached",
        "child_built_after",
        "cached_dfview",
        "sqlview_cache_table",
        "join_other",
        "join_self_table",
        "cache_sqlview_frame",
        "nested_cache",
        "nested_cache_child_first",
    ):
        todo.append(("V|" + kind, lambda k=kind: v_cell(k)))
    for kind in (
        "own_collect_collect",
        "own_count_collect",
        "own_collect_count",
        "collect_then_cache",
        "cache_insert_collect",
        "cache_insert_count_collect",
        "child_collect_twice",
        "child_count_collect",
    ):
        todo.append(("O|" + kind, lambda k=kind: o_cell(k)))
    for kind in (
        "alter_add",
        "alter_drop",
        "recreate",
        "alter_drop_insert",
        "rename_col",
        "rename_col_insert",
        "drop_table",
        "alter_add_only",
    ):
        todo.append(("D|" + kind, lambda k=kind: d_cell(k)))
    if ONLY:
        todo = [item for item in todo if item[0].split("|")[0] in ONLY or item[0] in ONLY]
    return todo


with Path(out).open("w") as handle:
    for tag, body in plan():
        try:
            got: Any = body()
        except Exception as error:
            got = "ERROR " + condition(error)
            with contextlib.suppress(Exception):
                s.catalog.clearCache()
        handle.write(json.dumps({"cell": tag, "ansi": ansi, "got": got}, default=str) + "\n")
        handle.flush()
print("DONE", counter[0], flush=True)
