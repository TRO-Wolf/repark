import json
import os
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from common import condition, rows, session

engine, parent, out, group, ansi = sys.argv[1:6]
s, wh, F = session(engine, parent, ansi)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db2").collect()
counter = [0]


def fresh_name(prefix: str) -> str:
    counter[0] += 1
    return prefix + str(counter[0])


def run(statement: str) -> None:
    s.sql(statement).collect()


def make(seed: str = "(1, 'a'), (2, 'b')") -> str:
    name = "lc.db." + fresh_name("t")
    run("CREATE TABLE " + name + " (n INT, c STRING) USING iceberg")
    if seed:
        run("INSERT INTO " + name + " VALUES " + seed)
    return name


def short(name: str) -> str:
    return name.split(".", 1)[1]


def shape(name: str) -> list[Any]:
    frame = s.table(name)
    return [list(frame.columns), rows(frame)]


def one(n: int, c: str) -> Any:
    return s.createDataFrame([(n, c)], "n INT, c STRING")


def w_recreate(t: str) -> None:
    run("DROP TABLE " + t)
    run("CREATE TABLE " + t + " (n INT, c STRING) USING iceberg")
    run("INSERT INTO " + t + " VALUES (5, 'r')")


def w_create(t: str) -> None:
    run("DROP TABLE " + t)
    one(7, "o").writeTo(t).create()


WRITES: dict[str, Callable[[str], Any]] = {
    "insert": lambda t: run("INSERT INTO " + t + " VALUES (9, 'z')"),
    "delete": lambda t: run("DELETE FROM " + t + " WHERE n = 1"),
    "alter_add": lambda t: (
        run("ALTER TABLE " + t + " ADD COLUMN d INT"),
        run("INSERT INTO " + t + " VALUES (9, 'z', 5)"),
    ),
    "alter_drop": lambda t: (
        run("ALTER TABLE " + t + " DROP COLUMN c"),
        run("INSERT INTO " + t + " VALUES (9)"),
    ),
    "alter_rename": lambda t: (
        run("ALTER TABLE " + t + " RENAME COLUMN c TO e"),
        run("INSERT INTO " + t + " VALUES (9, 'z')"),
    ),
    "recreate": w_recreate,
    "wt_create": w_create,
}


def hold(kind: str, t: str) -> tuple[Callable[[], Any], str | None]:
    if kind == "dfview":
        v = fresh_name("dv")
        s.table(t).createOrReplaceTempView(v)
        return (lambda: s.sql("SELECT * FROM " + v)), v
    frames = {
        "table": lambda: s.table(t),
        "sql": lambda: s.sql("SELECT * FROM " + t),
        "read": lambda: s.read.table(t),
        "filter": lambda: s.table(t).filter("n > 0"),
        "select": lambda: s.table(t).select("n"),
        "agg": lambda: s.table(t).groupBy("c").count(),
        "sqlf": lambda: s.sql("SELECT n, c FROM " + t + " WHERE n > 0"),
        "sel_all": lambda: s.table(t).select("n", "c"),
    }
    if kind in frames:
        held = frames[kind]()
        return (lambda: held), None
    held = s.table(t)
    return (lambda: held.join(s.table(t), ["n", "c"])), None


PRE = {"app", "ins_into", "insel"}


def act(action: str, t: str, frame: Callable[[], Any], view: str | None, pre: str) -> Any:
    target = frame()
    if action == "copy":
        target.writeTo(t + "_c").create()
        return shape(t + "_c")
    if action == "cor":
        target.writeTo(t + "_r").createOrReplace()
        return shape(t + "_r")
    if action == "app":
        target.writeTo(pre).append()
        return shape(pre)
    if action == "ins_into":
        target.write.insertInto(pre)
        return shape(pre)
    if action == "sat":
        target.write.saveAsTable(t + "_s")
        return shape(t + "_s")
    if action == "insel":
        if view is None:
            view = fresh_name("iv")
            target.createOrReplaceTempView(view)
        run("INSERT INTO " + pre + " SELECT * FROM " + view)
        return shape(pre)
    target.writeTo(t).append()
    return shape(t)


def bare_cell(kind: str, write: str, action: str) -> Any:
    t = make()
    frame, view = hold(kind, t)
    pre = ""
    if action in PRE:
        pre = t + "_p"
        frame().limit(0).writeTo(pre).create()
    WRITES[write](t)
    return act(action, t, frame, view, pre)


def m_rollback() -> Any:
    t = make()
    sid = s.sql("SELECT snapshot_id FROM " + t + ".snapshots").collect()[0][0]
    WRITES["insert"](t)
    held = s.table(t)
    run("CALL lc.system.rollback_to_snapshot('" + short(t) + "', " + str(sid) + ")")
    return [held.count(), rows(held), s.table(t).count()]


def m_rewrite_delete() -> Any:
    t = make()
    WRITES["insert"](t)
    held = s.table(t)
    options = "options => map('min-input-files', '1')"
    run("CALL lc.system.rewrite_data_files(table => '" + short(t) + "', " + options + ")")
    WRITES["delete"](t)
    return [held.count(), rows(held)]


def m_expire_after_ow() -> Any:
    t = make()
    held = s.table(t)
    run("INSERT OVERWRITE " + t + " VALUES (7, 'o')")
    time.sleep(0.05)
    expire = "older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1"
    run("CALL lc.system.expire_snapshots(table => '" + short(t) + "', " + expire + ")")
    return [held.count(), rows(held)]


def m_spec() -> Any:
    t = make()
    held = s.table(t)
    run("ALTER TABLE " + t + " ADD PARTITION FIELD c")
    WRITES["insert"](t)
    return [held.count(), rows(held)]


def m_sort() -> Any:
    t = make()
    held = s.table(t)
    run("ALTER TABLE " + t + " WRITE ORDERED BY n")
    WRITES["insert"](t)
    return [held.count(), rows(held)]


def m_other_ns() -> Any:
    t = make()
    held = s.table(t)
    other = "lc.db2." + t.split(".")[-1]
    run("DROP TABLE " + t)
    run("CREATE TABLE " + other + " (n INT, c STRING) USING iceberg")
    run("INSERT INTO " + other + " VALUES (5, 'r')")
    return [held.count(), rows(held)]


def m_merge_source() -> Any:
    t = make()
    target = make("")
    v = fresh_name("ms")
    s.table(t).createOrReplaceTempView(v)
    WRITES["insert"](t)
    merge = " AS x USING " + v + " AS u ON x.n = u.n WHEN NOT MATCHED THEN INSERT *"
    run("MERGE INTO " + target + merge)
    return rows(s.table(target))


def m_two_held() -> Any:
    t = make()
    h1 = s.table(t)
    h2 = s.table(t)
    h1.filter("n = 1").writeTo(t).append()
    return [h1.count(), h2.count(), rows(h2), s.table(t).count()]


def m_ow_mid_join() -> Any:
    t = make()
    o = make()
    joined = s.table(t).join(s.table(o).select("n"), "n")
    first = rows(joined)
    one(1, "o").writeTo(t).overwrite(F.expr("n > 0"))
    return [first, rows(joined.select("n", "c")), joined.count()]


def m_ten_rounds() -> Any:
    t = make()
    held = s.table(t)
    counts = []
    for _ in range(10):
        counts.append(held.count())
        WRITES["insert"](t)
    counts.append(held.count())
    return counts


def m_cast() -> Any:
    t = make("(1, '1'), (2, '2')")
    held = s.table(t).select(F.col("c").cast("int").alias("ci"), "n")
    run("INSERT INTO " + t + " VALUES (9, 'zz')")
    got: list[Any] = []
    for body in (lambda: rows(held), held.count):
        try:
            got.append(body())
        except Exception as error:
            got.append("ERROR " + condition(error))
    return got


def m_self_append() -> Any:
    t = make()
    held = s.table(t)
    WRITES["insert"](t)
    held.writeTo(t).append()
    return rows(s.table(t))


def m_first() -> Any:
    t = make()
    held = s.table(t)
    WRITES["delete"](t)
    return [repr(tuple(held.first())), [repr(tuple(r)) for r in held.head(5)]]


def pandas_rows(frame: Any) -> list[str]:
    table = frame.toPandas()
    return sorted(repr((int(a), str(b))) for a, b in zip(table["n"], table["c"], strict=True))


def arrow_rows(frame: Any) -> list[str]:
    table = frame.toArrow()
    pairs = zip(table.column("n").to_pylist(), table.column("c").to_pylist(), strict=True)
    return sorted(repr(pair) for pair in pairs)


def own(first: Callable[[Any], Any], second: Callable[[Any], Any]) -> Any:
    t = make()
    held = s.table(t)
    a = first(held)
    WRITES["insert"](t)
    return [a, second(held)]


def m_refresh_sql() -> Any:
    t = make()
    held = s.table(t)
    statement = "INSERT INTO " + t + " VALUES (9, 'z')"
    if engine == "spark":
        s.newSession().sql(statement).collect()
    else:
        command = os.environ["XWRITER"].split("|")
        environment = dict(os.environ)
        environment.update(json.loads(os.environ.get("XWRITER_ENV", "{}")))
        script = str(Path(__file__).with_name("writer.py"))
        subprocess.run(
            [command[0], script, command[1], wh, statement],
            env=environment,
            capture_output=True,
            check=False,
        )
    before = [held.count(), s.table(t).count()]
    run("REFRESH TABLE " + t)
    return [before, [held.count(), s.table(t).count()]]


MISSED: dict[str, Callable[[], Any]] = {
    "call_rollback": m_rollback,
    "call_rewrite_then_delete": m_rewrite_delete,
    "ins_ow_then_expire": m_expire_after_ow,
    "spec_change": m_spec,
    "sort_change": m_sort,
    "recreate_other_ns": m_other_ns,
    "merge_source_dfview": m_merge_source,
    "two_held_one_written": m_two_held,
    "overwrite_cond_mid_join": m_ow_mid_join,
    "ten_rounds": m_ten_rounds,
    "held_cast": m_cast,
    "self_append": m_self_append,
    "first_head_after_delete": m_first,
    "own_collect_topandas": lambda: own(rows, pandas_rows),
    "own_topandas_topandas": lambda: own(pandas_rows, pandas_rows),
    "own_collect_toarrow": lambda: own(rows, arrow_rows),
    "own_toarrow_toarrow": lambda: own(arrow_rows, arrow_rows),
    "own_toarrow_collect": lambda: own(arrow_rows, rows),
    "refresh_sql_external": m_refresh_sql,
}
HOLDS = ["table", "sql", "read", "filter", "select", "agg", "join_l", "dfview"]
ACTIONS = ["copy", "cor", "app", "ins_into", "sat", "insel", "self"]


def plan() -> list[tuple[str, Callable[[], Any]]]:
    todo: list[tuple[str, Callable[[], Any]]] = []
    if group == "B":
        for h in HOLDS:
            for w in WRITES:
                for a in ACTIONS:
                    tag = "B|" + h + "|" + w + "|" + a
                    todo.append((tag, lambda h=h, w=w, a=a: bare_cell(h, w, a)))
    if group == "B2":
        for h in ("sqlf", "sel_all"):
            for w in WRITES:
                for a in ACTIONS:
                    tag = "B|" + h + "|" + w + "|" + a
                    todo.append((tag, lambda h=h, w=w, a=a: bare_cell(h, w, a)))
    if group == "M":
        for name, body in MISSED.items():
            todo.append(("M|" + name + "|seq|seq", body))
    return todo


with Path(out).open("w") as handle:
    for tag, body in plan():
        try:
            got: Any = body()
        except Exception as error:
            got = "ERROR " + condition(error)
        g, h, w, a = tag.split("|")
        record = {"group": g, "hold": h, "write": w, "action": a, "ansi": ansi, "got": got}
        handle.write(json.dumps(record) + "\n")
        handle.flush()
