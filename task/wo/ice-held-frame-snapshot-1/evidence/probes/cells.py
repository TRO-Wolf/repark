import contextlib
import io
import json
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from common import condition, rows, session, shown

engine, parent, out, group, ansi = sys.argv[1:6]
s, wh, F = session(engine, parent, ansi)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
counter = [0]
PART = {"ins_ow_dyn", "wt_ow_parts"}


def fresh_name(prefix: str) -> str:
    counter[0] += 1
    return prefix + str(counter[0])


def run(statement: str) -> None:
    s.sql(statement).collect()


def make(part: bool = False, seed: bool = True) -> str:
    name = "lc.db." + fresh_name("t")
    clause = " PARTITIONED BY (c)" if part else ""
    run("CREATE TABLE " + name + " (n INT, c STRING) USING iceberg" + clause)
    if seed:
        run("INSERT INTO " + name + " VALUES (1, 'a'), (2, 'b')")
    return name


def location(name: str) -> str:
    return wh + "/lc/db/" + name.split(".")[-1]


def one(n: int, c: str) -> Any:
    return s.createDataFrame([(n, c)], "n INT, c STRING")


def w_dyn(t: str) -> None:
    run("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    try:
        run("INSERT OVERWRITE " + t + " VALUES (5, 'a')")
    finally:
        run("SET spark.sql.sources.partitionOverwriteMode=static")


def w_recreate(t: str) -> None:
    run("DROP TABLE " + t)
    run("CREATE TABLE " + t + " (n INT, c STRING) USING iceberg")
    run("INSERT INTO " + t + " VALUES (5, 'r')")


def w_create(t: str) -> None:
    run("DROP TABLE " + t)
    one(7, "o").writeTo(t).create()


SEVEN = "SELECT 7 AS n, 'o' AS c"
MERGE = (
    "MERGE INTO {t} AS d USING (SELECT 2 AS n, 'y' AS c UNION ALL SELECT 9 AS n, 'z' AS c) AS u"
    " ON d.n = u.n WHEN MATCHED THEN UPDATE SET c = u.c WHEN NOT MATCHED THEN INSERT *"
)
WRITES: dict[str, Callable[[str], None]] = {
    "none": lambda t: None,
    "insert": lambda t: run("INSERT INTO " + t + " VALUES (9, 'z')"),
    "delete": lambda t: run("DELETE FROM " + t + " WHERE n = 1"),
    "update": lambda t: run("UPDATE " + t + " SET n = n + 10"),
    "merge": lambda t: run(MERGE.format(t=t)),
    "ins_ow": lambda t: run("INSERT OVERWRITE " + t + " VALUES (7, 'o')"),
    "ins_ow_dyn": w_dyn,
    "wt_append": lambda t: one(9, "z").writeTo(t).append(),
    "wt_overwrite": lambda t: one(7, "o").writeTo(t).overwrite(F.expr("n > 0")),
    "wt_ow_parts": lambda t: one(5, "a").writeTo(t).overwritePartitions(),
    "wt_replace": lambda t: one(7, "o").writeTo(t).replace(),
    "wt_cor": lambda t: one(7, "o").writeTo(t).createOrReplace(),
    "wt_create": w_create,
    "rtas": lambda t: run("CREATE OR REPLACE TABLE " + t + " USING iceberg AS " + SEVEN),
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
}


def view_frame(v: str) -> Callable[[], Any]:
    return lambda: s.sql("SELECT * FROM " + v)


def hold(kind: str, t: str) -> tuple[Callable[[], Any], str | None]:
    if kind == "dfview":
        v = fresh_name("dv")
        s.table(t).createOrReplaceTempView(v)
        return view_frame(v), v
    if kind == "sqlview":
        v = fresh_name("sv")
        run("CREATE OR REPLACE TEMPORARY VIEW " + v + " AS SELECT * FROM " + t)
        return view_frame(v), v
    frames = {
        "table": lambda: s.table(t),
        "sql": lambda: s.sql("SELECT * FROM " + t),
        "read": lambda: s.read.table(t),
        "path": lambda: s.read.format("iceberg").load(location(t)),
        "filter": lambda: s.table(t).filter("n > 0"),
        "agg": lambda: s.table(t).groupBy("c").count(),
    }
    if kind in frames:
        held = frames[kind]()
        return (lambda: held), None
    held = s.table(t)
    if kind == "join_l":
        return (lambda: held.join(s.table(t), ["n", "c"])), None
    return (lambda: s.table(t).join(held, ["n", "c"])), None


def act(action: str, t: str, frame: Callable[[], Any], view: str | None) -> Any:
    target = frame()
    if action == "count":
        return target.count()
    if action == "collect":
        return rows(target)
    if action == "show":
        return shown(target)
    if action == "copy":
        target.writeTo(t + "_copy").create()
        return rows(s.table(t + "_copy"))
    if action == "insel":
        if view is None:
            view = fresh_name("iv")
            target.createOrReplaceTempView(view)
        u = "lc.db." + fresh_name("u")
        run("CREATE TABLE " + u + " USING iceberg AS SELECT * FROM " + view + " WHERE 1 = 0")
        run("INSERT INTO " + u + " SELECT * FROM " + view)
        return rows(s.table(u))
    key = target.columns[0]
    return rows(target.alias("x").join(target.alias("y"), key))


def cell(hold_kind: str, write: str, action: str) -> Any:
    t = make(write in PART)
    frame, view = hold(hold_kind, t)
    WRITES[write](t)
    return act(action, t, frame, view)


def snapshot_of(t: str) -> tuple[Any, str, str]:
    query = (
        "SELECT snapshot_id, CAST(committed_at AS STRING) AS ts,"
        " CAST(committed_at + INTERVAL 1 DAY AS STRING) AS future FROM "
        + t
        + ".snapshots ORDER BY committed_at DESC LIMIT 1"
    )
    first = s.sql(query).collect()[0]
    return first[0], first[1], first[2]


def pinned_hold(kind: str, t: str) -> Any:
    sid, ts, future = snapshot_of(t)
    if kind == "vao":
        return s.sql("SELECT * FROM " + t + " VERSION AS OF " + str(sid))
    if kind == "tao":
        return s.sql("SELECT * FROM " + t + " TIMESTAMP AS OF '" + ts + "'")
    if kind == "tao_future":
        return s.sql("SELECT * FROM " + t + " TIMESTAMP AS OF '" + future + "'")
    if kind == "rvao":
        return s.read.option("versionAsOf", sid).table(t)
    if kind == "branch_id":
        return s.table(t + ".branch_b")
    if kind == "branch_vao":
        return s.sql("SELECT * FROM " + t + " VERSION AS OF 'b'")
    if kind == "branch_opt":
        return s.read.option("branch", "b").table(t)
    if kind == "tag_id":
        return s.table(t + ".tag_g")
    if kind == "tag_vao":
        return s.sql("SELECT * FROM " + t + " VERSION AS OF 'g'")
    return s.table(t + "." + kind)


PINNED_WRITES: dict[str, Callable[[str], None]] = {
    "insert": WRITES["insert"],
    "delete": WRITES["delete"],
    "b_insert": lambda t: run("INSERT INTO " + t + ".branch_b VALUES (8, 'q')"),
    "tag_move": lambda t: (WRITES["insert"](t), run("ALTER TABLE " + t + " REPLACE TAG g")),
}


def pinned_cell(kind: str, write: str, action: str) -> Any:
    t = make()
    run("ALTER TABLE " + t + " CREATE BRANCH b")
    run("ALTER TABLE " + t + " CREATE TAG g")
    held = pinned_hold(kind, t)
    PINNED_WRITES[write](t)
    if action == "count":
        return held.count()
    if kind in META:
        return len(held.collect())
    return rows(held)


META = ("snapshots", "files", "history")


def seq(steps: list[str], empty: bool = False) -> list[Any]:
    t = make(seed=not empty)
    held = s.table(t)
    got: list[Any] = []
    for step in steps:
        if step in WRITES:
            WRITES[step](t)
        elif step == "count":
            got.append(held.count())
        elif step == "collect":
            got.append(rows(held))
        elif step == "iter":
            got.append(sorted(repr(tuple(row)) for row in held.toLocalIterator()))
        elif step == "tail":
            got.append(sorted(repr(tuple(row)) for row in held.tail(10)))
        elif step == "take":
            got.append(sorted(repr(tuple(row)) for row in held.take(10)))
        elif step == "show":
            got.append(shown(held))
        elif step == "child":
            got.append(rows(held.filter("n > 0")))
        elif step == "explain":
            with contextlib.redirect_stdout(io.StringIO()):
                held.explain()
        elif step == "columns":
            got.append(list(held.columns))
        elif step == "fresh":
            got.append(s.table(t).count())
    return got


SEQS = {
    "s_collect_collect": ["collect", "insert", "collect"],
    "s_collect_count": ["collect", "insert", "count"],
    "s_count_collect": ["count", "insert", "collect"],
    "s_collect_iter": ["collect", "insert", "iter"],
    "s_collect_tail": ["collect", "insert", "tail"],
    "s_collect_take": ["collect", "insert", "take"],
    "s_collect_show": ["collect", "insert", "show"],
    "s_collect_child": ["collect", "insert", "child"],
    "s_explain_collect": ["explain", "insert", "collect"],
    "s_columns_collect": ["columns", "insert", "collect"],
    "s_collect_delete_collect": ["collect", "delete", "collect"],
    "s_ins_collect_ins_collect": ["insert", "collect", "insert", "collect", "count"],
    "s_count_thrice": ["count", "insert", "count", "insert", "count", "fresh"],
    "s_iter_iter": ["iter", "insert", "iter"],
}


def external(writer: str, acted: bool, own_after: bool) -> list[Any]:
    t = make()
    held = s.table(t)
    got: list[Any] = []
    if acted:
        got.append(held.count())
    statement = "INSERT INTO " + t + " VALUES (9, 'z')"
    if writer == "session":
        other = s.newSession()
        other.sql(statement).collect()
    else:
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
        got.append("rc=" + str(done.returncode) + " " + tail[:80])
    got.append(["held", held.count(), rows(held), "fresh", s.table(t).count()])
    if own_after:
        run("INSERT INTO " + t + " VALUES (8, 'q')")
        got.append(["own", held.count(), s.table(t).count()])
    s.catalog.refreshTable(t)
    got.append(["refreshed", held.count(), s.table(t).count()])
    return got


def parquet(acted: bool) -> list[Any]:
    path = wh + "/" + fresh_name("pq")
    s.sql("SELECT 1 AS n").write.parquet(path)
    held = s.read.parquet(path)
    if acted:
        held.count()
    s.sql("SELECT 9 AS n").write.mode("append").parquet(path)
    return [held.count(), s.read.parquet(path).count()]


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
        WRITES["insert"](t)
    got.extend([held.count(), s.table(t).count()])
    s.catalog.clearCache()
    return got


HOLDS = ["table", "sql", "read", "path", "filter", "agg", "dfview", "sqlview", "join_l", "join_r"]
ACTS = ["count", "show", "copy", "insel", "selfjoin"]
PINNED = [
    "vao",
    "tao",
    "tao_future",
    "rvao",
    "branch_id",
    "branch_vao",
    "branch_opt",
    "tag_id",
    "tag_vao",
]


def plan() -> list[tuple[str, Callable[[], Any]]]:
    todo: list[tuple[str, Callable[[], Any]]] = []
    if group == "HW":
        for h in HOLDS:
            for w in WRITES:
                todo.append(
                    ("HW|" + h + "|" + w + "|collect", lambda h=h, w=w: cell(h, w, "collect"))
                )
    if group == "WA":
        for w in WRITES:
            for a in ACTS:
                todo.append(("WA|table|" + w + "|" + a, lambda w=w, a=a: cell("table", w, a)))
    if group == "HA":
        for h in HOLDS[1:]:
            for w in ("insert", "delete"):
                for a in ACTS:
                    todo.append(
                        ("HA|" + h + "|" + w + "|" + a, lambda h=h, w=w, a=a: cell(h, w, a))
                    )
    if group == "P":
        for k in PINNED + list(META):
            writes = ("insert", "delete") if k in META else tuple(PINNED_WRITES)
            for w in writes:
                for a in ("count", "collect"):
                    todo.append(
                        ("P|" + k + "|" + w + "|" + a, lambda k=k, w=w, a=a: pinned_cell(k, w, a))
                    )
    if group == "S":
        for name, steps in SEQS.items():
            todo.append(("S|" + name + "|seq|seq", lambda steps=steps: seq(steps)))
        todo.append(("S|s_empty_count|seq|seq", lambda: seq(["insert", "count", "collect"], True)))
        for k in ("cache_count", "cache_lazy", "cache_table", "cache_append"):
            todo.append(("C|" + k + "|insert|count", lambda k=k: cached(k)))
        todo.append(("Q|parquet_acted|append|count", lambda: parquet(True)))
        todo.append(("Q|parquet_never|append|count", lambda: parquet(False)))
    if group == "X":
        for writer in ("session", "process"):
            for acted in (True, False):
                tag = "X|" + writer + ("_acted" if acted else "_never") + "|external|seq"
                todo.append(
                    (tag, lambda writer=writer, acted=acted: external(writer, acted, False))
                )
            tag = "X|" + writer + "_own|external|seq"
            todo.append((tag, lambda writer=writer: external(writer, True, True)))
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
