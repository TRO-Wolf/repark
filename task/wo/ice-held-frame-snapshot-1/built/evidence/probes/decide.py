import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from common import condition, rows, session

engine, parent, out, ansi = sys.argv[1:5]
s, wh, F = session(engine, parent, ansi)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
counter = [0]


def run(statement: str) -> None:
    s.sql(statement).collect()


def make(columns: str = "n INT, c STRING", seed: str = "(1, 'a'), (2, 'b')") -> str:
    counter[0] += 1
    name = "lc.db.d" + str(counter[0])
    run("CREATE TABLE " + name + " (" + columns + ") USING iceberg")
    if seed:
        run("INSERT INTO " + name + " VALUES " + seed)
    return name


def guarded(body: Callable[[], Any]) -> Any:
    try:
        return body()
    except Exception as error:
        return "ERROR " + condition(error)


def answers(held: Any) -> list[Any]:
    return [guarded(held.count), guarded(lambda: rows(held))]


def held_after(steps: Callable[[str], Any], columns: str = "n INT, c STRING") -> Any:
    t = make(columns)
    held = s.table(t)
    derived = s.table(t).filter("n > 0")
    steps(t)
    copy = t + "_c"
    stored = guarded(lambda: (held.writeTo(copy).create(), rows(s.table(copy)))[1])
    return {"held": answers(held), "derived": answers(derived), "copy": stored}


def derived_write(steps: Callable[[str], Any], verb: str) -> Any:
    t = make()
    derived = s.table(t).filter("n > 0")
    other = t + "_o"
    if verb == "ins_into":
        derived.limit(0).writeTo(other).create()
    steps(t)
    if verb == "copy":
        return guarded(lambda: (derived.writeTo(other).create(), shape(other))[1])
    return guarded(lambda: (derived.write.insertInto(other), shape(other))[1])


def shape(name: str) -> list[Any]:
    frame = s.table(name)
    return [list(frame.columns), rows(frame)]


def readd(t: str) -> None:
    run("ALTER TABLE " + t + " DROP COLUMN c")
    run("ALTER TABLE " + t + " ADD COLUMN c STRING")
    run("INSERT INTO " + t + " VALUES (9, 'z')")


def widen(t: str) -> None:
    run("ALTER TABLE " + t + " ALTER COLUMN n TYPE BIGINT")
    run("INSERT INTO " + t + " VALUES (9, 'z')")


def reorder(t: str) -> None:
    run("ALTER TABLE " + t + " ALTER COLUMN c FIRST")
    run("INSERT INTO " + t + " VALUES ('z', 9)")


def relax(t: str) -> None:
    run("ALTER TABLE " + t + " ALTER COLUMN n DROP NOT NULL")
    run("INSERT INTO " + t + " VALUES (NULL, 'z')")


def rename_table(t: str) -> None:
    run("ALTER TABLE " + t + " RENAME TO " + t + "_moved")


def truncate(t: str) -> None:
    run("TRUNCATE TABLE " + t)


def props(t: str) -> None:
    run("ALTER TABLE " + t + " SET TBLPROPERTIES ('k1' = 'v1')")


def props_insert(t: str) -> None:
    props(t)
    run("INSERT INTO " + t + " VALUES (9, 'z')")


def expire_own_plan() -> Any:
    t = make()
    held = s.table(t)
    first = rows(held)
    run("INSERT OVERWRITE " + t + " VALUES (7, 'o')")
    expire = "older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1"
    run("CALL lc.system.expire_snapshots(table => '" + t.split(".", 1)[1] + "', " + expire + ")")
    return [first, guarded(lambda: rows(held)), guarded(held.count)]


def local_checkpoint(acted: bool) -> Any:
    t = make()
    held = s.table(t)
    first = rows(held) if acted else None
    run("INSERT INTO " + t + " VALUES (9, 'z')")
    point = held.localCheckpoint()
    return [first, guarded(point.count), guarded(lambda: rows(point)), guarded(held.count)]


CELLS: dict[str, Callable[[], Any]] = {
    "drop_readd_same_name": lambda: held_after(readd),
    "widen_int_bigint": lambda: held_after(widen),
    "reorder_c_first": lambda: held_after(reorder),
    "relax_not_null": lambda: held_after(relax, "n INT NOT NULL, c STRING"),
    "rename_table": lambda: held_after(rename_table),
    "truncate": lambda: held_after(truncate),
    "set_tblproperties": lambda: held_after(props),
    "set_tblproperties_insert": lambda: held_after(props_insert),
    "own_collect_ow_expire_collect": expire_own_plan,
    "derived_copy_after_reorder": lambda: derived_write(reorder, "copy"),
    "derived_insert_into_after_reorder": lambda: derived_write(reorder, "ins_into"),
    "derived_copy_after_readd": lambda: derived_write(readd, "copy"),
    "derived_copy_after_truncate": lambda: derived_write(truncate, "copy"),
    "derived_copy_after_props": lambda: derived_write(props_insert, "copy"),
    "local_checkpoint_after_collect": lambda: local_checkpoint(True),
    "local_checkpoint_never_acted": lambda: local_checkpoint(False),
}

with Path(out).open("w") as handle:
    for name, body in CELLS.items():
        got = guarded(body)
        record = {"group": "D", "hold": "table", "write": name, "action": "seq", "ansi": ansi}
        record["got"] = got
        handle.write(json.dumps(record) + "\n")
        handle.flush()
