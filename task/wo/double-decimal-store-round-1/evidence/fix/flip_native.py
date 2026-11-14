import sys
import tempfile
from collections.abc import Callable
from typing import Any

import repark
import repark._native as native_module
from repark._native import PyReparkSession

engine, ansi, wh_parent = sys.argv[1:4]
wh = tempfile.mkdtemp(prefix=f"flipn-{engine}-{ansi}-", dir=wh_parent)
print(native_module.__file__)
native = PyReparkSession.native(config={"spark.sql.ansi.enabled": ansi})
native.register_memory_catalog("ice", wh + "/ice")
native.create_namespace("ice", "ns", wh + "/ice/ns")
repark._ANSI_NATIVE = native


def q(sql: str) -> list[dict[str, Any]]:
    return repark.sql(sql).to_arrow().to_pylist()


def ids(sql: str) -> list[int]:
    return sorted(r["id"] for r in q(sql))


def attempt(label: str, fn: Callable[[], object]) -> None:
    try:
        print(label, fn())
    except Exception as e:
        print(label, "ERR", str(e).splitlines()[0][:200])


def scalar_cells(v: str) -> list[str]:
    d = f"CAST('{v}' AS DOUBLE)"
    sql = (
        f"SELECT ROUND({d}, 18) = {d} AS c1, CAST(ROUND({d}, 18) AS STRING) AS c2, "
        f"CAST({d} AS DECIMAL(38,18)) = {d} AS c3, "
        f"CAST(CAST({d} AS DECIMAL(38,18)) AS STRING) AS c4, "
        f"CAST(CAST('{v}' AS DECIMAL(38,18)) = {d} AS STRING) AS c5, "
        f"CAST(CAST(ROUND({d}, 2) AS DECIMAL(10,2)) = {d} AS STRING) AS c6, "
        f"CAST(CAST(ROUND({d}, 18) AS DECIMAL(38,18)) = {d} AS STRING) AS c7"
    )
    return [str(x) for x in q(sql)[0].values()]


def stored(table: str) -> list[tuple[int, str]]:
    return sorted((r["id"], str(r["d"])) for r in q(f"SELECT id, d FROM {table}"))


print("ansi", ansi)
for value in ("-22.129547339983247", "1.0000000000000004", "2.2300000000000004"):
    attempt(value, lambda value=value: scalar_cells(value))
q("CREATE TABLE ice.ns.t (id INT, d DECIMAL(10,2), v DOUBLE)")
q(
    "INSERT INTO ice.ns.t VALUES (1, 1.00, CAST('1.0000000000000004' AS DOUBLE)), "
    "(2, 1.47, CAST('1.4700000000000004' AS DOUBLE)), "
    "(3, 2.23, CAST('2.2300000000000004' AS DOUBLE)), (4, 1.50, CAST(1.5 AS DOUBLE))"
)
attempt("select d = v:", lambda: ids("SELECT id FROM ice.ns.t WHERE d = v"))
attempt("delete", lambda: q("DELETE FROM ice.ns.t WHERE d = v") and "ok")
attempt("after DELETE WHERE d = v:", lambda: ids("SELECT id FROM ice.ns.t"))
q("CREATE TABLE ice.ns.u (id INT, d DECIMAL(38,18), v DOUBLE)")
q(
    "INSERT INTO ice.ns.u VALUES (1, CAST('-22.129547339983247' AS DECIMAL(38,18)), "
    "CAST('-22.129547339983247' AS DOUBLE)), (2, 1.5, CAST(1.5 AS DOUBLE))"
)
attempt("u select d = v:", lambda: ids("SELECT id FROM ice.ns.u WHERE d = v"))
attempt("update", lambda: q("UPDATE ice.ns.u SET id = id + 100 WHERE d = v") and "ok")
attempt("u after UPDATE WHERE d = v:", lambda: ids("SELECT id FROM ice.ns.u"))
q("CREATE TABLE ice.ns.s (id INT, v DOUBLE)")
q(
    "INSERT INTO ice.ns.s VALUES (1, CAST('1.255' AS DOUBLE)), (2, CAST('1.005' AS DOUBLE)), "
    "(3, CAST('2.675' AS DOUBLE))"
)
q("CREATE TABLE ice.ns.w (id INT, d DECIMAL(10,2))")
attempt(
    "native ins_select",
    lambda: q("INSERT INTO ice.ns.w SELECT id, v FROM ice.ns.s") and stored("ice.ns.w"),
)
attempt(
    "native cast",
    lambda: [str(x) for x in q("SELECT CAST(v AS DECIMAL(10,2)) AS c FROM ice.ns.s ORDER BY id")],
)
