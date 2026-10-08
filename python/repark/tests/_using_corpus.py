from __future__ import annotations

import re
from typing import Any

TABLES: dict[str, tuple[str, list[tuple[Any, ...]]]] = {
    "tl": ("id INT, s STRING", [(1, "a"), (2, "b"), (3, "c")]),
    "tr": ("id INT, t STRING", [(2, "x"), (3, "y"), (4, "z")]),
    "tq": ("id INT, u STRING", [(2, "p"), (3, "q"), (4, "r")]),
    "tp": ("id INT, w STRING", [(3, "m"), (4, "n"), (5, "o")]),
    "ml": ("id INT, s STRING", [(1, "a"), (2, "b"), (3, "c")]),
    "mr": ("id INT, t STRING", [(1, "x"), (2, "y"), (3, "z")]),
    "mv1": ("id INT, v INT", [(1, 10), (2, 20)]),
    "mv2": ("id INT, v INT", [(1, 100), (3, 300)]),
    "nl": ("id INT, s STRING", [(1, "a"), (None, "n"), (3, "c")]),
    "nr": ("id INT, t STRING", [(None, "m"), (3, "y"), (4, "z")]),
    "tx": ("k1 INT, k2 INT", [(1, 1), (2, 2)]),
    "ty": ("k1 INT, yv STRING", [(2, "y2"), (3, "y3")]),
    "tz": ("k2 INT, zv STRING", [(2, "z2"), (9, "z9")]),
    "ka": ("a INT, b INT, s STRING", [(1, 1, "a"), (2, 2, "b")]),
    "kb": ("a INT, b INT, t STRING", [(2, 2, "x"), (3, 3, "y")]),
}

PLAIN = [
    "SELECT 1",
    "SELECT id, s FROM tl WHERE id > 1 ORDER BY id",
    "WITH c AS (SELECT id FROM tl) SELECT * FROM c",
    "WITH a AS (SELECT * FROM tl), b AS (SELECT * FROM tr) "
    "SELECT a.id, b.t FROM a JOIN b ON a.id = b.id",
    "SELECT * FROM (SELECT id, s FROM tl) x WHERE x.id IN (SELECT id FROM tr)",
    "SELECT id FROM tl UNION ALL SELECT id FROM tr",
    "SELECT id FROM tl INTERSECT SELECT id FROM tr",
    "SELECT id FROM tl EXCEPT SELECT id FROM tr ORDER BY id",
    "SELECT id, row_number() OVER (PARTITION BY s ORDER BY id) AS rn FROM tl",
    "SELECT id, sum(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS w FROM tl",
    "SELECT l.id, (SELECT max(r.id) FROM tr r WHERE r.id = l.id) AS m FROM tl l",
    "SELECT * FROM tl l WHERE EXISTS (SELECT 1 FROM tr r WHERE r.id = l.id)",
    "SELECT * FROM tl l WHERE NOT EXISTS (SELECT 1 FROM tr r WHERE r.id = l.id)",
    "SELECT * FROM tl l JOIN tr r ON l.id = r.id",
    "SELECT * FROM tl l FULL JOIN tr r ON l.id = r.id",
    "SELECT l.id, r.id FROM tl l RIGHT JOIN tr r ON l.id = r.id WHERE r.id > 2",
    "SELECT * FROM tl CROSS JOIN tr",
    "SELECT * FROM tl, tr WHERE tl.id = tr.id",
    "SELECT l.id, x.t FROM tl l, LATERAL (SELECT t FROM tr r WHERE r.id = l.id) x",
    "SELECT id, count(*) AS c FROM tl GROUP BY id HAVING count(*) > 0 ORDER BY id",
    "SELECT DISTINCT s FROM tl ORDER BY s",
    "SELECT id, s FROM tl ORDER BY id DESC LIMIT 2",
    "SELECT CASE WHEN id > 1 THEN 'x' ELSE s END AS c FROM tl",
    "SELECT transform(array(1, 2), id -> id + 1) AS x FROM tl",
    "SELECT * FROM tl l LEFT SEMI JOIN tr r ON l.id = r.id",
    "SELECT * FROM tl l LEFT ANTI JOIN tr r ON l.id = r.id",
    "SELECT a.id FROM tl a JOIN tl b ON a.id = b.id JOIN tr c ON b.id = c.id",
    "SELECT id FROM (SELECT id FROM tl UNION SELECT id FROM tr) u WHERE id > 1",
    "SELECT max(id), min(s) FROM tl",
    "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS v(id, s) WHERE id = 1",
    "SELECT l.*, r.t FROM tl l JOIN tr r ON l.id = r.id ORDER BY l.id, r.t",
    "SELECT id FROM tl WHERE id IN (SELECT id FROM tr WHERE id IN (SELECT id FROM tq))",
]

_PER_TYPE = [
    "SELECT * FROM tl l {how} JOIN tr r USING (id)",
    "SELECT id FROM tl {how} JOIN tr USING (id) WHERE id > 2",
    "SELECT id, transform(array(10, 20), id -> id + 1) AS x FROM tl {how} JOIN tr USING (id)",
    "SELECT id FROM tl {how} JOIN tr USING (id) WHERE exists(array(1, 2), id -> id = 2)",
    "SELECT DISTINCT id FROM tl {how} JOIN tr USING (id) ORDER BY id",
    "SELECT id, count(*) AS c FROM tl l {how} JOIN tr r USING (id) GROUP BY id ORDER BY id",
    "SELECT * FROM tl l {how} JOIN tr r USING (ID) WHERE Id > 2",
    "SELECT id + 1 AS n, upper(s) AS u FROM tl {how} JOIN tr USING (id) ORDER BY 1",
]

_OUTER = [
    "SELECT id FROM tl l {how} JOIN tr r USING (id) ORDER BY l.id, t",
    "SELECT id FROM tl l {how} JOIN tr r USING (id) ORDER BY l.id, upper(s)",
    "SELECT * FROM mv1 l {how} JOIN mv2 r USING (id) ORDER BY r.id",
    "SELECT id, l.v, r.v FROM mv1 l {how} JOIN mv2 r USING (id) ORDER BY l.id",
    "SELECT * FROM tl l {how} JOIN tr r USING (id) ORDER BY l.id",
    "SELECT * FROM tl l {how} JOIN tr r USING (id) ORDER BY r.id DESC LIMIT 2",
    "SELECT l.id, r.id FROM tl l {how} JOIN tr r USING (id)",
    "SELECT *, r.id FROM tl l {how} JOIN tr r USING (id)",
    "SELECT id, l.id, r.id FROM tl l {how} JOIN tr r USING (id)",
    "SELECT * FROM tl l {how} JOIN tr r USING (id) JOIN tq q ON r.id = q.id",
    "SELECT * FROM tl l {how} JOIN tr r USING (id) FULL JOIN tq q USING (id)",
    "SELECT id FROM tl l {how} JOIN tr r USING (id) FULL JOIN tq q USING (id) WHERE id > 3",
    "SELECT * FROM tl {how} JOIN tr USING (id) JOIN tq USING (id) JOIN tp USING (id)",
    "SELECT * FROM tl NATURAL {how} JOIN tr",
    "SELECT id FROM tl NATURAL {how} JOIN tr",
    "SELECT * FROM tl {how} JOIN (SELECT * FROM tr) USING (id)",
    "SELECT x.id FROM (SELECT * FROM tl l {how} JOIN tr r USING (id)) x WHERE x.id > 1",
    "WITH j AS (SELECT * FROM tl {how} JOIN tr USING (id)) SELECT id, t FROM j ORDER BY id",
    "SELECT s AS id FROM tl l {how} JOIN tr r USING (id) ORDER BY id",
    "SELECT * FROM nl l {how} JOIN nr r USING (id)",
    "SELECT id FROM nl {how} JOIN nr USING (id) WHERE id IS NULL",
    "SELECT * FROM ml l {how} JOIN mr r USING (id) ORDER BY l.id, t",
    "SELECT * FROM ka {how} JOIN kb USING (a, b)",
    "SELECT a, b FROM ka {how} JOIN kb USING (a, b) WHERE a > 1 AND b > 1",
    "SELECT * FROM tx {how} JOIN ty USING (k1) LEFT JOIN tz USING (k2)",
    "SELECT s FROM tl WHERE id IN (SELECT id FROM tl {how} JOIN tr USING (id) WHERE id > 3)",
    "SELECT (SELECT max(id) FROM tl {how} JOIN tr USING (id)) AS m",
    "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM tl {how} JOIN tr USING (id)",
    "SELECT id FROM tl {how} JOIN tr USING (id) UNION ALL SELECT id FROM tq",
    "SELECT id, count(*) AS c FROM tl {how} JOIN tr USING (id) GROUP BY id HAVING id > 2",
    "SELECT max(id) AS m, min(l.id) AS a, max(r.id) AS b FROM tl l {how} JOIN tr r USING (id)",
    "SELECT * FROM (SELECT id, s AS __repark_using_k0 FROM tl) l {how} JOIN tr r USING (id) "
    "ORDER BY l.id",
    "EXPLAIN SELECT id FROM tl l {how} JOIN tr r USING (id) WHERE id > 2",
    "SELECT l.* FROM tl l {how} JOIN tr r USING (id)",
    "SELECT r.* FROM tl l {how} JOIN tr r USING (id)",
    "SELECT r.*, l.s FROM tl l {how} JOIN tr r USING (id)",
    "SELECT s, t FROM tl l {how} JOIN tr r USING (id) ORDER BY l.id + 1, r.id",
    "SELECT l.id AS k, count(*) AS c FROM tl l {how} JOIN tr r USING (id) "
    "GROUP BY l.id ORDER BY l.id",
    "SELECT r.id AS k, count(*) AS c FROM tl l {how} JOIN tr r USING (id) "
    "GROUP BY r.id HAVING r.id > 2",
    "SELECT id, sum(r.id) OVER (ORDER BY l.id) AS w FROM tl l {how} JOIN tr r USING (id)",
    "SELECT DISTINCT l.id AS a, r.id AS b FROM tl l {how} JOIN tr r USING (id) ORDER BY a, b",
    "SELECT id, s FROM tl l {how} JOIN tr r USING (id) WHERE l.id IS NULL OR r.id IS NULL",
    "SELECT max(id) AS m FROM tl l {how} JOIN tr r USING (id) WHERE r.id > 1 GROUP BY l.id",
    "SELECT t FROM tl l {how} JOIN tr r USING (id) ORDER BY upper(s), l.id DESC",
    "SELECT q.u, id FROM tl l {how} JOIN tr r USING (id) JOIN tq q ON l.id = q.id",
    "SELECT id, count(*) AS c FROM tl l {how} JOIN tr r USING (id) GROUP BY id HAVING l.id > 1",
    "SELECT l.id AS a, id AS b, count(*) AS c FROM tl l {how} JOIN tr r USING (id) "
    "GROUP BY id, l.id",
    "SELECT id, count(*) AS c FROM tl l {how} JOIN tr r USING (id) GROUP BY id ORDER BY r.id",
    "SELECT DISTINCT id FROM tl l {how} JOIN tr r USING (id) ORDER BY l.id",
    "SELECT id, max(l.id) AS a FROM tl l {how} JOIN tr r USING (id) GROUP BY id",
    "SELECT l.id AS a FROM tl l {how} JOIN tr r USING (id) GROUP BY l.id HAVING max(id) > 2",
]

_HOWS = {
    "inner": "INNER",
    "left": "LEFT",
    "right": "RIGHT",
    "full": "FULL",
    "semi": "LEFT SEMI",
    "anti": "LEFT ANTI",
}


def statements() -> list[tuple[str, str]]:
    found = [(f"plain-{index:02d}", text) for index, text in enumerate(PLAIN)]
    for index, text in enumerate(_PER_TYPE):
        for name, how in _HOWS.items():
            found.append((f"type-{index:02d}-{name}", text.format(how=how)))
    for index, text in enumerate(_OUTER):
        for name in ("right", "full"):
            found.append((f"outer-{index:02d}-{name}", text.format(how=_HOWS[name])))
    return found


def load(session: Any) -> None:
    for name, (schema, rows) in TABLES.items():
        session.createDataFrame(rows, schema).createOrReplaceTempView(name)


def _plain(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if value is None or isinstance(value, (bool, int, str)):
        return value
    return str(value)


def answer(session: Any, text: str) -> dict[str, Any]:
    try:
        frame = session.sql(text)
        rows = [[_plain(value) for value in row] for row in frame.collect()]
    except Exception as error:
        getter = getattr(error, "getCondition", None) or getattr(error, "getErrorClass", None)
        try:
            condition = str(getter() or "") if callable(getter) else ""
        except Exception:
            condition = ""
        return {"refused": condition or type(error).__name__}
    if text.startswith("EXPLAIN"):
        return {"explained": True}
    if not re.search(r"ORDER BY", text.rsplit(")", 1)[-1]):
        rows = sorted(rows, key=lambda row: [str(value) for value in row])
    return {"cols": list(frame.columns), "rows": rows}
