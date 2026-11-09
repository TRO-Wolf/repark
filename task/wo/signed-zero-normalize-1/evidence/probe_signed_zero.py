from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import re
import shutil
import statistics
import sys
import tempfile
import time
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

ANSWERED = "ANSWERED"
REFUSED = "REFUSED"
PANIC = "PANIC"
SETUP_REFUSED = "SETUP-REFUSED"
NOT_EXPRESSIBLE = "NOT-EXPRESSIBLE"
SPARK_CATALOG = "probe_ice"
REPARK_CATALOG = "ice"
NS = "ns"
SESSION_ZONE = "America/New_York"
NATIVE_CONFIG = {"repark.sql.allowCreateFormatVersion3": "true"}
KINDS = ("v2-cow", "v2-mor", "v3")
KIND_SLUG = {"v2-cow": "cow", "v2-mor": "mor", "v3": "v3"}
KIND_PROPS = {
    "v2-cow": ("2", "copy-on-write"),
    "v2-mor": ("2", "merge-on-read"),
    "v3": ("3", "copy-on-write"),
}
COLUMNS = "id BIGINT, i INT, d DOUBLE, tf FLOAT, e DOUBLE, s STRING, flag STRING"
STATE_SQL = "SELECT id, d, tf, flag FROM {dm} ORDER BY id"
DATASETS = ("t", "u", "u1", "u2", "p", "nn")
T_ROWS = (
    "(1, 1, CAST('1.5' AS DOUBLE), CAST('1.5' AS FLOAT), NULL, 'a', 'f1')",
    "(2, NULL, NULL, NULL, NULL, NULL, 'f2')",
    "(3, NULL, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT), NULL, '', 'f3')",
    "(4, NULL, CAST('-0.0' AS DOUBLE), CAST('-0.0' AS FLOAT), NULL, 'a ', 'f4')",
    "(5, 2147483647, CAST('Infinity' AS DOUBLE), CAST('Infinity' AS FLOAT), NULL, NULL, 'f5')",
    "(6, -1, CAST('0.0' AS DOUBLE), CAST('0.0' AS FLOAT), NULL, 'A', 'f6')",
    "(7, NULL, NULL, NULL, NULL, 'bb', 'f7')",
    "(8, 1, CAST('1.5' AS DOUBLE), CAST('1.5' AS FLOAT), NULL, 'a', 'f8')",
)
U_ROWS = (
    "(10, NULL, CAST('0.0' AS DOUBLE), CAST('0.0' AS FLOAT), NULL, NULL, 'u0')",
    "(11, NULL, CAST('-0.0' AS DOUBLE), CAST('-0.0' AS FLOAT), NULL, NULL, 'u1')",
)
P_ROWS = (
    "(1, NULL, CAST('-0.0' AS DOUBLE), NULL, CAST('0.0' AS DOUBLE), NULL, 'p1')",
    "(2, NULL, CAST('0.0' AS DOUBLE), NULL, CAST('-0.0' AS DOUBLE), NULL, 'p2')",
    "(3, NULL, CAST('NaN' AS DOUBLE), NULL, CAST('NaN' AS DOUBLE), NULL, 'p3')",
    "(4, NULL, NULL, NULL, CAST('0.0' AS DOUBLE), NULL, 'p4')",
)
NN_ROWS = (
    "(1, NULL, CAST('NaN' AS DOUBLE), NULL, NULL, NULL, 'n1')",
    "(2, NULL, sqrt(-1.0), NULL, NULL, NULL, 'n2')",
    "(3, NULL, ln(-1.0), NULL, NULL, NULL, 'n3')",
    "(4, NULL, CAST('1.0' AS DOUBLE), NULL, NULL, NULL, 'n4')",
    "(5, NULL, CAST('0.0' AS DOUBLE), NULL, NULL, NULL, 'n5')",
)
DATA = {"t": T_ROWS, "u": U_ROWS, "u1": U_ROWS[:1], "u2": U_ROWS[1:], "p": P_ROWS, "nn": NN_ROWS}
READ_CHUNKS = {
    ("t", "one"): (T_ROWS,),
    ("t", "split"): tuple((row,) for row in T_ROWS),
    ("u", "one"): (U_ROWS,),
    ("u1", "one"): (U_ROWS[:1],),
    ("u2", "one"): (U_ROWS[1:],),
    ("p", "one"): (P_ROWS,),
    ("nn", "one"): (NN_ROWS,),
}
SEED_PLANS = {
    "zone": (False, ((3, 5),)),
    "zsplit": (False, ((3,), (5,))),
    "plain": (False, ((0, 3, 5),)),
    "part": (True, ((0,), (3,), (5,))),
    "m14": (False, ((0, 3),)),
    "m1s4": (False, ((0,), (3,))),
}
D1_PREDS = ("d = 0.0", "d = -0.0", "d IN (0.0, 1.0)", "0.0 = d")
D7_PREDS = (
    "d = 0.0",
    "d = -0.0",
    "d IN (0.0, 1.0)",
    "0.0 = d",
    "d <> 0.0",
    "d < 0.0",
    "NOT (d < 0.0)",
    "d <=> 0.0",
    "d BETWEEN -1.0 AND 0.0",
)
D2_CELLS = (
    ("1", "d = 1 - 1.0", "frame.filter(F.col('d') == (F.lit(1) - F.lit(1.0))).select('id')"),
    (
        "2",
        "d >= CAST(0 AS DOUBLE) * 1",
        "frame.filter(F.col('d') >= (F.lit(0).cast('double') * 1)).select('id')",
    ),
    ("3", "d < abs(-0.0)", "frame.filter(F.col('d') < F.abs(F.lit(-0.0))).select('id')"),
    (
        "4",
        "CAST(tf AS DOUBLE) = 0.0",
        "frame.filter(F.col('tf').cast('double') == 0.0).select('id')",
    ),
    ("5", "d * 0 <> 0", "frame.filter((F.col('d') * 0) != 0).select('id')"),
)
D4_SQL = (
    ("1", "d NOT IN (0.0, 1.0)", None),
    ("2", "d NOT IN (0.0, 10.0, 20.0, 30.0)", None),
    ("3", "d IN (-0.0, 10.0, 20.0, 30.0)", None),
    ("4", "d BETWEEN -1.0 AND 0.0", None),
    ("5", "d NOT BETWEEN 0.0 AND 1.0", None),
    ("6", "d BETWEEN CAST('-0.0' AS DOUBLE) AND 0.0", None),
    ("7", "NOT (d < 0.0)", None),
    ("8", "NOT (d >= CAST('-0.0' AS DOUBLE))", None),
    ("9", "d = 0.0 AND d = -0.0", None),
    ("10", "NOT (d <=> 0.0)", "SELECT id FROM {t} WHERE d IS DISTINCT FROM 0.0"),
)
D4_TYPED = (
    ("t1", "d = 0.0", "frame.filter(F.col('d') == 0.0).select('id')"),
    ("t2", "d <> -0.0", "frame.filter(F.col('d') != -0.0).select('id')"),
    ("t3", "d < -0.0", "frame.filter(F.col('d') < -0.0).select('id')"),
    ("t4", "d <= -0.0", "frame.filter(F.col('d') <= -0.0).select('id')"),
    ("t5", "d > -0.0", "frame.filter(F.col('d') > -0.0).select('id')"),
    ("t6", "d >= 0.0", "frame.filter(F.col('d') >= 0.0).select('id')"),
    ("t7", "d <=> 0.0", "frame.filter(F.col('d').eqNullSafe(0.0)).select('id')"),
    ("t8", "NOT (d <=> 0.0)", "frame.filter(~F.col('d').eqNullSafe(0.0)).select('id')"),
    (
        "t9",
        "d IN (0.0, 10.0, 20.0, 30.0)",
        "frame.filter(F.col('d').isin(0.0, 10.0, 20.0, 30.0)).select('id')",
    ),
    ("t10", "NOT (d IN (0.0, 1.0))", "frame.filter(~F.col('d').isin(0.0, 1.0)).select('id')"),
    ("t11", "d BETWEEN -1.0 AND 0.0", "frame.filter(F.col('d').between(-1.0, 0.0)).select('id')"),
    ("t12", "0.0 <= d", "frame.filter(F.lit(0.0) <= F.col('d')).select('id')"),
)
D9_STATEMENTS = (
    "SELECT d FROM {t} UNION SELECT d FROM {u}",
    "SELECT d FROM {t} INTERSECT ALL SELECT d FROM {u}",
    "SELECT d FROM {t} EXCEPT ALL SELECT d FROM {u}",
    "SELECT t.id, u.id FROM {t} AS t LEFT OUTER JOIN {u} AS u ON t.d = u.d",
    "SELECT t.id, u.id FROM {t} AS t RIGHT OUTER JOIN {u} AS u ON t.d = u.d",
    "SELECT t.id, u.id FROM {t} AS t FULL OUTER JOIN {u} AS u ON t.d = u.d",
    "SELECT t.id FROM {t} AS t LEFT SEMI JOIN {u} AS u ON t.d = u.d",
    "SELECT t.id FROM {t} AS t LEFT ANTI JOIN {u} AS u ON t.d = u.d",
    "SELECT t.id, u.id FROM {t} AS t JOIN {u} AS u USING (d)",
    "SELECT id FROM {t} WHERE d IN (SELECT d FROM {u1})",
    "SELECT id FROM {t} AS t WHERE EXISTS (SELECT 1 FROM {u1} AS u1 WHERE u1.d = t.d)",
    "SELECT id FROM {t} WHERE d = (SELECT max(d) FROM {u2})",
    "SELECT count(DISTINCT d) AS a, count(DISTINCT e) AS b FROM {p}",
    "SELECT sum(DISTINCT d) AS s FROM {t}",
    "SELECT d, count(*) AS c FROM {t} GROUP BY ROLLUP(d)",
    "SELECT d, id % 2 AS m, count(*) AS c FROM {t} GROUP BY CUBE(d, id % 2)",
    "SELECT id, DENSE_RANK() OVER (ORDER BY d) AS r FROM {t}",
    "SELECT id, ROW_NUMBER() OVER (ORDER BY d, id) AS r FROM {t}",
    "SELECT id, count(*) OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS c FROM {t}",
    "SELECT id, d FROM {u} ORDER BY d, id",
    "SELECT id FROM {t} ORDER BY d, id LIMIT 1",
    "SELECT d, count(*) AS c FROM (SELECT d FROM {t} DISTRIBUTE BY d) AS x GROUP BY d",
    "WITH RECURSIVE r(n) AS (SELECT CAST(0.0 AS DOUBLE) UNION SELECT n + 1 FROM r WHERE n < 2) "
    "SELECT n FROM r",
    "SELECT t.id, u.id FROM {t} AS t JOIN {u} AS u ON t.d = u.d",
    "SELECT id FROM vt9 WHERE d = 0.0",
)
D9_ORDERED = {20, 21}
D9_PRE = {
    24: {
        "spark": (
            "SET spark.sql.join.preferSortMergeJoin = true",
            "SET spark.sql.autoBroadcastJoinThreshold = -1",
        ),
        "repark": ("SET datafusion.optimizer.prefer_hash_join = false",),
    },
    25: {"both": ("CREATE OR REPLACE TEMPORARY VIEW vt9 AS SELECT * FROM {t}",)},
}
D9_POST = {
    24: {
        "spark": (
            "SET spark.sql.join.preferSortMergeJoin = false",
            "SET spark.sql.autoBroadcastJoinThreshold = 10485760",
        ),
        "repark": ("SET datafusion.optimizer.prefer_hash_join = true",),
    },
    25: {"both": ("DROP VIEW IF EXISTS vt9",)},
}
D10_STATEMENTS = (
    "SELECT d, count(*) AS c FROM {nn} GROUP BY d",
    "SELECT DISTINCT d FROM {nn}",
    "SELECT a.id, b.id FROM {nn} AS a JOIN {nn} AS b ON a.d = b.d",
    "SELECT id, d FROM {nn} ORDER BY d, id",
    "SELECT id FROM {nn} WHERE d = CAST('NaN' AS DOUBLE)",
    "SELECT id FROM {nn} WHERE d < 1.0",
)
D17_STATEMENTS = (
    ("1", "SELECT collect_set(d) AS x FROM {t}", False),
    ("2", "SELECT approx_count_distinct(d) AS x FROM {t}", False),
    ("3", "SELECT mode(d) AS x FROM {t}", False),
    ("4", "SELECT array_distinct(array(d, -d)) AS x FROM {t} WHERE id = 4", False),
    ("5", "SELECT array_contains(array(CAST('-0.0' AS DOUBLE)), 0.0) AS x", True),
    (
        "6",
        "SELECT array_union(array(d), array(CAST(0.0 AS DOUBLE))) AS x FROM {t} WHERE id = 4",
        False,
    ),
    ("7", "SELECT map(CAST('-0.0' AS DOUBLE), 'a')[0.0] AS x", True),
    (
        "8",
        "SELECT named_struct('x', d) AS k, count(*) AS c FROM {t} GROUP BY named_struct('x', d)",
        False,
    ),
    ("9", "SELECT array(d) AS k, count(*) AS c FROM {t} GROUP BY array(d)", False),
    ("10", "SELECT nullif(d, 0.0) AS x FROM {t} WHERE id IN (4, 6)", False),
    ("11", "SELECT greatest(CAST('-0.0' AS DOUBLE), 0.0) AS x", True),
)
D3_STATEMENTS = (
    ("1", "SELECT CAST('-0.0' AS DOUBLE) = 0.0 AS x"),
    ("2", "SELECT CAST('-0.0' AS DOUBLE) < 0.0 AS x"),
    ("3", "SELECT CAST('-0.0' AS DOUBLE) <=> 0.0 AS x"),
)
D6_CELLS = (
    ("1", "SELECT d = 0.0 FROM {t}", "frame.select(F.col('d') == 0.0)"),
    ("2", "SELECT d <=> 0.0 FROM {t}", "frame.select(F.col('d').eqNullSafe(0.0))"),
    ("3", "SELECT d, count(*) AS c FROM {t} GROUP BY d", None),
    ("4", "SELECT count(DISTINCT d) AS c FROM {t}", None),
    ("5", "SELECT x.d FROM (SELECT d FROM {t} GROUP BY d) AS x", None),
    ("6", "SELECT t.d FROM {t} AS t GROUP BY t.d", None),
)
D8_STATEMENTS = (
    ("1", "SELECT id FROM {p} WHERE d = e"),
    ("2", "SELECT id FROM {p} WHERE d <> e"),
    ("3", "SELECT id FROM {p} WHERE d < e"),
    ("4", "SELECT id FROM {p} WHERE d <= e"),
    ("5", "SELECT id FROM {p} WHERE d <=> e"),
    ("6", "SELECT id FROM {p} WHERE d BETWEEN e AND 1.0"),
    ("7", "SELECT id FROM {p} WHERE d IN (e, 1.0)"),
    ("8", "SELECT id, CASE d WHEN 0.0 THEN 'z' ELSE 'n' END AS c FROM {p}"),
    ("9", "SELECT id, CASE d WHEN e THEN 'z' END AS c FROM {p}"),
    ("10", "SELECT t.id, u.id FROM {p} AS t JOIN {u} AS u ON t.d < u.d"),
)
D13_CELLS = (
    (
        "1",
        "SELECT csc(CAST('-0.0' AS DOUBLE)) AS x FROM {t} WHERE id = 4",
        "frame.filter(F.col('id') == 4).select(F.csc(F.lit(-0.0)).alias('x'))",
    ),
    (
        "2",
        "SELECT csc(d) AS x FROM {t} WHERE id = 4",
        "frame.filter(F.col('id') == 4).select(F.csc(F.col('d')).alias('x'))",
    ),
)
SPILL_QUERIES = (
    ("E8-group-by-d", "SELECT d, count(*) AS c FROM {s} GROUP BY d"),
    ("E8-join-zeros", "SELECT count(*) AS c FROM {s} AS t JOIN {u} AS u ON t.d = u.d"),
    (
        "E8-self-join-d",
        "SELECT count(*) AS c FROM {s} AS a JOIN {s} AS b ON a.d = b.d WHERE a.id % 1000 = 0",
    ),
)
MERGE_FORMS = (
    (
        "Ma",
        "zone",
        "zsplit",
        "MERGE INTO {dm} AS t USING (SELECT CAST(4 AS BIGINT) AS id, CAST('0.0' AS DOUBLE) AS d) "
        "AS u "
        "ON t.id = u.id WHEN MATCHED AND t.d = 0.0 THEN UPDATE SET flag = 'ma'",
    ),
    (
        "Mb",
        "zone",
        "zsplit",
        "MERGE INTO {dm} AS t USING (SELECT CAST(10 AS BIGINT) AS id, CAST('0.0' AS DOUBLE) AS "
        "d) AS u "
        "ON t.id = u.id WHEN NOT MATCHED AND u.d = 0.0 THEN INSERT (id, d, flag) VALUES (u.id, "
        "u.d, 'mb')",
    ),
    (
        "Mc",
        "m14",
        "m1s4",
        "MERGE INTO {dm} AS t USING (SELECT CAST(6 AS BIGINT) AS id, CAST('0.0' AS DOUBLE) AS d) "
        "AS u "
        "ON t.d = u.d WHEN MATCHED THEN UPDATE SET flag = 'mc' "
        "WHEN NOT MATCHED THEN INSERT (id, d, flag) VALUES (u.id, u.d, 'mc')",
    ),
)
STORED_READBACK = "SELECT id, CAST(d AS STRING) AS d FROM {dst} ORDER BY id"


def sanitize(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def scrub(text: str, roots: list[tuple[str, str]]) -> str:
    for root, token in roots:
        if root:
            text = text.replace(root, token)
    return text


def excerpt(text: str, limit: int = 300) -> str:
    flat = " ".join(text.split())
    return flat if len(flat) <= limit else flat[:limit]


def norm(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value == float("inf"):
            return "Infinity"
        if value == float("-inf"):
            return "-Infinity"
        if value == 0.0 and str(value) == "-0.0":
            return "-0.0"
        return repr(value)
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC).replace(tzinfo=None)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): norm(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [norm(item) for item in value]
    if isinstance(value, (int, str)):
        return value
    return str(value)


def canon(rows: list[list[Any]]) -> list[list[Any]]:
    return [[norm(value) for value in row] for row in rows]


def row_keys(rows: list[list[Any]]) -> list[str]:
    return sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)


def to_arrow(frame: Any) -> Any:
    return frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()


def collect(table: Any) -> tuple[list[str], list[list[Any]]]:
    names = list(table.column_names)
    columns = [table.column(index).to_pylist() for index in range(table.num_columns)]
    rows = [list(values) for values in zip(*columns, strict=True)] if columns else []
    return names, canon(rows)


def run_sql(door: dict[str, Any], text: str) -> tuple[list[str], list[list[Any]]]:
    return collect(to_arrow(door["sql"](text)))


def run_frame(
    door: dict[str, Any], qualified: str, text: str, route: str
) -> tuple[list[str], list[list[Any]]]:
    frame = door["session"].table(qualified)
    functions = door["functions"]
    if route == "filter-str":
        result = frame.filter(text).select("id")
    elif route == "f-expr":
        result = frame.filter(functions.expr(text)).select("id")
    else:
        result = eval(text, {"F": functions, "frame": frame})
    return collect(to_arrow(result))


def attempt(
    ctx: dict[str, Any], call: Any, *args: Any
) -> tuple[str, dict[str, Any] | None, list[str], list[list[Any]]]:
    try:
        names, rows = call(*args)
        return ANSWERED, None, names, rows
    except KeyboardInterrupt:
        raise
    except BaseException as error:
        text = scrub(str(error), ctx["roots"])
        panic = type(error).__name__ == "PanicException" or "panicked at" in text
        error_record = {"class": type(error).__name__, "message": excerpt(text)}
        return (PANIC if panic else REFUSED), error_record, [], []


def create_sql(door_name: str, qualified: str, kind: str, partition: str | None = None) -> str:
    if door_name == "native":
        return f"CREATE TABLE {qualified} ({COLUMNS})"
    version, mode = KIND_PROPS[kind]
    props = (
        f"'format-version' = '{version}', 'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}', 'write.merge.mode' = '{mode}'"
    )
    part = f" PARTITIONED BY ({partition})" if partition else ""
    return f"CREATE TABLE {qualified} ({COLUMNS}) USING iceberg{part} TBLPROPERTIES ({props})"


def values_of(rows: tuple[str, ...]) -> str:
    return ", ".join(rows)


def setup(ctx: dict[str, Any], door: dict[str, Any], text: str, stage: str, name: str) -> bool:
    outcome, error, _, _ = attempt(ctx, run_sql, door, text)
    ctx["records"].append(
        {
            "record": "setup",
            "engine": ctx["engine"],
            "door": door["name"],
            "name": name,
            "stage": stage,
            "outcome": outcome,
            "error": error,
            "text": scrub(text, ctx["roots"]),
        }
    )
    if outcome != ANSWERED:
        ctx["failed"].add(name)
    return outcome == ANSWERED


def create_and_seed(
    ctx: dict[str, Any],
    door: dict[str, Any],
    qualified: str,
    kind: str,
    chunks: tuple[tuple[str, ...], ...],
    partition: bool,
) -> bool:
    ddl = create_sql(door["name"], qualified, kind, "d" if partition else None)
    if not setup(ctx, door, ddl, "create", qualified):
        return False
    for chunk in chunks:
        if not setup(
            ctx, door, f"INSERT INTO {qualified} VALUES {values_of(chunk)}", "seed", qualified
        ):
            return False
    return True


def read_names(engine: str, layout: str) -> dict[str, str]:
    if layout == "none":
        return {}
    if layout == "view":
        return {dataset: f"tv_{dataset}" for dataset in DATASETS}
    catalog = SPARK_CATALOG if engine == "spark" else REPARK_CATALOG
    names = {}
    for dataset in DATASETS:
        tag = "split" if layout == "split" and dataset == "t" else "one"
        names[dataset] = f"{catalog}.{NS}.{dataset}_{tag}_cow"
    return names


def placeholders_in(text: str) -> set[str]:
    return set(re.findall(r"\{(\w+)\}", text))


def fill(text: str, names: dict[str, str]) -> str:
    for key, value in names.items():
        text = text.replace("{" + key + "}", value)
    return text


def ensure_read_tables(ctx: dict[str, Any], door: dict[str, Any]) -> None:
    catalog = SPARK_CATALOG if ctx["engine"] == "spark" else REPARK_CATALOG
    for (dataset, tag), chunks in READ_CHUNKS.items():
        qualified = f"{catalog}.{NS}.{dataset}_{tag}_cow"
        create_and_seed(ctx, door, qualified, "v2-cow", chunks, False)


def ensure_views(ctx: dict[str, Any], door: dict[str, Any]) -> None:
    for dataset in DATASETS:
        values = values_of(DATA[dataset])
        text = (
            f"CREATE OR REPLACE TEMPORARY VIEW tv_{dataset} AS SELECT CAST(id AS BIGINT) AS id, "
            "CAST(i AS INT) AS i, CAST(d AS DOUBLE) AS d, CAST(tf AS FLOAT) AS tf, CAST(e AS "
            "DOUBLE) AS e, "
            "CAST(s AS STRING) AS s, CAST(flag AS STRING) AS flag "
            f"FROM VALUES {values} AS tv(id, i, d, tf, e, s, flag)"
        )
        setup(ctx, door, text, "view", f"tv_{dataset}")


def door_for(ctx: dict[str, Any], route: str) -> dict[str, Any]:
    if route == "native":
        return ctx["doors"]["native"]
    return ctx["doors"]["spark-sql"]


def uses_t(cell: dict[str, Any]) -> bool:
    texts = [cell["sql"], cell["native"]] + [
        item for key in ("pre", "post") for group in cell[key].values() for item in group
    ]
    return any("{t}" in item for item in texts)


def read_layouts(cell: dict[str, Any], engine: str) -> tuple[str, ...]:
    if cell["literal"]:
        return ("none",)
    layouts = ("view", "one", "split") if uses_t(cell) else ("view", "one")
    if engine == "repark":
        layouts = tuple(layout for layout in layouts if layout != "view")
    return layouts


def read_routes(cell: dict[str, Any], engine: str) -> list[str]:
    if cell["routes"] is not None:
        routes = list(cell["routes"])
    else:
        routes = ["spark-sql"]
        if cell["where"]:
            routes += ["filter-str", "f-expr"]
        if cell["typed"]:
            routes.append("typed")
        routes.append("native")
    if engine == "spark":
        routes = [route for route in routes if route != "native"]
    return routes


def read_cell(
    cid: str,
    group: str,
    sql: str | None = None,
    where: str | None = None,
    typed: str | None = None,
    native: str | None = None,
    routes: list[str] | None = None,
    ordered: bool = False,
    pre: dict[str, Any] | None = None,
    post: dict[str, Any] | None = None,
    literal: bool = False,
) -> dict[str, Any]:
    body = sql if sql is not None else f"SELECT id FROM {{t}} WHERE {where}"
    return {
        "cid": cid,
        "group": group,
        "sql": body,
        "where": where,
        "typed": typed,
        "native": native if native is not None else body,
        "routes": routes,
        "ordered": ordered,
        "pre": pre or {},
        "post": post or {},
        "literal": literal,
    }


def retarget(text: str) -> str:
    return re.sub(r"\bd\b", "tf", text)


def read_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for number, text in D3_STATEMENTS:
        cells.append(
            read_cell(f"D3-{number}", "D3", sql=text, routes=["spark-sql", "native"], literal=True)
        )
    for number, where, typed in D2_CELLS:
        cells.append(read_cell(f"D2-{number}", "D2", where=where, typed=typed))
    for number, where, native_text in D4_SQL:
        cells.append(read_cell(f"D4-{number}", "D4", where=where, native=native_text))
    for number, where, typed in D4_TYPED:
        cells.append(
            read_cell(
                f"D4-{number}",
                "D4",
                sql=f"SELECT id FROM {{t}} WHERE {where}",
                typed=typed,
                routes=["spark-sql", "typed"],
            )
        )
    for number, where, typed in D2_CELLS:
        if number == "4":
            continue
        cells.append(
            read_cell(
                f"D5-{number}",
                "D5",
                where=retarget(where),
                typed=typed.replace("F.col('d')", "F.col('tf')"),
            )
        )
    for number, where, native_text in D4_SQL:
        native = retarget(native_text) if native_text else None
        cells.append(read_cell(f"D5-d4-{number}", "D5", where=retarget(where), native=native))
    for number, where, typed in D4_TYPED:
        cells.append(
            read_cell(
                f"D5-{number}",
                "D5",
                sql=f"SELECT id FROM {{t}} WHERE {retarget(where)}",
                typed=typed.replace("F.col('d')", "F.col('tf')"),
                routes=["spark-sql", "typed"],
            )
        )
    for number, sql, typed in D6_CELLS:
        cells.append(read_cell(f"D6-{number}", "D6", sql=sql, typed=typed))
    for number, sql in D8_STATEMENTS:
        cells.append(read_cell(f"D8-{number}", "D8", sql=sql))
    for index, sql in enumerate(D9_STATEMENTS, start=1):
        pre = {}
        post = {}
        if index in D9_PRE:
            pre = D9_PRE[index]
            post = D9_POST[index]
        cells.append(
            read_cell(
                f"D9-{index:02d}", "D9", sql=sql, ordered=index in D9_ORDERED, pre=pre, post=post
            )
        )
    for index, sql in enumerate(D10_STATEMENTS, start=1):
        cells.append(read_cell(f"D10-{index}", "D10", sql=sql, ordered=index == 4))
    for number, sql, typed in D13_CELLS:
        cells.append(
            read_cell(f"D13-{number}", "D13", sql=sql, typed=typed, routes=["spark-sql", "typed"])
        )
    for number, sql, literal in D17_STATEMENTS:
        cells.append(
            read_cell(f"D17-{number}", "D17", sql=sql, literal=literal, routes=["spark-sql"])
        )
    return cells


def dml_item(
    cid: str,
    group: str,
    kind: str,
    stmt: str,
    doors: tuple[str, ...],
    truth: str,
    test: str,
    readback: str | None = STATE_SQL,
    extra: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "cid": cid,
        "group": group,
        "kind": kind,
        "stmt": stmt,
        "doors": doors,
        "truth": truth,
        "test": test,
        "readback": readback,
        "extra": extra,
    }


def dml_items() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for kind in KINDS:
        for number, pred in enumerate(D1_PREDS, start=1):
            items.append(
                dml_item(
                    f"D1-{number}-{KIND_SLUG[kind]}",
                    "D1",
                    kind,
                    f"DELETE FROM {{dm}} WHERE {pred}",
                    ("spark-sql", "native"),
                    "zone",
                    "zsplit",
                )
            )
    for number, pred in enumerate(D7_PREDS, start=1):
        for kind in KINDS:
            for verb, stmt in (
                ("DELETE", f"DELETE FROM {{dm}} WHERE {pred}"),
                ("UPDATE", f"UPDATE {{dm}} SET flag = 'upd' WHERE {pred}"),
            ):
                items.append(
                    dml_item(
                        f"D7-{verb.lower()}-{number}-{KIND_SLUG[kind]}",
                        "D7",
                        kind,
                        stmt,
                        ("spark-sql", "native"),
                        "zone",
                        "zsplit",
                    )
                )
        items.append(
            dml_item(
                f"D7-insert-{number}",
                "D7",
                "v2-cow",
                f"INSERT INTO {{dst}} SELECT * FROM {{dm}} WHERE {pred}",
                ("spark-sql",),
                "zone",
                "zsplit",
                readback="SELECT id, d, tf, flag FROM {dst} ORDER BY id",
                extra=(create_sql("spark-sql", "{dst}", "v2-cow"),),
            )
        )
        items.append(
            dml_item(
                f"D7-ctas-{number}",
                "D7",
                "v2-cow",
                f"CREATE TABLE {{dst}} USING iceberg AS SELECT * FROM {{dm}} WHERE {pred}",
                ("spark-sql",),
                "zone",
                "zsplit",
                readback="SELECT id, d, tf, flag FROM {dst} ORDER BY id",
            )
        )
    for kind in KINDS:
        for form, truth, test, stmt in MERGE_FORMS:
            if kind == "v3":
                continue
            items.append(
                dml_item(
                    f"D7-merge-{form}-{KIND_SLUG[kind]}",
                    "D7",
                    kind,
                    stmt,
                    ("spark-sql",),
                    truth,
                    test,
                )
            )
    for kind in KINDS:
        slug = KIND_SLUG[kind]
        dst_create = create_sql("spark-sql", "{dst}", kind)
        items.append(
            dml_item(
                f"D11-insert-{slug}",
                "D11",
                kind,
                "INSERT INTO {dst} SELECT id, d FROM {dm}",
                ("spark-sql",),
                "zone",
                "zsplit",
                readback=STORED_READBACK,
                extra=(dst_create.replace(COLUMNS, "id BIGINT, d DOUBLE"),),
            )
        )
        items.append(
            dml_item(
                f"D11-ctas-{slug}",
                "D11",
                kind,
                f"CREATE TABLE {{dst}} USING iceberg TBLPROPERTIES ({create_props(kind)}) AS "
                f"SELECT id, d FROM {{dm}}",
                ("spark-sql",),
                "zone",
                "zsplit",
                readback=STORED_READBACK,
            )
        )
        items.append(
            dml_item(
                f"D11-update-{slug}",
                "D11",
                kind,
                "UPDATE {dm} SET d = -0.0 WHERE id = 6",
                ("spark-sql",),
                "zone",
                "zsplit",
                readback=STORED_STATE_DM,
            )
        )
        items.append(
            dml_item(
                f"D11-updatecast-{slug}",
                "D11",
                kind,
                "UPDATE {dm} SET d = CAST('-0.0' AS DOUBLE) WHERE id = 6",
                ("spark-sql",),
                "zone",
                "zsplit",
                readback=STORED_STATE_DM,
            )
        )
        items.append(
            dml_item(
                f"D11-merge-{slug}",
                "D11",
                kind,
                "MERGE INTO {dm} AS t USING (SELECT CAST(6 AS BIGINT) AS id, CAST('-0.0' AS "
                "DOUBLE) AS d) AS u "
                "ON t.id = u.id WHEN MATCHED THEN UPDATE SET d = u.d",
                ("spark-sql",),
                "zone",
                "zsplit",
                readback=STORED_STATE_DM,
            )
        )
    for kind in ("v2-cow", "v2-mor"):
        slug = KIND_SLUG[kind]
        items.append(
            dml_item(
                f"D12-select-{slug}",
                "D12",
                kind,
                "SELECT id FROM {dm} WHERE d = 0.0",
                ("spark-sql",),
                "plain",
                "part",
                readback=None,
            )
        )
        items.append(
            dml_item(
                f"D12-delete-{slug}",
                "D12",
                kind,
                "DELETE FROM {dm} WHERE d = 0.0",
                ("spark-sql",),
                "plain",
                "part",
            )
        )
        items.append(
            dml_item(
                f"D12-group-{slug}",
                "D12",
                kind,
                "SELECT d, count(*) AS c FROM {dm} GROUP BY d",
                ("spark-sql",),
                "plain",
                "part",
                readback=None,
            )
        )
    for index, item in enumerate(items):
        item["idx"] = index
    return items


STORED_STATE_DM = "SELECT id, CAST(d AS STRING) AS d FROM {dm} ORDER BY id"


def create_props(kind: str) -> str:
    version, mode = KIND_PROPS[kind]
    return (
        f"'format-version' = '{version}', 'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}', 'write.merge.mode' = '{mode}'"
    )


def run_read_cells(ctx: dict[str, Any], cells: list[dict[str, Any]]) -> None:
    engine = ctx["engine"]
    for cell in cells:
        for layout in read_layouts(cell, engine):
            names = read_names(engine, layout)
            used = set()
            for field in ("sql", "where", "native", "typed"):
                if cell.get(field):
                    used |= placeholders_in(cell[field])
            missing = [name for name in used if names.get(name) in ctx["failed"]]
            for route in read_routes(cell, engine):
                door = door_for(ctx, route)
                if route in ("spark-sql", "native"):
                    text = fill(cell["native"] if route == "native" else cell["sql"], names)
                    target = None
                elif route in ("filter-str", "f-expr"):
                    text = cell["where"]
                    target = names["t"]
                else:
                    text = cell["typed"]
                    target = names["t"]
                record = {
                    "record": "cell",
                    "family": "read",
                    "cid": cell["cid"],
                    "group": cell["group"],
                    "kind": "v2-cow",
                    "engine": engine,
                    "route": route,
                    "door": door["name"],
                    "layout": layout,
                    "truth_layout": "none" if cell["literal"] else "view",
                    "test_layout": "none"
                    if cell["literal"]
                    else ("split" if uses_t(cell) else "one"),
                    "ordered": cell["ordered"],
                    "text": scrub(text, ctx["roots"]),
                }
                if missing:
                    record.update(
                        {
                            "outcome": SETUP_REFUSED,
                            "error": {"class": "setup", "message": ",".join(missing)},
                            "cols": [],
                            "rows": None,
                        }
                    )
                    ctx["records"].append(record)
                    continue
                if route == "native" and "native" not in ctx["doors"]:
                    record.update(
                        {"outcome": NOT_EXPRESSIBLE, "error": None, "cols": [], "rows": None}
                    )
                    ctx["records"].append(record)
                    continue
                pre_text = cell["pre"].get(engine) or cell["pre"].get("both") or ()
                post_text = cell["post"].get(engine) or cell["post"].get("both") or ()
                pre_errors = []
                for statement in pre_text:
                    outcome, error, _, _ = attempt(ctx, run_sql, door, fill(statement, names))
                    if outcome != ANSWERED:
                        pre_errors.append({"text": statement, "error": error})
                if route in ("spark-sql", "native"):
                    outcome, error, cols, rows = attempt(ctx, run_sql, door, text)
                else:
                    outcome, error, cols, rows = attempt(ctx, run_frame, door, target, text, route)
                for statement in post_text:
                    attempt(ctx, run_sql, door, fill(statement, names))
                record.update(
                    {
                        "outcome": outcome,
                        "error": error,
                        "cols": cols,
                        "rows": rows if outcome == ANSWERED else None,
                    }
                )
                if pre_errors:
                    record["pre_errors"] = pre_errors
                ctx["records"].append(record)


def run_dml_items(ctx: dict[str, Any], items: list[dict[str, Any]]) -> None:
    engine = ctx["engine"]
    catalog = SPARK_CATALOG if engine == "spark" else REPARK_CATALOG
    for item in items:
        for door_name in item["doors"]:
            if engine == "spark" and door_name != "spark-sql":
                continue
            door = ctx["doors"].get(door_name)
            if engine == "repark" and door_name == "native" and item["kind"] != "v2-cow":
                ctx["records"].append(
                    {
                        "record": "cell",
                        "family": "dml",
                        "cid": item["cid"],
                        "group": item["group"],
                        "kind": item["kind"],
                        "engine": engine,
                        "route": door_name,
                        "door": door_name,
                        "layout": item["test"],
                        "truth_layout": item["truth"],
                        "test_layout": item["test"],
                        "ordered": False,
                        "outcome": NOT_EXPRESSIBLE,
                        "error": {
                            "class": "door",
                            "message": "native door takes no TBLPROPERTIES; v2 CoW only",
                        },
                        "cols": [],
                        "rows": None,
                        "text": item["stmt"],
                    }
                )
                continue
            layouts = [item["test"]]
            if engine == "spark" and item["truth"] != item["test"]:
                layouts = [item["truth"], item["test"]]
            for layout in layouts:
                run_one_dml(ctx, item, door, door_name, catalog, layout)


def run_one_dml(
    ctx: dict[str, Any],
    item: dict[str, Any],
    door: dict[str, Any],
    door_name: str,
    catalog: str,
    layout: str,
) -> None:
    engine = ctx["engine"]
    dm = f"{catalog}.{NS}.dm_{item['idx']}_{layout}"
    dst = f"{catalog}.{NS}.dst_{item['idx']}_{layout}"
    names = {"dm": dm, "dst": dst}
    partition, chunks = SEED_PLANS[layout]
    seeded = [tuple(T_ROWS[index] for index in chunk) for chunk in chunks]
    record = {
        "record": "cell",
        "family": "dml",
        "cid": item["cid"],
        "group": item["group"],
        "kind": item["kind"],
        "engine": engine,
        "route": door_name,
        "door": door["name"],
        "layout": layout,
        "truth_layout": item["truth"],
        "test_layout": item["test"],
        "ordered": False,
        "text": scrub(item["stmt"], ctx["roots"]),
    }
    if not create_and_seed(ctx, door, dm, item["kind"], tuple(seeded), partition):
        record.update(
            {
                "outcome": SETUP_REFUSED,
                "error": {"class": "setup", "message": dm},
                "cols": [],
                "rows": None,
            }
        )
        ctx["records"].append(record)
        return
    extra_errors = []
    for statement in item["extra"]:
        text = fill(statement, names)
        if not setup(ctx, door, text, "extra", dst):
            extra_errors.append(text)
    outcome, error, cols, rows = attempt(ctx, run_sql, door, fill(item["stmt"], names))
    record.update(
        {
            "outcome": outcome,
            "error": error,
            "cols": cols,
            "rows": rows if outcome == ANSWERED else None,
        }
    )
    if item["readback"] is not None:
        read_outcome, read_error, read_cols, read_rows = attempt(
            ctx, run_sql, door, fill(item["readback"], names)
        )
        record.update(
            {
                "stmt_outcome": outcome,
                "stmt_error": error,
                "outcome": read_outcome if outcome == ANSWERED else outcome,
                "error": read_error if outcome == ANSWERED else error,
                "cols": read_cols,
                "rows": read_rows if read_outcome == ANSWERED else None,
                "readback": True,
            }
        )
    if extra_errors:
        record["extra_errors"] = extra_errors
    ctx["records"].append(record)


def run_timing(ctx: dict[str, Any]) -> None:
    door = ctx["doors"]["spark-sql"]
    big = f"{REPARK_CATALOG}.{NS}.big_cow"
    records = ctx["records"]
    created = create_and_seed(ctx, door, big, "v2-cow", (), False)
    if created:
        text = (
            f"INSERT INTO {big} (id, d, flag) SELECT id, "
            "CASE WHEN id % 4 = 0 THEN CAST('-0.0' AS DOUBLE) "
            "WHEN id % 4 = 1 THEN CAST('0.0' AS DOUBLE) ELSE CAST(id % 1000 AS DOUBLE) END AS d, "
            "'b' AS flag FROM range(0, 10000000)"
        )
        setup(ctx, door, text, "seed-10M", big)
    for name, text in (
        ("D15-group-by-d", f"SELECT d, count(*) AS c FROM {big} GROUP BY d"),
        (
            "D15-join-count",
            f"SELECT count(*) AS c FROM {big} AS t JOIN {REPARK_CATALOG}.{NS}.u_one_cow AS u ON "
            "t.d = u.d",
        ),
    ):
        timings = []
        loads = []
        answer = None
        error = None
        for _ in range(5):
            loads.append(os.getloadavg()[0])
            start = time.perf_counter()
            outcome, error, _names, rows = attempt(ctx, run_sql, door, text)
            timings.append(time.perf_counter() - start)
            if outcome != ANSWERED:
                break
            answer = rows
        records.append(
            {
                "record": "timing",
                "cid": name,
                "engine": "repark",
                "route": "spark-sql",
                "text": text,
                "outcome": ANSWERED if answer is not None else REFUSED,
                "error": error,
                "median_seconds": statistics.median(timings) if timings else None,
                "runs_seconds": timings,
                "load1_before": loads,
                "load1_after": os.getloadavg()[0],
                "rows": answer,
            }
        )


SPILL_COUNTER = re.compile(r"(spill_count|spilled_rows|spilled_bytes)=([0-9]+(?:\.[0-9]+)?)")


def repark_spill_counters(plan: str) -> list[list[Any]]:
    found: list[list[Any]] = []
    for line in plan.splitlines():
        operator = line.strip().split(":", 1)[0]
        for name, value in SPILL_COUNTER.findall(line):
            found.append([operator, name, value])
    return found


def spark_spill_metrics(door: dict[str, Any], text: str) -> tuple[list[list[Any]], list[Any]]:
    frame = door["sql"](text)
    to_arrow(frame)
    stack = [frame._jdf.queryExecution().executedPlan()]
    found: list[list[Any]] = []
    while stack:
        node = stack.pop()
        entries = node.metrics().iterator()
        while entries.hasNext():
            entry = entries.next()
            name = str(entry._1())
            if "spill" in name.lower():
                found.append([str(node.nodeName()), name, int(entry._2().value())])
        children = node.children().iterator()
        while children.hasNext():
            stack.append(children.next())
    return found, []


def run_spill(ctx: dict[str, Any], catalog: str, label: str, route_door: dict[str, Any]) -> None:
    door = route_door
    s = f"{catalog}.{NS}.x_s"
    u = f"{catalog}.{NS}.u_one_cow"
    created = create_and_seed(ctx, door, s, "v2-cow", (), False)
    if created:
        text = (
            f"INSERT INTO {s} (id, d, flag) SELECT id, "
            "CASE WHEN id % 4 = 0 THEN CAST('-0.0' AS DOUBLE) "
            "WHEN id % 4 = 1 THEN CAST('0.0' AS DOUBLE) ELSE CAST(id % 1000 AS DOUBLE) END AS d, "
            "'s' AS flag FROM range(0, 2000000)"
        )
        setup(ctx, door, text, "seed-2M", s)
    for cid, template in SPILL_QUERIES:
        text = template.replace("{s}", s).replace("{u}", u)
        outcome, error, _, rows = attempt(ctx, run_sql, door, text)
        explain = "EXPLAIN ANALYZE " + text
        plan_outcome, plan_error, _, plan_rows = attempt(ctx, run_sql, door, explain)
        plan_text = "\n".join(str(value) for row in plan_rows for value in row)
        counters = repark_spill_counters(plan_text)
        lines = [f"{node}: {name}={value}" for node, name, value in counters]
        metric_error = None
        metrics: list[list[Any]] = []
        if label.startswith("spark"):
            metric_outcome, metric_error, metrics, _ = attempt(ctx, spark_spill_metrics, door, text)
            if metric_outcome != ANSWERED:
                metrics = []
            lines = [f"{node}: {name} = {value}" for node, name, value in metrics]
        ctx["records"].append(
            {
                "record": "spill",
                "cid": cid,
                "label": label,
                "route": door["name"],
                "text": text,
                "outcome": outcome,
                "error": error,
                "rows": rows,
                "explain_outcome": plan_outcome,
                "explain_error": plan_error,
                "spill_lines": lines,
                "spill_nonzero": (
                    any(value > 0 for _, _, value in metrics)
                    if label.startswith("spark")
                    else any(float(value.replace(",", "")) > 0 for _, _, value in counters)
                ),
                "spark_metric_error": metric_error if label.startswith("spark") else None,
                "plan_lines": len(plan_text.splitlines()),
            }
        )


def build_spark_context(
    arguments: argparse.Namespace, warehouse: Path, spill: bool
) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as spark_functions

    root = warehouse / "spark"
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("signed-zero-normalize-1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", SESSION_ZONE)
        .config("spark.local.dir", str(warehouse / "spark-local"))
        .config("spark.sql.shuffle.partitions", "8" if spill else "2")
        .config("spark.jars", arguments.ivy_jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{SPARK_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.warehouse", str(root))
    )
    if spill:
        builder = (
            builder.config("spark.memory.fraction", "0.01")
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.sql.join.preferSortMergeJoin", "false")
            .config("spark.sql.adaptive.enabled", "false")
        )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    jar = Path(arguments.ivy_jar)
    door = {
        "name": "spark-sql",
        "sql": session.sql,
        "catalog": SPARK_CATALOG,
        "session": session,
        "functions": spark_functions,
    }
    run_sql(door, f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.{NS}")
    header = {
        "record": "leg",
        "engine": "spark",
        "label": arguments.label,
        "leg": arguments.part,
        "spark_version": session.version,
        "jdk_version": session.sparkContext._jvm.System.getProperty("java.version"),
        "session_time_zone": session.conf.get("spark.sql.session.timeZone"),
        "memory_fraction": session.conf.get("spark.memory.fraction", None),
        "iceberg_jar": jar.name,
        "iceberg_jar_sha256": sha256_of(jar),
        "python": sys.version.split()[0],
        "load1": os.getloadavg()[0],
    }
    return {
        "engine": "spark",
        "doors": {"spark-sql": door},
        "header": header,
        "close": session.stop,
    }


def build_repark_context(
    arguments: argparse.Namespace,
    warehouse: Path,
    facade_memory: str | None,
    native_memory: int | None,
) -> dict[str, Any]:
    import pyarrow

    import repark
    from repark import ReparkSession
    from repark import functions as repark_functions
    from repark._native import PyReparkSession

    native_root = warehouse / "native"
    facade_root = warehouse / "facade"
    native_root.mkdir(parents=True)
    facade_root.mkdir(parents=True)
    if native_memory is None:
        native = PyReparkSession.native(config=NATIVE_CONFIG)
    else:
        native = PyReparkSession.native(memory_limit_gb=native_memory, config=NATIVE_CONFIG)
    native.register_memory_catalog(REPARK_CATALOG, str(native_root))
    native.create_namespace(REPARK_CATALOG, NS, str(native_root / NS))
    repark._ANSI_NATIVE = native
    builder = (
        ReparkSession.builder.appName("signed-zero-normalize-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", SESSION_ZONE)
    )
    if facade_memory is not None:
        builder = builder.config("repark.memory.limit.gb", facade_memory)
    facade = builder.getOrCreate()
    facade.register_memory_catalog(REPARK_CATALOG, str(facade_root))
    facade_door = {
        "name": "spark-sql",
        "sql": facade.sql,
        "catalog": REPARK_CATALOG,
        "session": facade,
        "functions": repark_functions,
    }
    native_door = {
        "name": "native",
        "sql": repark.sql,
        "catalog": REPARK_CATALOG,
        "session": None,
        "functions": None,
    }
    wheel = Path(arguments.wheel)
    zone_rows = run_sql(facade_door, "SELECT current_timezone() AS z")
    header = {
        "record": "leg",
        "engine": "repark",
        "label": arguments.label,
        "leg": arguments.part,
        "wheel_file": wheel.name,
        "wheel_sha256": sha256_of(wheel),
        "repark_version": importlib.metadata.version("repark"),
        "pyarrow_version": pyarrow.__version__,
        "native_config": NATIVE_CONFIG,
        "facade_memory_limit_gb": facade_memory,
        "native_memory_limit_gb": native_memory,
        "facade_time_zone": zone_rows[1][0][0] if zone_rows[1] else None,
        "python": sys.version.split()[0],
        "load1": os.getloadavg()[0],
    }
    run_sql(facade_door, f"CREATE NAMESPACE {REPARK_CATALOG}.{NS} LOCATION '{facade_root / NS}'")
    return {
        "engine": "repark",
        "doors": {"spark-sql": facade_door, "native": native_door},
        "header": header,
        "close": lambda: None,
    }


def finish_context(ctx: dict[str, Any], warehouse: Path) -> None:
    ctx["roots"] = [
        (str(warehouse), "<warehouse>"),
        (tempfile.gettempdir(), "<tmp>"),
        (str(Path.home()), "<home>"),
    ]
    ctx["records"] = [ctx["header"]]
    ctx["failed"] = set()


def run_part(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
    part = arguments.part
    if arguments.engine == "spark":
        ctx = build_spark_context(arguments, warehouse, spill=part == "spill")
    elif part == "spill-facade":
        ctx = build_repark_context(arguments, warehouse, facade_memory="1", native_memory=None)
    elif part == "spill-native":
        ctx = build_repark_context(arguments, warehouse, facade_memory=None, native_memory=1)
    else:
        ctx = build_repark_context(arguments, warehouse, facade_memory=None, native_memory=None)
    finish_context(ctx, warehouse)
    if part == "reads":
        cells = [
            cell for cell in read_cells() if not arguments.only or cell["group"] in arguments.only
        ]
        for door in ctx["doors"].values():
            if door["name"] == "spark-sql":
                if arguments.engine == "spark":
                    ensure_views(ctx, door)
                ensure_read_tables(ctx, door)
        if arguments.engine == "repark":
            ensure_read_tables(ctx, ctx["doors"]["native"])
        run_read_cells(ctx, cells)
    elif part == "dml":
        items = [
            item for item in dml_items() if not arguments.only or item["group"] in arguments.only
        ]
        run_dml_items(ctx, items)
    elif part == "timing":
        ensure_read_tables(ctx, ctx["doors"]["spark-sql"])
        run_timing(ctx)
    elif part == "spill":
        ensure_read_tables(ctx, ctx["doors"]["spark-sql"])
        run_spill(ctx, SPARK_CATALOG, "spark-4.1.2-spill", ctx["doors"]["spark-sql"])
    elif part == "spill-facade":
        ensure_read_tables(ctx, ctx["doors"]["spark-sql"])
        run_spill(ctx, REPARK_CATALOG, "repark-facade-1gb", ctx["doors"]["spark-sql"])
    elif part == "spill-native":
        ensure_read_tables(ctx, ctx["doors"]["native"])
        run_spill(ctx, REPARK_CATALOG, "repark-native-1gb", ctx["doors"]["native"])
    return ctx


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True, default=str, ensure_ascii=False) + "\n")


def parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--raw-dir")
    parser.add_argument("--out-dir")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--engine", choices=("spark", "repark"))
    parser.add_argument(
        "--part", choices=("reads", "dml", "timing", "spill", "spill-facade", "spill-native")
    )
    parser.add_argument("--label")
    parser.add_argument("--warehouse")
    parser.add_argument("--out")
    parser.add_argument("--only", nargs="+")
    parser.add_argument("--ivy-jar")
    parser.add_argument("--wheel")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    if arguments.plan_only:
        print(f"read cells {len(read_cells())} dml items {len(dml_items())}")
        return 0
    if arguments.render:
        return render(Path(arguments.raw_dir), Path(arguments.out_dir))
    warehouse = Path(arguments.warehouse)
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True)
    ctx = run_part(arguments, warehouse)
    try:
        write_jsonl(Path(arguments.out), ctx["records"])
    finally:
        ctx["close"]()
    return 0


def unordered_cell(record: dict[str, Any]) -> None:
    if record.get("cid") != "D17-1" or record.get("rows") is None:
        return
    rows = []
    for row in record["rows"]:
        rows.append(
            [
                sorted(json.dumps(item, sort_keys=True, default=str) for item in value)
                if isinstance(value, list)
                else value
                for value in row
            ]
        )
    record["rows"] = rows


def load_raw(raw_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(raw_dir.glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                record = json.loads(line)
                record["_file"] = path.name
                records.append(record)
    return records


def same_rows(left: list[Any] | None, right: list[Any] | None, ordered: bool) -> bool:
    if left is None or right is None:
        return False
    if ordered:
        return [json.dumps(row, sort_keys=True, default=str) for row in left] == [
            json.dumps(row, sort_keys=True, default=str) for row in right
        ]
    return row_keys(left) == row_keys(right)


def sign_free(rows: list[Any] | None) -> list[Any] | None:
    if rows is None:
        return None
    return json.loads(json.dumps(rows).replace('"-0.0"', '"0.0"'))


def label_of(
    record: dict[str, Any] | None, truth: dict[str, Any] | None, same: dict[str, Any] | None
) -> list[str]:
    if record is None:
        return ["NO-RECORD"]
    outcome = record["outcome"]
    if outcome in (PANIC, NOT_EXPRESSIBLE, SETUP_REFUSED):
        return [outcome]
    if truth is None or truth["outcome"] in (SETUP_REFUSED, NOT_EXPRESSIBLE):
        return ["ORACLE-MISSING"]
    if truth["outcome"] == PANIC:
        return ["ORACLE-PANIC"]
    theirs = truth["outcome"]
    native = record.get("route") == "native"
    if outcome == REFUSED and theirs == REFUSED:
        return ["BOTH-REFUSED"]
    if outcome == ANSWERED and theirs == REFUSED:
        return ["ACCEPTED-WHERE-SPARK-REFUSED"]
    if outcome == REFUSED and theirs == ANSWERED:
        return ["NATIVE-REFUSED" if native else "REFUSED-WHERE-SPARK-ACCEPTED"]
    family = record["family"]
    if str(record.get("text", "")).lstrip().upper().startswith("SELECT"):
        family = "read"
    bad = "WRONG-WRITE" if family == "dml" else "WRONG-READ"
    ordered = bool(record.get("ordered"))
    mine, true_rows = record["rows"], truth["rows"]
    labels: list[str] = []
    if not same_rows(mine, true_rows, False):
        if same_rows(sign_free(mine), sign_free(true_rows), False):
            labels.append("SIGN-ONLY")
        spark_same = same if same is not None and same.get("outcome") == ANSWERED else None
        pruned = (
            spark_same is not None
            and not same_rows(spark_same["rows"], true_rows, False)
            and same_rows(mine, spark_same["rows"], False)
        )
        if (record["engine"] == "spark" and record["layout"] in PRUNABLE_LAYOUTS) or pruned:
            labels.append("PRUNED-WRONG")
        else:
            labels.append(bad)
    elif ordered and not same_rows(mine, true_rows, True):
        labels.append("ORDER-DIFFER")
    names_ref = same if same is not None and same.get("outcome") == ANSWERED else truth
    if (
        not labels
        and record["family"] == "read"
        and names_ref.get("cols")
        and record.get("cols")
        and record["cols"] != names_ref["cols"]
    ):
        labels.append(bad + " (names)")
    return labels or ["EQUAL"]


PRUNABLE_LAYOUTS = ("split", "zsplit", "part", "m1s4")


def show_record(record: dict[str, Any] | None) -> str:
    if record is None:
        return "-"
    if record["outcome"] != ANSWERED:
        error = record.get("error") or {}
        message = excerpt(str(error.get("message", "")), 90)
        return f"{record['outcome']} {error.get('class', '')}: {message}".strip()
    rows = record["rows"]
    if rows is None:
        return "-"
    if not rows:
        return "none"
    items = [
        "(" + ",".join("NULL" if value is None else str(value) for value in row) + ")"
        for row in rows
    ]
    text = "; ".join(items)
    return text if len(text) <= 180 else text[:177] + "..."


def cell_md(text: Any) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


IEEE_JOIN_COUNT = 10_000_000


def render(raw_dir: Path, out_dir: Path) -> int:
    records = load_raw(raw_dir)
    for record in records:
        if record.get("record") == "cell":
            unordered_cell(record)
    headers = [record for record in records if record.get("record") == "leg"]
    cells = [record for record in records if record.get("record") == "cell"]
    index: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for record in cells:
        index[(record["cid"], record["route"], record["layout"], record["engine"])] = record
    rows_out: list[str] = []
    counter: Counter[tuple[str, str, str, str]] = Counter()
    findings: list[str] = []
    for record in cells:
        if record["engine"] != "repark" or record["layout"] != record["test_layout"]:
            continue
        spark_route = "spark-sql" if record["route"] == "native" else record["route"]
        truth = index.get((record["cid"], "spark-sql", record["truth_layout"], "spark"))
        if record["family"] == "read" and record["truth_layout"] == "none":
            truth = index.get((record["cid"], "spark-sql", "none", "spark"))
        same = index.get((record["cid"], spark_route, record["test_layout"], "spark"))
        labels = label_of(record, truth, same)
        spark_label = label_of(same, truth, None) if same is not None else ["-"]
        control_s = index.get((record["cid"], spark_route, "one", "spark"))
        control_r = index.get((record["cid"], record["route"], "one", "repark"))
        control = "-"
        if record["family"] == "read" and record["test_layout"] == "split":
            control = "S: {}; R: {}".format(
                "/".join(label_of(control_s, truth, None)) if control_s else "-",
                "/".join(label_of(control_r, truth, control_s)) if control_r else "-",
            )
        counter[(record["group"], record["route"], "RePark", "+".join(labels))] += 1
        if same is not None:
            counter[(record["group"], spark_route, "Spark", "+".join(spark_label))] += 1
        text = record.get("text", "")
        line = (
            f"| {cell_md(record['cid'])} | {record['group']} | {record['kind']} | "
            f"{record['route']} | "
            f"{record['test_layout']} | {cell_md(text)} | {cell_md(show_record(truth))} | "
            f"{cell_md(show_record(same))} | {cell_md(show_record(record))} | {'+'.join(labels)} "
            f"| {cell_md(control)} |"
        )
        rows_out.append(line)
        if any(label.startswith(("WRONG-READ", "WRONG-WRITE", "PRUNED-WRONG")) for label in labels):
            findings.append(
                f"- {record['cid']} {record['route']} ({record['kind']}): {'+'.join(labels)}"
            )
    out_dir.mkdir(parents=True, exist_ok=True)
    header = [
        "# Signed zero domain table (SIGNED-ZERO-NORMALIZE-1, sketch section 9)",
        "",
        "One row per RePark cell on the test layout. Truth is the Spark SQL answer on the "
        "inline-VALUES view of the same rows",
        "(layout `view`, or `zone`, `plain` and `none` for the DML and literal cells); Spark is "
        "the Spark-on-Iceberg answer on the",
        "test layout and the same route; RePark-main is the release wheel answer. The control "
        "column carries the one-file layout.",
        "",
        "| cell | group | kind | route | layout | statement | truth | Spark | RePark-main | "
        "label | control |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    (out_dir / "domain-table.md").write_text("\n".join(header + rows_out) + "\n", encoding="utf-8")
    summary_lines = [
        "# Signed zero domain table: summary (generated)",
        "",
        "Counts by label, row group and route. RePark rows are the test layout on the release "
        "wheel; Spark rows are the",
        "Spark-on-Iceberg answer on the test layout for the same route.",
        "",
        "| group | route | engine | label | count |",
        "|---|---|---|---|---|",
    ]
    for (group, route, engine, label), count in sorted(counter.items()):
        summary_lines.append(f"| {group} | {route} | {engine} | {label} | {count} |")
    summary_lines += ["", "## Findings (WRONG-READ, WRONG-WRITE, PRUNED-WRONG)", ""]
    summary_lines += findings or ["none"]
    summary_lines += ["", "## Legs", ""]
    for header_record in headers:
        summary_lines.append(
            "- "
            + json.dumps(
                {k: v for k, v in header_record.items() if k != "record"},
                sort_keys=True,
                default=str,
            )
        )
    timings = [record for record in records if record.get("record") == "timing"]
    summary_lines += ["", "## Timing (RePark, pre-unit wheel only)", ""]
    for record in timings:
        summary_lines.append(
            f"- {record['cid']}: outcome {record['outcome']}, median "
            f"{record.get('median_seconds')} s over {len(record.get('runs_seconds', []))} runs, "
            f"load1 before {[round(v, 2) for v in record.get('load1_before', [])]}, rows "
            f"{show_record(record)}"
            + (
                f"; IEEE expectation from the seed: {IEEE_JOIN_COUNT} (5,000,000 zero rows, "
                "each matching two u rows)"
                if record["cid"] == "D15-join-count"
                else ""
            )
        )
    spills = [record for record in records if record.get("record") == "spill"]
    summary_lines += ["", "## Spill (D16)", ""]
    for record in spills:
        summary_lines.append(
            f"- {record['label']} {record['cid']}: answer {show_record(record)}; explain "
            f"{record['explain_outcome']}; "
            f"spill lines {len(record['spill_lines'])}; nonzero {record['spill_nonzero']}"
        )
    (out_dir / "summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print(
        f"rows {len(rows_out)} findings {len(findings)} timings {len(timings)} spills {len(spills)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
