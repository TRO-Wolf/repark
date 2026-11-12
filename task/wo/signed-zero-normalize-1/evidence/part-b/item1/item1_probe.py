from __future__ import annotations

import argparse
import contextlib
import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent


def load_base() -> Any:
    spec = importlib.util.spec_from_file_location(
        "szb_probe", HERE.parent / "step0" / "szb_probe.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["szb_probe"] = module
    spec.loader.exec_module(module)
    return module


base = load_base()

PA = base.PA
NS = base.NS
SPILL_TYPES = (("double", "d", "DOUBLE"), ("float", "f", "FLOAT"))
SPILL_CELLS = (
    (
        "SP-group",
        "GROUP BY on a float key with a million groups (hash aggregate spill)",
        "SELECT count(*) AS groups, max(c) AS biggest FROM (SELECT {k}, count(*) AS c FROM {s} "
        "GROUP BY {k}) x",
        False,
    ),
    (
        "SP-window-order",
        "row_number over ORDER BY a float key then id (full sort spill under the window)",
        "SELECT id FROM (SELECT id, row_number() OVER (ORDER BY {k}, id) AS rn FROM {s}) x WHERE "
        "rn <= 4",
        False,
    ),
    (
        "SP-window-partition",
        "rank over PARTITION BY a float key (partition sort spill)",
        "SELECT count(*) AS n FROM (SELECT id, rank() OVER (PARTITION BY {k} ORDER BY id) AS r "
        "FROM {s}) x "
        "WHERE r = 1",
        False,
    ),
    (
        "SP-sortmerge",
        "sort-merge self join on a float key (sort and buffered-side spill)",
        "SELECT count(*) AS c FROM {s} AS a JOIN {s} AS b ON a.{k} = b.{k} WHERE a.id % 1000 = 0",
        True,
    ),
)
SORT_MERGE_PRE = {
    "spark": (
        "SET spark.sql.join.preferSortMergeJoin = true",
        "SET spark.sql.autoBroadcastJoinThreshold = -1",
    ),
    "repark": ("SET datafusion.optimizer.prefer_hash_join = false",),
}
SORT_MERGE_POST = {
    "spark": ("SET spark.sql.autoBroadcastJoinThreshold = 10485760",),
    "repark": ("SET datafusion.optimizer.prefer_hash_join = true",),
}
EXPLAIN_CELLS = (
    ("X-agg-group", "Aggregate::group_expr", "SELECT d, count(*) AS c FROM {t} GROUP BY d", "all"),
    (
        "X-grouping-set",
        "GroupingSet members (ROLLUP)",
        "SELECT d, g, count(*) AS c FROM {t} GROUP BY ROLLUP (d, g)",
        "all",
    ),
    (
        "X-multi-distinct",
        "distinct aggregate args (multi-distinct)",
        "SELECT g, count(DISTINCT d) AS a, count(DISTINCT e) AS b FROM {t} GROUP BY g",
        "all",
    ),
    (
        "X-single-distinct",
        "single distinct aggregate (single_distinct_to_groupby)",
        "SELECT count(DISTINCT d) AS n FROM {t}",
        "all",
    ),
    ("X-distinct-all", "Distinct::All", "SELECT DISTINCT d FROM {t}", "all"),
    (
        "X-distinct-on",
        "Distinct::On",
        "SELECT DISTINCT ON (d) d, id FROM {t} ORDER BY d, id",
        "native",
    ),
    (
        "X-join-on",
        "Join::on (inner)",
        "SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u ON t.d = u.d",
        "all",
    ),
    (
        "X-join-filter",
        "Join::filter (non-literal float comparison)",
        "SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u ON t.g = u.g AND t.d < u.e",
        "all",
    ),
    (
        "X-intersect",
        "INTERSECT (left dedup + semi join)",
        "SELECT d FROM {t} INTERSECT SELECT d FROM {u}",
        "all",
    ),
    (
        "X-except",
        "EXCEPT (left dedup + anti join)",
        "SELECT d FROM {t} EXCEPT SELECT d FROM {u}",
        "all",
    ),
    (
        "X-union",
        "UNION distinct (Union + Distinct)",
        "SELECT d FROM {t} UNION SELECT d FROM {u}",
        "all",
    ),
    (
        "X-in-subquery",
        "InSubquery decorrelated to a semi join",
        "SELECT id FROM {t} WHERE d IN (SELECT d FROM {u})",
        "all",
    ),
    (
        "X-not-in-subquery",
        "NOT IN subquery (null-aware anti join)",
        "SELECT id FROM {t} WHERE d NOT IN (SELECT d FROM {u})",
        "all",
    ),
    (
        "X-scalar-subquery",
        "scalar subquery to join",
        "SELECT id FROM {t} WHERE d = (SELECT max(d) FROM {u})",
        "all",
    ),
    (
        "X-window",
        "Window partition_by and order_by",
        "SELECT id, rank() OVER (PARTITION BY d ORDER BY e) AS r FROM {t}",
        "all",
    ),
    (
        "X-repartition",
        "Repartition DistributeBy then Aggregate",
        "SELECT d, count(*) AS c FROM (SELECT * FROM {t} DISTRIBUTE BY d) x GROUP BY d",
        "all",
    ),
    ("X-sort", "Sort", "SELECT id, d FROM {t} ORDER BY d, id", "all"),
    ("X-topk", "Sort with fetch (TopK)", "SELECT id, d FROM {t} ORDER BY d, id LIMIT 3", "all"),
    (
        "X-compare-filter",
        "BinaryExpr in a Filter (and TableScan filters)",
        "SELECT id FROM {t} WHERE d = e",
        "all",
    ),
    (
        "X-compare-projection",
        "BinaryExpr <=> in a Projection",
        "SELECT id, d <=> e AS x FROM {t}",
        "all",
    ),
    (
        "X-between",
        "Between with a non-literal bound",
        "SELECT id FROM {t} WHERE d BETWEEN e AND 1.5",
        "all",
    ),
    (
        "X-simple-case",
        "simple CASE with a non-literal WHEN",
        "SELECT id, CASE d WHEN e THEN 1 ELSE 0 END AS x FROM {t}",
        "all",
    ),
    ("X-inlist", "non-literal InList", "SELECT id FROM {t} WHERE d IN (e, 1.5)", "all"),
    (
        "X-agg-filter",
        "aggregate FILTER",
        "SELECT count(*) FILTER (WHERE d = e) AS n FROM {t}",
        "all",
    ),
    (
        "X-recursive",
        "RecursiveQuery is_distinct (UNION)",
        "WITH RECURSIVE r(x) AS (SELECT d AS x FROM {t} UNION SELECT x FROM r WHERE x > 1.0) "
        "SELECT x FROM r",
        "all",
    ),
    (
        "X-float-double",
        "FLOAT key joined to a DOUBLE key (coercion cast)",
        "SELECT t.id, u.uid FROM {t} AS t JOIN {u} AS u ON t.f = u.d",
        "all",
    ),
)
ANSWER_CELLS = ("X-distinct-on", "X-float-double")
TYPED_EXPLAIN = (
    (
        "X-repartition-typed",
        "Repartition Hash (typed repartition) then Aggregate",
        "t.repartition(4, 'd').groupBy('d').agg(F.count(F.lit(1)).alias('c'))",
    ),
)


def seed_spill(ctx: dict[str, Any], door: dict[str, Any], s: str, rows: int) -> bool:
    columns = "id BIGINT, d DOUBLE, f FLOAT"
    if not base.setup(ctx, door, base.create_sql(door["name"], s, columns, "v2-cow", None)):
        return False
    key = (
        "CASE WHEN id % 4 = 0 THEN CAST('-0.0' AS DOUBLE) WHEN id % 4 = 1 THEN CAST('0.0' AS "
        "DOUBLE) "
        "ELSE CAST(id AS DOUBLE) END"
    )
    text = (
        f"INSERT INTO {s} SELECT id, d, CAST(d AS FLOAT) AS f FROM "
        f"(SELECT id, {key} AS d FROM range(0, {rows})) x"
    )
    return base.setup(ctx, door, text)


def spill_lines(
    ctx: dict[str, Any], door: dict[str, Any], text: str
) -> tuple[str, Any, list[list[Any]], list[str]]:
    outcome, error, _, plan_rows = PA.attempt(ctx, base.run_sql, door, "EXPLAIN ANALYZE " + text)
    plan_text = PA.scrub("\n".join(str(value) for row in plan_rows for value in row), ctx["roots"])
    operators = [line.strip().split(":", 1)[0] for line in plan_text.splitlines()]
    joins = sorted({name for name in operators if "Join" in name})
    return outcome, error, PA.repark_spill_counters(plan_text), joins


def run_spill(ctx: dict[str, Any], rows: int, partitions: int | None) -> None:
    for door_name, door in ctx["doors"].items():
        if partitions is not None:
            key = (
                "spark.sql.shuffle.partitions"
                if ctx["engine"] == "spark"
                else "datafusion.execution.target_partitions"
            )
            base.setup(ctx, door, f"SET {key} = {partitions}")
        catalog = base.SPARK_CATALOG if ctx["engine"] == "spark" else base.REPARK_CATALOG
        s = f"{catalog}.{NS}.sp_{'n' if door_name == 'native' else 's'}"
        if not seed_spill(ctx, door, s, rows):
            continue
        for site, seat, template, sort_merge in SPILL_CELLS:
            for type_name, column, _ in SPILL_TYPES:
                text = template.format(k=column, s=s)
                if sort_merge:
                    for statement in SORT_MERGE_PRE[ctx["engine"]]:
                        base.setup(ctx, door, statement)
                outcome, error, cols, answer = PA.attempt(ctx, base.run_sql, door, text)
                counters: list[list[Any]] = []
                joins: list[str] = []
                explain_outcome, explain_error = None, None
                if ctx["engine"] == "repark":
                    explain_outcome, explain_error, counters, joins = spill_lines(ctx, door, text)
                if sort_merge:
                    for statement in SORT_MERGE_POST[ctx["engine"]]:
                        base.setup(ctx, door, statement)
                ctx["records"].append(
                    {
                        "record": "cell",
                        "engine": ctx["engine"],
                        "family": "read",
                        "group": "spill",
                        "site": site,
                        "seat": seat,
                        "type": type_name,
                        "build": None,
                        "cid": f"{site}|{type_name}",
                        "route": door_name,
                        "layout": "one",
                        "test_layout": "one",
                        "truth_layout": "one",
                        "ordered": False,
                        "text": PA.scrub(text, ctx["roots"]),
                        "outcome": outcome,
                        "error": error,
                        "cols": cols,
                        "rows": answer,
                        "explain_outcome": explain_outcome,
                        "explain_error": explain_error,
                        "spill_counters": counters,
                        "join_operators": joins,
                        "spill_count": sum(
                            float(value.replace(",", ""))
                            for _, name, value in counters
                            if name == "spill_count"
                        ),
                    }
                )


def explain_text(engine: str, text: str) -> str:
    return ("EXPLAIN EXTENDED " if engine == "spark" else "EXPLAIN ") + text


def typed_explain(
    door: dict[str, Any], code: str, names: dict[str, str]
) -> tuple[list[str], list[list[Any]]]:
    scope = {"F": door["functions"], "W": door["window"], "t": door["session"].table(names["t"])}
    frame = eval(code, scope)
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        frame.explain(True)
    return ["plan"], [[buffer.getvalue()]]


def run_explain(ctx: dict[str, Any]) -> None:
    for door_name, door in ctx["doors"].items():
        base.ensure_read_tables(ctx, door)
        names = base.table_names(ctx["engine"], door_name, "one")
        for site, seat, template, doors in EXPLAIN_CELLS:
            if doors == "native" and door_name != "native":
                continue
            text = template.format(t=names["t"], u=names["un"])
            outcome, error, cols, plan = PA.attempt(
                ctx, base.run_sql, door, explain_text(ctx["engine"], text)
            )
            ctx["records"].append(
                {
                    "record": "explain",
                    "engine": ctx["engine"],
                    "site": site,
                    "seat": seat,
                    "route": door_name,
                    "text": PA.scrub(text, ctx["roots"]),
                    "outcome": outcome,
                    "error": error,
                    "cols": cols,
                    "plan": [[PA.scrub(str(value), ctx["roots"]) for value in row] for row in plan],
                }
            )
            if site in ANSWER_CELLS:
                outcome, error, cols, answer = PA.attempt(ctx, base.run_sql, door, text)
                ctx["records"].append(
                    {
                        "record": "cell",
                        "engine": ctx["engine"],
                        "family": "read",
                        "group": "item1",
                        "site": site,
                        "seat": seat,
                        "type": "double",
                        "build": "un",
                        "cid": f"{site}|double|un",
                        "route": door_name,
                        "layout": "one",
                        "test_layout": "one",
                        "truth_layout": "one",
                        "ordered": "ORDER BY" in text,
                        "text": PA.scrub(text, ctx["roots"]),
                        "outcome": outcome,
                        "error": error,
                        "cols": cols,
                        "rows": answer,
                    }
                )
        if door_name != "spark-sql":
            continue
        for site, seat, code in TYPED_EXPLAIN:
            outcome, error, cols, plan = PA.attempt(ctx, typed_explain, door, code, names)
            ctx["records"].append(
                {
                    "record": "explain",
                    "engine": ctx["engine"],
                    "site": site,
                    "seat": seat,
                    "route": "typed",
                    "text": code,
                    "outcome": outcome,
                    "error": error,
                    "cols": cols,
                    "plan": [[PA.scrub(str(value), ctx["roots"]) for value in row] for row in plan],
                }
            )


def build_repark(warehouse: Path, memory: str | None) -> dict[str, Any]:
    import pyarrow

    import repark
    from repark import ReparkSession
    from repark import functions as repark_functions
    from repark._native import PyReparkSession
    from repark.spark.window import Window

    native_root = warehouse / "native"
    facade_root = warehouse / "facade"
    native_root.mkdir(parents=True)
    facade_root.mkdir(parents=True)
    native_config = dict(base.NATIVE_CONFIG)
    if memory is not None:
        native_config["datafusion.runtime.memory_limit"] = memory
    native = PyReparkSession.native(config=native_config)
    native.register_memory_catalog(base.REPARK_CATALOG, str(native_root))
    native.create_namespace(base.REPARK_CATALOG, NS, str(native_root / NS))
    repark._ANSI_NATIVE = native
    builder = (
        ReparkSession.builder.appName("szb-item1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", base.SESSION_ZONE)
    )
    if memory is not None:
        builder = builder.config("datafusion.runtime.memory_limit", memory)
    facade = builder.getOrCreate()
    facade.register_memory_catalog(base.REPARK_CATALOG, str(facade_root))
    facade.sql(f"CREATE NAMESPACE {base.REPARK_CATALOG}.{NS} LOCATION '{facade_root / NS}'")
    so = Path(repark.__file__).parent / "_native.abi3.so"
    clone = base.CLONE
    head = subprocess.run(
        ["git", "-C", str(clone), "rev-parse", "HEAD"], capture_output=True, text=True
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "-C", str(clone), "status", "--porcelain"], capture_output=True, text=True
    ).stdout.strip()
    header = {
        "record": "leg",
        "engine": "repark",
        "repark_file": str(Path(repark.__file__).resolve().relative_to(clone.resolve())),
        "native_so_sha256": PA.sha256_of(so),
        "clone_head": head,
        "clone_dirty": bool(dirty),
        "repark_version": __import__("importlib.metadata").metadata.version("repark"),
        "pyarrow_version": pyarrow.__version__,
        "python": sys.version.split()[0],
        "load1": os.getloadavg()[0],
        "native_config": native_config,
        "facade_memory_limit": memory,
        "cpu_count": len(os.sched_getaffinity(0)),
    }
    doors = {
        "spark-sql": {
            "name": "spark-sql",
            "sql": facade.sql,
            "session": facade,
            "functions": repark_functions,
            "window": Window,
        },
        "native": {
            "name": "native",
            "sql": repark.sql,
            "session": None,
            "functions": None,
            "window": None,
        },
    }
    return {"engine": "repark", "doors": doors, "header": header, "close": lambda: None}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("spark", "repark"), required=True)
    parser.add_argument("--part", choices=("spill", "explain"), required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--ivy-jar")
    parser.add_argument("--memory")
    parser.add_argument("--rows", type=int, default=2000000)
    parser.add_argument("--partitions", type=int)
    arguments = parser.parse_args()
    warehouse = Path(arguments.warehouse)
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True)
    if arguments.engine == "spark":
        ctx = base.build_spark(arguments, warehouse)
    else:
        ctx = build_repark(warehouse, arguments.memory if arguments.part == "spill" else None)
    ctx["roots"] = [(str(warehouse), "<warehouse>"), (str(Path.home()), "<home>")]
    ctx["header"]["part"] = arguments.part
    ctx["header"]["rows"] = arguments.rows if arguments.part == "spill" else None
    ctx["header"]["partitions"] = arguments.partitions if arguments.part == "spill" else None
    ctx["records"] = [ctx["header"]]
    try:
        if arguments.part == "spill":
            run_spill(ctx, arguments.rows, arguments.partitions)
        else:
            run_explain(ctx)
    finally:
        with Path(arguments.out).open("w", encoding="utf-8") as handle:
            for record in ctx["records"]:
                handle.write(
                    json.dumps(record, sort_keys=True, default=str, ensure_ascii=False) + "\n"
                )
        ctx["close"]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
