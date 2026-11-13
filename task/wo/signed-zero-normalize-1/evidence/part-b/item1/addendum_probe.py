from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent


def load(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ITEM1 = load("item1_probe", HERE / "item1_probe.py")
base = ITEM1.base
PA = base.PA
NS = base.NS
NZ = "CAST('-0.0' AS DOUBLE)"
PZ = "CAST('0.0' AS DOUBLE)"
NAN = "CAST('NaN' AS DOUBLE)"
SQRT_NAN = "sqrt(CAST(-1.0 AS DOUBLE))"
D = "CAST({} AS DOUBLE)"
TABLES = {
    "na": ("id BIGINT, d DOUBLE, k INT", (f"(1, {NZ}, 1)", f"(2, {D.format('1.5')}, 1)")),
    "nb": ("d DOUBLE, k INT", (f"({PZ}, 1)",)),
    "sl": ("a INT", ("(1)", "(1)", "(1)", "(2)", "(2)")),
    "sr": ("a INT", ("(1)", "(2)", "(2)", "(3)")),
    "w": ("id BIGINT, m INT", ("(1, 10)", "(2, CAST(NULL AS INT))", "(3, 30)")),
    "iv": ("a INT, b INT", ("(0, 5)", "(0, -5)", "(1, 1)")),
    "ia": ("a INT, b INT", ("(0, 5)",)),
    "ib": ("a INT, b INT", ("(0, -5)",)),
}
INSERT_SELECT = {
    "sn": (
        "id BIGINT, d DOUBLE",
        f"SELECT 1 AS id, {D.format('1.5')} AS d UNION ALL SELECT 2, {SQRT_NAN} "
        f"UNION ALL SELECT 3, {D.format('9.5')}",
    ),
    "sc": (
        "id BIGINT, d DOUBLE",
        f"SELECT 1 AS id, {D.format('1.5')} AS d UNION ALL SELECT 2, {NAN} "
        f"UNION ALL SELECT 3, {D.format('9.5')}",
    ),
}
SQL_CELLS = (
    ("q1-notin2", "N1-one-column", "SELECT id FROM {na} WHERE d NOT IN (SELECT d FROM {nb})"),
    (
        "q1-notin2",
        "N2-two-column",
        "SELECT id FROM {na} WHERE (d, k) NOT IN (SELECT d, k FROM {nb})",
    ),
    ("intkey", "IK-group", "SELECT a / b AS k, count(*) AS c FROM {iv} GROUP BY a / b"),
    ("intkey", "IK-distinct", "SELECT count(DISTINCT a / b) AS n FROM {iv}"),
    (
        "intkey",
        "IK-join",
        "SELECT count(*) AS n FROM {ia} AS x JOIN {ib} AS y ON x.a / x.b = y.a / y.b",
    ),
    (
        "intkey",
        "IK-window",
        "SELECT a, b, count(*) OVER (PARTITION BY a / b) AS c FROM {iv}",
    ),
    (
        "intkey",
        "IK-cast-group",
        "SELECT k, count(*) AS c FROM (SELECT CAST(a AS DOUBLE) * -1 AS k FROM {iv} WHERE a = 0 "
        "UNION ALL SELECT CAST(0 AS DOUBLE) AS k) AS x GROUP BY k",
    ),
    ("nan-sign", "NS-bits-sqrt", "SELECT sqrt(CAST(-1.0 AS DOUBLE)) AS x"),
    ("nan-sign", "NS-bits-acosh", "SELECT acosh(CAST(0.5 AS DOUBLE)) AS x"),
    ("nan-sign", "NS-bits-atanh", "SELECT atanh(CAST(2.0 AS DOUBLE)) AS x"),
    ("nan-sign", "NS-bits-cast", f"SELECT {NAN} AS x"),
    ("nan-sign", "NS-max-min", "SELECT max(d) AS hi, min(d) AS lo FROM {sn}"),
    (
        "nan-sign",
        "NS-greatest-least",
        "SELECT greatest(d, CAST(100 AS DOUBLE)) AS g, least(d, CAST(-100 AS DOUBLE)) AS l "
        "FROM {sn} WHERE id = 2",
    ),
    ("nan-sign", "NS-order-by", "SELECT id FROM {sn} ORDER BY d"),
    ("nan-sign", "NS-gt-literal", "SELECT id FROM {sn} WHERE d > 9.5"),
    ("nan-sign", "NC-max-min", "SELECT max(d) AS hi, min(d) AS lo FROM {sc}"),
    (
        "nan-sign",
        "NC-greatest-least",
        "SELECT greatest(d, CAST(100 AS DOUBLE)) AS g, least(d, CAST(-100 AS DOUBLE)) AS l "
        "FROM {sc} WHERE id = 2",
    ),
    ("nan-sign", "NC-order-by", "SELECT id FROM {sc} ORDER BY d"),
    ("nan-sign", "NC-gt-literal", "SELECT id FROM {sc} WHERE d > 9.5"),
)
LOG_FORMS = (
    "ln(CAST(-1.0 AS DOUBLE))",
    "ln(CAST(0.0 AS DOUBLE))",
    "ln(CAST(1.0 AS DOUBLE))",
    "log10(CAST(-1.0 AS DOUBLE))",
    "log10(CAST(0.0 AS DOUBLE))",
    "log2(CAST(-1.0 AS DOUBLE))",
    "log2(CAST(0.0 AS DOUBLE))",
    "log(CAST(-1.0 AS DOUBLE))",
    "log(CAST(2.0 AS DOUBLE), CAST(-1.0 AS DOUBLE))",
    "log(CAST(2.0 AS DOUBLE), CAST(8.0 AS DOUBLE))",
    "log1p(CAST(-2.0 AS DOUBLE))",
    "log1p(CAST(-1.0 AS DOUBLE))",
    "log1p(CAST(1.0 AS DOUBLE))",
    "pow(CAST(-8.0 AS DOUBLE), CAST(0.5 AS DOUBLE))",
)
SETOP_WRITES = (
    ("SW-except-all", "INSERT INTO {x} SELECT a FROM {sl} EXCEPT ALL SELECT a FROM {sr}"),
    ("SW-intersect-all", "INSERT INTO {x} SELECT a FROM {sl} INTERSECT ALL SELECT a FROM {sr}"),
    (
        "SW-cte-except-all",
        "WITH y AS (SELECT a FROM {sl} EXCEPT ALL SELECT a FROM {sr}) INSERT INTO {x} SELECT a "
        "FROM y",
    ),
    (
        "SW-cte-intersect-all",
        "WITH y AS (SELECT a FROM {sl} INTERSECT ALL SELECT a FROM {sr}) "
        "INSERT INTO {x} SELECT a FROM y",
    ),
    (
        "SW-insert-cte-except-all",
        "INSERT INTO {x} WITH y AS (SELECT a FROM {sl} EXCEPT ALL SELECT a FROM {sr}) SELECT a "
        "FROM y",
    ),
    (
        "SW-insert-cte-intersect-all",
        "INSERT INTO {x} WITH y AS (SELECT a FROM {sl} INTERSECT ALL SELECT a FROM {sr}) "
        "SELECT a FROM y",
    ),
)
FRAME_CELLS = (
    ("F6-sql-asc", "sql", "SELECT id, row_number() OVER (ORDER BY m) AS r FROM {w}", True),
    ("F6-sql-desc", "sql", "SELECT id, row_number() OVER (ORDER BY m DESC) AS r FROM {w}", True),
    (
        "F6-fexpr-asc",
        "frame",
        "w.select('id', F.expr('row_number() OVER (ORDER BY m)').alias('r'))",
        True,
    ),
    (
        "F6-fexpr-desc",
        "frame",
        "w.select('id', F.expr('row_number() OVER (ORDER BY m DESC)').alias('r'))",
        True,
    ),
    (
        "F6-fexpr-first-value-desc",
        "frame",
        "w.select(F.expr('first_value(id) OVER (ORDER BY m DESC)').alias('r')).distinct()",
        False,
    ),
    (
        "F6-fexpr-dedup-desc",
        "frame",
        "w.withColumn('r', F.expr('row_number() OVER (ORDER BY m DESC)')).filter('r = 1')"
        ".select('id')",
        False,
    ),
    (
        "F6-selectexpr-asc",
        "frame",
        "w.selectExpr('id', 'row_number() OVER (ORDER BY m) AS r')",
        True,
    ),
    (
        "F6-selectexpr-desc",
        "frame",
        "w.selectExpr('id', 'row_number() OVER (ORDER BY m DESC) AS r')",
        True,
    ),
    ("F6-orderby-fexpr-asc", "frame", "w.orderBy(F.expr('m')).select('id')", False),
    ("F6-orderby-fexpr-desc", "frame", "w.orderBy(F.expr('m').desc()).select('id')", False),
    ("F6-sort-string", "frame", "w.sort('m').select('id')", False),
)
PART_ROWS = (
    f"(1, {PZ})",
    f"(2, {NZ})",
    f"(3, {NAN})",
    "(4, CAST(NULL AS DOUBLE))",
    f"(5, {D.format('1.5')})",
)


def cell(
    ctx: dict[str, Any], route: str, cid: str, group: str, text: str, result: tuple, **extra: Any
) -> None:
    outcome, error, cols, rows, bits = result
    record = {
        "record": "cell",
        "engine": ctx["engine"],
        "family": extra.pop("family", "read"),
        "group": group,
        "site": cid.split("|")[0],
        "seat": group,
        "type": "double",
        "build": None,
        "cid": cid,
        "route": route,
        "layout": "one",
        "test_layout": "one",
        "truth_layout": "one",
        "ordered": extra.pop("ordered", False),
        "text": PA.scrub(text, ctx["roots"]),
        "outcome": outcome,
        "error": error,
        "cols": cols,
        "rows": rows,
        "bits": bits,
    }
    record.update(extra)
    ctx["records"].append(record)


def raw_sql(door: dict[str, Any], text: str) -> tuple[list[str], list[list[Any]], list[list[Any]]]:
    return base.collect_raw(PA.to_arrow(door["sql"](text)))


def attempt(ctx: dict[str, Any], call: Any, *args: Any) -> tuple:
    holder: dict[str, Any] = {}

    def wrapped(*inner: Any) -> tuple[list[str], list[list[Any]]]:
        names, rows, bits = call(*inner)
        holder["bits"] = bits
        return names, rows

    outcome, error, cols, rows = PA.attempt(ctx, wrapped, *args)
    return outcome, error, cols, rows, holder.get("bits")


def names_for(ctx: dict[str, Any], door_name: str) -> dict[str, str]:
    catalog = base.SPARK_CATALOG if ctx["engine"] == "spark" else base.REPARK_CATALOG
    tag = "n" if door_name == "native" else "s"
    return {name: f"{catalog}.{NS}.a_{name}_{tag}" for name in (*TABLES, *INSERT_SELECT, "x", "m")}


def make_tables(ctx: dict[str, Any], door: dict[str, Any], names: dict[str, str]) -> None:
    for name, (columns, rows) in TABLES.items():
        if base.setup(
            ctx, door, base.create_sql(door["name"], names[name], columns, "v2-cow", None)
        ):
            base.setup(ctx, door, f"INSERT INTO {names[name]} VALUES {', '.join(rows)}")
    for name, (columns, select) in INSERT_SELECT.items():
        if base.setup(
            ctx, door, base.create_sql(door["name"], names[name], columns, "v2-cow", None)
        ):
            base.setup(ctx, door, f"INSERT INTO {names[name]} {select}")


def run_sql_cells(ctx: dict[str, Any], door_name: str, door: dict[str, Any], names: dict) -> None:
    route = "native" if door_name == "native" else "spark-sql"
    for group, site, template in SQL_CELLS:
        text = template.format(**names)
        ordered = "ORDER BY" in text and "OVER" not in text
        cell(ctx, route, site, group, text, attempt(ctx, raw_sql, door, text), ordered=ordered)
    for form in LOG_FORMS:
        text = f"SELECT {form} AS x"
        cell(
            ctx,
            route,
            f"LG-{form.split('(')[0]}|{form}",
            "log",
            text,
            attempt(ctx, raw_sql, door, text),
        )
    for site, template in SETOP_WRITES:
        x = names["x"] + "_" + site.replace("-", "_").lower()
        base.setup(ctx, door, base.create_sql(door["name"], x, "a INT", "v2-cow", None))
        text = template.format(x=x, **{k: v for k, v in names.items() if k != "x"})
        outcome, error, cols, rows, bits = attempt(ctx, raw_sql, door, text)
        if outcome == PA.ANSWERED:
            outcome, error, cols, rows, bits = attempt(ctx, raw_sql, door, f"SELECT a FROM {x}")
        cell(
            ctx,
            route,
            site,
            "q2-setop-write",
            text,
            (outcome, error, cols, rows, bits),
            family="dml",
        )


def run_merge(ctx: dict[str, Any], door: dict[str, Any]) -> None:
    catalog = base.SPARK_CATALOG if ctx["engine"] == "spark" else base.REPARK_CATALOG
    for kind in ("v2-cow", "v2-mor", "v3"):
        tag = kind.replace("-", "_")
        target = f"{catalog}.{NS}.a_merge_t_{tag}"
        source = f"{catalog}.{NS}.a_merge_s_{tag}"
        ok = base.setup(
            ctx,
            door,
            base.create_sql("spark-sql", target, "id BIGINT, d DOUBLE, flag STRING", kind, None),
        )
        ok = ok and base.setup(
            ctx, door, f"INSERT INTO {target} VALUES (1, {PZ}, 'a'), (2, {D.format('1.5')}, 'b')"
        )
        ok = ok and base.setup(
            ctx, door, base.create_sql("spark-sql", source, "d DOUBLE", kind, None)
        )
        ok = ok and base.setup(ctx, door, f"INSERT INTO {source} VALUES ({NZ}), ({PZ})")
        text = f"MERGE INTO {target} AS t USING {source} AS s ON t.d = s.d WHEN MATCHED THEN DELETE"
        result = attempt(ctx, raw_sql, door, text) if ok else (PA.SETUP_REFUSED, None, [], [], None)
        if result[0] == PA.ANSWERED:
            result = attempt(ctx, raw_sql, door, f"SELECT id FROM {target}")
        cell(
            ctx,
            "spark-sql",
            f"Q7-merge-delete-only|{kind}",
            "q7-merge-delete",
            text,
            result,
            family="dml",
            kind=kind,
        )


def frame_call(
    door: dict[str, Any], code: str, table: str
) -> tuple[list[str], list[list[Any]], list[list[Any]]]:
    scope = {"F": door["functions"], "w": door["session"].table(table)}
    return base.collect_raw(PA.to_arrow(eval(code, scope)))


def run_frames(ctx: dict[str, Any], door: dict[str, Any], names: dict[str, str]) -> None:
    for site, kind, code, by_id in FRAME_CELLS:
        if kind == "sql":
            text = code.format(**names)
            result = attempt(ctx, raw_sql, door, text)
        else:
            text = code
            result = attempt(ctx, frame_call, door, code, names["w"])
        if by_id and result[0] == PA.ANSWERED:
            order = sorted(range(len(result[3])), key=lambda i: result[3][i][0])
            result = (result[0], result[1], result[2], [result[3][i] for i in order], None)
        ordered = site.startswith("F6-orderby") or site == "F6-sort-string"
        cell(ctx, "spark-sql", site, "q6-fexpr-window", text, result, ordered=ordered)


def run_partition(ctx: dict[str, Any], door: dict[str, Any], cross: str | None) -> None:
    for prefix, catalog in (("own", "hd"),) + ((("repark-written", "rp"),) if cross else ()):
        for ty, partitioned in (("DOUBLE", True), ("FLOAT", True), ("DOUBLE", False)):
            name = f"{catalog}.ns.p_{ty.lower()}_{'part' if partitioned else 'flat'}"
            if prefix == "own":
                part = " PARTITIONED BY (d)" if partitioned else ""
                base.setup(
                    ctx, door, f"CREATE TABLE {name} (id BIGINT, d {ty}) USING iceberg{part}"
                )
                rows = ", ".join(r.replace("DOUBLE)", f"{ty})") for r in PART_ROWS)
                base.setup(ctx, door, f"INSERT INTO {name} VALUES {rows}")
            checks = (
                ("rows", f"SELECT id, d FROM {name}"),
                ("isnan", f"SELECT id FROM {name} WHERE isnan(d)"),
                ("eq-nan", f"SELECT id FROM {name} WHERE d = CAST('NaN' AS {ty})"),
            ) + (
                (("files", f"SELECT partition, record_count FROM {name}.files"),)
                if partitioned
                else ()
            )
            for check, text in checks:
                cid = f"NP-{check}|{ty.lower()}|{'part' if partitioned else 'flat'}|{prefix}"
                cell(
                    ctx, "spark-sql", cid, "nan-partition", text, attempt(ctx, raw_sql, door, text)
                )


def build(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
    hd = {
        "spark.sql.catalog.hd": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.hd.type": "hadoop",
        "spark.sql.catalog.hd.warehouse": str(warehouse / "hd"),
    }
    if arguments.engine == "repark":
        ctx = ITEM1.build_repark(warehouse, None, hd)
    else:
        ctx = base.build_spark(arguments, warehouse)
        session = ctx["doors"]["spark-sql"]["session"]
        for key, value in hd.items():
            session.conf.set(key, value)
        if arguments.cross:
            session.conf.set("spark.sql.catalog.rp", "org.apache.iceberg.spark.SparkCatalog")
            session.conf.set("spark.sql.catalog.rp.type", "hadoop")
            session.conf.set("spark.sql.catalog.rp.warehouse", arguments.cross)
    ctx["doors"]["spark-sql"]["sql"]("CREATE NAMESPACE IF NOT EXISTS hd.ns")
    return ctx


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("spark", "repark"), required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--ivy-jar")
    parser.add_argument("--cross")
    arguments = parser.parse_args()
    warehouse = Path(arguments.warehouse)
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True)
    ctx = build(arguments, warehouse)
    roots = [(str(warehouse), "<warehouse>"), (str(Path.home()), "<home>")]
    if arguments.cross:
        roots.insert(0, (arguments.cross, "<repark-warehouse>"))
    ctx["roots"] = roots
    ctx["header"]["part"] = "addendum"
    ctx["header"]["cross_read"] = bool(arguments.cross)
    ctx["records"] = [ctx["header"]]
    try:
        for door_name, door in ctx["doors"].items():
            names = names_for(ctx, door_name)
            make_tables(ctx, door, names)
            run_sql_cells(ctx, door_name, door, names)
            if door_name == "spark-sql":
                run_merge(ctx, door)
                run_frames(ctx, door, names)
                run_partition(ctx, door, arguments.cross)
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
