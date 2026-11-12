from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import struct
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import szb_cells as cells_mod

CLONE = Path(os.environ["SZB_CLONE"])
PART_A = CLONE / "task/wo/signed-zero-normalize-1/evidence/probe_signed_zero.py"
SPARK_CATALOG = "probe_ice"
REPARK_CATALOG = "ice"
NS = "szb"
SESSION_ZONE = "America/New_York"
NATIVE_CONFIG = {"repark.sql.allowCreateFormatVersion3": "true"}
KIND_PROPS = {
    "v2-cow": ("2", "copy-on-write"),
    "v2-mor": ("2", "merge-on-read"),
    "v3": ("3", "copy-on-write"),
}


def load_part_a() -> Any:
    spec = importlib.util.spec_from_file_location("probe_sz", PART_A)
    module = importlib.util.module_from_spec(spec)
    sys.modules["probe_sz"] = module
    spec.loader.exec_module(module)
    return module


PA = load_part_a()


def bits_of(value: Any) -> str | None:
    if isinstance(value, float):
        return struct.pack(">d", value).hex()
    return None


def collect_raw(table: Any) -> tuple[list[str], list[list[Any]], list[list[Any]]]:
    names = list(table.column_names)
    columns = [table.column(index).to_pylist() for index in range(table.num_columns)]
    rows = [list(values) for values in zip(*columns, strict=True)] if columns else []
    return names, PA.canon(rows), [[bits_of(v) for v in r] for r in rows]


def run_sql(door: dict[str, Any], text: str) -> tuple[list[str], list[list[Any]]]:
    return PA.collect(PA.to_arrow(door["sql"](text)))


def run_frame(door: dict[str, Any], code: str, names: dict[str, str]) -> tuple[list[str], list[list[Any]]]:
    session = door["session"]
    frames = {"t": session.table(names["t"])}
    if names.get("u"):
        frames["u"] = session.table(names["u"])
    scope = {"F": door["functions"], "W": door["window"], **frames}
    return PA.collect(PA.to_arrow(eval(code, scope)))


def record_setup(ctx: dict[str, Any], door: str, text: str, outcome: str, error: Any) -> None:
    ctx["records"].append(
        {
            "record": "setup",
            "engine": ctx["engine"],
            "door": door,
            "outcome": outcome,
            "error": error,
            "text": PA.scrub(text, ctx["roots"]),
        }
    )


def setup(ctx: dict[str, Any], door: dict[str, Any], text: str) -> bool:
    outcome, error, _, _ = PA.attempt(ctx, run_sql, door, text)
    record_setup(ctx, door["name"], text, outcome, error)
    return outcome == PA.ANSWERED


def create_sql(door: str, qualified: str, columns: str, kind: str, partition: str | None) -> str:
    if door == "native":
        return f"CREATE TABLE {qualified} ({columns})"
    version, mode = KIND_PROPS[kind]
    props = (
        f"'format-version' = '{version}', 'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}', 'write.merge.mode' = '{mode}'"
    )
    part = f" PARTITIONED BY ({partition})" if partition else ""
    return f"CREATE TABLE {qualified} ({columns}) USING iceberg{part} TBLPROPERTIES ({props})"


def seed_table(
    ctx: dict[str, Any], door: dict[str, Any], qualified: str, dataset: str, kind: str
) -> bool:
    columns = cells_mod.COLUMNS_T if dataset == "t" else cells_mod.COLUMNS_U
    if not setup(ctx, door, create_sql(door["name"], qualified, columns, kind, None)):
        return False
    rows = ", ".join(cells_mod.DATA[dataset])
    return setup(ctx, door, f"INSERT INTO {qualified} VALUES {rows}")


def seed_view(ctx: dict[str, Any], door: dict[str, Any], dataset: str) -> None:
    names = cells_mod.NAMES_T if dataset == "t" else cells_mod.NAMES_U
    rows = ", ".join(cells_mod.DATA[dataset])
    text = (
        f"CREATE OR REPLACE TEMPORARY VIEW tv_{dataset} AS SELECT * FROM VALUES {rows} "
        f"AS tv({', '.join(names)})"
    )
    setup(ctx, door, text)


def readback(ctx: dict[str, Any], door: dict[str, Any], qualified: str, tag: str, columns: str) -> None:
    try:
        names, rows, bits = collect_raw(PA.to_arrow(door["sql"](f"SELECT {columns} FROM {qualified}")))
        ctx["records"].append(
            {"record": "seed", "engine": ctx["engine"], "door": door["name"], "tag": tag,
             "cols": names, "rows": rows, "bits": bits}
        )
    except BaseException as error:
        ctx["records"].append(
            {"record": "seed", "engine": ctx["engine"], "door": door["name"], "tag": tag,
             "error": PA.excerpt(PA.scrub(str(error), ctx["roots"]))}
        )


def table_names(engine: str, door: str, layout: str) -> dict[str, str]:
    if layout == "view":
        return {dataset: f"tv_{dataset}" for dataset in ("t", "up", "un", "ub", "nn")}
    catalog = SPARK_CATALOG if engine == "spark" else REPARK_CATALOG
    tag = "n" if door == "native" else "s"
    return {dataset: f"{catalog}.{NS}.{dataset}_{tag}" for dataset in ("t", "up", "un", "ub", "nn")}


def ensure_read_tables(ctx: dict[str, Any], door: dict[str, Any]) -> None:
    names = table_names(ctx["engine"], door["name"], "one")
    for dataset in ("t", "up", "un", "ub"):
        seed_table(ctx, door, names[dataset], dataset, "v2-cow")
    readback(ctx, door, names["t"], f"t-{door['name']}", "id, d, f, m, e, fe, me")
    columns = "id BIGINT, d DOUBLE"
    if setup(ctx, door, create_sql(door["name"], names["nn"], columns, "v2-cow", None)):
        setup(ctx, door, f"INSERT INTO {names['nn']} VALUES {', '.join(cells_mod.NN_ROWS)}")
    readback(ctx, door, names["nn"], f"nn-{door['name']}", "id, d")


def ensure_views(ctx: dict[str, Any], door: dict[str, Any]) -> None:
    for dataset in ("t", "up", "un", "ub"):
        seed_view(ctx, door, dataset)
    setup(
        ctx,
        door,
        f"CREATE OR REPLACE TEMPORARY VIEW tv_nn AS SELECT * FROM VALUES {', '.join(cells_mod.NN_ROWS)} AS tv(id, d)",
    )
    readback(ctx, door, "tv_t", "t-view", "id, d, f, m, e, fe, me")
    readback(ctx, door, "tv_nn", "nn-view", "id, d")


def expand_reads() -> list[dict[str, Any]]:
    out = []
    for spec in cells_mod.read_specs():
        for type_name in spec["types"]:
            ty = cells_mod.TYPES[type_name]
            for build in spec["builds"]:
                cid = "|".join(x for x in (spec["site"], type_name, build) if x)
                out.append({**spec, "cid": cid, "type": type_name, "ty": ty, "build": build})
    for site, sql, ordered in cells_mod.NN_SPECS:
        out.append(
            {"site": site, "group": "nan-payload", "sql": sql, "native": sql, "fexpr": None,
             "typed": None, "ordered": ordered, "cid": f"{site}|double", "type": "double",
             "ty": cells_mod.TYPES["double"], "build": None, "pre": {}, "post": {},
             "seat": "NaN payload key"}
        )
    return out


def fill(text: str, item: dict[str, Any], names: dict[str, str]) -> str:
    out = text.replace("{k}", item["ty"]["k"]).replace("{k2}", item["ty"]["k2"]).replace("{ksql}", item["ty"]["sql"])
    out = out.replace("{t}", names["t"]).replace("{nn}", names["nn"])
    if item.get("build"):
        out = out.replace("{u}", names[item["build"]])
    return out


def side_statements(ctx: dict[str, Any], item: dict[str, Any], key: str, names: dict[str, str]) -> list[str]:
    stmts = list(item[key].get("both", ())) + list(item[key].get(ctx["engine"], ()))
    return [fill(s, item, names) for s in stmts]


def run_reads(ctx: dict[str, Any], routes: list[tuple[str, str, str]]) -> None:
    for item in expand_reads():
        for route, door_name, layout in routes:
            door = ctx["doors"][door_name]
            names = table_names(ctx["engine"], door_name, layout)
            if route == "spark-sql" or route == "native":
                template = item["sql"] if route == "spark-sql" else item["native"]
                if template is None:
                    continue
                text = fill(template, item, names)
                call, args = run_sql, (door, text)
            else:
                code = item["fexpr"] if route == "f-expr" else item["typed"]
                if code is None or layout == "view":
                    continue
                text = code.replace("{k}", item["ty"]["k"]).replace("{k2}", item["ty"]["k2"])
                frame_names = {"t": names["t"], "u": names[item["build"]] if item["build"] else None}
                call, args = run_frame, (door, text, frame_names)
            for stmt in side_statements(ctx, item, "pre", names):
                PA.attempt(ctx, run_sql, door, stmt)
            outcome, error, cols, rows = PA.attempt(ctx, call, *args)
            for stmt in side_statements(ctx, item, "post", names):
                PA.attempt(ctx, run_sql, door, stmt)
            ctx["records"].append(
                {
                    "record": "cell",
                    "engine": ctx["engine"],
                    "family": "read",
                    "group": item["group"],
                    "site": item["site"],
                    "seat": item["seat"],
                    "type": item["type"],
                    "build": item["build"],
                    "cid": item["cid"],
                    "route": route,
                    "layout": layout,
                    "test_layout": "one",
                    "truth_layout": "view",
                    "ordered": item["ordered"],
                    "text": PA.scrub(text, ctx["roots"]),
                    "outcome": outcome,
                    "error": error,
                    "cols": cols,
                    "rows": rows,
                }
            )


def run_dml(ctx: dict[str, Any], doors: list[str]) -> None:
    counter = 0
    for site, template, builds, verb in cells_mod.DML_SPECS:
        for type_name, ty in cells_mod.TYPES.items():
            for build in builds:
                for door_name in doors:
                    door = ctx["doors"][door_name]
                    kinds = cells_mod.DML_KINDS if door_name != "native" else ("v2-cow",)
                    for kind in kinds:
                        counter += 1
                        catalog = SPARK_CATALOG if ctx["engine"] == "spark" else REPARK_CATALOG
                        base = f"{catalog}.{NS}.w{counter}"
                        names = {"t": f"{base}_t", "nn": "", "x": f"{base}_x"}
                        ok = seed_table(ctx, door, names["t"], "t", kind)
                        if build:
                            names[build] = f"{base}_{build}"
                            ok = ok and seed_table(ctx, door, names[build], build, kind)
                        item = {"ty": ty, "build": build}
                        text = fill(template, item, names).replace("{x}", names["x"])
                        cid = "|".join(x for x in (site, type_name, build, kind) if x)
                        route = "spark-sql" if door_name == "spark-sql" else "native"
                        if not ok:
                            outcome, error, cols, rows = PA.SETUP_REFUSED, None, [], []
                        else:
                            naaj_off = site.endswith("-ieee") and ctx["engine"] == "spark"
                            if naaj_off:
                                run_sql(door, "SET spark.sql.optimizeNullAwareAntiJoin = false")
                            outcome, error, _, _ = PA.attempt(ctx, run_sql, door, text)
                            if naaj_off:
                                run_sql(door, "SET spark.sql.optimizeNullAwareAntiJoin = true")
                            cols, rows = [], []
                            if outcome == PA.ANSWERED:
                                state = (
                                    f"SELECT * FROM {names['x']}"
                                    if verb == "ctas"
                                    else f"SELECT id, {ty['k']}, flag FROM {names['t']}"
                                )
                                s_out, s_err, cols, rows = PA.attempt(ctx, run_sql, door, state)
                                if s_out != PA.ANSWERED:
                                    outcome, error = s_out, s_err
                        ctx["records"].append(
                            {
                                "record": "cell",
                                "engine": ctx["engine"],
                                "family": "dml",
                                "group": "write",
                                "site": site,
                                "seat": verb,
                                "type": type_name,
                                "build": build,
                                "kind": kind,
                                "cid": cid,
                                "route": route,
                                "layout": "one",
                                "test_layout": "one",
                                "truth_layout": "one",
                                "ordered": False,
                                "text": PA.scrub(text, ctx["roots"]),
                                "outcome": outcome,
                                "error": error,
                                "cols": cols,
                                "rows": rows,
                            }
                        )


def run_partitions(ctx: dict[str, Any]) -> None:
    door = ctx["doors"]["spark-sql"]
    catalog = SPARK_CATALOG if ctx["engine"] == "spark" else REPARK_CATALOG
    for type_name, ty in cells_mod.TYPES.items():
        qualified = f"{catalog}.{NS}.pt_{type_name}"
        ddl = create_sql("spark-sql", qualified, cells_mod.COLUMNS_T, "v2-cow", ty["k"])
        ok = setup(ctx, door, ddl) and setup(
            ctx, door, f"INSERT INTO {qualified} VALUES {', '.join(cells_mod.DATA['t'])}"
        )
        for site, template, seat in cells_mod.PART_SPECS:
            text = template.replace("{k}", ty["k"]).replace("{t}", qualified)
            if ok:
                outcome, error, cols, rows = PA.attempt(ctx, run_sql, door, text)
            else:
                outcome, error, cols, rows = PA.SETUP_REFUSED, None, [], []
            ctx["records"].append(
                {
                    "record": "cell", "engine": ctx["engine"], "family": "read",
                    "group": "partition", "site": site, "seat": seat, "type": type_name,
                    "build": None, "cid": f"{site}|{type_name}", "route": "spark-sql",
                    "layout": "one", "test_layout": "one", "truth_layout": "one",
                    "ordered": False, "text": PA.scrub(text, ctx["roots"]), "outcome": outcome,
                    "error": error, "cols": cols, "rows": rows,
                }
            )


def run_floatkeys(ctx: dict[str, Any]) -> None:
    door = ctx["doors"]["spark-sql"]
    catalog = SPARK_CATALOG if ctx["engine"] == "spark" else REPARK_CATALOG
    counter = 0
    for variant, (seed_t, seed_u) in cells_mod.FLOAT_SEED_VARIANTS.items():
        for shape in cells_mod.FLOAT_SHAPES:
            for ty in ("DOUBLE", "FLOAT"):
                counter += 1
                base = f"{catalog}.{NS}.fk{counter}"
                names = {"t": f"{base}_t", "u": f"{base}_u"}
                ok = True
                for key, seed in (("t", seed_t), ("u", seed_u)):
                    ddl = (
                        f"CREATE TABLE {names[key]} (id BIGINT NOT NULL, d {ty}, flag STRING) "
                        "USING iceberg TBLPROPERTIES ('format-version'='2')"
                    )
                    rows = ", ".join(r.replace("{ty}", ty) for r in seed)
                    ok = ok and setup(ctx, door, ddl) and setup(ctx, door, f"INSERT INTO {names[key]} VALUES {rows}")
                if ok and shape == "K1-group-by":
                    readback(ctx, door, names["t"], f"fk-{variant}-{ty}-t", "id, d")
                    readback(ctx, door, names["u"], f"fk-{variant}-{ty}-u", "id, d")
                text = cells_mod.FLOAT_SQL[shape].format(t=names["t"], u=names["u"])
                cid = f"float|{shape}|{ty.lower()}|{variant}"
                if not ok:
                    outcome, error, cols, rows = PA.SETUP_REFUSED, None, [], []
                else:
                    outcome, error, cols, rows = PA.attempt(ctx, run_sql, door, text)
                    if outcome == PA.ANSWERED and shape in cells_mod.FLOAT_WRITE_SHAPES:
                        outcome, error, cols, rows = PA.attempt(
                            ctx, run_sql, door, f"SELECT id, flag FROM {names['t']}"
                        )
                ctx["records"].append(
                    {
                        "record": "cell",
                        "engine": ctx["engine"],
                        "family": "dml" if shape in cells_mod.FLOAT_WRITE_SHAPES else "read",
                        "group": "floatkeys",
                        "site": shape,
                        "seat": variant,
                        "type": ty.lower(),
                        "build": None,
                        "cid": cid,
                        "route": "spark-sql",
                        "layout": "one",
                        "test_layout": "one",
                        "truth_layout": "one",
                        "ordered": False,
                        "text": PA.scrub(text, ctx["roots"]),
                        "outcome": outcome,
                        "error": error,
                        "cols": cols,
                        "rows": rows,
                    }
                )


def build_spark(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as spark_functions

    session = (
        SparkSession.builder.master("local[2]")
        .appName("szb-step0")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", SESSION_ZONE)
        .config("spark.local.dir", str(warehouse / "spark-local"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars", arguments.ivy_jar)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.warehouse", str(warehouse / "spark"))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    door = {"name": "spark-sql", "sql": session.sql, "session": session,
            "functions": spark_functions, "window": Window}
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.{NS}")
    jar = Path(arguments.ivy_jar)
    header = {
        "record": "leg", "engine": "spark", "spark_version": session.version,
        "jdk_version": session.sparkContext._jvm.System.getProperty("java.version"),
        "iceberg_jar": jar.name, "iceberg_jar_sha256": PA.sha256_of(jar),
        "python": sys.version.split()[0], "load1": os.getloadavg()[0],
        "master": "local[2]", "shuffle_partitions": 4,
    }
    return {"engine": "spark", "doors": {"spark-sql": door}, "header": header, "close": session.stop}


def build_repark(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
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
    native = PyReparkSession.native(config=NATIVE_CONFIG)
    native.register_memory_catalog(REPARK_CATALOG, str(native_root))
    native.create_namespace(REPARK_CATALOG, NS, str(native_root / NS))
    repark._ANSI_NATIVE = native
    facade = (
        ReparkSession.builder.appName("szb-step0")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", SESSION_ZONE)
        .getOrCreate()
    )
    facade.register_memory_catalog(REPARK_CATALOG, str(facade_root))
    facade.sql(f"CREATE NAMESPACE {REPARK_CATALOG}.{NS} LOCATION '{facade_root / NS}'")
    so = Path(repark.__file__).parent / "_native.abi3.so"
    head = subprocess.run(["git", "-C", str(CLONE), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(CLONE), "status", "--porcelain"], capture_output=True, text=True).stdout.strip()
    header = {
        "record": "leg", "engine": "repark", "repark_file": str(Path(repark.__file__).resolve().relative_to(CLONE.resolve())),
        "native_so_sha256": PA.sha256_of(so), "clone_head": head, "clone_dirty": bool(dirty),
        "repark_version": __import__("importlib.metadata").metadata.version("repark"),
        "pyarrow_version": pyarrow.__version__, "python": sys.version.split()[0],
        "load1": os.getloadavg()[0], "native_config": NATIVE_CONFIG,
    }
    doors = {
        "spark-sql": {"name": "spark-sql", "sql": facade.sql, "session": facade,
                      "functions": repark_functions, "window": Window},
        "native": {"name": "native", "sql": repark.sql, "session": None, "functions": None, "window": None},
    }
    return {"engine": "repark", "doors": doors, "header": header, "close": lambda: None}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("spark", "repark"), required=True)
    parser.add_argument("--part", choices=("reads", "dml", "floatkeys"), required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--ivy-jar")
    arguments = parser.parse_args()
    warehouse = Path(arguments.warehouse)
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True)
    ctx = build_spark(arguments, warehouse) if arguments.engine == "spark" else build_repark(arguments, warehouse)
    ctx["roots"] = [(str(warehouse), "<warehouse>"), (str(Path.home()), "<home>")]
    ctx["header"]["part"] = arguments.part
    ctx["records"] = [ctx["header"]]
    try:
        if arguments.part == "reads":
            if arguments.engine == "spark":
                door = ctx["doors"]["spark-sql"]
                ensure_views(ctx, door)
                ensure_read_tables(ctx, door)
                routes = [("spark-sql", "spark-sql", "view"), ("spark-sql", "spark-sql", "one"),
                          ("f-expr", "spark-sql", "one"), ("typed", "spark-sql", "one")]
            else:
                ensure_read_tables(ctx, ctx["doors"]["spark-sql"])
                ensure_read_tables(ctx, ctx["doors"]["native"])
                routes = [("spark-sql", "spark-sql", "one"), ("f-expr", "spark-sql", "one"),
                          ("typed", "spark-sql", "one"), ("native", "native", "one")]
            run_reads(ctx, routes)
        elif arguments.part == "dml":
            run_dml(ctx, ["spark-sql"] if arguments.engine == "spark" else ["spark-sql", "native"])
            run_partitions(ctx)
        else:
            run_floatkeys(ctx)
    finally:
        with Path(arguments.out).open("w", encoding="utf-8") as handle:
            for record in ctx["records"]:
                handle.write(json.dumps(record, sort_keys=True, default=str, ensure_ascii=False) + "\n")
        ctx["close"]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
