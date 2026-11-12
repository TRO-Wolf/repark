import argparse
import importlib.util
import shutil
import sys
from pathlib import Path
from typing import Any

DE = Path(__file__).resolve().parent.parent


def load_probe() -> Any:
    spec = importlib.util.spec_from_file_location("probe_sz", DE / "probe_signed_zero.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["probe_sz"] = module
    spec.loader.exec_module(module)
    return module


INT_WHERE = (
    ("1", "d = 0", "frame.filter(F.col('d') == 0).select('id')"),
    ("2", "d = -0", "frame.filter(F.col('d') == -0).select('id')"),
    ("3", "d <> 0", "frame.filter(F.col('d') != 0).select('id')"),
    ("4", "d <= -0", "frame.filter(F.col('d') <= -0).select('id')"),
    ("5", "d >= 0", "frame.filter(F.col('d') >= 0).select('id')"),
    ("6", "d < 0", "frame.filter(F.col('d') < 0).select('id')"),
    ("7", "d > -0", "frame.filter(F.col('d') > -0).select('id')"),
    ("8", "d IN (0, 1)", "frame.filter(F.col('d').isin(0, 1)).select('id')"),
    ("9", "d BETWEEN -1 AND 0", "frame.filter(F.col('d').between(-1, 0)).select('id')"),
    ("10", "0 = d", "frame.filter(F.lit(0) == F.col('d')).select('id')"),
    ("11", "tf = 0", "frame.filter(F.col('tf') == 0).select('id')"),
    ("12", "tf = -0", "frame.filter(F.col('tf') == -0).select('id')"),
    ("13", "d <=> 0", "frame.filter(F.col('d').eqNullSafe(0)).select('id')"),
)

CASE_WHERE = (
    ("1", "CASE d WHEN 0.0 THEN true ELSE false END"),
    ("2", "CASE d WHEN -0.0 THEN true ELSE false END"),
    ("3", "CASE d WHEN CAST('-0.0' AS DOUBLE) THEN true ELSE false END"),
    ("4", "CASE d WHEN 0 THEN true ELSE false END"),
    ("5", "CASE d WHEN 1.5 THEN false WHEN 0.0 THEN true ELSE false END"),
    ("6", "CASE tf WHEN 0.0 THEN true ELSE false END"),
    ("7", "CASE d WHEN CAST('NaN' AS DOUBLE) THEN true ELSE false END"),
)

CASE_SELECT = (
    ("p1", "SELECT id, CASE d WHEN 0.0 THEN 'z' ELSE 'n' END AS c FROM {t}"),
    (
        "p2",
        "SELECT id, CASE d WHEN CAST('-0.0' AS DOUBLE) THEN 'z' WHEN 1.5 THEN 'p' END AS c "
        "FROM {t}",
    ),
)

PREPARE = (
    ("1", "DOUBLE", "d = $1", "0.0", "d = ?", "CAST('0.0' AS DOUBLE)"),
    ("2", "DOUBLE", "d = $1", "-0.0", "d = ?", "CAST('-0.0' AS DOUBLE)"),
    ("3", "DOUBLE", "d >= $1", "0.0", "d >= ?", "CAST('0.0' AS DOUBLE)"),
    ("4", "DOUBLE", "d <= $1", "-0.0", "d <= ?", "CAST('-0.0' AS DOUBLE)"),
    ("5", "DOUBLE", "d <> $1", "0.0", "d <> ?", "CAST('0.0' AS DOUBLE)"),
    ("6", "DOUBLE", "d IN ($1, 1.0)", "-0.0", "d IN (?, 1.0)", "CAST('-0.0' AS DOUBLE)"),
    ("7", "INT", "d = $1", "0", "d = ?", "0"),
)


def build_cells(probe: Any) -> Any:
    cells = []
    for number, where, typed in INT_WHERE:
        cells.append(probe.read_cell(f"A1-{number}", "A1", where=where, typed=typed))
    for number, where in CASE_WHERE:
        cells.append(probe.read_cell(f"A3-{number}", "A3", where=where))
    for number, sql in CASE_SELECT:
        cells.append(probe.read_cell(f"A3-{number}", "A3", sql=sql, routes=["spark-sql", "native"]))
    return cells


def prepare_records(probe: Any, ctx: Any) -> Any:
    engine = ctx["engine"]
    layouts = ("view", "one", "split") if engine == "spark" else ("one", "split")
    for number, ptype, native_pred, value, spark_pred, spark_value in PREPARE:
        cid = f"A2-{number}"
        for layout in layouts:
            names = probe.read_names(engine, layout)
            table = names["t"]
            routes = ["spark-sql", "native"] if engine == "repark" else ["spark-sql"]
            for route in routes:
                door = probe.door_for(ctx, route)
                name = f"p_sz_{number}_{layout}"
                if engine == "spark":
                    steps = [f"SELECT id FROM {table} WHERE {spark_pred.replace('?', spark_value)}"]
                else:
                    steps = [
                        f"PREPARE {name}({ptype}) AS SELECT id FROM {table} WHERE {native_pred}",
                        f"EXECUTE {name}({value})",
                    ]
                errors = []
                outcome, error, cols, rows = None, None, [], []
                for step in steps:
                    outcome, error, cols, rows = probe.attempt(ctx, probe.run_sql, door, step)
                    if outcome != probe.ANSWERED:
                        errors.append({"text": step, "error": error})
                        break
                if engine == "repark":
                    probe.attempt(ctx, probe.run_sql, door, f"DEALLOCATE {name}")
                ctx["records"].append(
                    {
                        "record": "cell",
                        "family": "read",
                        "cid": cid,
                        "group": "A2",
                        "kind": "v2-cow",
                        "engine": engine,
                        "route": route,
                        "door": door["name"],
                        "layout": layout,
                        "truth_layout": "view",
                        "test_layout": "split",
                        "ordered": False,
                        "text": probe.scrub(" ; ".join(steps), ctx["roots"]),
                        "outcome": outcome,
                        "error": error,
                        "cols": cols,
                        "rows": rows if outcome == probe.ANSWERED else None,
                        "step_errors": errors,
                    }
                )


def main() -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("spark", "repark"), required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--ivy-jar")
    args = parser.parse_args()
    probe = load_probe()
    warehouse = Path(args.warehouse)
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True)
    ns = argparse.Namespace(part="add", label=args.label, ivy_jar=args.ivy_jar, wheel=None)
    if args.engine == "spark":
        ctx = probe.build_spark_context(ns, warehouse, spill=False)
    else:
        import repark

        ns.wheel = str(Path(repark.__file__).parent / "_native.abi3.so")
        ctx = probe.build_repark_context(ns, warehouse, facade_memory=None, native_memory=None)
    probe.finish_context(ctx, warehouse)
    door = ctx["doors"]["spark-sql"]
    if args.engine == "spark":
        probe.ensure_views(ctx, door)
    probe.ensure_read_tables(ctx, door)
    if args.engine == "repark":
        probe.ensure_read_tables(ctx, ctx["doors"]["native"])
    probe.run_read_cells(ctx, build_cells(probe))
    prepare_records(probe, ctx)
    try:
        probe.write_jsonl(Path(args.out), ctx["records"])
    finally:
        ctx["close"]()
    print("records", len(ctx["records"]))


if __name__ == "__main__":
    main()
