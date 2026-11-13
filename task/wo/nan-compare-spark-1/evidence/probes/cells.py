from __future__ import annotations

import argparse
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
import common as cm
import ops

ICEBERG = [
    f"{w}_{f}_{k}" for w in ("rb", "sp") for f in ("parquet", "orc", "avro") for k in ("l1", "l5")
]
PARTS = [f"{w}_{f}_part" for w in ("rb", "sp") for f in ("parquet", "orc", "avro")]
PART_LIKE = [*PARTS, "part_created"]
PARQUET = [f"pq_{w}_{k}" for w in ("pa", "rb", "sp") for k in ("l1", "l5")]
NATIVE_WRITTEN = ["nat_parquet_l5"]
WIDE_SOURCES = ["created", "computed", *ICEBERG, *PARQUET]
OPS: set[str] | None = None
EXPLAIN_OPS = {"f_gt_half", "f_gt_inf", "f_eq_nan", "f_gt_nan", "f_in_nan", "agg_max", "agg_min"}
D_COLS = ("dc", "ds", "dp", "dq")
F_COLS = ("fc", "fs", "fp", "fq")


def computed_sql() -> str:
    rows = []
    for row_id in cm.ALL_IDS:
        value = cm.CONTROLS.get(row_id)
        if row_id in cm.NAN_IDS or value is None:
            x = "CAST(NULL AS DOUBLE)"
        else:
            x = f"CAST('{cm.token(value).replace('inf', 'Infinity')}' AS DOUBLE)"
        rows.append(
            f"(CAST({row_id} AS BIGINT), {x}, CAST(-1.0 AS DOUBLE), CAST('Infinity' AS DOUBLE), "
            f"CAST('NaN' AS DOUBLE))"
        )
    base = f"(VALUES {', '.join(rows)}) AS b(id, x, m, i, n)"
    pick = "CASE WHEN id IN (2, 9) THEN {e} ELSE x END"
    inner = (
        f"SELECT id, {pick.format(e=ops.lit('NaN', 'DOUBLE'))} AS dc, "
        f"{pick.format(e='sqrt(m)')} AS ds, "
        f"{pick.format(e='i - i')} AS dp, {pick.format(e='-n')} AS dq FROM {base}"
    )
    floats = ", ".join(f"CAST(d{s} AS FLOAT) AS f{s}" for s in cm.SHAPES)
    return f"SELECT id, dc, ds, dp, dq, {floats} FROM ({inner}) w"


def created_fill_sql(raw: str) -> str:
    parts = []
    for column in cm.WIDE_COLUMNS:
        t = "DOUBLE" if column[0] == "d" else "FLOAT"
        parts.append(
            f"CASE id WHEN 3 THEN CAST('Infinity' AS {t}) WHEN 4 THEN CAST('-Infinity' AS {t}) "
            f"ELSE {column} END AS {column}"
        )
    return f"SELECT id, {', '.join(parts)} FROM {raw}"


def part_fill_sql(raw: str) -> str:
    fill = (
        "CASE id WHEN 5 THEN CAST('Infinity' AS DOUBLE) "
        "WHEN 6 THEN CAST('-Infinity' AS DOUBLE) ELSE {c} END"
    )
    return f"SELECT id, {fill.format(c='pk')} AS pk, {fill.format(c='v')} AS v FROM {raw}"


def part_created_rows() -> list[tuple[Any, ...]]:
    return [(r[0], None, None) if r[0] in (5, 6) else r for r in cm.part_rows()]


def created_rows() -> list[tuple[Any, ...]]:
    rows = cm.wide_rows(cm.ALL_IDS)
    return [tuple(None if r[0] in (3, 4) and i > 0 else v for i, v in enumerate(r)) for r in rows]


class Engine:
    def __init__(self, kind: str, warehouse: Path, root: Path, ansi: str, reg: Path) -> None:
        self.kind = kind
        self.root = root
        self.warehouse = warehouse
        self.reg = reg
        self.native: Any = None
        home = warehouse if kind == "spark" else reg
        home.mkdir(parents=True, exist_ok=True)
        catalog = {
            f"spark.sql.catalog.{cm.CAT}.type": "hadoop",
            f"spark.sql.catalog.{cm.CAT}.warehouse": str(home),
        }
        if kind == "spark":
            from pyspark.sql import SparkSession, Window, functions

            builder = (
                SparkSession.builder.master("local[2]")
                .appName("nan-compare-cells")
                .config("spark.jars", cm.JAR)
                .config("spark.ui.enabled", "false")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.shuffle.partitions", "4")
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config(f"spark.sql.catalog.{cm.CAT}", "org.apache.iceberg.spark.SparkCatalog")
            )
        else:
            import repark
            from repark import ReparkSession, Window, functions
            from repark._native import PyReparkSession

            builder = ReparkSession.builder.appName("nan-compare-cells")
            try:
                self.native = PyReparkSession.native(config={"spark.sql.ansi.enabled": ansi})
            except Exception:
                self.native = PyReparkSession.native()
            self.native.register_late_catalogs(catalog)
            repark._ANSI_NATIVE = self.native
            self.native_sql = repark.sql
        for key, value in catalog.items():
            builder = builder.config(key, value)
        self.session = builder.config("spark.sql.ansi.enabled", ansi).getOrCreate()
        if kind == "spark":
            self.session.sparkContext.setLogLevel("ERROR")
        else:
            self.session.sql(f"CREATE NAMESPACE IF NOT EXISTS {cm.CAT}.{cm.NS}").collect()
            cm.attempt(
                lambda: (
                    self.native_sql(f"CREATE NAMESPACE IF NOT EXISTS {cm.CAT}.{cm.NS}").collect(),
                    {},
                )[1]
            )
        self.F = functions
        self.W = Window

    def doors(self) -> list[str]:
        base = ["sql", "frame", "fexpr"]
        return [*base, "native"] if self.kind == "repark" else base

    def relation(self, source: str) -> str:
        if source in NATIVE_WRITTEN:
            return "nat.ns.t"
        if source in ICEBERG or source in PARTS:
            return f"{cm.CAT}.{cm.NS}.{source}"
        return f"src_{source}"

    def native_written(self) -> dict[str, Any]:
        home = self.reg / "nat"
        self.native.register_memory_catalog("nat", str(home))
        self.native.create_namespace("nat", "ns", str(home / "ns"))
        self.native_sql(f"CREATE TABLE nat.ns.t ({cm.WIDE_DDL})").collect()
        for index in range(len(cm.LAYOUTS["l5"])):
            frame = self.native.read_parquet(str(self.root / "pa_l5" / f"part-{index}.parquet"))
            self.native.create_or_replace_temp_view("nat_src", frame)
            self.native_sql("INSERT INTO nat.ns.t SELECT * FROM nat_src").collect()
        return {}

    def setup(self, source: str) -> dict[str, Any]:
        s = self.session
        name = self.relation(source)
        if source in NATIVE_WRITTEN:
            return {"frame": None, "native": cm.attempt(self.native_written)}
        if source == "created":
            raw = s.createDataFrame(created_rows(), cm.WIDE_DDL)
            raw.createOrReplaceTempView("src_created_raw")
            s.sql(created_fill_sql("src_created_raw")).createOrReplaceTempView(name)
        elif source == "computed":
            s.sql(computed_sql()).createOrReplaceTempView(name)
        elif source == "part_created":
            raw = s.createDataFrame(part_created_rows(), cm.PART_DDL)
            raw.createOrReplaceTempView("src_part_raw")
            s.sql(part_fill_sql("src_part_raw")).createOrReplaceTempView(name)
        elif source.startswith("pq_"):
            s.read.parquet(str(self.root / source[3:])).createOrReplaceTempView(name)
        elif self.kind == "repark":
            s.sql(self.register_text(source)).collect()
        out: dict[str, Any] = {"frame": s.table(name)}
        if self.native is not None:
            out["native"] = cm.attempt(lambda: self.native_setup(source, name))
        return out

    def register_text(self, source: str) -> str:
        folder = self.warehouse / cm.NS / source / "metadata"
        latest = max(folder.glob("v*.metadata.json"), key=lambda p: int(p.name[1:].split(".")[0]))
        return (
            f"CALL {cm.CAT}.system.register_table(table => '{cm.NS}.{source}', "
            f"metadata_file => '{latest}')"
        )

    def native_setup(self, source: str, name: str) -> dict[str, Any]:
        if source in ICEBERG or source in PARTS:
            self.native_sql(self.register_text(source)).collect()
        elif source == "created":
            self.native.register_arrow_stream_as_temp_view(name, cm.arrow_wide(cm.ALL_IDS))
        elif source == "part_created":
            self.native.register_arrow_stream_as_temp_view(name, cm.arrow_part())
        elif source == "computed":
            self.native_sql(f"SELECT count(*) FROM ({computed_sql()}) w").collect()
        elif source.startswith("pq_"):
            frame = self.native.read_parquet(str(self.root / source[3:]))
            self.native.create_or_replace_temp_view(name, frame)
        return {}

    def run_sql(self, door: str, text: str) -> Any:
        if door != "native":
            return self.session.sql(text)
        if "src_computed" in text:
            text = f"WITH src_computed AS ({computed_sql()}) {text}"
        return self.native_sql(native_dialect(text))

    def explain(self, door: str, text: str) -> list[str]:
        plan = cm.collect(self.run_sql(door, "EXPLAIN ANALYZE " + text))
        keep = []
        for row in plan["rows"]:
            for cell in row:
                for line in str(cell).splitlines():
                    if any(
                        k in line
                        for k in ("IcebergTableScan", "DataSourceExec", "PlaceholderRowExec")
                    ):
                        keep.append(line.strip()[:400])
        return keep[:4]


def native_dialect(text: str) -> str:
    text = text.replace("sort_array(collect_list(", "array_sort(array_agg(")
    text = text.replace("collect_list(", "array_agg(")
    text = text.replace("approx_count_distinct(", "approx_distinct(")
    return text.replace("), false)", "), 'DESC')")


def collect_any(frame: Any) -> dict[str, Any]:
    if not hasattr(frame, "toArrow") and hasattr(frame, "to_arrow"):
        return cm.collect(SimpleNamespace(toArrow=frame.to_arrow))
    return cm.collect(frame)


def sorted_rows(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("ok"):
        result["rows"] = sorted(result["rows"], key=lambda r: str(r))
    return result


def column_plan(source: str) -> list[tuple[str, str, list[tuple[str, str, str]]]]:
    if source in PART_LIKE:
        double = [o for o in ops.sql_ops("DOUBLE") if o[0] not in ops.NEEDS_KEY]
        return [("pk", "DOUBLE", double), ("v", "DOUBLE", double)]
    out = [(c, "DOUBLE", ops.sql_ops("DOUBLE")) for c in D_COLS]
    floats = [o for o in ops.sql_ops("FLOAT") if o[0] in ops.FLOAT_OPS]
    out += [(c, "FLOAT", floats) for c in F_COLS]
    return out


def run_source(engine: Engine, source: str, doors: list[str], sink: Any) -> None:
    handles = cm.attempt(lambda: {"h": engine.setup(source)})
    if not handles["ok"]:
        cm.dump(sink, {"record": "setup", "src": source, **handles})
        return
    frame = handles["h"]["frame"]
    native_ok = handles["h"].get("native", {"ok": False})
    if engine.native is not None and not native_ok.get("ok"):
        cm.dump(sink, {"record": "setup", "src": source, "door": "native", **native_ok})
    rel = engine.relation(source)
    for door in doors:
        if door == "native" and not native_ok.get("ok"):
            continue
        if frame is None and door != "native":
            continue
        for column, sql_type, op_list in column_plan(source):
            key = "dc" if sql_type == "DOUBLE" else "fc"
            frame_table = ops.frame_ops(engine.F, engine.W, sql_type.lower())
            for name, kind, frag in op_list:
                if OPS is not None and name not in OPS:
                    continue
                text_frag = frag.format(c=column, k=key, t=rel)
                base = {"record": "cell", "door": door, "src": source, "col": column, "op": name}
                if door in ("sql", "native"):
                    text = ops.sql_text(kind, text_frag, rel)
                    result = cm.attempt(lambda t=text, d=door: collect_any(engine.run_sql(d, t)))
                    if name == "group_by":
                        result = sorted_rows(result)
                    wants_plan = name in EXPLAIN_OPS and source.endswith(("l5", "part"))
                    if (door != "sql" or engine.kind == "repark") and wants_plan:
                        plan = cm.attempt(lambda t=text, d=door: {"plan": engine.explain(d, t)})
                        result["plan"] = plan.get("plan", plan.get("error"))
                    cm.dump(sink, {**base, **result})
                elif door == "frame":
                    action = frame_table.get(name)
                    if action is None:
                        continue
                    result = cm.attempt(
                        lambda a=action, c=column, k=key: cm.collect(a(frame, c, k))
                    )
                    cm.dump(sink, {**base, **result})
                else:
                    if name in ops.NEEDS_KEY and kind == "special":
                        continue
                    builder = ops.fexpr_frame(engine.F, kind, text_frag)
                    if builder is None:
                        continue
                    result = cm.attempt(lambda b=builder: cm.collect(b(frame)))
                    cm.dump(sink, {**base, **result})
        if door in ("sql", "native") and source not in PART_LIKE and (OPS is None or "mix" in OPS):
            for name, text in ops.mix_ops(rel, D_COLS):
                result = cm.attempt(lambda t=text, d=door: collect_any(engine.run_sql(d, t)))
                if name == "mix_group":
                    result = sorted_rows(result)
                cm.dump(
                    sink,
                    {
                        "record": "cell",
                        "door": door,
                        "src": source,
                        "col": "mix",
                        "op": name,
                        **result,
                    },
                )


def run_producers(engine: Engine, sink: Any) -> None:
    doors = ["sql", "native"] if engine.kind == "repark" else ["sql"]
    for door in doors:
        for name, folded, column in ops.PRODUCERS:
            for form, text in (
                ("folded", f"SELECT {folded} AS r"),
                ("column", f"SELECT {column} AS r FROM {ops.PRODUCER_SRC}"),
            ):
                result = cm.attempt(lambda t=text, d=door: collect_any(engine.run_sql(d, t)))
                cm.dump(
                    sink, {"record": "producer", "door": door, "op": name, "form": form, **result}
                )
            if engine.kind == "spark":
                text = (
                    "SELECT java_method('java.lang.Double', 'doubleToRawLongBits', "
                    f"{column}) AS raw FROM {ops.PRODUCER_SRC}"
                )
                result = cm.attempt(lambda t=text: cm.collect(engine.session.sql(t)))
                if result.get("ok"):
                    result["rows"] = [
                        [f"{int(r[0]) & 0xFFFFFFFFFFFFFFFF:016x}" if r[0] is not None else None]
                        for r in result["rows"]
                    ]
                cm.dump(
                    sink,
                    {"record": "producer", "door": door, "op": name, "form": "jvm_raw", **result},
                )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("engine", choices=("spark", "repark"))
    parser.add_argument("warehouse")
    parser.add_argument("parquet_root")
    parser.add_argument("out")
    parser.add_argument("--ansi", default="true")
    parser.add_argument("--sources", default="all")
    parser.add_argument("--reg", default="")
    parser.add_argument("--ops", default="")
    args = parser.parse_args()
    global OPS
    OPS = set(args.ops.split(",")) if args.ops else None
    reg = Path(args.reg) if args.reg else Path(args.out).with_suffix(".reg")
    engine = Engine(args.engine, Path(args.warehouse), Path(args.parquet_root), args.ansi, reg)
    every = [*WIDE_SOURCES, *PARTS, *(NATIVE_WRITTEN if args.engine == "repark" else [])]
    sources = every if args.sources == "all" else args.sources.split(",")
    with Path(args.out).open("w") as sink:
        cm.dump(sink, {**cm.header(args.engine), "ansi": args.ansi, "sources": sources})
        if OPS is None:
            run_producers(engine, sink)
        for source in sources:
            run_source(engine, source, engine.doors(), sink)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
