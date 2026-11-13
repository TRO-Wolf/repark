from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
import cells
import common as cm
import write_sources as ws

NAN = float("nan")
INF = math.inf
GRP_ROWS = [(1, NAN), (1, 0.0), (2, 5.0), (2, NAN), (3, -INF), (4, INF), (5, NAN), (6, 1.0)]
GRP_DDL = "g INT, d DOUBLE, f FLOAT"
FORMATS = ("parquet", "orc", "avro")
META_FOR = ("base", "1325", "head3", "spark")
S_NAN = "-n"
C_NAN = "n"


def write_files(root: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    def put(folder: str, index: int, table: Any) -> None:
        target = root / folder
        target.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, target / f"part-{index:02d}.parquet")

    put(
        "pa_grp",
        0,
        pa.table(
            {
                "g": pa.array([r[0] for r in GRP_ROWS], pa.int32()),
                "d": pa.array([r[1] for r in GRP_ROWS], pa.float64()),
                "f": pa.array([r[1] for r in GRP_ROWS], pa.float32()),
            }
        ),
    )
    target = root / "pa_part"
    target.mkdir(parents=True, exist_ok=True)
    pq.write_table(cm.arrow_part(), target / "part-0.parquet")
    put("pa_const", 0, pa.table({"id": pa.array([20, 21], pa.int64()), "d": pa.array([NAN, 1.0])}))
    for index in range(48):
        values = [NAN, 1.0] if index % 3 == 2 else [2.0]
        ids = list(range(index * 10, index * 10 + len(values)))
        put("pa_const48", index, pa.table({"id": pa.array(ids, pa.int64()), "d": pa.array(values)}))
        values = [NAN, 0.5, 1.0] if index % 3 == 2 else [5.0, 6.0]
        ids = list(range(index * 10, index * 10 + len(values)))
        put("pa_dyn", index, pa.table({"id": pa.array(ids, pa.int64()), "d": pa.array(values)}))


def write_tables(engine: str, warehouse: Path, root: Path, log: Any) -> None:
    session = ws.session_for(engine, warehouse)
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {cm.CAT}.{cm.NS}").collect()
    tag = "sp" if engine == "spark" else "rb"
    for fmt in FORMATS:
        name = f"{cm.CAT}.{cm.NS}.{tag}_grp_{fmt}"
        session.sql(f"CREATE TABLE {name} ({GRP_DDL}) USING iceberg {ws.props(fmt)}").collect()
        session.read.parquet(str(root / "pa_grp" / "part-00.parquet")).coalesce(1).writeTo(
            name
        ).append()
        cm.dump(log, {"record": "table", "name": name})
    if engine == "spark":
        return
    for owner in META_FOR:
        name = f"{cm.CAT}.{cm.NS}.rb_meta_{owner.replace('1325', 'n1325')}"
        session.sql(
            f"CREATE TABLE {name} ({cm.PART_DDL}) USING iceberg PARTITIONED BY (pk) "
            f"{ws.props('parquet')}"
        ).collect()
        session.read.parquet(str(root / "pa_part" / "part-0.parquet")).coalesce(1).writeTo(
            name
        ).append()
        cm.dump(log, {"record": "table", "name": name})


def mem_cells() -> list[tuple[str, str]]:
    src = (
        "(VALUES (1, CAST('NaN' AS DOUBLE)), (2, CAST(1.0 AS DOUBLE)), "
        "(3, CAST(2.0 AS DOUBLE))) AS v(id, n)"
    )
    grp = (
        "(VALUES (1, CAST('NaN' AS DOUBLE)), (1, CAST(0.0 AS DOUBLE)), (2, CAST(5.0 AS DOUBLE)), "
        "(2, CAST('NaN' AS DOUBLE)), (3, CAST('-Infinity' AS DOUBLE)), "
        "(4, CAST('Infinity' AS DOUBLE)), "
        "(5, CAST('NaN' AS DOUBLE)), (6, CAST(1.0 AS DOUBLE))) AS v(g, d)"
    )
    one = "(VALUES (CAST('NaN' AS DOUBLE), CAST(-1.0 AS DOUBLE))) AS v(n, m)"
    out = [
        ("grp_mem_double", f"SELECT g, max(d), min(d) FROM {grp} GROUP BY g ORDER BY g"),
        (
            "grp_mem_float",
            "SELECT g, max(CAST(d AS FLOAT)), min(CAST(d AS FLOAT)) "
            f"FROM {grp} GROUP BY g ORDER BY g",
        ),
    ]
    for label, x in (("sign", S_NAN), ("canon", C_NAN)):
        out += [
            (f"median_{label}", f"SELECT median({x}) FROM {src}"),
            (
                f"pct_{label}",
                f"SELECT percentile_approx({x}, 0.0), percentile_approx({x}, 0.5), "
                f"percentile_approx({x}, 1.0) FROM {src}",
            ),
            (f"nullif_{label}", f"SELECT nullif({x}, CAST('NaN' AS DOUBLE)) IS NULL FROM {one}"),
            (
                f"array_contains_{label}",
                f"SELECT array_contains(array({x}), CAST('NaN' AS DOUBLE)) FROM {one}",
            ),
            (
                f"array_position_{label}",
                f"SELECT array_position(array(CAST(1.0 AS DOUBLE), {x}), "
                f"CAST('NaN' AS DOUBLE)) FROM {one}",
            ),
            (f"array_distinct_{label}", f"SELECT size(array_distinct(array(n, {x}))) FROM {one}"),
            (f"arrays_overlap_{label}", f"SELECT arrays_overlap(array({x}), array(n)) FROM {one}"),
            (
                f"array_remove_{label}",
                f"SELECT size(array_remove(array({x}, CAST(1.0 AS DOUBLE)), "
                f"CAST('NaN' AS DOUBLE))) FROM {one}",
            ),
            (f"array_union_{label}", f"SELECT size(array_union(array(n), array({x}))) FROM {one}"),
            (
                f"array_intersect_{label}",
                f"SELECT size(array_intersect(array(n), array({x}))) FROM {one}",
            ),
            (
                f"array_except_{label}",
                f"SELECT size(array_except(array(n), array({x}))) FROM {one}",
            ),
            (
                f"struct_max_{label}",
                f"SELECT max(s).a FROM (SELECT named_struct('a', {x}) AS s FROM {one} "
                f"UNION ALL SELECT named_struct('a', m) FROM {one}) t",
            ),
            (
                f"struct_order_{label}",
                f"SELECT s.a FROM (SELECT named_struct('a', {x}) AS s FROM {one} UNION ALL "
                f"SELECT named_struct('a', m) FROM {one}) t ORDER BY s",
            ),
            (
                f"struct_distinct_{label}",
                f"SELECT count(*) FROM (SELECT DISTINCT named_struct('a', y) AS s FROM "
                f"(SELECT n AS y FROM {one} UNION ALL SELECT {x} AS y FROM {one}) u) t",
            ),
        ]
    return out


class Probe:
    def __init__(self, kind: str, warehouse: Path, root: Path, reg: Path, build: str) -> None:
        self.engine = cells.Engine(kind, warehouse, root, "true", reg)
        self.kind = kind
        self.root = root
        self.build = build

    def view(self, folder: str) -> str:
        name = f"src_{folder}"
        self.engine.session.read.parquet(str(self.root / folder)).createOrReplaceTempView(name)
        if self.engine.native is not None:
            frame = self.engine.native.read_parquet(str(self.root / folder))
            self.engine.native.create_or_replace_temp_view(name, frame)
        return name

    def run(self, door: str, text: str) -> dict[str, Any]:
        return cm.attempt(lambda: cells.collect_any(self.engine.run_sql(door, text)))

    def doors(self) -> list[str]:
        return ["sql", "native"] if self.kind == "repark" else ["sql"]

    def table(self, name: str) -> str:
        if self.kind == "repark":
            self.engine.session.sql(self.engine.register_text(name)).collect()
        return f"{cm.CAT}.{cm.NS}.{name}"


def run_cells(probe: Probe, sink: Any) -> None:
    def emit(
        group: str, cell: str, door: str, text: str, extra: dict[str, Any] | None = None
    ) -> None:
        result = probe.run(door, text)
        cm.dump(
            sink,
            {
                "record": "cell",
                "group": group,
                "cell": cell,
                "door": door,
                **result,
                **(extra or {}),
            },
        )

    for door in probe.doors():
        for cell, text in mem_cells():
            emit("memory", cell, door, text)
    for writer in ("rb", "sp"):
        for fmt in FORMATS:
            name = f"{writer}_grp_{fmt}"
            rel = cm.attempt(lambda n=name: {"rel": probe.table(n)})
            if not rel["ok"]:
                cm.dump(sink, {"record": "setup", "cell": name, **rel})
                continue
            text = (
                f"SELECT g, max(d), min(d), max(f), min(f) FROM {rel['rel']} GROUP BY g ORDER BY g"
            )
            emit("grouped-iceberg", name, "sql", text)
            emit(
                "grouped-iceberg",
                f"{name}_ungrouped",
                "sql",
                f"SELECT max(d), min(d) FROM {rel['rel']}",
            )
    const = probe.view("pa_const")
    const48 = probe.view("pa_const48")
    dyn = probe.view("pa_dyn")
    trip = probe.root / f"rt_{probe.build}"
    if probe.kind == "repark":
        frame = probe.engine.session.read.parquet(str(probe.root / "pa_const"))
        cm.attempt(lambda: (frame.coalesce(1).write.mode("overwrite").parquet(str(trip)), {})[1])
    rt_views = {}
    for build in ("base", "1325", "head3"):
        if (probe.root / f"rt_{build}").exists():
            rt_views[build] = probe.view(f"rt_{build}")
    for door in probe.doors():
        emit("constant", "pa_const_rows", door, f"SELECT id, d FROM {const} ORDER BY id")
        emit("constant", "pa_const_max", door, f"SELECT max(d) FROM {const}")
        emit("constant", "pa_const_isnan", door, f"SELECT count(*) FROM {const} WHERE isnan(d)")
        emit("constant", "pa_const48_isnan", door, f"SELECT count(*) FROM {const48} WHERE isnan(d)")
        emit(
            "constant",
            "pa_const48_eq_nan",
            door,
            f"SELECT count(*) FROM {const48} WHERE d = CAST('NaN' AS DOUBLE)",
        )
        emit("constant", "pa_const48_max", door, f"SELECT max(d) FROM {const48}")
        for build, name in rt_views.items():
            emit(
                "constant", f"roundtrip_{build}_rows", door, f"SELECT id, d FROM {name} ORDER BY id"
            )
        for state in ("on", "off"):
            if state == "off":
                probe.run(door, "SET datafusion.optimizer.enable_dynamic_filter_pushdown = false")
            emit(
                "dynamic",
                f"topk_desc_{state}",
                door,
                f"SELECT d FROM {dyn} ORDER BY d DESC LIMIT 1",
            )
            emit("dynamic", f"agg_max_{state}", door, f"SELECT max(d), min(d) FROM {dyn}")
            if probe.kind == "repark" and state == "on":
                plan = cm.attempt(
                    lambda d=door: {
                        "plan": probe.engine.explain(
                            d, f"SELECT d FROM {dyn} ORDER BY d DESC LIMIT 1"
                        )
                    }
                )
                cm.dump(sink, {"record": "plan", "cell": "topk_desc", "door": door, **plan})
        probe.run(door, "SET datafusion.optimizer.enable_dynamic_filter_pushdown = true")
    owner = "n1325" if probe.build == "1325" else probe.build
    name = f"rb_meta_{owner}"
    rel = cm.attempt(lambda: {"rel": probe.table(name)})
    if rel["ok"]:
        emit("meta-delete", "before", "sql", f"SELECT id FROM {rel['rel']} ORDER BY id")
        emit("meta-delete", "delete", "sql", f"DELETE FROM {rel['rel']} WHERE pk < 100")
        emit("meta-delete", "after", "sql", f"SELECT id FROM {rel['rel']} ORDER BY id")
    else:
        cm.dump(sink, {"record": "setup", "cell": name, **rel})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("write", "cells"))
    parser.add_argument("engine", choices=("spark", "repark"))
    parser.add_argument("warehouse")
    parser.add_argument("parquet_root")
    parser.add_argument("out")
    parser.add_argument("--build", default="spark")
    parser.add_argument("--reg", default="")
    args = parser.parse_args()
    warehouse, root, out = Path(args.warehouse), Path(args.parquet_root), Path(args.out)
    with out.open("w") as sink:
        cm.dump(sink, {**cm.header(args.engine), "mode": args.mode, "build": args.build})
        if args.mode == "write":
            if args.engine == "repark":
                write_files(root)
            write_tables(args.engine, warehouse, root, sink)
            return 0
        reg = Path(args.reg) if args.reg else out.with_suffix(".reg")
        run_cells(Probe(args.engine, warehouse, root, reg, args.build), sink)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
