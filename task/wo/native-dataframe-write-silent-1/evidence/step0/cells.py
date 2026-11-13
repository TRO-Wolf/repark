from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

E_ROWS = [(7, 1), (8, 2)]
SRC_ROWS = [(9, 1), (10, 3)]
SRC2_ROWS = [(11, 1)]
REFUSE = "refuse"
APPENDS = ("sat_append", "save_name", "save_option", "ins_append", "v2_append")
CREATES_ON_NEW = ("sat_append", "save_name", "save_option")
VIEW_KINDS = ("view", "temp_view")


def p_is_one() -> Any:
    from repark.spark import functions

    return functions.expr("p = 1")


VERBS: dict[str, Callable[[Any, str], None]] = {
    "sat_append": lambda f, t: f.write.mode("append").saveAsTable(t),
    "sat_overwrite": lambda f, t: f.write.mode("overwrite").saveAsTable(t),
    "sat_error": lambda f, t: f.write.mode("error").saveAsTable(t),
    "sat_errorifexists": lambda f, t: f.write.mode("errorifexists").saveAsTable(t),
    "sat_ignore": lambda f, t: f.write.mode("ignore").saveAsTable(t),
    "ins_append": lambda f, t: f.write.insertInto(t),
    "ins_overwrite": lambda f, t: f.write.insertInto(t, overwrite=True),
    "v2_append": lambda f, t: f.writeTo(t).append(),
    "v2_overwrite": lambda f, t: f.writeTo(t).overwrite(p_is_one()),
    "v2_overwrite_partitions": lambda f, t: f.writeTo(t).overwritePartitions(),
    "v2_create": lambda f, t: f.writeTo(t).create(),
    "v2_create_or_replace": lambda f, t: f.writeTo(t).createOrReplace(),
    "v2_replace": lambda f, t: f.writeTo(t).replace(),
    "save_name": lambda f, t: f.write.format("iceberg").mode("append").save(t),
    "save_option": lambda f, t: f.write.format("iceberg").mode("append").option("path", t).save(),
}
PATH_VERBS: dict[str, Callable[[Any, str], None]] = {
    "save_path": lambda f, t: f.write.format("iceberg").mode("append").save(t),
    "parquet_append": lambda f, t: f.write.mode("append").parquet(t),
    "parquet_overwrite": lambda f, t: f.write.mode("overwrite").parquet(t),
    "csv_append": lambda f, t: f.write.mode("append").option("header", True).csv(t),
    "csv_overwrite": lambda f, t: f.write.mode("overwrite").option("header", True).csv(t),
    "json_append": lambda f, t: f.write.mode("append").json(t),
    "json_overwrite": lambda f, t: f.write.mode("overwrite").json(t),
    "text_append": lambda f, t: (
        f.selectExpr("CAST(id AS STRING) AS value").write.mode("append").text(t)
    ),
    "text_overwrite": lambda f, t: (
        f.selectExpr("CAST(id AS STRING) AS value").write.mode("overwrite").text(t)
    ),
}
PATH_KINDS = ("path_new", "path_existing")

NATIVE_TARGETS = (
    "ice_existing",
    "hd_existing",
    "ice_new",
    "hd_new",
    "ice_memory_schema",
    "memory_qualified",
    "memory_bare",
    "view",
    "temp_view",
)
SPARK_TARGETS = ("hd_existing", "hd_partitioned", "hd_new", "temp_view")
FRAMES = ("sql", "filter", "select", "union")
POOL_SIZE = 15 * 4
RETURNED: list[Any] = []


def install_collecting_wrapper() -> None:
    from repark import _native

    original = _native.session_sql_with_write_options

    def collecting(*args: Any, **kwargs: Any) -> Any:
        frame = original(*args, **kwargs)
        RETURNED.append(frame.count())
        return frame

    _native.session_sql_with_write_options = collecting


def hadoop_config(root: Path) -> dict[str, str]:
    return {
        "spark.sql.catalog.hd": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.hd.type": "hadoop",
        "spark.sql.catalog.hd.warehouse": str(root / "hd"),
    }


def metadata_order(path: Path) -> tuple[int, str]:
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def table_files(table_dir: Path) -> dict[str, Any]:
    metadata = table_dir / "metadata"
    documents = sorted(metadata.glob("*.metadata.json"), key=metadata_order)
    snapshots = None
    if documents:
        document = json.loads(documents[-1].read_text(encoding="utf-8"))
        snapshots = len(document.get("snapshots") or [])
    data = [p for p in (table_dir / "data").rglob("*") if p.is_file()]
    meta = [p for p in metadata.rglob("*") if p.is_file()]
    return {"snapshots": snapshots, "data_files": len(data), "metadata_files": len(meta)}


def path_format(verb: str) -> str:
    head = verb.split("_")[0]
    return "iceberg" if head == "save" else head


def path_rows(path: Path, fmt: str) -> list[list[int]] | None:
    if not path.exists():
        return None
    import pyarrow.csv as pcsv
    import pyarrow.json as pjson
    import pyarrow.parquet as pq

    rows: list[list[int]] = []
    for file in sorted(p for p in path.rglob("*") if p.is_file() and p.stat().st_size > 0):
        if file.name.startswith(".") or file.name.startswith("_"):
            continue
        if fmt == "text":
            lines = file.read_text(encoding="utf-8").splitlines()
            rows.extend([int(line)] for line in lines if line.strip())
            continue
        if fmt == "csv":
            table = pcsv.read_csv(file)
        elif fmt == "json":
            table = pjson.read_json(file)
        else:
            table = pq.read_table(file)
        rows.extend([row["id"], row["p"]] for row in table.to_pylist())
    return sorted(rows)


def seed_path(path: Path, fmt: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.mkdir(parents=True)
    if fmt == "text":
        (path / "part-seed.txt").write_text("".join(f"{a}\n" for a, _ in E_ROWS))
    elif fmt == "csv":
        body = "".join(f"{a},{b}\n" for a, b in E_ROWS)
        (path / "part-seed.csv").write_text("id,p\n" + body)
    elif fmt == "json":
        body = "".join(json.dumps({"id": a, "p": b}) + "\n" for a, b in E_ROWS)
        (path / "part-seed.json").write_text(body)
    else:
        ids = pa.array([a for a, _ in E_ROWS], pa.int32())
        parts = pa.array([b for _, b in E_ROWS], pa.int32())
        pq.write_table(pa.table({"id": ids, "p": parts}), path / "part-seed.parquet")


def multiset_minus(left: list[list[int]], right: list[list[int]]) -> list[list[int]]:
    remaining = Counter(map(tuple, left))
    remaining.subtract(Counter(map(tuple, right)))
    return sorted([list(key) for key, count in remaining.items() for _ in range(max(count, 0))])


def expected_rows(
    verb: str, kind: str, before: list[list[int]] | None, frame: list[list[int]]
) -> Any:
    if kind in PATH_KINDS:
        if verb == "save_path":
            return REFUSE
        if verb.endswith("_append"):
            return sorted((before or []) + frame)
        return sorted(frame)
    exists = before is not None
    current = before or []
    partitioned = kind == "hd_partitioned"
    if verb in APPENDS:
        if exists:
            return sorted(current + frame)
        return sorted(frame) if verb in CREATES_ON_NEW else REFUSE
    if verb in ("sat_overwrite", "v2_create_or_replace"):
        return sorted(frame)
    if verb in ("sat_error", "sat_errorifexists", "v2_create"):
        return REFUSE if exists else sorted(frame)
    if verb == "sat_ignore":
        return current if exists else sorted(frame)
    if not exists:
        return REFUSE
    if verb in ("ins_overwrite", "v2_replace"):
        return sorted(frame)
    if verb == "v2_overwrite":
        return sorted([row for row in current if row[1] != 1] + frame)
    if verb == "v2_overwrite_partitions":
        if not partitioned:
            return sorted(frame)
        written = {row[1] for row in frame}
        return sorted([row for row in current if row[1] not in written] + frame)
    raise ValueError(verb)


def verdict(outcome: dict[str, Any], before: Any, after: Any, expected: Any) -> str:
    before_rows = before.get("rows")
    after_rows = after.get("rows")
    if outcome["kind"] == "refused":
        if after_rows != before_rows or after.get("snapshots") != before.get("snapshots"):
            return "refused-with-effect"
        return "refused" if expected == REFUSE else "refused-model-stores"
    if expected != REFUSE and after_rows == expected:
        return "stores"
    if (after_rows or []) == (before_rows or []):
        if expected == REFUSE:
            return "returned-unchanged-model-refuses"
        return "SILENT"
    if multiset_minus(before_rows or [], after_rows or []) and not after_rows:
        return "S1-REMOVES-ROWS"
    return "WRONG"


class Door:
    def __init__(self, name: str, root: Path) -> None:
        self.name = name
        self.root = root
        self.slot = 0
        self.pool: list[str] = []
        self.spark: Any = None
        self.native: Any = None

    def open(self) -> None:
        from repark import ReparkSession

        builder = ReparkSession.builder.appName("native-dataframe-write-silent-step0")
        for key, value in hadoop_config(self.root).items():
            builder = builder.config(key, value)
        self.spark = builder.getOrCreate()
        self.spark.sql("CREATE NAMESPACE hd.ns").collect()
        seed = ", ".join(f"({a}, {b})" for a, b in E_ROWS)
        self.pool = [f"hd.ns.p{k}" for k in range(POOL_SIZE)]
        for target in self.pool:
            self.spark_run(
                f"CREATE TABLE {target} (id INT, p INT) USING iceberg PARTITIONED BY (p)"
            )
            self.spark_run(f"INSERT INTO {target} VALUES {seed}")
        if self.name == "native":
            import repark
            from repark._native import PyReparkSession

            self.native = PyReparkSession.native(config=hadoop_config(self.root))
            self.native.register_memory_catalog("ice", str(self.root / "ice"))
            self.native.create_namespace("ice", "ns", str(self.root / "ice" / "ns"))
            self.native.create_namespace("hd", "ns", None)
            repark._ANSI_NATIVE = self.native
            self.run("CREATE SCHEMA ice.ms")
        self.seed_sources()

    def run(self, sql: str) -> list[list[Any]]:
        if self.name == "native":
            import repark

            rows = repark.sql(sql).to_arrow().to_pylist()
        else:
            rows = self.spark.sql(sql).toArrow().to_pylist()
        return [list(row.values()) for row in rows]

    def spark_run(self, sql: str) -> None:
        self.spark.sql(sql).collect()

    def seed_sources(self) -> None:
        catalog = "ice" if self.name == "native" else "hd"
        for table, rows in (("src", SRC_ROWS), ("src2", SRC2_ROWS)):
            self.run(f"CREATE TABLE {catalog}.ns.{table} (id INT, p INT)")
            body = ", ".join(f"({a}, {b})" for a, b in rows)
            self.run(f"INSERT INTO {catalog}.ns.{table} VALUES {body}")

    def frames(self) -> dict[str, Any]:
        if self.name == "native":
            import repark

            base = repark.sql("SELECT id, p FROM ice.ns.src")
            other = repark.sql("SELECT id, p FROM ice.ns.src2")
        else:
            base = self.spark.sql("SELECT id, p FROM hd.ns.src")
            other = self.spark.sql("SELECT id, p FROM hd.ns.src2")
        return {
            "sql": (base, SRC_ROWS),
            "filter": (base.filter("id >= 9"), SRC_ROWS),
            "select": (base.select("id", "p"), SRC_ROWS),
            "union": (base.union(other), SRC_ROWS + SRC2_ROWS),
        }

    def make_target(self, kind: str, verb: str) -> tuple[str, Path | None]:
        self.slot += 1
        n = self.slot
        seed = ", ".join(f"({a}, {b})" for a, b in E_ROWS)
        if kind == "ice_existing":
            target = f"ice.ns.e{n}"
            self.run(f"CREATE TABLE {target} (id INT, p INT)")
            self.run(f"INSERT INTO {target} VALUES {seed}")
            return target, self.root / "ice" / "ns" / f"e{n}"
        if kind == "hd_existing":
            target = f"hd.ns.e{n}"
            self.run(f"CREATE TABLE {target} (id INT, p INT)")
            self.run(f"INSERT INTO {target} VALUES {seed}")
            return target, self.root / "hd" / "ns" / f"e{n}"
        if kind == "hd_partitioned":
            target = self.pool.pop(0)
            return target, self.root / "hd" / "ns" / target.rsplit(".", 1)[1]
        if kind == "ice_new":
            return f"ice.ns.n{n}", self.root / "ice" / "ns" / f"n{n}"
        if kind == "hd_new":
            return f"hd.ns.n{n}", self.root / "hd" / "ns" / f"n{n}"
        if kind == "ice_memory_schema":
            target = f"ice.ms.m{n}"
            self.run(f"CREATE TABLE {target} (id INT, p INT)")
            self.run(f"INSERT INTO {target} VALUES {seed}")
            return target, None
        if kind in ("memory_qualified", "memory_bare"):
            self.run(f"CREATE TABLE m{n} (id INT, p INT)")
            self.run(f"INSERT INTO m{n} VALUES {seed}")
            return (f"datafusion.public.m{n}" if kind == "memory_qualified" else f"m{n}"), None
        if kind == "view":
            self.run(f"CREATE VIEW v{n} AS SELECT * FROM (VALUES {seed}) AS x(id, p)")
            return f"v{n}", None
        if kind == "temp_view":
            if self.name == "native":
                import repark

                frame = repark.sql(f"SELECT * FROM (VALUES {seed}) AS x(id, p)")
            else:
                frame = self.spark.sql(f"SELECT * FROM VALUES {seed} AS x(id, p)")
            frame.createOrReplaceTempView(f"tv{n}")
            return f"tv{n}", None
        if kind in PATH_KINDS:
            path = self.root / "paths" / f"x{n}"
            if kind == "path_existing":
                seed_path(path, path_format(verb))
            return str(path), path
        raise ValueError(kind)

    def state(self, kind: str, verb: str, target: str, location: Path | None) -> dict[str, Any]:
        if kind in PATH_KINDS:
            assert location is not None
            files = [p for p in location.rglob("*") if p.is_file()] if location.exists() else []
            suffixes = sorted(p.suffix for p in files)
            rows = path_rows(location, path_format(verb))
            return {"rows": rows, "data_files": len(files), "file_suffixes": suffixes}
        try:
            rows: list[list[int]] | None = sorted(self.run(f"SELECT id, p FROM {target}"))
            read_error = None
        except Exception as error:
            rows, read_error = None, f"{type(error).__name__}: {error}"
        state: dict[str, Any] = {"rows": rows}
        if read_error is not None and not kind.endswith("_new"):
            state["read_error"] = read_error
        if location is not None:
            state.update(table_files(location))
        if kind in VIEW_KINDS:
            try:
                shadow = sorted(self.run(f"SELECT id, p FROM spark_catalog.default.{target}"))
            except Exception:
                shadow = None
            state["view_rows"] = rows
            state["rows"] = shadow
        return state

    def partition_reach(self) -> list[dict[str, Any]]:
        attempts = {
            "native_ddl_ice": "CREATE TABLE ice.ns.reach_p (id INT, p INT) PARTITIONED BY (p)",
            "native_ddl_hd": "CREATE TABLE hd.ns.reach_p (id INT, p INT) PARTITIONED BY (p)",
            "native_read_spark_door_table": f"SELECT id, p FROM {self.pool[0]}",
        }
        records = []
        for name, sql in attempts.items():
            try:
                rows = self.run(sql)
                outcome: dict[str, Any] = {"kind": "returned", "rows": rows}
            except Exception as error:
                outcome = {"kind": "refused", "class": type(error).__name__, "text": str(error)}
            records.append({"door": self.name, "reach": name, "sql": sql, "outcome": outcome})
        return records

    def workarounds(self) -> list[dict[str, Any]]:
        records = []
        statements = {
            "insert_into": "INSERT INTO {t} SELECT id, p FROM ice.ns.src",
            "insert_overwrite": "INSERT OVERWRITE {t} SELECT id, p FROM ice.ns.src",
        }
        kinds = ("ice_existing", "hd_existing", "ice_memory_schema", "memory_qualified")
        for kind in kinds:
            for name, template in statements.items():
                target, location = self.make_target(kind, name)
                records.append(self.workaround(kind, name, template, target, location))
        copy = "COPY (SELECT id, p FROM ice.ns.src) TO '{t}/part-0.parquet' STORED AS PARQUET"
        for kind in PATH_KINDS:
            target, location = self.make_target(kind, "parquet_copy")
            records.append(self.workaround(kind, "copy_parquet", copy, target, location))
        return records

    def workaround(
        self, kind: str, name: str, template: str, target: str, location: Path | None
    ) -> dict[str, Any]:
        verb = "parquet_workaround" if kind in PATH_KINDS else name
        before = self.state(kind, verb, target, location)
        sql = template.format(t=target)
        try:
            self.run(sql)
            outcome: dict[str, Any] = {"kind": "returned"}
        except Exception as error:
            outcome = {"kind": "refused", "class": type(error).__name__, "text": str(error)}
        return {
            "door": self.name,
            "workaround": name,
            "target_kind": kind,
            "sql": sql,
            "outcome": outcome,
            "before": before,
            "after": self.state(kind, verb, target, location),
        }

    def cell(self, kind: str, verb: str, call: Callable[[Any, str], None]) -> dict[str, Any]:
        frame_cells = {}
        for frame_name, (frame, frame_rows) in self.frames().items():
            RETURNED.clear()
            target, location = self.make_target(kind, verb)
            before = self.state(kind, verb, target, location)
            frame_list = [list(row) for row in frame_rows]
            if verb.startswith("text_"):
                frame_list = [[row[0]] for row in frame_rows]
            expected = expected_rows(verb, kind, before["rows"], frame_list)
            try:
                call(frame, target)
                outcome: dict[str, Any] = {"kind": "returned"}
            except Exception as error:
                outcome = {"kind": "refused", "class": type(error).__name__, "text": str(error)}
            after = self.state(kind, verb, target, location)
            record = {
                "door": self.name,
                "target_kind": kind,
                "verb": verb,
                "frame": frame_name,
                "target": target,
                "outcome": outcome,
                "before": before,
                "after": after,
                "expected": expected,
                "lost_rows": multiset_minus(before["rows"] or [], after["rows"] or []),
                "verdict": verdict(outcome, before, after, expected),
                "collected_returned_counts": list(RETURNED),
            }
            frame_cells[frame_name] = record
        return frame_cells


def main() -> None:
    door_name, root_arg, out_arg = sys.argv[1], sys.argv[2], sys.argv[3]
    root = Path(root_arg).resolve()
    door = Door(door_name, root)
    door.open()
    if os.environ.get("CELLS_COLLECT_RETURNED") == "1":
        install_collecting_wrapper()
    targets = NATIVE_TARGETS if door_name == "native" else SPARK_TARGETS
    records: list[dict[str, Any]] = []
    for kind in targets:
        for verb, call in VERBS.items():
            records.extend(door.cell(kind, verb, call).values())
    for kind in PATH_KINDS:
        for verb, call in PATH_VERBS.items():
            records.extend(door.cell(kind, verb, call).values())
    if door_name == "native":
        records.extend(door.partition_reach())
        records.extend(door.workarounds())
    text = "\n".join(json.dumps(record, sort_keys=True) for record in records) + "\n"
    Path(out_arg).write_text(text.replace(str(root), "<warehouse>"), encoding="utf-8")


if __name__ == "__main__":
    main()
