from __future__ import annotations

import json
import re
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

E_ROWS = [(7, 1), (8, 2)]
SRC_ROWS = [(9, 1), (10, 3)]
SRC2_ROWS = [(11, 1)]
EXPECTED_FILE = Path(__file__).with_name("native_dataframe_write_silent_1_main.json")
STAGING = re.compile(r"repark-staging-[0-9a-f]{32}-")


def p_is_one() -> Any:
    from repark.spark import functions

    return functions.expr("p = 1")


TABLE_VERBS: dict[str, Callable[[Any, str], None]] = {
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
    "save_name_overwrite": lambda f, t: f.write.format("iceberg").mode("overwrite").save(t),
    "ins_partition_by": lambda f, t: f.write.partitionBy("p").insertInto(t),
    "v2_property_append": lambda f, t: f.writeTo(t).tableProperty("k", "v").append(),
    "v2_partitioned_append": lambda f, t: f.writeTo(t).partitionedBy("p").append(),
    "merge_insert_all": lambda f, t: f.mergeInto(t, "id").whenNotMatched().insertAll().merge(),
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
    "parquet_default": lambda f, t: f.write.parquet(t),
    "parquet_error": lambda f, t: f.write.mode("error").parquet(t),
    "parquet_errorifexists": lambda f, t: f.write.mode("errorifexists").parquet(t),
    "parquet_ignore": lambda f, t: f.write.mode("ignore").parquet(t),
    "csv_default": lambda f, t: f.write.option("header", True).csv(t),
    "json_default": lambda f, t: f.write.json(t),
    "parquet_partitioned_append": lambda f, t: f.write.partitionBy("p").mode("append").parquet(t),
    "parquet_partitioned_overwrite": lambda f, t: (
        f.write.partitionBy("p").mode("overwrite").parquet(t)
    ),
    "csv_partitioned_overwrite": lambda f, t: (
        f.write.partitionBy("p").mode("overwrite").option("header", True).csv(t)
    ),
    "json_partitioned_overwrite": lambda f, t: f.write.partitionBy("p").mode("overwrite").json(t),
    "parquet_format_save": lambda f, t: f.write.format("parquet").mode("overwrite").save(t),
    "csv_save_kwargs": lambda f, t: f.write.save(t, format="csv", mode="overwrite", header=True),
    "parquet_option_path_save": lambda f, t: (
        f.write.format("parquet").mode("overwrite").option("path", t).save()
    ),
}
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
PATH_KINDS = ("path_new", "path_existing")
FRAMES = ("sql", "filter", "select", "union", "cache", "local_checkpoint", "join")
OVERWRITE_VERBS = frozenset({"ins_overwrite", "save_name_overwrite"})
DYNAMIC_VERBS = frozenset({"v2_overwrite_partitions"})
CARD = "NATIVE-DATAFRAME-WRITE-SILENT-1"


def table_refusal(verb: str, target: str) -> str:
    lead = (
        "DataFrame.write and writeTo cannot write a repark.sql() frame into the table "
        f"{target}: the write would return without storing any rows, so nothing was written; "
    )
    if verb in DYNAMIC_VERBS:
        tail = (
            "write the frame from a ReparkSession, whose overwritePartitions replaces only the "
            "frame's partitions"
        )
    elif verb in OVERWRITE_VERBS:
        tail = (
            f"to replace an Iceberg table's rows, run INSERT OVERWRITE {target} SELECT ... "
            "through repark.sql(...) and collect the result; otherwise write the frame from a "
            "ReparkSession"
        )
    else:
        tail = (
            f"run INSERT INTO {target} SELECT ... through repark.sql(...) and collect the "
            "result, or write the frame from a ReparkSession"
        )
    return f"{lead}{tail} ({CARD})"


def path_refusal(path: str) -> str:
    return (
        f"DataFrame.write cannot write a repark.sql() frame to the path {path}: the write would "
        "return without storing any rows, so nothing was written; write the frame from a "
        f"ReparkSession ({CARD})"
    )


def hadoop_config(root: Path) -> dict[str, str]:
    return {
        "spark.sql.catalog.hd": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.hd.type": "hadoop",
        "spark.sql.catalog.hd.warehouse": str(root / "hd"),
    }


def path_format(verb: str) -> str:
    for fmt in ("csv", "json", "text"):
        if verb.startswith(fmt):
            return fmt
    return "parquet"


def path_rows(path: Path, fmt: str) -> list[list[int]] | None:
    if not path.exists():
        return None
    import pyarrow.csv as pcsv
    import pyarrow.json as pjson
    import pyarrow.parquet as pq

    rows: list[list[int]] = []
    for file in sorted(p for p in path.rglob("*") if p.is_file() and p.stat().st_size > 0):
        if file.name.startswith((".", "_")):
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
        partition = [part for part in file.parts if part.startswith("p=")]
        for row in table.to_pylist():
            rows.append([row["id"], row.get("p", int(partition[-1][2:]) if partition else None)])
    return sorted(rows)


def seed_path(path: Path, fmt: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.mkdir(parents=True)
    if fmt == "text":
        body = "".join(f"{a}\n" for a, _ in E_ROWS)
        (path / "part-seed.txt").write_text(body, encoding="utf-8")
    elif fmt == "csv":
        body = "id,p\n" + "".join(f"{a},{b}\n" for a, b in E_ROWS)
        (path / "part-seed.csv").write_text(body, encoding="utf-8")
    elif fmt == "json":
        body = "".join(json.dumps({"id": a, "p": b}) + "\n" for a, b in E_ROWS)
        (path / "part-seed.json").write_text(body, encoding="utf-8")
    else:
        ids = pa.array([a for a, _ in E_ROWS], pa.int32())
        parts = pa.array([b for _, b in E_ROWS], pa.int32())
        pq.write_table(pa.table({"id": ids, "p": parts}), path / "part-seed.parquet")


def metadata_state(table_dir: Path) -> dict[str, Any]:
    metadata = table_dir / "metadata"
    documents = sorted(
        metadata.glob("*.metadata.json"),
        key=lambda p: (int(m.group(1)) if (m := re.match(r"v?(\d+)", p.name)) else -1, p.name),
    )
    snapshots = None
    if documents:
        document = json.loads(documents[-1].read_text(encoding="utf-8"))
        snapshots = len(document.get("snapshots") or [])
    data = [p for p in (table_dir / "data").rglob("*") if p.is_file()]
    meta = [p for p in metadata.rglob("*") if p.is_file()]
    return {"snapshots": snapshots, "data_files": len(data), "metadata_files": len(meta)}


class Door:
    def __init__(self, name: str, root: Path) -> None:
        self.name = name
        self.root = root
        self.slot = 0
        self.pool: list[str] = []
        self.spark: Any = None
        self.native: Any = None
        self.returned = 0
        self.restore: Callable[[], None] = lambda: None

    def open(self) -> None:
        import repark
        from repark import ReparkSession
        from repark import _native as native_module
        from repark._native import PyReparkSession

        builder = ReparkSession.builder.appName("native-dataframe-write-silent-1")
        for key, value in hadoop_config(self.root).items():
            builder = builder.config(key, value)
        self.spark = builder.getOrCreate()
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS hd.ns").collect()
        seed = ", ".join(f"({a}, {b})" for a, b in E_ROWS)
        if self.name == "spark-door":
            self.pool = [f"hd.ns.p{k}" for k in range(len(TABLE_VERBS) * len(FRAMES))]
            for target in self.pool:
                self.spark.sql(
                    f"CREATE TABLE {target} (id INT, p INT) USING iceberg PARTITIONED BY (p)"
                ).collect()
                self.spark.sql(f"INSERT INTO {target} VALUES {seed}").collect()
        else:
            self.native = PyReparkSession.native(config=hadoop_config(self.root))
            self.native.register_memory_catalog("ice", str(self.root / "ice"))
            self.native.create_namespace("ice", "ns", str(self.root / "ice" / "ns"))
            self.native.create_namespace("hd", "ns", None)
            repark._ANSI_NATIVE = self.native
            self.run("CREATE SCHEMA ice.ms")
        original = native_module.session_sql_with_write_options

        def counting(*args: Any, **kwargs: Any) -> Any:
            frame = original(*args, **kwargs)
            self.returned += 1
            return frame

        native_module.session_sql_with_write_options = counting
        self.restore = lambda: setattr(native_module, "session_sql_with_write_options", original)
        catalog = "ice" if self.name == "native" else "hd"
        for table, rows in (("src", SRC_ROWS), ("src2", SRC2_ROWS)):
            self.run(f"CREATE TABLE {catalog}.ns.{table} (id INT, p INT)")
            body = ", ".join(f"({a}, {b})" for a, b in rows)
            self.run(f"INSERT INTO {catalog}.ns.{table} VALUES {body}")

    def close(self) -> None:
        from repark.spark.session import _reset_active_session_for_tests

        self.restore()
        self.spark.stop()
        _reset_active_session_for_tests()

    def run(self, sql: str) -> list[list[Any]]:
        import repark

        if self.name == "native":
            rows = repark.sql(sql).to_arrow().to_pylist()
        else:
            rows = self.spark.sql(sql).toArrow().to_pylist()
        return [list(row.values()) for row in rows]

    def frames(self) -> dict[str, Any]:
        import repark

        catalog = "ice" if self.name == "native" else "hd"
        reader = repark.sql if self.name == "native" else self.spark.sql
        base = reader(f"SELECT id, p FROM {catalog}.ns.src")
        other = reader(f"SELECT id, p FROM {catalog}.ns.src2")
        return {
            "sql": base,
            "filter": base.filter("id >= 9"),
            "select": base.select("id", "p"),
            "union": base.union(other),
            "cache": base.cache(),
            "local_checkpoint": base.localCheckpoint(),
            "join": base.alias("a").join(base.alias("b"), "id").select("id", "a.p"),
        }

    def make_target(self, kind: str, verb: str) -> tuple[str, Path | None]:
        self.slot += 1
        n = self.slot
        seed = ", ".join(f"({a}, {b})" for a, b in E_ROWS)
        if kind in ("ice_existing", "hd_existing"):
            catalog = kind.split("_")[0]
            self.run(f"CREATE TABLE {catalog}.ns.e{n} (id INT, p INT)")
            self.run(f"INSERT INTO {catalog}.ns.e{n} VALUES {seed}")
            return f"{catalog}.ns.e{n}", self.root / catalog / "ns" / f"e{n}"
        if kind == "hd_partitioned":
            target = self.pool.pop(0)
            return target, self.root / "hd" / "ns" / target.rsplit(".", 1)[1]
        if kind in ("ice_new", "hd_new"):
            catalog = kind.split("_")[0]
            return f"{catalog}.ns.n{n}", self.root / catalog / "ns" / f"n{n}"
        if kind == "ice_memory_schema":
            self.run(f"CREATE TABLE ice.ms.m{n} (id INT, p INT)")
            self.run(f"INSERT INTO ice.ms.m{n} VALUES {seed}")
            return f"ice.ms.m{n}", None
        if kind in ("memory_qualified", "memory_bare"):
            self.run(f"CREATE TABLE m{n} (id INT, p INT)")
            self.run(f"INSERT INTO m{n} VALUES {seed}")
            return (f"datafusion.public.m{n}" if kind == "memory_qualified" else f"m{n}"), None
        if kind == "view":
            self.run(f"CREATE VIEW v{n} AS SELECT * FROM (VALUES {seed}) AS x(id, p)")
            return f"v{n}", None
        if kind == "temp_view":
            import repark

            if self.name == "native":
                frame = repark.sql(f"SELECT * FROM (VALUES {seed}) AS x(id, p)")
            else:
                frame = self.spark.sql(f"SELECT * FROM VALUES {seed} AS x(id, p)")
            frame.createOrReplaceTempView(f"tv{n}")
            return f"tv{n}", None
        path = self.root / "paths" / f"x{n}"
        if kind == "path_existing":
            seed_path(path, path_format(verb))
        return str(path), path

    def state(self, kind: str, verb: str, target: str, location: Path | None) -> dict[str, Any]:
        if kind in PATH_KINDS:
            assert location is not None
            files = [p for p in location.rglob("*") if p.is_file()] if location.exists() else []
            siblings = list(location.parent.iterdir()) if location.parent.exists() else []
            staging = [p.name for p in siblings if STAGING.match(p.name)]
            return {
                "rows": path_rows(location, path_format(verb)),
                "files": sorted(str(p.relative_to(location)) for p in files),
                "staging": staging,
            }
        if kind in ("view", "temp_view"):
            target = f"spark_catalog.default.{target}"
        try:
            rows: list[list[int]] | None = sorted(self.run(f"SELECT id, p FROM {target}"))
        except Exception:
            rows = None
        state: dict[str, Any] = {"rows": rows}
        if location is not None:
            state.update(metadata_state(location))
        return state

    def cells(self) -> Iterator[dict[str, Any]]:
        targets = NATIVE_TARGETS if self.name == "native" else SPARK_TARGETS
        plan = [(kind, verb, call) for kind in targets for verb, call in TABLE_VERBS.items()]
        plan += [(kind, verb, call) for kind in PATH_KINDS for verb, call in PATH_VERBS.items()]
        for kind, verb, call in plan:
            for frame_name, frame in self.frames().items():
                target, location = self.make_target(kind, verb)
                before = self.state(kind, verb, target, location)
                self.returned = 0
                try:
                    call(frame, target)
                    outcome: dict[str, Any] = {"kind": "returned"}
                except Exception as error:
                    outcome = {"kind": "refused", "class": type(error).__name__}
                    outcome["text"] = str(error)
                yield {
                    "key": f"{self.name}|{kind}|{verb}|{frame_name}",
                    "target": target,
                    "location": location,
                    "outcome": outcome,
                    "seat_returned": self.returned,
                    "before": before,
                    "after": self.state(kind, verb, target, location),
                }


DATA_FILE = re.compile(r"/data/[0-9a-f-]{36}-\d+\.parquet")


def scrub(text: str, root: Path) -> str:
    return DATA_FILE.sub("/data/<file>.parquet", text.replace(str(root), "<warehouse>"))


def record(root: Path) -> dict[str, Any]:
    expected: dict[str, Any] = {}
    for door_name in ("native", "spark-door"):
        door = Door(door_name, root / door_name)
        door.open()
        try:
            for cell in door.cells():
                outcome = dict(cell["outcome"])
                if "text" in outcome:
                    outcome["text"] = scrub(outcome["text"], door.root)
                expected[cell["key"]] = {
                    "guard": door_name == "native" and bool(cell["seat_returned"]),
                    "outcome": outcome,
                    "after_rows": cell["after"]["rows"],
                }
        finally:
            door.close()
    return expected


def main() -> None:
    expected = record(Path(sys.argv[1]).resolve())
    text = json.dumps(expected, sort_keys=True, separators=(",", ":"))
    EXPECTED_FILE.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
