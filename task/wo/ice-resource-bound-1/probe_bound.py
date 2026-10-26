"""ICE-RESOURCE-BOUND-1 step 0: measure the ordered write, the MoR MERGE and the changelog view.

``cell`` runs one statement in this process and prints one JSON line. ``grid`` runs every cell
in its own process under the lane memory cap and ``/usr/bin/time -v``. ``render`` writes the
markdown table from the JSON.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from pydantic import BaseModel

CATALOG = "bound"
NAMESPACE = "w"
TABLE = f"{CATALOG}.{NAMESPACE}.t"
MERGE_FILES = 32
MERGE_POOLS = (32, 128, 512, 2048)
MERGE_SCALE_POOLS = (2048, 8192)
CENSUS_POOLS = (32, 2048)
PARTITIONS = 32
CHANGELOG_SNAPSHOTS = 10
LOAD_LIMIT = 6.0
LOAD_WAIT_SECONDS = 60
LOAD_WAIT_MAX = 10
SAMPLE_SECONDS = 0.02
CAP = "40G"
SCOPE = ["systemd-run", "--user", "--scope", "-p", f"MemoryMax={CAP}", "--same-dir", "--quiet"]
PINNING = ["taskset", "-c", "0-25"]
TIMER = ["/usr/bin/time", "-v"]
PROBE = "task/wo/ice-resource-bound-1/probe_bound.py"
SPARK_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
COLUMNS_DDL = "id BIGINT, k BIGINT, p INT, a DOUBLE, b DOUBLE, s STRING, c INT, flag BOOLEAN"
SELECT_LIST = (
    "id",
    "pmod(id * 7919, {rows}) AS k",
    "CAST(pmod(id, 32) AS INT) AS p",
    "CAST(id AS DOUBLE) * 0.5 AS a",
    "CAST(pmod(id * 13, 1000) AS DOUBLE) AS b",
    "concat('s', lpad(CAST(pmod(id * 31, 100000000) AS STRING), 12, '0')) AS s",
    "CAST(pmod(id, 1000) AS INT) AS c",
    "pmod(id, 2) = 0 AS flag",
)


class Cell(BaseModel):
    """One measurement cell: the path, the door and the sizes."""

    path: str
    door: str
    rows: int
    format_version: int
    ordered: bool = False
    partitioned: bool = False
    writers: int = 1
    pool_mib: int = 32
    match_percent: int = 0
    not_matched_by_source: bool = False
    query: str = ""
    engine: str = "repark"


def read_status_kib(field: str) -> int:
    """Return one ``/proc/self/status`` field in KiB."""
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith(f"{field}:"):
            return int(line.split()[1])
    return -1


def reset_peak_rss() -> bool:
    """Reset this process's peak RSS (``VmHWM``) to its current RSS."""
    try:
        Path("/proc/self/clear_refs").write_text("5")
    except OSError:
        return False
    return True


def directory_bytes(root: Path) -> int:
    """Return the total size of the regular files under ``root``."""
    total = 0
    for directory, _, names in os.walk(root):
        for name in names:
            try:
                total += (Path(directory) / name).stat().st_size
            except OSError:
                continue
    return total


class SpillSampler(threading.Thread):
    """Sample the spill directory size until stopped and keep the peak."""

    def __init__(self, root: Path) -> None:
        """Watch ``root``."""
        super().__init__(daemon=True)
        self.root = root
        self.peak = 0
        self.stop_event = threading.Event()

    def run(self) -> None:
        """Poll the directory size."""
        while not self.stop_event.is_set():
            self.peak = max(self.peak, directory_bytes(self.root))
            time.sleep(SAMPLE_SECONDS)
        self.peak = max(self.peak, directory_bytes(self.root))


def build_session(cell: Cell, warehouse: Path, spill: Path) -> Any:
    """Build the facade session with the cell's pool, spill directory and writers."""
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("ice-resource-bound-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("datafusion.runtime.memory_limit", f"{cell.pool_mib}M")
        .config("datafusion.runtime.temp_directory", str(spill))
        .config("repark.write.max-concurrent-files", str(cell.writers))
        .getOrCreate()
    )
    session.register_memory_catalog(CATALOG, warehouse)
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    return session


def source_sql(rows: int, start: int, stop: int) -> str:
    """Return the eight-column source query over ids ``start`` to ``stop``."""
    columns = ", ".join(SELECT_LIST).format(rows=rows)
    return f"SELECT {columns} FROM src_range WHERE id >= {start} AND id < {stop}"


def register_source(session: Any, rows: int) -> None:
    """Register ``src_range`` and the full eight-column ``src`` view."""
    session.range(rows).createOrReplaceTempView("src_range")
    session.sql(source_sql(rows, 0, rows)).createOrReplaceTempView("src")


def table_properties(cell: Cell, merge_on_read: bool) -> str:
    """Return the TBLPROPERTIES clause of the target table."""
    pairs = [f"'format-version'='{cell.format_version}'"]
    if merge_on_read:
        pairs += [
            "'write.merge.mode'='merge-on-read'",
            "'write.update.mode'='merge-on-read'",
            "'write.delete.mode'='merge-on-read'",
        ]
    return "TBLPROPERTIES (" + ", ".join(pairs) + ")"


def create_target(session: Any, cell: Cell, merge_on_read: bool, order: bool = True) -> None:
    """Create the target table, partitioned and, when ``order`` holds, ordered as the cell says."""
    partition = " PARTITIONED BY (p)" if cell.partitioned else ""
    properties = table_properties(cell, merge_on_read)
    session.sql(f"CREATE TABLE {TABLE} ({COLUMNS_DDL}) USING iceberg{partition} {properties}")
    if cell.ordered and order:
        order_target(session)


def order_target(session: Any) -> None:
    """Declare ``k`` as the target's write order."""
    session.sql(f"ALTER TABLE {TABLE} WRITE ORDERED BY k")


def seed_target(session: Any, rows: int, files: int) -> None:
    """Write ``rows`` rows into the target as ``files`` appends."""
    step = rows // files
    for index in range(files):
        start = index * step
        stop = rows if index == files - 1 else start + step
        session.sql(f"INSERT INTO {TABLE} {source_sql(rows, start, stop)}").collect()


def ordered_statement(session: Any, cell: Cell) -> None:
    """Run the cell's ordered-write door over ``src``."""
    if cell.door == "D1":
        session.sql(f"INSERT INTO {TABLE} SELECT * FROM src").collect()
    elif cell.door == "D2":
        session.sql("SELECT * FROM src").writeTo(TABLE).option(
            "snapshot-property.ice-resource-bound-1", "probe"
        ).append()
    elif cell.door == "D3":
        session.sql(f"INSERT INTO {TABLE} BY NAME SELECT * FROM src").collect()
    elif cell.door in {"D4", "spark"}:
        session.sql(f"INSERT OVERWRITE {TABLE} SELECT * FROM src").collect()
    elif cell.door == "D5":
        session.sql(f"INSERT INTO {TABLE} REPLACE WHERE id >= 0 SELECT * FROM src").collect()
    elif cell.door == "D6":
        session.sql(f"CREATE TABLE {TABLE} USING iceberg AS SELECT * FROM src").collect()
    elif cell.door == "D8":
        session.sql(
            f"MERGE INTO {TABLE} t USING src s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.a = s.a + 1.0"
        ).collect()
    elif cell.door == "D10":
        session.sql(f"UPDATE {TABLE} SET a = a + 1.0").collect()
    else:
        raise ValueError(f"no ordered-write spelling for door {cell.door}")


def merge_statement(cell: Cell) -> str:
    """Return the MoR MERGE text of the cell."""
    text = (
        f"MERGE INTO {TABLE} t USING msrc s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.a = s.a"
    )
    if cell.not_matched_by_source:
        text += " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.c = -1"
    return text


def prepare(session: Any, cell: Cell) -> str:
    """Create and seed what the cell needs; return the statement kind."""
    register_source(session, cell.rows)
    if cell.path == "PP-01":
        if cell.door == "D6":
            return "ordered"
        seeded = cell.door in {"D8", "D10"}
        create_target(session, cell, merge_on_read=False, order=not seeded)
        if seeded:
            seed_target(session, cell.rows, 1)
            if cell.ordered:
                order_target(session)
        return "ordered"
    if cell.path == "PP-02":
        create_target(session, cell, merge_on_read=True)
        seed_target(session, cell.rows, MERGE_FILES)
        session.sql(
            f"SELECT id, a + 1.0 AS a FROM src WHERE pmod(id * 7, 100) < {cell.match_percent}"
        ).createOrReplaceTempView("msrc")
        return "merge"
    create_target(session, cell, merge_on_read=False)
    seed_target(session, cell.rows, CHANGELOG_SNAPSHOTS)
    session.sql(f"CALL {CATALOG}.system.create_changelog_view(table => '{NAMESPACE}.t')").collect()
    return "changelog"


def execute(session: Any, cell: Cell, kind: str) -> int:
    """Run the measured statement; return the result row count for a query."""
    if kind == "ordered":
        ordered_statement(session, cell)
        return 0
    if kind == "merge":
        session.sql(merge_statement(cell)).collect()
        return 0
    return len(session.sql(cell.query).collect())


def snapshot_count(session: Any, kind: str) -> int:
    """Return the number of snapshots of the target table."""
    if kind == "ordered" and not session.catalog.tableExists(TABLE):
        return 0
    return int(session.sql(f"SELECT count(*) AS n FROM {TABLE}.snapshots").collect()[0]["n"])


def latest_summary(session: Any) -> dict[str, str]:
    """Return the current snapshot's summary map."""
    rows = session.sql(
        f"SELECT summary FROM {TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not rows:
        return {}
    summary = rows[0]["summary"]
    if isinstance(summary, dict):
        return {str(key): str(value) for key, value in summary.items()}
    return {str(key): str(value) for key, value in summary}


def data_file_paths(session: Any, snapshot_files: dict[str, str]) -> list[str]:
    """Return the data file paths the current snapshot added."""
    rows = session.sql(f"SELECT file_path, content FROM {TABLE}.files").collect()
    paths = [str(row["file_path"]) for row in rows if int(row["content"]) == 0]
    added = int(snapshot_files.get("added-data-files", "0") or 0)
    return paths if added else []


def per_file_order(paths: list[str]) -> tuple[bool, int]:
    """Return whether every file's ``k`` column is non-decreasing, and the file count."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    held = True
    for path in paths:
        column = pq.read_table(path.removeprefix("file://"), columns=["k"]).column("k")
        if len(column) > 1:
            steps = pc.subtract(column.slice(1), column.slice(0, len(column) - 1))
            held = held and pc.min(steps).as_py() >= 0
    return held, len(paths)


def last_doors(session: Any) -> list[str]:
    """Return the doors the session recorded for its last write."""
    from repark import _native

    return list(_native.last_write_doors(session._ensure_alive()))


def run_cell(cell: Cell, workdir: Path) -> dict[str, Any]:
    """Prepare, measure and inspect one cell in this process."""
    workdir = workdir.resolve()
    warehouse = workdir / "warehouse"
    spill = workdir / "spill"
    warehouse.mkdir(parents=True, exist_ok=True)
    spill.mkdir(parents=True, exist_ok=True)
    session = build_session(cell, warehouse, spill)
    kind = prepare(session, cell)
    snapshots_before = snapshot_count(session, kind)
    rss_before = read_status_kib("VmRSS")
    reset = reset_peak_rss()
    sampler = SpillSampler(spill)
    sampler.start()
    started = time.monotonic()
    outcome = "committed" if kind != "changelog" else "answered"
    error = ""
    result_rows = 0
    try:
        result_rows = execute(session, cell, kind)
    except Exception as failure:
        outcome = "error"
        error = f"{type(failure).__name__}: {str(failure)[:600]}"
    wall = time.monotonic() - started
    statement_peak = read_status_kib("VmHWM")
    sampler.stop_event.set()
    sampler.join()
    doors = last_doors(session) if kind != "changelog" else []
    snapshots_after = snapshot_count(session, kind)
    summary: dict[str, str] = {}
    order_held: bool | None = None
    files = 0
    if outcome == "committed":
        summary = latest_summary(session)
        paths = data_file_paths(session, summary)
        files = len(paths)
        if cell.ordered:
            order_held, files = per_file_order(paths)
    return {
        "outcome": outcome,
        "error": error,
        "doors_recorded": doors,
        "statement_wall_s": round(wall, 3),
        "rss_before_statement_kib": rss_before,
        "statement_peak_rss_kib": statement_peak if reset else None,
        "peak_reset": reset,
        "spill_peak_bytes": sampler.peak,
        "files_written": files,
        "added_delete_files": int(summary.get("added-delete-files", "0") or 0),
        "per_file_order_held": order_held,
        "result_rows": result_rows,
        "pool_peak_bytes": None,
        "snapshots_before": snapshots_before,
        "snapshots_after": snapshots_after,
    }


def build_spark(warehouse: Path, spill: Path) -> Any:
    """Build a ``local[2]`` Spark 4.1.2 session with a 1 GiB driver and a hadoop catalog."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("ice-resource-bound-1-spark")
        .config("spark.driver.memory", "1g")
        .config("spark.local.dir", str(spill))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.jars.packages", SPARK_RUNTIME)
        .config("spark.sql.extensions", SPARK_EXTENSIONS)
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(warehouse))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    return session


def spark_spill(session: Any) -> dict[str, int]:
    """Return the memory and disk spill bytes over every completed Spark stage."""
    from urllib.request import urlopen

    context = session.sparkContext
    url = f"{context.uiWebUrl}/api/v1/applications/{context.applicationId}/stages"
    with urlopen(url, timeout=30) as response:
        stages = json.loads(response.read())
    return {
        "memory_bytes_spilled": sum(stage.get("memoryBytesSpilled", 0) for stage in stages),
        "disk_bytes_spilled": sum(stage.get("diskBytesSpilled", 0) for stage in stages),
    }


def run_spark_cell(cell: Cell, workdir: Path) -> dict[str, Any]:
    """Prepare and run one cell on Spark and read its spill from the stage metrics."""
    workdir = workdir.resolve()
    warehouse = workdir / "warehouse"
    spill = workdir / "spill"
    warehouse.mkdir(parents=True, exist_ok=True)
    spill.mkdir(parents=True, exist_ok=True)
    session = build_spark(warehouse, spill)
    kind = prepare(session, cell)
    before = spark_spill(session)
    started = time.monotonic()
    outcome = "committed" if kind != "changelog" else "answered"
    error = ""
    try:
        execute(session, cell, kind)
    except Exception as failure:
        outcome = "error"
        error = f"{type(failure).__name__}: {str(failure)[:600]}"
    wall = time.monotonic() - started
    time.sleep(3)
    after = spark_spill(session)
    session.stop()
    return {
        "outcome": outcome,
        "error": error,
        "statement_wall_s": round(wall, 3),
        "spark_memory_bytes_spilled": after["memory_bytes_spilled"]
        - before["memory_bytes_spilled"],
        "spark_disk_bytes_spilled": after["disk_bytes_spilled"] - before["disk_bytes_spilled"],
        "driver_memory": "1g",
    }


def spark_grid() -> list[Cell]:
    """Return one Spark cell per bounded path at 4M rows."""
    return [
        Cell(
            path="PP-01",
            door="spark",
            rows=4_000_000,
            format_version=2,
            ordered=True,
            engine="spark",
        ),
        Cell(
            path="PP-02",
            door="spark",
            rows=4_000_000,
            format_version=2,
            match_percent=100,
            engine="spark",
        ),
        Cell(
            path="PA-11",
            door="spark",
            rows=4_000_000,
            format_version=2,
            query="SELECT id FROM t_changes LIMIT 10",
            engine="spark",
        ),
    ]


def cell_arguments(cell: Cell) -> list[str]:
    """Return the ``cell`` sub-command arguments of one cell."""
    command = "spark-cell" if cell.engine == "spark" else "cell"
    return [command, "--spec", cell.model_dump_json()]


def read_load() -> float:
    """Return the one-minute load average."""
    return float(Path("/proc/loadavg").read_text().split()[0])


def wait_for_load() -> tuple[float, int]:
    """Wait while the load is above the limit, at most ten single sleeps; return load and waits."""
    waits = 0
    load = read_load()
    while load > LOAD_LIMIT and waits < LOAD_WAIT_MAX:
        time.sleep(LOAD_WAIT_SECONDS)
        waits += 1
        load = read_load()
    return load, waits


def parse_timer(text: str) -> dict[str, Any]:
    """Return peak RSS, wall and exit facts from ``/usr/bin/time -v`` output."""
    peak = re.search(r"Maximum resident set size \(kbytes\): (\d+)", text)
    wall = re.search(r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\): ([\d:.]+)", text)
    signal = re.search(r"Command terminated by signal (\d+)", text)
    seconds = None
    if wall:
        parts = [float(part) for part in wall.group(1).split(":")]
        seconds = round(sum(value * 60**index for index, value in enumerate(reversed(parts))), 2)
    return {
        "process_peak_rss_kib": int(peak.group(1)) if peak else None,
        "process_wall_s": seconds,
        "signal": int(signal.group(1)) if signal else None,
    }


def run_one(cell: Cell, scratch: Path, python: str) -> dict[str, Any]:
    """Run one cell in its own capped, timed process and return its row."""
    workdir = scratch / "cell"
    shutil.rmtree(workdir, ignore_errors=True)
    workdir.mkdir(parents=True)
    load, waits = wait_for_load()
    command = [*SCOPE, *PINNING, *TIMER, python, PROBE, *cell_arguments(cell), "--workdir"]
    environment = dict(os.environ, CARGO_BUILD_JOBS="8")
    completed = subprocess.run(
        [*command, str(workdir)],
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )
    timer = parse_timer(completed.stderr)
    row: dict[str, Any] = {**cell.model_dump(), "start_load": load, "load_waits": waits}
    row.update(timer)
    row["exit_code"] = completed.returncode
    lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
    if lines:
        row.update(json.loads(lines[-1]))
    elif timer["signal"] == 9 or completed.returncode in {137, -9}:
        row["outcome"] = "exceeded 40G cap"
    else:
        row["outcome"] = "process failed"
        row["error"] = completed.stderr[-600:]
    row["command"] = " ".join([*command, "<workdir>"])
    shutil.rmtree(workdir, ignore_errors=True)
    return row


def ordered_grid() -> list[Cell]:
    """Return the PP-01 base grid on door D4."""
    cells = []
    for version in (2, 3):
        for rows in (100_000, 1_000_000):
            for ordered in (True, False):
                for partitioned in (False, True):
                    for writers in (1, 4):
                        for pool in (32, 128):
                            cells.append(
                                Cell(
                                    path="PP-01",
                                    door="D4",
                                    rows=rows,
                                    format_version=version,
                                    ordered=ordered,
                                    partitioned=partitioned,
                                    writers=writers,
                                    pool_mib=pool,
                                )
                            )
    return cells


def merge_grid() -> list[Cell]:
    """Return the PP-02 base grid on door D8 against an unordered MoR target."""
    cells = []
    legs = ((1, False), (10, False), (100, False), (10, True))
    for version in (2, 3):
        for rows in (200_000, 1_000_000):
            for percent, by_source in legs:
                for writers in (1, 4):
                    for pool in MERGE_POOLS:
                        cells.append(
                            Cell(
                                path="PP-02",
                                door="D8",
                                rows=rows,
                                format_version=version,
                                writers=writers,
                                pool_mib=pool,
                                match_percent=percent,
                                not_matched_by_source=by_source,
                            )
                        )
    return cells


def changelog_grid(rows_list: tuple[int, ...]) -> list[Cell]:
    """Return the PA-11 cells: LIMIT with a narrow projection, and a full narrow scan."""
    cells = []
    queries = (
        "SELECT id FROM t_changes LIMIT 10",
        "SELECT count(id) AS n FROM t_changes",
    )
    for version in (2, 3):
        for rows in rows_list:
            for query in queries:
                cells.append(
                    Cell(
                        path="PA-11",
                        door="none",
                        rows=rows,
                        format_version=version,
                        pool_mib=32,
                        query=query,
                    )
                )
    return cells


def census_grid() -> list[Cell]:
    """Return one PP-01 cell per measurable Spark door on an ordered table."""
    return [
        Cell(
            path="PP-01",
            door=door,
            rows=1_000_000,
            format_version=2,
            ordered=door != "D6",
            pool_mib=pool,
        )
        for pool in CENSUS_POOLS
        for door in ("D1", "D2", "D3", "D4", "D5", "D6", "D8", "D10")
    ]


def scaled_ordered(rows: int) -> list[Cell]:
    """Return the PP-01 scale cells at ``rows``."""
    cells = []
    for version in (2, 3):
        for partitioned in (False, True):
            for writers in (1, 4):
                cells.append(
                    Cell(
                        path="PP-01",
                        door="D4",
                        rows=rows,
                        format_version=version,
                        ordered=True,
                        partitioned=partitioned,
                        writers=writers,
                        pool_mib=32,
                    )
                )
        cells.append(Cell(path="PP-01", door="D4", rows=rows, format_version=version, pool_mib=32))
    return cells


def scaled_merge(rows: int) -> list[Cell]:
    """Return the PP-02 scale cells at ``rows``."""
    return [
        Cell(
            path="PP-02",
            door="D8",
            rows=rows,
            format_version=version,
            pool_mib=pool,
            match_percent=percent,
            not_matched_by_source=by_source,
        )
        for version in (2, 3)
        for pool in MERGE_SCALE_POOLS
        for percent, by_source in ((100, False), (10, True))
    ]


def series_name(cell: Cell) -> str:
    """Return the key of the scale series a cell belongs to."""
    fields = cell.model_dump(exclude={"rows"})
    return json.dumps(fields, sort_keys=True)


def run_grid(name: str, cells: list[Cell], scratch: Path, output: Path, python: str) -> None:
    """Run cells in order, append each row to ``output`` and stop a series at its first kill."""
    killed: set[str] = set()
    for cell in cells:
        if series_name(cell) in killed:
            continue
        row = run_one(cell, scratch, python)
        row["grid"] = name
        with output.open("a") as handle:
            handle.write(json.dumps(row) + "\n")
        print(
            json.dumps(
                {
                    key: row.get(key)
                    for key in ("grid", "path", "door", "rows", "format_version", "outcome")
                }
            ),
            flush=True,
        )
        if row.get("outcome") == "exceeded 40G cap":
            killed.add(series_name(cell))


def grids_for(name: str) -> list[Cell]:
    """Return the cells of a named grid."""
    table = {
        "census": census_grid,
        "census-seeded": lambda: [cell for cell in census_grid() if cell.door in {"D8", "D10"}],
        "spark-ordered": lambda: [cell for cell in spark_grid() if cell.path == "PP-01"],
        "spark": spark_grid,
        "ordered": ordered_grid,
        "merge": merge_grid,
        "changelog": lambda: changelog_grid((1_000_000, 4_000_000, 16_000_000)),
        "ordered-scale": lambda: scaled_ordered(4_000_000) + scaled_ordered(16_000_000),
        "merge-scale": lambda: scaled_merge(4_000_000) + scaled_merge(16_000_000),
    }
    return table[name]()


def calibrate(workdir: Path) -> dict[str, Any]:
    """Return the Arrow bytes per source row, measured on 100k rows."""
    cell = Cell(path="calibrate", door="none", rows=100_000, format_version=2, pool_mib=2048)
    workdir = workdir.resolve()
    (workdir / "warehouse").mkdir(parents=True, exist_ok=True)
    (workdir / "spill").mkdir(parents=True, exist_ok=True)
    session = build_session(cell, workdir / "warehouse", workdir / "spill")
    register_source(session, cell.rows)
    table = session.sql("SELECT * FROM src").to_arrow()
    narrow = session.sql("SELECT id, a FROM src").to_arrow()
    return {
        "rows": cell.rows,
        "arrow_bytes": table.nbytes,
        "bytes_per_row": table.nbytes / cell.rows,
        "merge_source_bytes_per_row": narrow.nbytes / cell.rows,
    }


def shape_of(row: dict[str, Any]) -> str:
    """Return the short shape label of a result row."""
    if row["path"] == "PP-01":
        order = "ordered" if row["ordered"] else "unordered"
        return f"{order} {PARTITIONS} parts" if row["partitioned"] else f"{order} unpart"
    if row["path"] == "PP-02":
        suffix = " +NMBS" if row["not_matched_by_source"] else ""
        return f"{row['match_percent']}% match{suffix}"
    return row["query"]


def matched_bytes(row: dict[str, Any], input_bytes: int) -> int | None:
    """Return the bytes of the rows a PP-02 MERGE updates, or ``None`` on other paths."""
    if row["path"] != "PP-02":
        return None
    if row["not_matched_by_source"]:
        return input_bytes
    return round(input_bytes * row["match_percent"] / 100)


def spill_bytes(row: dict[str, Any]) -> int:
    """Return the spill peak of a RePark row or the disk spill of a Spark row."""
    if row.get("engine") == "spark":
        return int(row.get("spark_disk_bytes_spilled") or 0)
    return int(row.get("spill_peak_bytes") or 0)


def mebibytes(value: float | None) -> str:
    """Return ``value`` bytes as MiB text, or a dash."""
    return "-" if value is None else f"{value / 2**20:.1f}"


def markdown_row(row: dict[str, Any], bytes_per_row: float) -> str:
    """Return one markdown table line for a result row."""
    input_bytes = round(row["rows"] * bytes_per_row)
    matched = matched_bytes(row, input_bytes)
    peak = row.get("process_peak_rss_kib")
    statement = row.get("statement_peak_rss_kib")
    before = row.get("rss_before_statement_kib")
    growth = (statement - before) * 1024 if statement and before else None
    order = row.get("per_file_order_held")
    doors = ",".join(row.get("doors_recorded") or []) or row["door"]
    cells = [
        row["grid"],
        row["path"],
        f"{row['door']} ({doors})",
        f"v{row['format_version']}",
        f"{row['rows']:,}",
        shape_of(row),
        str(row["writers"]),
        mebibytes(input_bytes),
        mebibytes(matched),
        "driver 1g" if row.get("engine") == "spark" else str(row["pool_mib"]),
        "-" if row.get("engine") == "spark" else f"{input_bytes / (row['pool_mib'] * 2**20):.2f}",
        "n/a",
        mebibytes(spill_bytes(row)),
        mebibytes(peak * 1024 if peak else None),
        mebibytes(growth),
        f"{growth / input_bytes:.2f}" if growth is not None else "-",
        str(row.get("statement_wall_s", "-")),
        f"{row.get('files_written', '-')}/{row.get('added_delete_files', '-')}",
        "-" if order is None else ("yes" if order else "no"),
        f"{row.get('snapshots_before', '-')}->{row.get('snapshots_after', '-')}",
        f"{row['start_load']:.2f}",
        row.get("outcome", "-"),
    ]
    return "| " + " | ".join(cells) + " |"


HEADER = (
    "| grid | path | door (recorded) | format | rows | shape | writers | input MiB | matched MiB "
    "| pool MiB | input/pool | pool peak | spill peak MiB | process peak RSS MiB "
    "| statement RSS growth MiB | growth/input | wall s | data/delete files | per-file order "
    "| snapshots | start load | outcome |"
)


def render(source: Path, target: Path, proposals: Path | None) -> None:
    """Write the markdown table of every row in the JSON document."""
    document = json.loads(source.read_text())
    bytes_per_row = document["calibration"]["bytes_per_row"]
    lines = [f"# ICE-RESOURCE-BOUND-1 step 0 bound table ({document['date']})", "", HEADER]
    lines.append("|" + "---|" * (HEADER.count("|") - 1))
    lines += [markdown_row(row, bytes_per_row) for row in document["cells"]]
    if proposals is not None:
        lines += ["", *proposals.read_text().splitlines()]
    target.write_text("\n".join(lines) + "\n")


def assemble(rows_file: Path, calibration: Path, target: Path, date: str) -> None:
    """Combine the JSON lines and the calibration into one JSON document."""
    cells = [json.loads(line) for line in rows_file.read_text().splitlines() if line.strip()]
    document = {
        "unit": "ICE-RESOURCE-BOUND-1",
        "step": 0,
        "date": date,
        "probe": PROBE,
        "cap": f"MemoryMax={CAP}",
        "calibration": json.loads(calibration.read_text()),
        "columns": [
            "path",
            "format_version",
            "input bytes",
            "pool_mib",
            "pool_peak_bytes",
            "process_peak_rss_kib",
            "statement_wall_s",
            "files_written",
            "per_file_order_held",
            "start_load",
        ],
        "cells": cells,
    }
    target.write_text(json.dumps(document, indent=1) + "\n")


def main() -> None:
    """Dispatch the sub-command."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    one = commands.add_parser("cell")
    one.add_argument("--spec", required=True)
    one.add_argument("--workdir", required=True, type=Path)
    spark_one = commands.add_parser("spark-cell")
    spark_one.add_argument("--spec", required=True)
    spark_one.add_argument("--workdir", required=True, type=Path)
    many = commands.add_parser("grid")
    many.add_argument("--name", required=True)
    many.add_argument("--scratch", required=True, type=Path)
    many.add_argument("--output", required=True, type=Path)
    many.add_argument("--python", default=sys.executable)
    measure = commands.add_parser("calibrate")
    measure.add_argument("--workdir", required=True, type=Path)
    join = commands.add_parser("assemble")
    join.add_argument("--rows", required=True, type=Path)
    join.add_argument("--calibration", required=True, type=Path)
    join.add_argument("--output", required=True, type=Path)
    join.add_argument("--date", required=True)
    table = commands.add_parser("render")
    table.add_argument("--source", required=True, type=Path)
    table.add_argument("--output", required=True, type=Path)
    table.add_argument("--proposals", type=Path)
    arguments = parser.parse_args()
    if arguments.command == "cell":
        cell = Cell.model_validate_json(arguments.spec)
        print(json.dumps(run_cell(cell, arguments.workdir)), flush=True)
    elif arguments.command == "spark-cell":
        cell = Cell.model_validate_json(arguments.spec)
        print(json.dumps(run_spark_cell(cell, arguments.workdir)), flush=True)
    elif arguments.command == "grid":
        arguments.scratch.mkdir(parents=True, exist_ok=True)
        cells = grids_for(arguments.name)
        run_grid(arguments.name, cells, arguments.scratch, arguments.output, arguments.python)
    elif arguments.command == "calibrate":
        print(json.dumps(calibrate(arguments.workdir)), flush=True)
    elif arguments.command == "assemble":
        assemble(arguments.rows, arguments.calibration, arguments.output, arguments.date)
    else:
        render(arguments.source, arguments.output, arguments.proposals)


if __name__ == "__main__":
    main()
