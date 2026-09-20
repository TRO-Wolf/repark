"""Record or re-check the ICE-PROCS-ROUTE-1 Spark oracle cells on live PySpark.

Usage (PySpark interpreter with the Iceberg runtime on ``spark.jars.packages``)::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
        PYSPARK_SUBMIT_ARGS="--conf spark.jars.ivy=<ivy> --conf spark.driver.memory=2g \
        --conf spark.ui.enabled=false pyspark-shell" \
        PYTHONPATH=<sparkenv> python python/repark/tests/_record_ice_procs_route_1_oracle.py \
        --warehouse /tmp/procs-oracle-wh record
    ... check

``record`` prints the fixture JSON to stdout; ``check`` re-derives every cell
and exits non-zero naming the first mismatch against the committed
``ice_procs_route_1_spark_oracle.json`` (``secs`` excluded). Every run-varying
token is normalized at record time to its writer shape (canonical uuids, snap
manifest names, data-file stems, staging directories, wall paths), so two
derivations compare equal with no second pass. The Iceberg runtime GAV comes
from :mod:`_oracle_pins`. Seed and normalization mirror the measured
``cells_qc3.py`` shapes cell for cell.

pins: ice-procs-route-1/C-001, C-002, C-003
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import pyarrow.parquet as parquet

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

FIXTURE = Path(__file__).with_name("ice_procs_route_1_spark_oracle.json")
ICEBERG_SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
IN_MEMORY_IMPL = "org.apache.iceberg.inmemory.InMemoryCatalog"
CANON_UUID = r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
STAGING_DIR = re.compile(r"copy-table-staging-" + CANON_UUID)
DATA_BASENAME = re.compile(r"[0-9]+-[0-9]+-" + CANON_UUID + r"-([0-9]+)-([0-9]+)\.parquet")
DELETES_BASENAME = re.compile(r"[0-9]+-[0-9]+-" + CANON_UUID + r"-([0-9]+)-deletes\.parquet")
SNAP_BASENAME = re.compile(r"snap-([0-9]+)-([0-9]+)-" + CANON_UUID + r"\.avro")
MANIFEST_BASENAME = re.compile(CANON_UUID + r"-m0\.avro")
VERSION_BASENAME = re.compile(r"v[0-9]+\.metadata\.json")


def live_session(warehouse: Path) -> Any:
    """Build the live two-catalog Spark session over the given warehouse."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("ice-procs-route-1-oracle")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config("spark.sql.extensions", ICEBERG_SPARK_EXTENSIONS)
        .config("spark.sql.catalog.sc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sc.catalog-impl", IN_MEMORY_IMPL)
        .config("spark.sql.catalog.sc.warehouse", str(warehouse))
        .config("spark.sql.catalog.hc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hc.type", "hadoop")
        .config("spark.sql.catalog.hc.warehouse", str(warehouse / "hc"))
        .config("spark.sql.session.timeZone", "UTC")
    )
    ivy = os.environ.get("REPARK_ORACLE_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


class OracleContext:
    """Per-cell live table naming, snapshot map, and metadata readers."""

    def __init__(self, session: Any, warehouse: Path, cell: str, catalog: str) -> None:
        """Bind a cell id to unique table names under both catalog doors."""
        self.session = session
        self.warehouse = warehouse
        self.catalog = catalog
        safe = re.sub(r"[^a-z0-9]", "_", cell.lower())
        self.table = f"{catalog}.ns.t_{safe}"
        self.short = f"ns.t_{safe}"
        self.tname = f"t_{safe}"
        self.step = "start"
        self.snapmap: dict[int, str] = {}
        self.obs: dict[str, Any] = {}

    def run(self, sql: str, step: str | None = None) -> Any:
        """Collect a SQL statement, recording the step name for errors."""
        self.step = step or sql[:160]
        return self.session.sql(sql).collect()

    def snaps(self) -> list[int]:
        """Return commit-ordered snapshot ids, extending the symbolic map."""
        self.step = "snapshots"
        rows = self.session.sql(
            f"SELECT snapshot_id, committed_at FROM {self.table}.snapshots "
            "ORDER BY committed_at, snapshot_id"
        ).collect()
        ids = [int(row[0]) for row in rows]
        for sid in ids:
            self.snapmap.setdefault(sid, f"S{len(self.snapmap)}")
        return ids

    def metadata(self) -> dict[str, Any]:
        """Return the current table metadata document over the JVM bridge."""
        jvm = self.session._jvm
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            self.session._jsparkSession, self.table
        )
        raw = jvm.org.apache.iceberg.TableMetadataParser.toJson(table.operations().current())
        return json.loads(str(raw))

    def location(self) -> str:
        """Return the table location without a trailing slash."""
        return str(self.metadata().get("location", "")).rstrip("/")

    def observe(self, name: str, value: Any) -> None:
        """Store one observation under the cell record."""
        self.obs[name] = value


def snapshot_order(metadata: dict[str, Any]) -> dict[int, int]:
    """Map snapshot ids to commit-order positions like the oracle."""
    ordered = sorted(
        metadata.get("snapshots", []),
        key=lambda s: (s["timestamp-ms"], s["snapshot-id"]),
    )
    return {s["snapshot-id"]: i for i, s in enumerate(ordered)}


def blob_row(order: dict[int, int], blob: dict[str, Any]) -> list[Any]:
    """Normalize one statistics blob with snapshot positions like the oracle."""
    props = blob.get("properties") or {}
    return [
        blob.get("type"),
        blob.get("fields", []),
        order.get(blob.get("snapshot-id")),
        blob.get("sequence-number"),
        [[key, value] for key, value in sorted(props.items())],
    ]


def norm_struct(value: Any) -> Any:
    """Normalize nested structs to sorted pair lists like the harness."""
    if isinstance(value, dict):
        return sorted([[key, norm_struct(item)] for key, item in value.items()], key=repr)
    if isinstance(value, list):
        return [norm_struct(item) for item in value]
    return value


def statistics_observation(context: OracleContext) -> list[list[list[Any]]]:
    """Normalize the statistics entries of current table metadata."""
    metadata = context.metadata()
    order = snapshot_order(metadata)
    out = []
    for entry in metadata.get("statistics", []):
        fields = {
            "snapshot": order.get(entry.get("snapshot-id")),
            "blobs": sorted(
                [blob_row(order, b) for b in entry.get("blob-metadata", [])],
                key=repr,
            ),
            "size>0": entry.get("file-size-in-bytes", 0) > 0,
            "footer>0": entry.get("file-footer-size-in-bytes", 0) > 0,
        }
        out.append([[key, fields[key]] for key, _ in sorted(fields.items(), key=repr)])
    return sorted(out, key=lambda pairs: repr(dict(pairs).get("snapshot")))


def seed_three(
    context: OracleContext, part: str = "PARTITIONED BY (cat)", version: str = "2"
) -> list[int]:
    """Create the shared seed table and return its three snapshot ids."""
    context.run(
        f"CREATE TABLE {context.table} (id BIGINT, data STRING, cat STRING) USING iceberg "
        f"{part} TBLPROPERTIES ('format-version'='{version}')",
        step="create",
    )
    context.run(
        f"INSERT INTO {context.table} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (6, 'f', 'x')",
        step="i1",
    )
    context.run(f"INSERT INTO {context.table} VALUES (3, 'c', 'x'), (7, 'g', 'x')", step="i2")
    context.run(
        f"INSERT INTO {context.table} VALUES (4, 'd', 'x'), (5, 'e', 'y'), (8, 'h', 'x')",
        step="i3",
    )
    return context.snaps()


def ancestors_cell(context: OracleContext, sql: str) -> None:
    """Run one ancestors_of CALL and record its columns and symbolic rows."""
    stamps = {s["snapshot-id"]: s["timestamp-ms"] for s in context.metadata().get("snapshots", [])}
    frame = context.session.sql(sql)
    context.observe("cols", [[f.name, f.dataType.simpleString()] for f in frame.schema.fields])
    rows = [list(r) for r in frame.collect()]
    context.snaps()
    context.observe(
        "rows",
        [[context.snapmap.get(int(r[0]), "?"), bool(r[1] == stamps.get(int(r[0])))] for r in rows],
    )


def record_ancestors(context: OracleContext, kind: str) -> None:
    """Replay one ancestors_of cell by kind against the live session."""
    catalog = context.catalog
    if kind == "ANC-DEFAULT":
        seed_three(context)
        ancestors_cell(context, f"CALL {catalog}.system.ancestors_of('{context.short}')")
    elif kind == "ANC-ID":
        ids = seed_three(context)
        ancestors_cell(
            context,
            f"CALL {catalog}.system.ancestors_of(table => '{context.short}', "
            f"snapshot_id => {ids[1]})",
        )
    elif kind == "ANC-ROLLBACK":
        ids = seed_three(context)
        context.run(f"CALL {catalog}.system.rollback_to_snapshot('{context.short}', {ids[0]})")
        context.run(f"INSERT INTO {context.table} VALUES (9, 'i', 'y')")
        ancestors_cell(context, f"CALL {catalog}.system.ancestors_of('{context.short}')")
    elif kind == "ANC-BRANCH":
        ids = seed_three(context)
        context.run(f"ALTER TABLE {context.table} CREATE BRANCH b AS OF VERSION {ids[0]}")
        context.run(f"INSERT INTO {context.table}.branch_b VALUES (9, 'i', 'y')")
        head = context.session.sql(
            f"SELECT snapshot_id FROM {context.table}.refs WHERE name = 'b'"
        ).collect()[0][0]
        ancestors_cell(
            context,
            f"CALL {catalog}.system.ancestors_of(table => '{context.short}', "
            f"snapshot_id => {head})",
        )
    elif kind == "ANC-EMPTY":
        context.run(f"CREATE TABLE {context.table} (id BIGINT) USING iceberg")
        ancestors_cell(context, f"CALL {catalog}.system.ancestors_of('{context.short}')")
    elif kind == "ANC-MISSING-ID":
        seed_three(context)
        ancestors_cell(
            context,
            f"CALL {catalog}.system.ancestors_of(table => '{context.short}', snapshot_id => 12345)",
        )
    elif kind == "ANC-POSITIONAL-ID":
        ids = seed_three(context)
        ancestors_cell(context, f"CALL {catalog}.system.ancestors_of('{context.short}', {ids[2]})")
    else:
        raise AssertionError(f"unknown ancestors cell {kind}")


def stats_path_label(path: str, metadata: dict[str, Any]) -> str:
    """Normalize a statistics path with snapshot and uuid markers."""
    out = re.sub(r"^.*/metadata/", "metadata/", path)
    out = out.replace(str(metadata["snapshots"][0]["snapshot-id"]), "<s0>")
    out = out.replace(str(metadata.get("current-snapshot-id")), "<cur>")
    return re.sub(CANON_UUID, "<uuid>", out)


def table_stats_cell(context: OracleContext, extra: str) -> None:
    """Run one compute_table_stats CALL and record path, columns, statistics."""
    frame = context.session.sql(
        f"CALL {context.catalog}.system.compute_table_stats(table => '{context.short}'{extra})"
    )
    cols = [[f.name, f.dataType.simpleString()] for f in frame.schema.fields]
    rows = [list(r) for r in frame.collect()]
    metadata = context.metadata()
    context.observe("cols", cols)
    context.observe(
        "out-rel",
        [stats_path_label(str(r[0]), metadata) if r[0] else None for r in rows],
    )
    context.observe("statistics", statistics_observation(context))


def record_table_stats(context: OracleContext, kind: str) -> None:
    """Replay one compute_table_stats cell by kind against the live session."""
    if kind == "CTS-TYPES":
        context.run(
            f"CREATE TABLE {context.table} (i INT, l BIGINT, d DOUBLE, s STRING, dt DATE, "
            "ts TIMESTAMP, b BOOLEAN, dec DECIMAL(10,2)) USING iceberg"
        )
        context.run(
            f"INSERT INTO {context.table} VALUES (1, 1, 1.5, 'a', DATE'2024-01-01', "
            "TIMESTAMP'2024-01-01 00:00:00', true, 1.25), (2, 2, 2.5, 'b', "
            "DATE'2024-01-02', TIMESTAMP'2024-01-02 00:00:00', false, 2.50), "
            "(2, 3, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        context.snaps()
        table_stats_cell(context, "")
    elif kind == "CTS-EMPTY":
        context.run(f"CREATE TABLE {context.table} (id BIGINT) USING iceberg")
        table_stats_cell(context, "")
    elif kind == "CTS-NESTED":
        context.run(
            f"CREATE TABLE {context.table} (id BIGINT, st STRUCT<a: INT, b: STRING>) USING iceberg"
        )
        context.run(
            f"INSERT INTO {context.table} VALUES (1, named_struct('a', 1, 'b', 'x')), "
            "(2, named_struct('a', 2, 'b', 'y'))"
        )
        context.snaps()
        table_stats_cell(context, "")
    elif kind == "CTS-TWICE":
        seed_three(context)
        stats_call = (
            f"CALL {context.catalog}.system.compute_table_stats(table => '{context.short}')"
        )
        context.run(stats_call)
        table_stats_cell(context, "")
    elif kind == "CTS-SNAPSHOT":
        ids = seed_three(context)
        table_stats_cell(context, f", snapshot_id => {ids[0]}")
    elif kind in ("CTS-DEFAULT", "CTS-COLUMNS", "CTS-COLUMNS-TWO", "CTS-COLUMNS-UNKNOWN"):
        extras = {
            "CTS-DEFAULT": "",
            "CTS-COLUMNS": ", columns => array('id')",
            "CTS-COLUMNS-TWO": ", columns => array('data', 'id')",
            "CTS-COLUMNS-UNKNOWN": ", columns => array('nope')",
        }
        seed_three(context)
        table_stats_cell(context, extras[kind])
    else:
        raise AssertionError(f"unknown table-stats cell {kind}")


def partition_stats_cell(context: OracleContext, extra: str, first_id: int) -> None:
    """Run one compute_partition_stats CALL and record file, entry, contents."""
    frame = context.session.sql(
        f"CALL {context.catalog}.system.compute_partition_stats(table => '{context.short}'{extra})"
    )
    cols = [[f.name, f.dataType.simpleString()] for f in frame.schema.fields]
    rows = [list(r) for r in frame.collect()]
    metadata = context.metadata()
    context.observe("cols", cols)
    context.observe(
        "out-rel",
        [partition_path_label(r[0], metadata, first_id) for r in rows],
    )
    order = snapshot_order(metadata)
    context.observe(
        "partition-statistics",
        sorted(
            [
                [order.get(s.get("snapshot-id")), s.get("file-size-in-bytes", 0) > 0]
                for s in metadata.get("partition-statistics", [])
            ],
            key=repr,
        ),
    )
    context.observe("contents", partition_stats_contents(metadata))


def partition_path_label(path: Any, metadata: dict[str, Any], first_id: int) -> Any:
    """Normalize a partition-statistics path with snapshot and uuid markers."""
    if path is None:
        return None
    out = re.sub(r"^.*/metadata/", "metadata/", str(path))
    out = re.sub(CANON_UUID, "<uuid>", out)
    out = out.replace(str(metadata.get("current-snapshot-id")), "<cur>")
    return out.replace(str(first_id), "<s0>")


def partition_stats_contents(metadata: dict[str, Any]) -> list[Any]:
    """Read every registered partition-statistics file minus volatile columns."""
    drop = {"last_updated_at", "last_updated_snapshot_id", "total_data_file_size_in_bytes"}
    contents = []
    for entry in metadata.get("partition-statistics", []):
        table = parquet.read_table(entry["statistics-path"].replace("file:", ""))
        dicts = sorted(
            [{k: v for k, v in r.items() if k not in drop} for r in table.to_pylist()],
            key=repr,
        )
        contents.append([table.schema.names, [norm_struct(d) for d in dicts]])
    return contents


def record_partition_stats(context: OracleContext, kind: str) -> None:
    """Replay one compute_partition_stats cell by kind against the live session."""
    if kind == "CPS-UNPARTITIONED":
        seed_three(context, part="")
        partition_stats_cell(context, "", 0)
    elif kind == "CPS-V3":
        ids = seed_three(context, version="3")
        partition_stats_cell(context, "", ids[0])
    elif kind == "CPS-SNAPSHOT":
        ids = seed_three(context)
        partition_stats_cell(context, f", snapshot_id => {ids[0]}", ids[0])
    elif kind == "CPS-DEFAULT":
        ids = seed_three(context)
        partition_stats_cell(context, "", ids[0])
    else:
        raise AssertionError(f"unknown partition-stats cell {kind}")


def relative_path(value: Any, source: str, target: str, staging: str, warehouse: Path) -> Any:
    """Replace run-stamped path prefixes and staging uuids with stable markers."""
    if not isinstance(value, str):
        return value
    out = value.replace(source, "<src>")
    out = out.replace(target, "<dst>")
    out = out.replace(staging, "<stg>")
    out = out.replace(str(warehouse), "<wh>")
    return STAGING_DIR.sub("copy-table-staging-<uuid>", out)


def normalize_staged_basename(name: str) -> str:
    """Map one staged file basename to its stable form with a file placeholder."""
    match = DATA_BASENAME.fullmatch(name)
    if match is not None:
        return "<file>-" + match.group(1) + "-" + match.group(2) + ".parquet"
    match = DELETES_BASENAME.fullmatch(name)
    if match is not None:
        return "<file>-" + match.group(1) + "-deletes.parquet"
    match = SNAP_BASENAME.fullmatch(name)
    if match is not None:
        return "snap-<snap>-" + match.group(2) + "-<file>.avro"
    if MANIFEST_BASENAME.fullmatch(name) is not None:
        return "<file>-m0.avro"
    if VERSION_BASENAME.fullmatch(name) is not None:
        return name
    raise AssertionError(f"unknown rewrite file-list basename {name!r}")


def normalize_file_list_line(
    line: str, source: str, target: str, staging: str, warehouse: Path
) -> str:
    """Replace both sides of one file-list line with stable markers."""
    marked = relative_path(line, source, target, staging, warehouse)
    assert isinstance(marked, str)
    sides = marked.split(",")
    assert len(sides) == 2, f"file-list line has no src,target pair: {line!r}"
    fixed = []
    for side in sides:
        head, sep, base = side.rpartition("/")
        fixed.append(head + sep + normalize_staged_basename(base))
    return ",".join(fixed)


def number_file_placeholders(lines: list[str]) -> list[str]:
    """Number each file placeholder with its stable ordinal within identical lines."""
    out: list[str] = []
    pos = 0
    while pos < len(lines):
        end = pos + 1
        while end < len(lines) and lines[end] == lines[pos]:
            end += 1
        for slot in range(end - pos):
            out.append(lines[pos].replace("<file>", f"<file#{slot}>"))
        pos = end
    return out


def file_list_observation(
    context: OracleContext, listed: str, target: str, staging: str, source: str
) -> None:
    """Record the normalized file list and staged metadata location."""
    path = Path(listed.replace("file:", ""))
    if not path.exists():
        return
    raw_lines = path.read_text(encoding="utf-8").splitlines()
    normalized = sorted(
        normalize_file_list_line(line, source, target, staging, context.warehouse)
        for line in raw_lines
    )
    context.observe("file-list", number_file_placeholders(normalized))
    staged = [
        line.split(",")[0] for line in raw_lines if line.split(",")[0].endswith(".metadata.json")
    ]
    if staged:
        staged_doc = json.loads(Path(staged[-1].replace("file:", "")).read_text(encoding="utf-8"))
        context.observe(
            "staged-metadata-location",
            relative_path(staged_doc.get("location"), source, target, staging, context.warehouse),
        )


def rewrite_path_cell(context: OracleContext, extra: str, target: str, staging: str) -> None:
    """Run one rewrite_table_path CALL and record rows, file list, staged metadata."""
    frame = context.session.sql(
        f"CALL {context.catalog}.system.rewrite_table_path(table => '{context.short}'{extra})"
    )
    context.observe("cols", [[f.name, f.dataType.simpleString()] for f in frame.schema.fields])
    rows = [list(r) for r in frame.collect()]
    source = context.location()
    context.observe(
        "rows",
        [[relative_path(v, source, target, staging, context.warehouse) for v in r] for r in rows],
    )
    for row in rows:
        listed = row[1]
        if isinstance(listed, str) and listed not in ("N/A",):
            file_list_observation(context, listed, target, staging, source)


def rewrite_path_prefixes(context: OracleContext) -> tuple[str, str, str]:
    """Return the source, target, and staging prefixes for one rewrite cell."""
    return (
        context.location(),
        "/tmp/qc-rtp-target/" + context.tname,
        str(context.warehouse / "qc-rtp-staging" / context.tname),
    )


def record_rewrite_path(context: OracleContext, kind: str) -> None:
    """Replay one rewrite_table_path cell by kind against the live session."""
    seed_three(context)
    if kind == "RTP-MOR-DELETES":
        context.run(
            f"ALTER TABLE {context.table} SET TBLPROPERTIES ('write.delete.mode'='merge-on-read')"
        )
        context.run(f"DELETE FROM {context.table} WHERE id = 1")
    source, target, staging = rewrite_path_prefixes(context)
    args = {
        "RTP-DEFAULT": f", source_prefix => '{source}', target_prefix => '{target}'",
        "RTP-STAGING": (
            f", source_prefix => '{source}', target_prefix => '{target}', "
            f"staging_location => '{staging}'"
        ),
        "RTP-NO-FILE-LIST": (
            f", source_prefix => '{source}', target_prefix => '{target}', create_file_list => false"
        ),
        "RTP-MISSING-PREFIX-ERR": ", source_prefix => '/nope', target_prefix => '/x'",
        "RTP-END-VERSION": (
            f", source_prefix => '{source}', target_prefix => '{target}', "
            "end_version => 'v3.metadata.json'"
        ),
        "RTP-START-VERSION": (
            f", source_prefix => '{source}', target_prefix => '{target}', "
            "start_version => 'v2.metadata.json'"
        ),
        "RTP-MOR-DELETES": f", source_prefix => '{source}', target_prefix => '{target}'",
    }[kind]
    rewrite_path_cell(context, args, target, staging)


CELL_KINDS: tuple[tuple[str, str, str], ...] = (
    ("QP-ANC-DEFAULT", "ancestors", "ANC-DEFAULT"),
    ("QP-ANC-ID", "ancestors", "ANC-ID"),
    ("QP-ANC-ROLLBACK", "ancestors", "ANC-ROLLBACK"),
    ("QP-ANC-BRANCH", "ancestors", "ANC-BRANCH"),
    ("QP-ANC-EMPTY", "ancestors", "ANC-EMPTY"),
    ("QP-ANC-MISSING-ID", "ancestors", "ANC-MISSING-ID"),
    ("QP-ANC-POSITIONAL-ID", "ancestors", "ANC-POSITIONAL-ID"),
    ("QP-CTS-DEFAULT", "table-stats", "CTS-DEFAULT"),
    ("QP-CTS-SNAPSHOT", "table-stats", "CTS-SNAPSHOT"),
    ("QP-CTS-COLUMNS", "table-stats", "CTS-COLUMNS"),
    ("QP-CTS-COLUMNS-TWO", "table-stats", "CTS-COLUMNS-TWO"),
    ("QP-CTS-COLUMNS-UNKNOWN", "table-stats", "CTS-COLUMNS-UNKNOWN"),
    ("QP-CTS-TYPES", "table-stats", "CTS-TYPES"),
    ("QP-CTS-EMPTY", "table-stats", "CTS-EMPTY"),
    ("QP-CTS-NESTED", "table-stats", "CTS-NESTED"),
    ("QP-CTS-TWICE", "table-stats", "CTS-TWICE"),
    ("QP-CPS-DEFAULT", "partition-stats", "CPS-DEFAULT"),
    ("QP-CPS-SNAPSHOT", "partition-stats", "CPS-SNAPSHOT"),
    ("QP-CPS-UNPARTITIONED", "partition-stats", "CPS-UNPARTITIONED"),
    ("QP-CPS-V3", "partition-stats", "CPS-V3"),
    ("QP-RTP-DEFAULT", "rewrite-path", "RTP-DEFAULT"),
    ("QP-RTP-STAGING", "rewrite-path", "RTP-STAGING"),
    ("QP-RTP-NO-FILE-LIST", "rewrite-path", "RTP-NO-FILE-LIST"),
    ("QP-RTP-MISSING-PREFIX-ERR", "rewrite-path", "RTP-MISSING-PREFIX-ERR"),
    ("QP-RTP-END-VERSION", "rewrite-path", "RTP-END-VERSION"),
    ("QP-RTP-START-VERSION", "rewrite-path", "RTP-START-VERSION"),
    ("QP-RTP-MOR-DELETES", "rewrite-path", "RTP-MOR-DELETES"),
)

CELL_TITLES = {
    "QP-ANC-DEFAULT": "ancestors_of(table)",
    "QP-ANC-ID": "ancestors_of(table, snapshot_id)",
    "QP-ANC-ROLLBACK": "ancestors_of after rollback + new commit",
    "QP-ANC-BRANCH": "ancestors_of a branch head",
    "QP-ANC-EMPTY": "ancestors_of a table with no snapshot",
    "QP-ANC-MISSING-ID": "ancestors_of unknown snapshot id",
    "QP-ANC-POSITIONAL-ID": "ancestors_of positional snapshot id",
    "QP-CTS-DEFAULT": "compute_table_stats (table)",
    "QP-CTS-SNAPSHOT": "compute_table_stats snapshot_id",
    "QP-CTS-COLUMNS": "compute_table_stats columns => array('id')",
    "QP-CTS-COLUMNS-TWO": "compute_table_stats columns two, reversed",
    "QP-CTS-COLUMNS-UNKNOWN": "compute_table_stats unknown column raises",
    "QP-CTS-TYPES": "compute_table_stats every primitive type",
    "QP-CTS-EMPTY": "compute_table_stats table with no snapshot",
    "QP-CTS-NESTED": "compute_table_stats struct column",
    "QP-CTS-TWICE": "compute_table_stats second run on same snapshot",
    "QP-CPS-DEFAULT": "compute_partition_stats (table)",
    "QP-CPS-SNAPSHOT": "compute_partition_stats snapshot_id",
    "QP-CPS-UNPARTITIONED": "compute_partition_stats unpartitioned table",
    "QP-CPS-V3": "compute_partition_stats format v3",
    "QP-RTP-DEFAULT": "rewrite_table_path (source_prefix, target_prefix)",
    "QP-RTP-STAGING": "rewrite_table_path staging_location",
    "QP-RTP-NO-FILE-LIST": "rewrite_table_path create_file_list false",
    "QP-RTP-MISSING-PREFIX-ERR": "rewrite_table_path wrong source_prefix raises",
    "QP-RTP-END-VERSION": "rewrite_table_path end_version",
    "QP-RTP-START-VERSION": "rewrite_table_path start_version incremental",
    "QP-RTP-MOR-DELETES": "rewrite_table_path with position deletes",
}

RTP_CATALOGS = {
    "QP-RTP-DEFAULT",
    "QP-RTP-STAGING",
    "QP-RTP-NO-FILE-LIST",
    "QP-RTP-MISSING-PREFIX-ERR",
    "QP-RTP-END-VERSION",
    "QP-RTP-START-VERSION",
    "QP-RTP-MOR-DELETES",
}


def error_record(kind: str, exc: BaseException, step: str) -> dict[str, Any]:
    """Build the error cell record with the JVM exception shape."""
    record: dict[str, Any] = {"type": type(exc).__name__}
    for attr in ("getCondition", "getErrorClass", "getSqlState"):
        func = getattr(exc, attr, None)
        record[attr] = None
        if callable(func):
            try:
                record[attr] = func()
            except Exception:
                record[attr] = None
    message = str(exc).strip()
    message = re.sub(r"\n\s*(JVM stacktrace|at |\tat ).*", "", message, flags=re.S)
    record["msg"] = message[:700]
    return {"kind": kind, "error": record, "error_step": step}


def cell_catalog(cell_id: str, family: str) -> str:
    """Return the catalog door for one oracle cell."""
    if cell_id in RTP_CATALOGS or family == "partition-stats":
        return "hc"
    return "sc"


def record_cell(
    session: Any, warehouse: Path, cell_id: str, family: str, kind: str
) -> dict[str, Any]:
    """Replay one oracle cell and return its record."""
    context = OracleContext(session, warehouse, cell_id, cell_catalog(cell_id, family))
    try:
        if family == "ancestors":
            record_ancestors(context, kind)
        elif family == "table-stats":
            record_table_stats(context, kind)
        elif family == "partition-stats":
            record_partition_stats(context, kind)
        else:
            record_rewrite_path(context, kind)
        return {
            "id": cell_id,
            "group": "QPROC",
            "title": CELL_TITLES[cell_id],
            "engine": "spark",
            "status": "ok",
            "obs": context.obs,
            "notes": [],
            "secs": 0.0,
        }
    except Exception as exc:
        entry = error_record(kind, exc, context.step)
        if family == "rewrite-path":
            source, target, staging = rewrite_path_prefixes(context)
            entry["error"]["msg"] = relative_path(
                entry["error"]["msg"], source, target, staging, context.warehouse
            )
        return {
            "id": cell_id,
            "group": "QPROC",
            "title": CELL_TITLES[cell_id],
            "engine": "spark",
            "status": "error",
            "error": entry["error"],
            "error_step": entry["error_step"],
            "obs": {},
            "notes": [],
            "secs": 0.0,
        }


def record_oracle(warehouse: Path) -> list[dict[str, Any]]:
    """Derive all 27 oracle cells on live Spark and return the records."""
    session = live_session(warehouse)
    for catalog in ("sc", "hc"):
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns").collect()
    try:
        return [
            record_cell(session, warehouse, cell, family, kind) for cell, family, kind in CELL_KINDS
        ]
    finally:
        session.stop()


def drop_secs(record: dict[str, Any]) -> dict[str, Any]:
    """Return one oracle record without its measured timing."""
    return {key: value for key, value in record.items() if key != "secs"}


def check_oracle(warehouse: Path, fixture: list[dict[str, Any]]) -> None:
    """Re-derive the oracle on live Spark and fail naming the first mismatch."""
    recorded = [drop_secs(record) for record in record_oracle(warehouse)]
    expected = [drop_secs(record) for record in fixture]
    want = {r["id"]: r for r in expected}
    for record in recorded:
        cell_id = str(record["id"])
        assert cell_id in want, f"recorded cell {cell_id} is not in the fixture"
        assert record == want[cell_id], f"cell {cell_id} differs from the fixture"
    assert {r["id"] for r in recorded} == set(want), "cell id sets differ"


def main(argv: list[str]) -> int:
    """Run the ``record`` or ``check`` command over a scratch warehouse."""
    parser = argparse.ArgumentParser(description="Re-derive the ICE-PROCS-ROUTE-1 oracle.")
    parser.add_argument("--warehouse", required=True, type=Path)
    parser.add_argument("command", choices=("record", "check"))
    args = parser.parse_args(argv)
    if args.command == "record":
        print(json.dumps(record_oracle(args.warehouse), indent=1))
        return 0
    check_oracle(args.warehouse, json.loads(FIXTURE.read_text(encoding="utf-8")))
    print("oracle check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
