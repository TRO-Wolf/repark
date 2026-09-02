"""MW-7 scale measurement: what a merge-on-read table costs as MERGEs accumulate.

Measure-only: drives the shipped `CALL` procedures and the shipped scan path. Each
leg is a partitioned format-v2 table — merge-on-read (MOR) against a copy-on-write
(COW) control — run through N MERGEs with a census and timed scans every
`checkpoint_every` merges, then the full maintenance sequence with a per-step
census, then the same scans again.

The MOR leg sets `write.delete.granularity = 'partition'` so the arithmetic is one
position-delete file per `(spec, partition)` per commit (Spark's unset default is
`file`, registry row `MOR-2`). Read the MOR-minus-COW gap as what merge-on-read
costs on READ, not delete-file cost alone: every MERGE also appends data files
instead of rewriting in place. Separating the two needs a compacting third leg this
driver does not run.

Wall-clock here is one machine's number, never a CI pin. Ratios are the deliverable.
"""

from __future__ import annotations

import resource
import statistics
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import BaseModel

from repark import ReparkSession
from repark.errors import UnsupportedOperationException

# Iceberg `FileContent`. `files` reports 0 for data and 1 for position deletes.
DATA_CONTENT = 0
POSITION_DELETE_CONTENT = 1

# `expire_snapshots` is driven by `retain_last`, not file age, so push `older_than` one
# day into the future.
EXPIRE_OLDER_THAN_FUTURE_MS = 86_400_000

# `remove_orphan_files` enforces Spark's 24-hour floor (MW-3); 25 hours clears it. On a
# fresh warehouse the dry run lists zero files by construction.
ORPHAN_OLDER_THAN_PAST_MS = 25 * 60 * 60 * 1000

ALLOW_CREATE_FORMAT_VERSION_3_KEY = "repark.sql.allowCreateFormatVersion3"
DEFAULT_FORMAT_VERSION = 2
REFUSING_ON_V3_PROCEDURE = "rewrite_position_delete_files"
STARTED_AT_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

# pins: mw-9-delete-granularity/C-008
MOR_PROPERTIES = (
    "'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read', "
    "'write.delete.granularity' = 'partition'"
)
COW_PROPERTIES = (
    "'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', "
    "'write.merge.mode' = 'copy-on-write'"
)

# `value` and `amount` are hashes of `id` scaled by the uint64 span into [0, 1). Fixed
# seeds keep a re-run reproducible.
UINT64_SPAN = 1.8446744073709552e19
VALUE_HASH_SEED = 11
AMOUNT_HASH_SEED = 22
QUANTITY_HASH_SEED = 33
VALUE_RANGE = 1000.0

# Untimed passes before the timed repetitions at every checkpoint. See `time_sql`.
SCAN_WARMUPS = 1

# The narrow id window the point-lookup probe reads. Wide enough to return rows from every
# partition, narrow enough that Iceberg prunes to a handful of data files.
POINT_WINDOW_IDS = 2_000


class ScanTiming(BaseModel):
    """Wall milliseconds for one scan repeated `reps` times, plus the answer it gave."""

    label: str
    sql: str
    reps: int
    warmups: int
    p50_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    samples_ms: list[float]
    answer: list[Any]


class FileCensus(BaseModel):
    """What the table's metadata tables say the table is made of."""

    data_files: int
    data_bytes: int
    delete_files: int
    delete_bytes: int
    delete_records: int
    manifests: int
    manifest_list_bytes: int
    snapshots: int


class Checkpoint(BaseModel):
    """One measurement point: the census plus the timed scans after `merges_done` MERGEs."""

    merges_done: int
    census: FileCensus
    row_count: int
    scans: list[ScanTiming]


class MaintenanceStep(BaseModel):
    """One `CALL` in the maintenance sequence and the table census it left behind."""

    procedure: str
    sql: str
    wall_seconds: float
    result_rows: int
    result_first_row: dict[str, Any]
    refusal: str = ""
    census_after: FileCensus


class LegResult(BaseModel):
    """One write-mode leg end to end."""

    mode: str
    table: str
    format_version: int
    rows: int
    partitions: int
    merges: int
    rows_per_merge: int
    target_file_size_bytes: int
    ctas_seconds: float
    merge_seconds: list[float]
    checkpoints: list[Checkpoint]
    maintenance: list[MaintenanceStep]
    after_maintenance: Checkpoint
    warehouse_bytes_before_maintenance: int
    warehouse_bytes_after_maintenance: int
    peak_rss_bytes: int
    wall_seconds: float


class RunResult(BaseModel):
    """Every leg of one MW-7 run, with the parameters that produced it."""

    started_at: str
    host_note: str
    format_version: int
    rows: int
    merges: int
    partitions: int
    rows_per_merge: int
    checkpoint_every: int
    reps: int
    target_file_size_bytes: int
    legs: list[LegResult]
    peak_rss_bytes: int
    wall_seconds: float


def peak_rss_bytes() -> int:
    """Peak resident set size of this process so far, in bytes.

    `ru_maxrss` is kilobytes on Linux; the value is a process-wide high-water mark.
    """
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


def directory_bytes(root: Path) -> int:
    """Total size of every regular file under `root`."""
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def seed_frame(rows: int, partitions: int) -> pl.DataFrame:
    """The seed table: `rows` rows of the six-column schema, `part = id % partitions`.

    Built as vectorised polars: Python lists cost minutes and gigabytes at 1e7 rows, and a
    SQL `VALUES` literal re-plans on every action. `value`/`amount` are deterministic
    hashes of `id` cast to doubles; they are used because they do not compress — a table
    that compresses to nothing writes one data file per partition and cannot show what
    delete-file layout costs.

    Args:
        rows: number of rows to build.
        partitions: identity-partition cardinality.

    Returns:
        A polars frame with columns `id, part, value, amount, quantity, name`.
    """
    return pl.select(id=pl.int_range(0, rows, dtype=pl.Int64)).with_columns(
        part=(pl.col("id") % partitions).cast(pl.Int32),
        value=pl.col("id").hash(seed=VALUE_HASH_SEED).cast(pl.Float64) / UINT64_SPAN * VALUE_RANGE,
        amount=pl.col("id").hash(seed=AMOUNT_HASH_SEED).cast(pl.Float64) / UINT64_SPAN * 10_000.0,
        quantity=(pl.col("id").hash(seed=QUANTITY_HASH_SEED) % 10_000).cast(pl.Int32),
        name=pl.lit("n") + pl.col("id").cast(pl.Utf8),
    )


def merge_frame(start_id: int, count: int, partitions: int, generation: int) -> pl.DataFrame:
    """The source of one MERGE: `count` consecutive ids starting at `start_id`.

    A contiguous id window is what a batch upsert pipeline produces, and keeps the COW leg
    tractable — a scattered 2 % rewrites every data file on every merge. `part = id %
    partitions` still spreads the window over every partition. `generation` is folded into
    every mutable column, so each MERGE really changes each row it matches.

    Args:
        start_id: first id in the window.
        count: how many ids the window covers.
        partitions: identity-partition cardinality (must match the table).
        generation: merge number, 1-based.

    Returns:
        A polars frame with the table's six columns.
    """
    return pl.select(id=pl.int_range(start_id, start_id + count, dtype=pl.Int64)).with_columns(
        part=(pl.col("id") % partitions).cast(pl.Int32),
        value=(
            pl.col("id").hash(seed=VALUE_HASH_SEED + generation).cast(pl.Float64)
            / UINT64_SPAN
            * VALUE_RANGE
        ),
        amount=(
            pl.col("id").hash(seed=AMOUNT_HASH_SEED + generation).cast(pl.Float64)
            / UINT64_SPAN
            * 10_000.0
        ),
        quantity=(pl.col("id").hash(seed=QUANTITY_HASH_SEED + generation) % 10_000).cast(pl.Int32),
        name=pl.lit(f"m{generation}_") + pl.col("id").cast(pl.Utf8),
    )


def register_parquet_view(spark: ReparkSession, frame: pl.DataFrame, path: Path, view: str) -> None:
    """Write `frame` to `path` as Parquet and register it as temp view `view`."""
    frame.write_parquet(path)
    spark.read.parquet(str(path)).createOrReplaceTempView(view)


def create_table(
    spark: ReparkSession,
    table: str,
    view: str,
    mode: str,
    target_file_size_bytes: int,
    format_version: int = DEFAULT_FORMAT_VERSION,
) -> float:
    """CTAS the partitioned table for one leg. Returns the wall seconds.

    Args:
        spark: an open session with the catalog and namespace already registered.
        table: fully-qualified table name.
        view: temp view holding the seed rows.
        mode: `"mor"` or `"cow"` — selects the three write-mode properties.
        target_file_size_bytes: `write.target-file-size-bytes`, set explicitly so a
            1e7-row table holds several data files per partition; the default gives one
            file per partition and delete attachment then has nothing to attach to.

    Returns:
        Wall seconds for the CTAS.
    """
    write_modes = MOR_PROPERTIES if mode == "mor" else COW_PROPERTIES
    started = time.perf_counter()
    spark.sql(
        f"CREATE TABLE {table} USING iceberg PARTITIONED BY (part) TBLPROPERTIES ("
        f"'format-version' = '{format_version}', {write_modes}, "
        f"'write.target-file-size-bytes' = '{target_file_size_bytes}'"
        f") AS SELECT * FROM {view}"
    )
    return time.perf_counter() - started


def merge_sql(table: str, view: str) -> str:
    """The keyed upsert every merge runs (`UPDATE SET *` / `INSERT *` on `id`)."""
    return (
        f"MERGE INTO {table} AS target USING {view} AS source ON target.id = source.id "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def file_census(spark: ReparkSession, table: str) -> FileCensus:
    """Read the table's file, manifest and snapshot counts from its metadata tables.

    `manifest_list_bytes` is the CURRENT snapshot's manifest list on disk — the file a
    reader opens first, so the one metadata size a scan always pays.
    """
    files = spark.sql(
        f"SELECT content, file_size_in_bytes, record_count FROM {table}.files"
    ).to_arrow()
    contents = files.column("content").to_pylist()
    sizes = files.column("file_size_in_bytes").to_pylist()
    records = files.column("record_count").to_pylist()

    data_files = 0
    data_bytes = 0
    delete_files = 0
    delete_bytes = 0
    delete_records = 0
    for content, size, record_count in zip(contents, sizes, records, strict=True):
        if content is None:
            continue
        if int(content) == DATA_CONTENT:
            data_files += 1
            data_bytes += int(size or 0)
        elif int(content) == POSITION_DELETE_CONTENT:
            delete_files += 1
            delete_bytes += int(size or 0)
            delete_records += int(record_count or 0)

    manifests = spark.sql(f"SELECT COUNT(*) AS n FROM {table}.manifests").to_arrow()
    snapshots = spark.sql(f"SELECT COUNT(*) AS n FROM {table}.snapshots").to_arrow()
    return FileCensus(
        data_files=data_files,
        data_bytes=data_bytes,
        delete_files=delete_files,
        delete_bytes=delete_bytes,
        delete_records=delete_records,
        manifests=int(manifests.column("n")[0].as_py()),
        manifest_list_bytes=current_manifest_list_bytes(spark, table),
        snapshots=int(snapshots.column("n")[0].as_py()),
    )


def current_manifest_list_bytes(spark: ReparkSession, table: str) -> int:
    """Size on disk of the newest snapshot's manifest list, or 0 if there is none."""
    listing = spark.sql(
        f"SELECT manifest_list FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).to_arrow()
    if listing.num_rows == 0:
        return 0
    location = listing.column("manifest_list")[0].as_py()
    if not isinstance(location, str):
        return 0
    path = Path(location.removeprefix("file://"))
    return path.stat().st_size if path.is_file() else 0


def time_sql(
    spark: ReparkSession,
    label: str,
    sql: str,
    reps: int,
    warmups: int = SCAN_WARMUPS,
) -> ScanTiming:
    """Run `sql` `warmups` times untimed, then `reps` times timed, on the Arrow path.

    The warm-up is load-bearing: without it the merge-0 baseline is the only checkpoint
    whose files were never read, so every "N merges cost this much more" ratio is
    computed against a number from a different regime. One untimed pass at EVERY
    checkpoint makes the checkpoints comparable, which is what a ratio needs.

    `p99` over a small `reps` is the maximum by construction; that is stated rather than
    interpolated, which would invent precision.
    """
    for _ in range(warmups):
        spark.sql(sql).to_arrow()
    samples: list[float] = []
    answer: list[Any] = []
    for _ in range(reps):
        started = time.perf_counter()
        arrow = spark.sql(sql).to_arrow()
        samples.append((time.perf_counter() - started) * 1000.0)
        answer = arrow.to_pylist()
    ordered = sorted(samples)
    percentile_index = min(len(ordered) - 1, round(0.99 * (len(ordered) - 1)))
    return ScanTiming(
        label=label,
        sql=sql,
        reps=reps,
        warmups=warmups,
        p50_ms=statistics.median(ordered),
        p99_ms=ordered[percentile_index],
        min_ms=ordered[0],
        max_ms=ordered[-1],
        samples_ms=samples,
        answer=answer,
    )


def scan_specs(table: str, rows: int, partitions: int) -> list[tuple[str, str]]:
    """The fixed scans every checkpoint runs, as `(label, sql)` pairs.

    * `count_star` — the MW-0/MW-5 continuity probe; must answer `rows` forever.
    * `predicate_partition` — the charter's fixed predicate scan; prunes to one
      partition's data files and delete files.
    * `predicate_point` — narrow id window; data files prune, but `partition`-granularity
      deletes force a read of EVERY delete file in the touched partitions. This probe
      decides whether MW-9 is urgent.

    Both predicates aggregate the integer `quantity`: summing float `value` moves by one
    ULP across `rewrite_data_files` (order-dependent float addition, docs/testing.md
    "float aggregation across partitions") and would break the exact before/after
    identity check.
    """
    probe_partition = partitions // 2
    point_start = rows // 2
    point_end = point_start + POINT_WINDOW_IDS - 1
    return [
        ("count_star", f"SELECT COUNT(*) AS n FROM {table}"),
        (
            "predicate_partition",
            f"SELECT COUNT(*) AS n, SUM(quantity) AS s FROM {table} "
            f"WHERE part = {probe_partition} AND value >= 500.0",
        ),
        (
            "predicate_point",
            f"SELECT COUNT(*) AS n, SUM(quantity) AS s FROM {table} "
            f"WHERE id BETWEEN {point_start} AND {point_end}",
        ),
    ]


def checkpoint(
    spark: ReparkSession,
    table: str,
    merges_done: int,
    rows: int,
    partitions: int,
    reps: int,
) -> Checkpoint:
    """Census the table, then run every fixed scan `reps` times."""
    census = file_census(spark, table)
    scans = [
        time_sql(spark, label, sql, reps) for label, sql in scan_specs(table, rows, partitions)
    ]
    count_scan = next(scan for scan in scans if scan.label == "count_star")
    return Checkpoint(
        merges_done=merges_done,
        census=census,
        row_count=int(count_scan.answer[0]["n"]),
        scans=scans,
    )


def run_maintenance_step(
    spark: ReparkSession,
    table: str,
    procedure: str,
    sql: str,
    capture_refusal: bool = False,
) -> MaintenanceStep:
    """Run one maintenance `CALL`, time it, and census the table afterwards."""
    started = time.perf_counter()
    try:
        result = spark.sql(sql).to_arrow()
    except UnsupportedOperationException as refused:
        if not capture_refusal:
            raise
        return MaintenanceStep(
            procedure=procedure,
            sql=sql,
            wall_seconds=time.perf_counter() - started,
            result_rows=0,
            result_first_row={},
            refusal=str(refused),
            census_after=file_census(spark, table),
        )
    wall = time.perf_counter() - started
    rows = result.to_pylist()
    return MaintenanceStep(
        procedure=procedure,
        sql=sql,
        wall_seconds=wall,
        result_rows=result.num_rows,
        result_first_row=rows[0] if rows else {},
        census_after=file_census(spark, table),
    )


def maintenance_sequence(
    catalog: str, table_arg: str, clock: Callable[[], float] = time.time
) -> list[tuple[str, str]]:
    """The Airflow-shaped sequence MW-8 will document, as `(procedure, sql)` pairs.

    Order is load-bearing: fold the delete files first so `rewrite_data_files` rewrites
    fewer of them, compact the data, re-cluster the churned manifests, expire snapshots,
    then look for orphans. `remove_orphan_files` runs LAST in its dry-run default — the
    one procedure with no undo.
    """
    expire_older_than = int(clock() * 1000) + EXPIRE_OLDER_THAN_FUTURE_MS
    orphan_older_than = int(clock() * 1000) - ORPHAN_OLDER_THAN_PAST_MS
    return [
        (
            "rewrite_position_delete_files",
            f"CALL {catalog}.system.rewrite_position_delete_files(table => '{table_arg}')",
        ),
        ("rewrite_data_files", f"CALL {catalog}.system.rewrite_data_files(table => '{table_arg}')"),
        ("rewrite_manifests", f"CALL {catalog}.system.rewrite_manifests(table => '{table_arg}')"),
        (
            "expire_snapshots",
            f"CALL {catalog}.system.expire_snapshots(table => '{table_arg}', "
            f"older_than => {expire_older_than}, retain_last => 1)",
        ),
        (
            "remove_orphan_files",
            f"CALL {catalog}.system.remove_orphan_files(table => '{table_arg}', "
            f"older_than => {orphan_older_than})",
        ),
    ]


def run_leg(
    spark: ReparkSession,
    warehouse: Path,
    scratch: Path,
    catalog: str,
    namespace: str,
    mode: str,
    rows: int,
    merges: int,
    partitions: int,
    rows_per_merge: int,
    checkpoint_every: int,
    reps: int,
    target_file_size_bytes: int,
    format_version: int,
    clock: Callable[[], float],
) -> LegResult:
    """Drive one write-mode leg: CTAS, N MERGEs with checkpoints, maintenance, re-scan.

    Args:
        spark: an open session.
        warehouse: the leg's warehouse root, used for the on-disk byte totals.
        scratch: directory for the Parquet seed and per-merge source files.
        catalog: registered catalog name.
        namespace: namespace holding the leg's table.
        mode: `"mor"` or `"cow"`.
        rows: seed row count.
        merges: how many MERGEs to run.
        partitions: identity-partition cardinality.
        rows_per_merge: ids each MERGE window covers.
        checkpoint_every: measure after every this-many merges.
        reps: repetitions per timed scan.
        target_file_size_bytes: `write.target-file-size-bytes` on the CTAS.

    Returns:
        Every number the leg produced.
    """
    leg_started = time.perf_counter()
    table_name = f"scale_{mode}"
    table = f"{catalog}.{namespace}.{table_name}"
    table_arg = f"{namespace}.{table_name}"
    seed_view = f"mw7_seed_{mode}"
    merge_view = f"mw7_merge_{mode}"

    register_parquet_view(
        spark, seed_frame(rows, partitions), scratch / f"seed_{mode}.parquet", seed_view
    )
    ctas_seconds = create_table(
        spark, table, seed_view, mode, target_file_size_bytes, format_version
    )
    spark.catalog.dropTempView(seed_view)

    checkpoints = [checkpoint(spark, table, 0, rows, partitions, reps)]
    merge_seconds: list[float] = []
    merge_source = scratch / f"merge_{mode}.parquet"
    for merge_index in range(1, merges + 1):
        start_id = ((merge_index - 1) * rows_per_merge) % rows
        window = min(rows_per_merge, rows - start_id)
        register_parquet_view(
            spark, merge_frame(start_id, window, partitions, merge_index), merge_source, merge_view
        )
        started = time.perf_counter()
        spark.sql(merge_sql(table, merge_view))
        merge_seconds.append(time.perf_counter() - started)
        spark.catalog.dropTempView(merge_view)
        if merge_index % checkpoint_every == 0 or merge_index == merges:
            checkpoints.append(checkpoint(spark, table, merge_index, rows, partitions, reps))

    warehouse_before = directory_bytes(warehouse)
    maintenance = [
        run_maintenance_step(
            spark,
            table,
            procedure,
            sql,
            format_version >= 3 and procedure == REFUSING_ON_V3_PROCEDURE,
        )
        for procedure, sql in maintenance_sequence(catalog, table_arg, clock)
    ]
    warehouse_after = directory_bytes(warehouse)
    after = checkpoint(spark, table, merges, rows, partitions, reps)

    return LegResult(
        mode=mode,
        table=table,
        format_version=format_version,
        rows=rows,
        partitions=partitions,
        merges=merges,
        rows_per_merge=rows_per_merge,
        target_file_size_bytes=target_file_size_bytes,
        ctas_seconds=ctas_seconds,
        merge_seconds=merge_seconds,
        checkpoints=checkpoints,
        maintenance=maintenance,
        after_maintenance=after,
        warehouse_bytes_before_maintenance=warehouse_before,
        warehouse_bytes_after_maintenance=warehouse_after,
        peak_rss_bytes=peak_rss_bytes(),
        wall_seconds=time.perf_counter() - leg_started,
    )


def run_scale_measurement(
    root: Path,
    rows: int,
    merges: int,
    partitions: int,
    touch_fraction: float,
    checkpoint_every: int,
    reps: int,
    target_file_size_bytes: int,
    modes: list[str],
    host_note: str = "",
    format_version: int = DEFAULT_FORMAT_VERSION,
    clock: Callable[[], float] = time.time,
) -> RunResult:
    """Run every requested leg in one process and return the whole measurement.

    One process on purpose: peak RSS is a process-wide high-water mark.

    Args:
        root: scratch root holding the warehouse and Parquet directories; the caller
            deletes the whole tree afterwards. Never a committed path.
        rows: seed rows per leg.
        merges: MERGEs per leg.
        partitions: identity-partition cardinality.
        touch_fraction: fraction of `rows` each MERGE touches (the charter's ~2 %).
        checkpoint_every: measure after every this-many merges.
        reps: repetitions per timed scan (the charter's floor is 5).
        target_file_size_bytes: `write.target-file-size-bytes` on each CTAS.
        modes: which legs to run, in order — any of `"mor"`, `"cow"`.
        host_note: free text recorded with the result (machine, date, anything a reader
            needs to know the numbers are not portable).

    Returns:
        The full result, ready to serialise to JSON.
    """
    started_wall = time.perf_counter()
    started_at = time.strftime(STARTED_AT_FORMAT, time.localtime(clock()))
    rows_per_merge = max(1, int(rows * touch_fraction))
    scratch = root / "parquet"
    scratch.mkdir(parents=True, exist_ok=True)

    legs: list[LegResult] = []
    for mode in modes:
        warehouse = root / f"warehouse_{mode}"
        warehouse.mkdir(parents=True, exist_ok=True)
        catalog = f"mw7_{mode}"
        namespace = "ns"
        builder = ReparkSession.builder.appName(f"mw7-scale-{mode}")
        if format_version >= 3:
            builder = builder.config(ALLOW_CREATE_FORMAT_VERSION_3_KEY, "true")
        session = builder.getOrCreate()
        try:
            session.register_memory_catalog(catalog, str(warehouse))
            session.sql(
                f"CREATE NAMESPACE {catalog}.{namespace} LOCATION '{warehouse / namespace}'"
            )
            legs.append(
                run_leg(
                    session,
                    warehouse,
                    scratch,
                    catalog,
                    namespace,
                    mode,
                    rows,
                    merges,
                    partitions,
                    rows_per_merge,
                    checkpoint_every,
                    reps,
                    target_file_size_bytes,
                    format_version,
                    clock,
                )
            )
        finally:
            session.stop()

    return RunResult(
        started_at=started_at,
        host_note=host_note,
        format_version=format_version,
        rows=rows,
        merges=merges,
        partitions=partitions,
        rows_per_merge=rows_per_merge,
        checkpoint_every=checkpoint_every,
        reps=reps,
        target_file_size_bytes=target_file_size_bytes,
        legs=legs,
        peak_rss_bytes=peak_rss_bytes(),
        wall_seconds=time.perf_counter() - started_wall,
    )
