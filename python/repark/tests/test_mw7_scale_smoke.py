"""MW-7: prove the scale-measurement driver's metric collection tells the truth.

The MW-7 numbers were measured at 1e7 rows x 50 MERGEs, which no gate can run. This module
runs the SAME driver at a scale CI can afford and pins the machinery: the census counts what
the metadata tables actually hold, delete files grow one per partition per MERGE and
compaction reclaims them, the manifest count drops when `rewrite_manifests` runs, the
copy-on-write leg is a zero-delete control, and a timing is never recorded for a scan that
answered differently.

The 1e7 wall-clock figures live in
`task/ledgers/completed/mw-7-scale-measurement-ledger.md` as dated MEASUREMENTS — one
machine's numbers, deliberately NOT asserted here: a timing pin on CI hardware is not the
MW-7 claim (`test_mw5_baseline_delta.py` precedent).

pins: rp-6-fork-repin/C-004
"""

from __future__ import annotations

import os
import shutil
import sys
import time
import types
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

pytest.importorskip("polars")

from repark import ReparkSession
from repark.errors import UnsupportedOperationException

# pins: mw-7-scale-measurement/C-001, C-002, C-003, C-004, C-005
# pins: mw-7-scale-measurement/C-006, C-007, C-008, C-009, C-010, C-011
# pins: rp-3-fork-repin/C-006

# The driver lives beside the other measurement harnesses, under
# python/repark-parity/bench/. That tree is not on `repark_parity`'s import path, so it is
# loaded as a synthetic package — the same shim `test_tpch_smoke.py` uses.
_BENCH_DIR = Path(__file__).resolve().parents[2] / "repark-parity" / "bench"

# Small enough for a gate, large enough that every claim has something to bite on: six
# MERGEs over two partitions leave twelve position-delete files, which is above the
# min-input-files floor `rewrite_position_delete_files` needs before it folds anything.
SMOKE_ROWS = 20_000
SMOKE_MERGES = 6
SMOKE_PARTITIONS = 2
SMOKE_TOUCH_FRACTION = 0.02
SMOKE_CHECKPOINT_EVERY = 3
SMOKE_REPS = 3
SMOKE_TARGET_FILE_SIZE = 256 * 1024

MAINTENANCE_ORDER = [
    "rewrite_position_delete_files",
    "rewrite_data_files",
    "rewrite_manifests",
    "expire_snapshots",
    "remove_orphan_files",
]

ONE_DAY_MS = 24 * 60 * 60 * 1000

# C-011's fixture. 2,500 rows of the six-column schema write ONE ~68 KB data file, which sits
# inside the bin-pack band for a 64 KiB target — so only the delete-RATIO clause can ever make
# it a candidate, and a MERGE that deletes every one of its rows is what raises that ratio.
# Java's band, from `BinPackRewriteFilePlanner`: [0.75 x target, 1.8 x target].
C011_ROWS = 2_500
C011_TARGET_FILE_SIZE = 64 * 1024
C011_BAND_LOW = 0.75
C011_BAND_HIGH = 1.8

DELETE_FILE_PATH_FIELD_ID = 2147483546

V3_ALLOW_CREATE_KEY = "repark.sql.allowCreateFormatVersion3"
V3_SMOKE_REPS = 1
V3_DV_MERGES = 2
STARTED_AT_ROWS = 500
STARTED_AT_MERGES = 1
STARTED_AT_STEP_SECONDS = 2.0
STARTED_AT_BACKDATE_SECONDS = 86_400.0
V3_DELETE_FILE_FORMAT = "PUFFIN"
V3_SEEDED_DATA_FILES = SMOKE_PARTITIONS
V3_ORACLE_ROWS = 4_000
V3_ORACLE_MERGES = 3
V3_ORACLE_ROWS_PER_MERGE = int(V3_ORACLE_ROWS * SMOKE_TOUCH_FRACTION)
V3_TABLE_PROPERTIES = (
    "'format-version' = '3', "
    "'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', "
    "'write.merge.mode' = 'merge-on-read', "
    "'write.delete.granularity' = 'partition', "
    f"'write.target-file-size-bytes' = '{SMOKE_TARGET_FILE_SIZE}'"
)
LIVE = os.environ.get("REPARK_PARITY_LIVE") == "1"
LIVE_SKIP = "REPARK_PARITY_LIVE != 1 — the live v3 scale oracle is skipped (CI is JVM-free)"


def _load_measure() -> Any:
    """Import `mw7.measure` from the bench tree as a synthetic package."""
    import importlib

    package_name = "repark_mw7_bench"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_BENCH_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package
    return importlib.import_module(f"{package_name}.mw7.measure")


measure = _load_measure()


@pytest.fixture(scope="module")
def smoke_run(tmp_path_factory: pytest.TempPathFactory) -> Any:
    """One full driver run at smoke scale: both legs, all checkpoints, all maintenance."""
    root = tmp_path_factory.mktemp("mw7-smoke")
    return measure.run_scale_measurement(
        root=root,
        rows=SMOKE_ROWS,
        merges=SMOKE_MERGES,
        partitions=SMOKE_PARTITIONS,
        touch_fraction=SMOKE_TOUCH_FRACTION,
        checkpoint_every=SMOKE_CHECKPOINT_EVERY,
        reps=SMOKE_REPS,
        target_file_size_bytes=SMOKE_TARGET_FILE_SIZE,
        modes=["mor", "cow"],
        host_note="pytest smoke",
    )


@pytest.fixture(scope="module")
def v3_smoke_run(tmp_path_factory: pytest.TempPathFactory) -> Any:
    """The same driver run at the same smoke shape, on format-version 3 tables."""
    root = tmp_path_factory.mktemp("mw7-smoke-v3")
    return measure.run_scale_measurement(
        root=root,
        rows=SMOKE_ROWS,
        merges=SMOKE_MERGES,
        partitions=SMOKE_PARTITIONS,
        touch_fraction=SMOKE_TOUCH_FRACTION,
        checkpoint_every=SMOKE_CHECKPOINT_EVERY,
        reps=V3_SMOKE_REPS,
        target_file_size_bytes=SMOKE_TARGET_FILE_SIZE,
        modes=["mor", "cow"],
        host_note="pytest smoke v3",
        format_version=3,
    )


def _v3_session(name: str) -> ReparkSession:
    """An opt-in session that may CREATE format-version 3 tables."""
    return ReparkSession.builder.appName(name).config(V3_ALLOW_CREATE_KEY, "true").getOrCreate()


def _delete_files(spark: ReparkSession, table: str) -> list[tuple[int, str, int, str]]:
    """Live delete files as `(content, file_format, record_count, referenced_data_file)`."""
    arrow = spark.sql(
        f"SELECT content, file_format, record_count, referenced_data_file FROM {table}.files "
        f"WHERE content = 1"
    ).to_arrow()
    return [
        (
            int(row["content"]),
            str(row["file_format"]).upper(),
            int(row["record_count"]),
            str(row["referenced_data_file"]),
        )
        for row in arrow.to_pylist()
    ]


def _build_v3_leg(
    spark: ReparkSession,
    tmp_path: Path,
    catalog: str,
    mode: str,
    rows: int,
    merges: int,
    rows_per_merge: int,
) -> str:
    """CTAS a format-3 leg of `mode` and run `merges` MERGEs over it; returns the table."""
    warehouse = tmp_path / f"wh_{catalog}"
    warehouse.mkdir()
    spark.register_memory_catalog(catalog, str(warehouse))
    spark.sql(f"CREATE NAMESPACE {catalog}.ns LOCATION '{warehouse / 'ns'}'")
    table = f"{catalog}.ns.leg"
    measure.register_parquet_view(
        spark,
        measure.seed_frame(rows, SMOKE_PARTITIONS),
        tmp_path / f"seed_{catalog}.parquet",
        f"seed_{catalog}",
    )
    measure.create_table(spark, table, f"seed_{catalog}", mode, SMOKE_TARGET_FILE_SIZE, 3)
    spark.catalog.dropTempView(f"seed_{catalog}")
    for generation in range(1, merges + 1):
        measure.register_parquet_view(
            spark,
            measure.merge_frame(
                (generation - 1) * rows_per_merge, rows_per_merge, SMOKE_PARTITIONS, generation
            ),
            tmp_path / f"src_{catalog}.parquet",
            f"src_{catalog}",
        )
        spark.sql(measure.merge_sql(table, f"src_{catalog}"))
        spark.catalog.dropTempView(f"src_{catalog}")
    return table


def _leg(run: Any, mode: str) -> Any:
    """The leg of `run` with the given write mode."""
    return next(leg for leg in run.legs if leg.mode == mode)


def _scan(point: Any, label: str) -> Any:
    """The recorded timing for one scan label at one checkpoint."""
    return next(scan for scan in point.scans if scan.label == label)


def _data_files(spark: ReparkSession, table: str) -> list[tuple[str, int, int]]:
    """Live data files as `(path, size_bytes, record_count)`."""
    arrow = spark.sql(
        f"SELECT content, file_path, file_size_in_bytes, record_count FROM {table}.files"
    ).to_arrow()
    return [
        (str(row["file_path"]), int(row["file_size_in_bytes"]), int(row["record_count"]))
        for row in arrow.to_pylist()
        if row["content"] == 0
    ]


def _path_bound(entries: Any) -> str | None:
    """The `file_path` bound in one manifest bounds map, or `None` when it is absent."""
    for key, value in entries or []:
        if int(key) == DELETE_FILE_PATH_FIELD_ID:
            return bytes(value).decode()
    return None


def _position_delete_path_bounds(
    spark: ReparkSession, table: str
) -> list[tuple[str | None, str | None]]:
    """Every live position-delete file's `(lower, upper)` manifest bound on `file_path`."""
    arrow = spark.sql(f"SELECT content, lower_bounds, upper_bounds FROM {table}.files").to_arrow()
    return [
        (_path_bound(row["lower_bounds"]), _path_bound(row["upper_bounds"]))
        for row in arrow.to_pylist()
        if row["content"] == 1
    ]


def test_census_matches_the_metadata_tables(tmp_path: Path) -> None:
    """C-001: the census counts what `files`, `manifests` and the manifest list really hold.

    The driver is only worth reading if its numbers come from the table. This rebuilds a
    tiny merge-on-read table by hand, counts everything independently through SQL and
    `Path.stat`, and requires the driver's census to agree on every field.
    """
    spark = ReparkSession.builder.appName("pytest-mw7-census").getOrCreate()
    try:
        warehouse = tmp_path / "wh"
        warehouse.mkdir()
        spark.register_memory_catalog("mw7c", str(warehouse))
        spark.sql(f"CREATE NAMESPACE mw7c.ns LOCATION '{warehouse / 'ns'}'")
        table = "mw7c.ns.census"

        measure.register_parquet_view(
            spark, measure.seed_frame(2_000, 2), tmp_path / "seed.parquet", "mw7c_seed"
        )
        measure.create_table(spark, table, "mw7c_seed", "mor", SMOKE_TARGET_FILE_SIZE)
        spark.catalog.dropTempView("mw7c_seed")
        measure.register_parquet_view(
            spark, measure.merge_frame(0, 200, 2, 1), tmp_path / "src.parquet", "mw7c_src"
        )
        spark.sql(measure.merge_sql(table, "mw7c_src"))
        spark.catalog.dropTempView("mw7c_src")

        census = measure.file_census(spark, table)

        files = spark.sql(
            f"SELECT content, file_path, file_size_in_bytes, record_count FROM {table}.files"
        ).to_arrow()
        contents = files.column("content").to_pylist()
        paths = files.column("file_path").to_pylist()
        sizes = files.column("file_size_in_bytes").to_pylist()
        records = files.column("record_count").to_pylist()
        expected_data = [index for index, value in enumerate(contents) if value == 0]
        expected_deletes = [index for index, value in enumerate(contents) if value == 1]

        assert census.data_files == len(expected_data)
        assert census.data_bytes == sum(sizes[index] for index in expected_data)
        assert census.data_file_paths == sorted(str(paths[index]) for index in expected_data)
        assert census.delete_files == len(expected_deletes)
        assert census.delete_bytes == sum(sizes[index] for index in expected_deletes)
        assert census.delete_records == sum(records[index] for index in expected_deletes)
        assert census.delete_files > 0, "the fixture must leave the census something to count"

        manifests = spark.sql(f"SELECT COUNT(*) AS n FROM {table}.manifests").to_arrow()
        assert census.manifests == int(manifests.column("n")[0].as_py())
        snapshots = spark.sql(f"SELECT COUNT(*) AS n FROM {table}.snapshots").to_arrow()
        assert census.snapshots == int(snapshots.column("n")[0].as_py())

        newest = spark.sql(
            f"SELECT manifest_list FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).to_arrow()
        location = str(newest.column("manifest_list")[0].as_py())
        on_disk = Path(location.removeprefix("file://"))
        assert census.manifest_list_bytes == on_disk.stat().st_size
        assert census.manifest_list_bytes > 0
    finally:
        spark.stop()


def test_delete_files_grow_one_per_partition_per_merge(smoke_run: Any) -> None:
    """C-002: MOR delete files = partitions x merges, and `COUNT(*)` never moves.

    The MOR fixture sets `write.delete.granularity = 'partition'`, so delete files
    equal `partitions x merges`. That is the MW-7 measurement; Spark's unset default
    is `file` (MOR-2, closed by MW-9). The arithmetic below IS that layout.
    """
    leg = _leg(smoke_run, "mor")
    for point in leg.checkpoints:
        assert point.census.delete_files == point.merges_done * SMOKE_PARTITIONS, (
            f"after {point.merges_done} MERGEs: {point.census.delete_files} delete files, "
            f"expected {point.merges_done * SMOKE_PARTITIONS}"
        )
        assert point.row_count == SMOKE_ROWS
    final = leg.checkpoints[-1]
    assert final.merges_done == SMOKE_MERGES
    assert final.census.delete_records == SMOKE_MERGES * leg.rows_per_merge


def test_copy_on_write_leg_is_a_zero_delete_control(smoke_run: Any) -> None:
    """C-003: the COW leg writes no delete files — the zero-delete control.

    MOR-minus-COW bundles the delete-read cost with the data-file fan-out
    merge-on-read leaves behind; this unit does not separate the two.
    """
    leg = _leg(smoke_run, "cow")
    assert [point.census.delete_files for point in leg.checkpoints] == [0] * len(leg.checkpoints)
    assert leg.after_maintenance.census.delete_files == 0
    assert all(point.row_count == SMOKE_ROWS for point in leg.checkpoints)
    # The control must still be doing the work: COW rewrites data files on every MERGE.
    before = set(leg.checkpoints[0].census.data_file_paths)
    after = set(leg.checkpoints[-1].census.data_file_paths)
    assert after != before, f"the COW leg rewrote no data file across {SMOKE_MERGES} MERGEs"


def test_compaction_reclaims_delete_files_and_data_files(smoke_run: Any) -> None:
    """C-004: the per-step census shows deletes folded to one per partition, data compacted."""
    leg = _leg(smoke_run, "mor")
    steps = {step.procedure: step for step in leg.maintenance}
    before_deletes = leg.checkpoints[-1].census.delete_files
    after_deletes = steps["rewrite_position_delete_files"].census_after.delete_files
    assert before_deletes == SMOKE_MERGES * SMOKE_PARTITIONS
    assert after_deletes == SMOKE_PARTITIONS, (
        f"rewrite_position_delete_files: {before_deletes} -> {after_deletes}, "
        f"expected one delete file per partition"
    )
    before_data = steps["rewrite_position_delete_files"].census_after.data_files
    after_data = steps["rewrite_data_files"].census_after.data_files
    assert after_data < before_data, f"rewrite_data_files: {before_data} -> {after_data}"
    assert leg.after_maintenance.row_count == SMOKE_ROWS


def test_rewrite_manifests_drops_the_manifest_count(smoke_run: Any) -> None:
    """C-005: the manifest count falls across the `rewrite_manifests` step, on both legs."""
    for mode in ("mor", "cow"):
        leg = _leg(smoke_run, mode)
        steps = {step.procedure: step for step in leg.maintenance}
        before = steps["rewrite_data_files"].census_after.manifests
        after = steps["rewrite_manifests"].census_after.manifests
        assert after < before, f"{mode}: rewrite_manifests {before} -> {after} manifests"
        assert steps["rewrite_manifests"].result_rows == 1
        assert set(steps["rewrite_manifests"].result_first_row) == {
            "rewritten_manifests_count",
            "added_manifests_count",
        }


def test_maintenance_is_the_charters_sequence(smoke_run: Any) -> None:
    """C-006: five procedures, the charter's order, orphan cleanup last and dry-run.

    `remove_orphan_files` is the one procedure with no undo. It must stay last, must carry
    no `dry_run` argument (the engine's default is true — registry row `ORPHAN-2`), and its
    `older_than` must clear Spark's 24-hour floor.
    """
    for mode in ("mor", "cow"):
        leg = _leg(smoke_run, mode)
        assert [step.procedure for step in leg.maintenance] == MAINTENANCE_ORDER
    orphan_sql = _leg(smoke_run, "mor").maintenance[-1].sql
    assert "dry_run" not in orphan_sql
    assert measure.ORPHAN_OLDER_THAN_PAST_MS > ONE_DAY_MS
    assert measure.EXPIRE_OLDER_THAN_FUTURE_MS > 0


def test_timings_carry_their_answer_and_are_ordered(smoke_run: Any) -> None:
    """C-007: every timing keeps its samples, is ordered min<=p50<=p99<=max, and carries its answer.

    A wall-clock number attached to a scan nobody checked is how a measurement lies. The
    driver keeps the collected rows next to the timing, and the row set must not drift
    across the maintenance sequence. Every checkpoint is warmed the same way, so a
    checkpoint-to-checkpoint ratio is not a page-cache artefact.
    """
    for mode in ("mor", "cow"):
        leg = _leg(smoke_run, mode)
        for point in [*leg.checkpoints, leg.after_maintenance]:
            for scan in point.scans:
                assert scan.reps == SMOKE_REPS
                assert len(scan.samples_ms) == SMOKE_REPS
                assert scan.warmups >= 1, "a cold baseline is not comparable to a warm one"
                assert scan.min_ms <= scan.p50_ms <= scan.p99_ms <= scan.max_ms
                assert scan.min_ms == min(scan.samples_ms)
                assert scan.max_ms == max(scan.samples_ms)
                assert scan.answer, f"{mode}/{scan.label} recorded a timing with no answer"
        last = leg.checkpoints[-1]
        for label in ("count_star", "predicate_partition", "predicate_point"):
            assert _scan(last, label).answer == _scan(leg.after_maintenance, label).answer, (
                f"{mode}/{label}: maintenance changed the answer"
            )


def test_scan_battery_is_fixed_across_checkpoints(smoke_run: Any) -> None:
    """C-008: the three probes keep identical SQL at every checkpoint.

    A ratio between two checkpoints is only a ratio if the query did not change under it.
    """
    expected = dict(measure.scan_specs("mw7_mor.ns.scale_mor", SMOKE_ROWS, SMOKE_PARTITIONS))
    assert list(expected) == ["count_star", "predicate_partition", "predicate_point"]
    leg = _leg(smoke_run, "mor")
    for point in [*leg.checkpoints, leg.after_maintenance]:
        assert {scan.label: scan.sql for scan in point.scans} == expected


def test_peak_rss_is_a_monotone_high_water_mark(smoke_run: Any) -> None:
    """C-009: RSS is reported as a process-wide peak that never decreases."""
    assert smoke_run.peak_rss_bytes > 0
    peaks = [leg.peak_rss_bytes for leg in smoke_run.legs]
    assert peaks == sorted(peaks)
    assert smoke_run.peak_rss_bytes >= max(peaks)


def test_generated_frames_are_deterministic() -> None:
    """C-010: a re-run rebuilds byte-identical seed and merge frames.

    Reproducibility is the whole reason the generator is checked in and the data is not.
    """
    first = measure.seed_frame(5_000, 4)
    assert first.equals(measure.seed_frame(5_000, 4))
    assert first.columns == ["id", "part", "value", "amount", "quantity", "name"]
    arrow: pa.Table = first.to_arrow()
    assert arrow.schema.field("id").type == pa.int64()
    assert arrow.schema.field("part").type == pa.int32()
    assert arrow.schema.field("value").type == pa.float64()

    source = measure.merge_frame(100, 50, 4, 3)
    assert source.equals(measure.merge_frame(100, 50, 4, 3))
    assert source.columns == first.columns
    # Every mutable column must move with the generation, or a MERGE writes no delete.
    overlap = measure.merge_frame(100, 50, 4, 4)
    assert not source.equals(overlap)


def test_delete_laden_in_band_file_is_rewritten_and_its_delete_file_dies(tmp_path: Path) -> None:
    """C-011: a 100 %-dead in-band file is a candidate; the runbook ends at zero deletes."""
    spark = ReparkSession.builder.appName("pytest-mw7-c011").getOrCreate()
    try:
        warehouse = tmp_path / "wh"
        warehouse.mkdir()
        spark.register_memory_catalog("mw7d", str(warehouse))
        spark.sql(f"CREATE NAMESPACE mw7d.ns LOCATION '{warehouse / 'ns'}'")
        table, table_arg = "mw7d.ns.tile", "ns.tile"

        measure.register_parquet_view(
            spark, measure.seed_frame(C011_ROWS, 1), tmp_path / "seed.parquet", "mw7d_seed"
        )
        measure.create_table(spark, table, "mw7d_seed", "mor", C011_TARGET_FILE_SIZE)
        spark.catalog.dropTempView("mw7d_seed")

        seeded = _data_files(spark, table)
        assert len(seeded) == 1, f"fixture must seed exactly one data file, got {len(seeded)}"
        seeded_path, seeded_size, seeded_records = seeded[0]
        assert seeded_records == C011_ROWS
        assert (
            C011_BAND_LOW * C011_TARGET_FILE_SIZE
            <= seeded_size
            <= C011_BAND_HIGH * C011_TARGET_FILE_SIZE
        ), f"seeded file {seeded_size} B is outside the bin-pack band for {C011_TARGET_FILE_SIZE}"

        measure.register_parquet_view(
            spark, measure.merge_frame(0, C011_ROWS, 1, 1), tmp_path / "src.parquet", "mw7d_src"
        )
        spark.sql(measure.merge_sql(table, "mw7d_src"))
        spark.catalog.dropTempView("mw7d_src")

        after_merge = measure.file_census(spark, table)
        assert after_merge.delete_files == 1
        assert after_merge.delete_records == C011_ROWS, (
            "the MERGE must delete every row of the seeded file, or the file is not 100 % dead"
        )

        bounds = _position_delete_path_bounds(spark, table)
        assert bounds == [(seeded_path, seeded_path)], (
            "the delete file must carry exact, equal `file_path` bounds, or the delete-ratio "
            f"clause cannot see it: {bounds}"
        )

        results: dict[str, dict[str, object]] = {}
        for procedure, sql in measure.maintenance_sequence("mw7d", table_arg):
            rows = spark.sql(sql).to_arrow().to_pylist()
            results[procedure] = rows[0] if rows else {}

        rewrite = results["rewrite_data_files"]
        assert int(rewrite["rewritten_data_files_count"]) > 0, (
            "the fixture must give compaction real work, or the clause proves nothing"
        )
        assert int(rewrite["removed_delete_files_count"]) == 1, (
            "the file-scoped delete file dies with the data file it covered"
        )

        live = {path for path, _size, _records in _data_files(spark, table)}
        assert seeded_path not in live, (
            "the 100 %-dead seeded file must be gone: the ratio clause made it a candidate"
        )

        census = measure.file_census(spark, table)
        assert census.delete_files == 0, "the runbook ends with no delete file"
        assert census.delete_records == 0

        assert not _position_delete_path_bounds(spark, table)

        rows_after = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow()
        assert int(rows_after.column("n")[0].as_py()) == C011_ROWS, (
            "the reclaimed rows must not resurrect: the answer is the MERGE's 2,500"
        )
    finally:
        spark.stop()


def test_the_cli_defaults_to_format_version_two() -> None:
    """C-001: `--format-version` defaults to 2, takes 3, and refuses anything else."""
    import importlib

    assert measure.DEFAULT_FORMAT_VERSION == 2
    runner = importlib.import_module("repark_mw7_bench.mw7.run_mw7")
    parsed = runner.build_parser().parse_args(["--scratch", "/dev/null"])
    assert parsed.format_version == 2
    assert (
        runner.build_parser()
        .parse_args(["--scratch", "/dev/null", "--format-version", "3"])
        .format_version
        == 3
    )
    with pytest.raises(SystemExit):
        runner.build_parser().parse_args(["--scratch", "/dev/null", "--format-version", "4"])


def test_each_leg_records_the_format_version_it_was_built_at(
    smoke_run: Any, v3_smoke_run: Any
) -> None:
    """C-001: the v2 fixture reports 2 on every leg and the v3 fixture reports 3."""
    assert [leg.format_version for leg in smoke_run.legs] == [2, 2]
    assert smoke_run.format_version == 2
    assert [leg.format_version for leg in v3_smoke_run.legs] == [3, 3]
    assert v3_smoke_run.format_version == 3


def test_v3_mor_delete_files_are_one_per_seeded_data_file(v3_smoke_run: Any) -> None:
    """C-001: v3 MERGEs fold into one deletion vector per touched data file, not one per commit."""
    leg = _leg(v3_smoke_run, "mor")
    for point in leg.checkpoints:
        expected = 0 if point.merges_done == 0 else V3_SEEDED_DATA_FILES
        assert point.census.delete_files == expected, (
            f"after {point.merges_done} MERGEs: {point.census.delete_files} delete files, "
            f"expected {expected}"
        )
        assert point.census.delete_records == point.merges_done * leg.rows_per_merge
        assert point.row_count == SMOKE_ROWS
    final = leg.checkpoints[-1]
    assert final.merges_done == SMOKE_MERGES
    assert final.census.delete_files < SMOKE_MERGES * SMOKE_PARTITIONS


def test_v3_mor_delete_files_are_file_scoped_deletion_vectors(tmp_path: Path) -> None:
    """C-001: every v3 delete file is a Puffin DV naming exactly one live data file."""
    spark = _v3_session("pytest-mw7-v3-dv")
    try:
        table = _build_v3_leg(spark, tmp_path, "mw7v3", "mor", SMOKE_ROWS, V3_DV_MERGES, 400)
        deletes = _delete_files(spark, table)
        assert len(deletes) == V3_SEEDED_DATA_FILES, deletes
        assert {content for content, _fmt, _records, _ref in deletes} == {1}
        assert {fmt for _content, fmt, _records, _ref in deletes} == {V3_DELETE_FILE_FORMAT}
        referenced = {ref for _content, _fmt, _records, ref in deletes}
        assert len(referenced) == len(deletes), "a DV names exactly one data file"
        live = {path for path, _size, _records in _data_files(spark, table)}
        assert referenced <= live
        assert sum(records for _c, _f, records, _r in deletes) == V3_DV_MERGES * 400
        rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow()
        assert int(rows.column("n")[0].as_py()) == SMOKE_ROWS
    finally:
        spark.stop()


def test_v3_cow_leg_keeps_row_lineage(tmp_path: Path) -> None:
    """C-001: the v3 copy-on-write leg writes no delete file and `_row_id` stays readable."""
    spark = _v3_session("pytest-mw7-v3-lineage")
    try:
        table = _build_v3_leg(spark, tmp_path, "mw7v3c", "cow", SMOKE_ROWS, 2, 400)
        assert _delete_files(spark, table) == []
        lineage = spark.sql(
            f"SELECT id, _row_id, _last_updated_sequence_number FROM {table} "
            f"WHERE id < 3 ORDER BY id"
        ).to_arrow()
        rows = [
            (int(row["id"]), row["_row_id"], row["_last_updated_sequence_number"])
            for row in lineage.to_pylist()
        ]
        assert len(rows) == 3
        assert all(row_id is not None for _id, row_id, _seq in rows)
        assert len({row_id for _id, row_id, _seq in rows}) == 3
        untouched = spark.sql(f"SELECT _row_id FROM {table} WHERE id = {SMOKE_ROWS - 1}").to_arrow()
        assert untouched.column("_row_id")[0].as_py() is not None
    finally:
        spark.stop()


def test_v3_position_delete_compaction_returns_zeros_and_the_sequence_continues(
    v3_smoke_run: Any,
) -> None:
    """C-001: rewrite_position_delete_files returns zeros on live DVs."""
    leg = _leg(v3_smoke_run, "mor")
    assert [step.procedure for step in leg.maintenance] == MAINTENANCE_ORDER
    first = leg.maintenance[0]
    assert first.procedure == "rewrite_position_delete_files"
    assert first.result_rows == 1
    assert first.refusal == ""
    assert first.result_first_row["rewritten_delete_files_count"] == 0
    assert first.result_first_row["added_delete_files_count"] == 0
    assert [step.refusal for step in leg.maintenance] == ["", "", "", "", ""]
    assert leg.maintenance[1].result_first_row["rewritten_data_files_count"] > 0
    assert leg.after_maintenance.row_count == SMOKE_ROWS
    cow = _leg(v3_smoke_run, "cow")
    assert [step.refusal for step in cow.maintenance] == ["", "", "", "", ""]
    assert [point.census.delete_files for point in cow.checkpoints] == [0] * len(cow.checkpoints)


@pytest.mark.skipif(not LIVE, reason=LIVE_SKIP)
def test_v3_delete_file_layout_matches_live_spark(tmp_path: Path) -> None:
    """C-001: at a matched layout, repark and Spark agree on the v3 delete-file census."""
    import tempfile

    import _live_parity as live_parity

    spark = _v3_session("pytest-mw7-v3-oracle")
    try:
        table = _build_v3_leg(
            spark,
            tmp_path,
            "mw7v3o",
            "mor",
            V3_ORACLE_ROWS,
            V3_ORACLE_MERGES,
            V3_ORACLE_ROWS_PER_MERGE,
        )
        engine_deletes = sorted(
            (content, fmt, records) for content, fmt, records, _ref in _delete_files(spark, table)
        )
        engine_rows = int(
            spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow().column("n")[0].as_py()
        )
    finally:
        spark.stop()

    warehouse = Path(tempfile.mkdtemp(prefix="repark-scale-v3-live-"))
    try:
        oracle = live_parity.build_spark_iceberg_engine(warehouse)
        catalog = live_parity.LIFECYCLE_SPARK_CATALOG
        try:
            session = oracle.session
            session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
            measure.seed_frame(V3_ORACLE_ROWS, SMOKE_PARTITIONS).write_parquet(
                tmp_path / "oracle_seed.parquet"
            )
            session.read.parquet(str(tmp_path / "oracle_seed.parquet")).createOrReplaceTempView(
                "oracle_seed"
            )
            session.sql(
                f"CREATE TABLE {catalog}.ns.leg USING iceberg PARTITIONED BY (part) "
                f"TBLPROPERTIES ({V3_TABLE_PROPERTIES}) AS SELECT * FROM oracle_seed"
            )
            for generation in range(1, V3_ORACLE_MERGES + 1):
                measure.merge_frame(
                    (generation - 1) * V3_ORACLE_ROWS_PER_MERGE,
                    V3_ORACLE_ROWS_PER_MERGE,
                    SMOKE_PARTITIONS,
                    generation,
                ).write_parquet(tmp_path / "oracle_src.parquet")
                session.read.parquet(str(tmp_path / "oracle_src.parquet")).createOrReplaceTempView(
                    "oracle_src"
                )
                session.sql(measure.merge_sql(f"{catalog}.ns.leg", "oracle_src"))
            files = session.sql(
                f"SELECT content, file_format, record_count FROM {catalog}.ns.leg.files "
                f"WHERE content = 1"
            ).toArrow()
            spark_deletes = sorted(
                (int(row["content"]), str(row["file_format"]).upper(), int(row["record_count"]))
                for row in files.to_pylist()
            )
            spark_rows = int(
                session.sql(f"SELECT COUNT(*) AS n FROM {catalog}.ns.leg").toArrow()["n"][0].as_py()
            )
        finally:
            oracle.session.stop()
    finally:
        shutil.rmtree(warehouse, ignore_errors=True)

    assert engine_deletes == spark_deletes, (engine_deletes, spark_deletes)
    assert engine_rows == spark_rows == V3_ORACLE_ROWS
    assert len(spark_deletes) == V3_SEEDED_DATA_FILES
    assert {fmt for _content, fmt, _records in spark_deletes} == {V3_DELETE_FILE_FORMAT}
    assert (
        sum(records for _c, _f, records in spark_deletes)
        == V3_ORACLE_MERGES * V3_ORACLE_ROWS_PER_MERGE
    )


def test_a_refusal_is_recorded_only_when_the_step_is_armed(tmp_path: Path) -> None:
    """C-001: `capture_refusal` records a refusing CALL; unarmed, the same CALL still raises."""
    spark = ReparkSession.builder.appName("pytest-mw7-refusal").getOrCreate()
    try:
        warehouse = tmp_path / "wh"
        warehouse.mkdir()
        spark.register_memory_catalog("mw7r", str(warehouse))
        spark.sql(f"CREATE NAMESPACE mw7r.ns LOCATION '{warehouse / 'ns'}'")
        table = "mw7r.ns.armed"
        measure.register_parquet_view(
            spark, measure.seed_frame(200, 1), tmp_path / "seed.parquet", "mw7r_seed"
        )
        measure.create_table(spark, table, "mw7r_seed", "mor", SMOKE_TARGET_FILE_SIZE)
        spark.catalog.dropTempView("mw7r_seed")

        refusing = "CALL mw7r.system.migrate(table => 'ns.armed')"
        with pytest.raises(UnsupportedOperationException):
            measure.run_maintenance_step(spark, table, "migrate", refusing)

        step = measure.run_maintenance_step(spark, table, "migrate", refusing, True)
        assert step.result_rows == 0
        assert step.result_first_row == {}
        assert step.refusal
        assert step.census_after.data_files > 0
    finally:
        spark.stop()


class _FakeClock:
    """A strictly increasing clock that records every reading it hands out."""

    def __init__(self, base: float, step: float) -> None:
        self.base = base
        self.step = step
        self.readings: list[float] = []

    def __call__(self) -> float:
        reading = self.base + len(self.readings) * self.step
        self.readings.append(reading)
        return reading


def test_started_at_records_the_start_of_the_run_not_its_end(tmp_path: Path) -> None:
    """C-002: `started_at` formats the FIRST reading of the run's clock, never a later one."""
    clock = _FakeClock(time.time() - STARTED_AT_BACKDATE_SECONDS, STARTED_AT_STEP_SECONDS)
    run = measure.run_scale_measurement(
        root=tmp_path,
        rows=STARTED_AT_ROWS,
        merges=STARTED_AT_MERGES,
        partitions=1,
        touch_fraction=SMOKE_TOUCH_FRACTION,
        checkpoint_every=1,
        reps=1,
        target_file_size_bytes=SMOKE_TARGET_FILE_SIZE,
        modes=["cow"],
        host_note="pytest started_at",
        clock=clock,
    )
    assert len(clock.readings) > 1, "the run must read its clock again after it stamps the start"
    stamped = time.strftime(measure.STARTED_AT_FORMAT, time.localtime(clock.readings[0]))
    latest = time.strftime(measure.STARTED_AT_FORMAT, time.localtime(clock.readings[-1]))
    assert stamped != latest, "the fixture must separate the first reading from the last"
    assert stamped != time.strftime(measure.STARTED_AT_FORMAT), (
        "the fixture must separate the injected clock from the real one"
    )
    assert run.started_at == stamped
