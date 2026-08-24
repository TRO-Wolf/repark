"""MW-7: prove the scale-measurement driver's metric collection tells the truth.

The MW-7 numbers were measured at 1e7 rows x 50 MERGEs, which no gate can run. This
module runs the SAME driver at a scale CI can afford and pins the machinery: that the
census counts what the metadata tables actually hold, that delete files grow one per
partition per MERGE and compaction reclaims them, that the manifest count drops when
`rewrite_manifests` runs, that the copy-on-write leg really is a zero-delete control, and
that a timing is never recorded for a scan that answered differently.

The 1e7 wall-clock figures are recorded in
`task/ledgers/completed/mw-7-scale-measurement-ledger.md` as dated MEASUREMENTS. They are
one machine's numbers and are deliberately NOT asserted here — a timing pin on CI hardware
is not the MW-7 claim (the MW-5 precedent, `test_mw5_baseline_delta.py`).
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

pytest.importorskip("polars")

from repark import ReparkSession

# pins: mw-7-scale-measurement/C-001, C-002, C-003, C-004, C-005
# pins: mw-7-scale-measurement/C-006, C-007, C-008, C-009, C-010, C-011

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
# inside the bin-pack band for a 64 KiB target — so `rewrite_data_files` never selects it as a
# candidate, and a MERGE that deletes every one of its rows cannot get them reclaimed.
# Java's band, from `BinPackRewriteFilePlanner`: [0.75 x target, 1.8 x target].
C011_ROWS = 2_500
C011_TARGET_FILE_SIZE = 64 * 1024
C011_BAND_LOW = 0.75
C011_BAND_HIGH = 1.8


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


def _delete_file_references(spark: ReparkSession, table: str) -> set[str]:
    """Every data-file path the live position-delete files name.

    Read from the delete files themselves (`file_path`, `pos`), not from metadata: whether a
    surviving delete file still points at something live is the whole question in C-011, and a
    count of delete files cannot answer it.
    """
    arrow = spark.sql(f"SELECT content, file_path FROM {table}.files").to_arrow()
    referenced: set[str] = set()
    for row in arrow.to_pylist():
        if row["content"] != 1:
            continue
        deletes = spark.read.parquet(str(row["file_path"]).removeprefix("file://")).to_arrow()
        referenced |= {str(value) for value in deletes.column("file_path").to_pylist()}
    return referenced


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
            f"SELECT content, file_size_in_bytes, record_count FROM {table}.files"
        ).to_arrow()
        contents = files.column("content").to_pylist()
        sizes = files.column("file_size_in_bytes").to_pylist()
        records = files.column("record_count").to_pylist()
        expected_data = [index for index, value in enumerate(contents) if value == 0]
        expected_deletes = [index for index, value in enumerate(contents) if value == 1]

        assert census.data_files == len(expected_data)
        assert census.data_bytes == sum(sizes[index] for index in expected_data)
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

    This engine writes one position-delete file per `(spec, partition)` per commit —
    Iceberg `partition` granularity, registry row `MOR-2`. The arithmetic below IS that
    behaviour, so the growth curve in the ledger is not an artefact of the driver.
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
    """C-003: the COW leg writes no delete files, so MOR-minus-COW is the delete cost."""
    leg = _leg(smoke_run, "cow")
    assert [point.census.delete_files for point in leg.checkpoints] == [0] * len(leg.checkpoints)
    assert leg.after_maintenance.census.delete_files == 0
    assert all(point.row_count == SMOKE_ROWS for point in leg.checkpoints)
    # The control must still be doing the work: COW rewrites data files on every MERGE.
    assert leg.checkpoints[-1].census.data_files > leg.checkpoints[0].census.data_files


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


def test_delete_laden_in_band_file_survives_the_runbook(tmp_path: Path) -> None:
    """C-011: a data file whose rows are ALL deleted is never a rewrite candidate, and its
    position-delete file outlives the whole maintenance sequence still pointing at it.

    This is the characterization behind ledger finding F-MW7-1, at a scale a gate can run.
    The shape is the 1e7 run's, shrunk: one CTAS data file sized INSIDE Java's bin-pack band,
    then one MERGE that deletes every row in it.

    `rewrite_data_files` selects a file when it is outside the size band or carries at least
    `delete_file_threshold` delete files. The fork at pin `5e7b2e4` defaults that threshold to
    `usize::MAX` and DEFERS Java's third clause, `tooHighDeleteRatio`
    (`DELETE_RATIO_THRESHOLD_DEFAULT = 0.3`) — its module doc says "the ratio clause never
    fires here". So a file that is 100 % dead but correctly sized is invisible to compaction,
    and `removed_delete_files_count` is 0, so nothing drops the delete file either.

    The answers stay correct throughout; what is retained is dead bytes and a delete file that
    every scan opens. Registry row `RDF-1`; fork ask F-16.
    """
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
        # The precondition the whole clause rests on: this file is correctly sized, so only the
        # deferred ratio clause could ever make it a candidate.
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

        results: dict[str, dict[str, object]] = {}
        for procedure, sql in measure.maintenance_sequence("mw7d", table_arg):
            rows = spark.sql(sql).to_arrow().to_pylist()
            results[procedure] = rows[0] if rows else {}

        rewrite = results["rewrite_data_files"]
        assert int(rewrite["rewritten_data_files_count"]) > 0, (
            "the fixture must give compaction real work, or the clause proves nothing"
        )
        assert int(rewrite["removed_delete_files_count"]) == 0, (
            "F-MW7-1: compaction removes no delete file"
        )

        live = {path for path, _size, _records in _data_files(spark, table)}
        assert seeded_path in live, (
            "the 100 %-dead seeded file is still live: it was never a rewrite candidate"
        )

        census = measure.file_census(spark, table)
        assert census.delete_files >= 1, "a delete file outlives the whole sequence"
        assert census.delete_records == C011_ROWS

        # Not dangling. Every path the surviving delete files name is a LIVE data file, so the
        # deletes are still doing work — they are shadowing rows nothing can reclaim.
        referenced = _delete_file_references(spark, table)
        assert referenced, "the surviving delete file must name the file it covers"
        assert referenced <= live, f"references not live: {referenced - live}"
        assert seeded_path in referenced

        rows_after = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow()
        assert int(rows_after.column("n")[0].as_py()) == C011_ROWS, (
            "the answers stay correct; what is retained is dead bytes"
        )
    finally:
        spark.stop()
