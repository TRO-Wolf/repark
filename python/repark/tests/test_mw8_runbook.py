"""MW-8: run the documented maintenance runbook end to end and pin what each step changes.

The guide section `docs/guide/iceberg-guide.md` "The maintenance sequence" tells an operator
to run six procedures in one order, on a schedule. This module runs that sequence against a
local catalog and asserts the effect of every step. A guide that drifts from the engine reds
here.

The scale is a gate's, not production's. The measured production numbers behind the guide's
cadence, its budget and its 43x warehouse figure live in
`task/ledgers/completed/mw-7-scale-measurement-ledger.md` section 6. They are cited by the
guide and are deliberately not re-asserted here: one host's wall clock is not a proposition
(the MW-5 and MW-7 precedent).

The fixture reproduces the 1e7 x 50 run's residue at gate scale. Both CTAS data files sit
inside Java's bin-pack band, the six MERGEs delete every row in them, and the complete
sequence still cannot reclaim them — registry row `RDF-1`, fork ask F-16.
"""

from __future__ import annotations

import ast
import importlib
import re
import sys
import time
import types
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

pytest.importorskip("polars")

from repark import ReparkSession
from repark.errors import PySparkException

# pins: mw-8-maintenance-runbook/C-001, C-002, C-003, C-004, C-005
# pins: mw-8-maintenance-runbook/C-006, C-007, C-008, C-009, C-010

# The MW-7 driver lives under python/repark-parity/bench/, which is not on `repark_parity`'s
# import path. It is loaded as a synthetic package — the shim `test_mw7_scale_smoke.py` and
# `test_tpch_smoke.py` both use. This module reuses that driver rather than writing a second
# implementation of the census, the generator or the sequence.
_BENCH_DIR = Path(__file__).resolve().parents[2] / "repark-parity" / "bench"
_REPO_ROOT = Path(__file__).resolve().parents[3]
_GUIDE = _REPO_ROOT / "docs" / "guide" / "iceberg-guide.md"

# The runbook cycle the guide documents, at a scale that runs in seconds. Six MERGEs over two
# partitions leave twelve position-delete files, which clears the five-file floor
# `rewrite_position_delete_files` needs before it folds anything.
RUNBOOK_ROWS = 6_000
RUNBOOK_MERGES = 6
RUNBOOK_PARTITIONS = 2
RUNBOOK_ROWS_PER_MERGE = 600
RUNBOOK_TARGET_FILE_SIZE = 64 * 1024

# Java's `BinPackRewriteFilePlanner` band: [0.75 x target, 1.8 x target]. A file inside it is
# never a rewrite candidate, however many of its rows are dead (registry `RDF-1`).
BAND_LOW = 0.75
BAND_HIGH = 1.8

# Steps 2 to 6 of the guide's sequence. Step 1 is the merge workload; step 7 arms the orphan
# call. The order is the driver's, read from `measure.maintenance_sequence`, not restated.
RUNBOOK_PROCEDURES = [
    "rewrite_position_delete_files",
    "rewrite_data_files",
    "rewrite_manifests",
    "expire_snapshots",
    "remove_orphan_files",
]

# `expire_snapshots` in the driver's sequence keeps one snapshot.
EXPIRE_RETAIN_LAST = 1

ONE_HOUR_MS = 60 * 60 * 1000

# Every source the guide's runbook section must link. A number without its home is a number
# that goes stale silently, so the citation is checked rather than trusted.
REQUIRED_CITATIONS = [
    "mw-7-scale-measurement-ledger.md",
    "mw-8-maintenance-runbook-ledger.md",
    "#rdf-1",
    "#mor-2",
    "#orphan-1",
    "#orphan-2",
    "#manifest-1",
    "#manifest-2",
    "#manifest-3",
]

RUNBOOK_HEADING = "### The maintenance runbook"

# `CALL {}.system.<procedure>(` — the catalog is an f-string placeholder in the guide's block
# and a real name in the driver's, so the procedure is read after the placeholder either way.
CALL_PROCEDURE = re.compile(r"system\.(\w+)\(")
CALL_ARGUMENT = re.compile(r"(\w+)\s*=>\s*([^,)]+)")

# The catalog and table the driver's sequence is rendered with for the comparison. Procedure
# names, argument names, and literal argument values are compared. A placeholder value is
# skipped: the guide passes a `TIMESTAMP` literal where the driver passes epoch milliseconds.
DRIVER_CATALOG = "mw8"
DRIVER_TABLE_ARG = "ns.orders"


def _load_measure() -> Any:
    """Import `mw7.measure` from the bench tree as a synthetic package."""
    package_name = "repark_mw7_bench"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_BENCH_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package
    return importlib.import_module(f"{package_name}.mw7.measure")


measure = _load_measure()


def _guide_section() -> str:
    """The runbook section of the guide, from its heading to the next `##`."""
    guide = _GUIDE.read_text(encoding="utf-8")
    assert RUNBOOK_HEADING in guide, f"{_GUIDE} has no {RUNBOOK_HEADING!r} section"
    return guide.split(RUNBOOK_HEADING, 1)[1].split("\n## ", 1)[0]


def _f_string_text(node: ast.expr) -> str:
    """The literal text of an f-string, with every placeholder rendered as `{}`."""
    if isinstance(node, ast.Constant):
        return str(node.value)
    if not isinstance(node, ast.JoinedStr):
        return ""
    return "".join(
        piece.value if isinstance(piece, ast.Constant) else "{}" for piece in node.values
    )


def _calls_of(statements: list[str]) -> list[tuple[str, list[tuple[str, str]]]]:
    """Each `CALL` as `(procedure, [(argument name, argument text)])`, in source order."""
    calls: list[tuple[str, list[tuple[str, str]]]] = []
    for statement in statements:
        procedure = CALL_PROCEDURE.search(statement)
        assert procedure, f"not a CALL statement: {statement!r}"
        arguments = [(name, value.strip()) for name, value in CALL_ARGUMENT.findall(statement)]
        calls.append((procedure.group(1), arguments))
    return calls


def _printed_cycle() -> list[str]:
    """The `MAINTENANCE_CYCLE` statements the guide prints, read out of its python block.

    The block is PARSED, not pattern-matched over the page. Two of the statements are
    f-strings split across two source lines, and a regex over the markdown silently keeps
    only the first half — which is the half without the arguments that matter.
    """
    block = re.search(r"```python\n(.*?)```", _guide_section(), re.S)
    assert block, "the runbook section must carry exactly one python block"
    for node in ast.walk(ast.parse(block.group(1))):
        if not isinstance(node, ast.Assign):
            continue
        names = [target.id for target in node.targets if isinstance(target, ast.Name)]
        if "MAINTENANCE_CYCLE" in names and isinstance(node.value, ast.List):
            return [_f_string_text(element) for element in node.value.elts]
    raise AssertionError("the guide's python block defines no MAINTENANCE_CYCLE list")


class RunbookCycle(BaseModel):
    """What one documented runbook cycle did, measured at every boundary.

    The driver's own models fill the `Any` fields (`FileCensus`, `MaintenanceStep`). They
    arrive through a dynamic import, so they cannot be named in a static annotation here.
    """

    seeded_files: list[tuple[str, int, int]]
    census_before: Any
    steps: list[Any]
    dry_run_columns: list[str]
    dry_run_rows: int
    armed_columns: list[str]
    armed_rows: int
    floor_refusal: str
    live_files_after: list[str]
    census_after: Any
    rows_before: int
    rows_after: int
    row_count_type: str


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


def _count_star(spark: ReparkSession, table: str) -> tuple[int, str]:
    """`COUNT(*)` on the Arrow path: the value and the Arrow type it came back as."""
    arrow = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").to_arrow()
    return int(arrow.column("n")[0].as_py()), str(arrow.schema.field("n").type)


def _run_merge_workload(spark: ReparkSession, table: str, scratch: Path) -> None:
    """Step 1 of the runbook: the merge workload the cycle is scheduled behind."""
    for generation in range(1, RUNBOOK_MERGES + 1):
        start_id = ((generation - 1) * RUNBOOK_ROWS_PER_MERGE) % RUNBOOK_ROWS
        measure.register_parquet_view(
            spark,
            measure.merge_frame(start_id, RUNBOOK_ROWS_PER_MERGE, RUNBOOK_PARTITIONS, generation),
            scratch / "merge.parquet",
            "mw8_merge",
        )
        spark.sql(measure.merge_sql(table, "mw8_merge"))
        spark.catalog.dropTempView("mw8_merge")


def _floor_refusal_message(spark: ReparkSession, catalog: str, table_arg: str) -> str:
    """Arm the orphan call inside the 24-hour floor and return the refusal it raises.

    The floor holds on the ARMED form too. That is the cell that matters: `dry_run => false`
    is the one call in the runbook that destroys data.
    """
    inside_floor = int(time.time() * 1000) - ONE_HOUR_MS
    with pytest.raises(PySparkException, match=r"less than 24 hours") as raised:
        spark.sql(
            f"CALL {catalog}.system.remove_orphan_files(table => '{table_arg}', "
            f"older_than => {inside_floor}, dry_run => false)"
        )
    return str(raised.value)


@pytest.fixture(scope="module")
def cycle(tmp_path_factory: pytest.TempPathFactory) -> Any:
    """Seed, merge, then run the documented sequence once, censusing after every step."""
    root = tmp_path_factory.mktemp("mw8-runbook")
    warehouse = root / "warehouse"
    warehouse.mkdir()
    scratch = root / "parquet"
    scratch.mkdir()
    catalog, namespace, table_name = "mw8", "ns", "orders"
    table = f"{catalog}.{namespace}.{table_name}"
    table_arg = f"{namespace}.{table_name}"

    spark = ReparkSession.builder.appName("pytest-mw8-runbook").getOrCreate()
    try:
        spark.register_memory_catalog(catalog, str(warehouse))
        spark.sql(f"CREATE NAMESPACE {catalog}.{namespace} LOCATION '{warehouse / namespace}'")

        measure.register_parquet_view(
            spark,
            measure.seed_frame(RUNBOOK_ROWS, RUNBOOK_PARTITIONS),
            scratch / "seed.parquet",
            "mw8_seed",
        )
        measure.create_table(spark, table, "mw8_seed", "mor", RUNBOOK_TARGET_FILE_SIZE)
        spark.catalog.dropTempView("mw8_seed")
        seeded = _data_files(spark, table)
        rows_before, row_count_type = _count_star(spark, table)

        _run_merge_workload(spark, table, scratch)
        census_before = measure.file_census(spark, table)

        steps = [
            measure.run_maintenance_step(spark, table, procedure, sql)
            for procedure, sql in measure.maintenance_sequence(catalog, table_arg)
        ]

        # The dry run's schema. `MaintenanceStep` keeps the first row, and a zero-row answer
        # has none, so the column list is read from a second execution of the same SQL.
        dry_run = spark.sql(steps[-1].sql).to_arrow()

        floor_refusal = _floor_refusal_message(spark, catalog, table_arg)
        armed = spark.sql(
            f"CALL {catalog}.system.remove_orphan_files(table => '{table_arg}', "
            f"older_than => {int(time.time() * 1000) - measure.ORPHAN_OLDER_THAN_PAST_MS}, "
            "dry_run => false)"
        ).to_arrow()

        rows_after, _ = _count_star(spark, table)
        yield RunbookCycle(
            seeded_files=seeded,
            census_before=census_before,
            steps=steps,
            dry_run_columns=list(dry_run.schema.names),
            dry_run_rows=dry_run.num_rows,
            armed_columns=list(armed.schema.names),
            armed_rows=armed.num_rows,
            floor_refusal=floor_refusal,
            live_files_after=[path for path, _size, _records in _data_files(spark, table)],
            census_after=measure.file_census(spark, table),
            rows_before=rows_before,
            rows_after=rows_after,
            row_count_type=row_count_type,
        )
    finally:
        spark.stop()


def _step(cycle: RunbookCycle, procedure: str) -> Any:
    """The recorded step for one procedure."""
    return next(step for step in cycle.steps if step.procedure == procedure)


def test_the_runbook_runs_the_documented_procedures_in_order(cycle: RunbookCycle) -> None:
    """C-001: the sequence the guide documents is the sequence that runs.

    Order is load-bearing. Folding the delete files first is what stops `rewrite_data_files`
    reading every one of them, and `remove_orphan_files` stays last because it is the one
    procedure with no undo. The order comes from the driver, so the guide, the MW-7
    measurement and this test cannot disagree about it.
    """
    assert [step.procedure for step in cycle.steps] == RUNBOOK_PROCEDURES
    assert "dry_run" not in _step(cycle, "remove_orphan_files").sql, (
        "step 6 must take the dry-run default (registry ORPHAN-2)"
    )
    assert measure.ORPHAN_OLDER_THAN_PAST_MS > 24 * ONE_HOUR_MS


def test_position_delete_compaction_folds_the_deletes_to_one_per_partition(
    cycle: RunbookCycle,
) -> None:
    """C-002: step 2 folds `partitions x merges` delete files down to one per partition.

    This engine writes one position-delete file per `(spec, partition)` per commit — Iceberg
    `partition` granularity, registry row `MOR-2`. So the count before the step is arithmetic,
    not an observation, and the count after it is the floor the layout allows.
    """
    before = cycle.census_before.delete_files
    step = _step(cycle, "rewrite_position_delete_files")
    assert before == RUNBOOK_MERGES * RUNBOOK_PARTITIONS
    assert step.census_after.delete_files == RUNBOOK_PARTITIONS, (
        f"rewrite_position_delete_files: {before} -> {step.census_after.delete_files}, "
        f"expected one delete file per partition"
    )
    assert int(step.result_first_row["rewritten_delete_files_count"]) == before
    assert int(step.result_first_row["added_delete_files_count"]) == RUNBOOK_PARTITIONS


def test_data_compaction_reduces_the_data_file_count(cycle: RunbookCycle) -> None:
    """C-003: step 3 compacts the files the MERGEs fanned out.

    Every merge-on-read MERGE appends the updated rows as a new small file. The data-file
    count is what a scan opens, so this step is the one that pays back on read.
    """
    step = _step(cycle, "rewrite_data_files")
    before = _step(cycle, "rewrite_position_delete_files").census_after.data_files
    after = step.census_after.data_files
    assert before > cycle.seeded_files.__len__(), "the merge workload must fan the table out"
    assert after < before, f"rewrite_data_files: {before} -> {after} data files"
    assert int(step.result_first_row["rewritten_data_files_count"]) > 0
    assert int(step.result_first_row["failed_data_files_count"]) == 0


def test_delete_laden_seed_files_survive_the_whole_runbook(cycle: RunbookCycle) -> None:
    """C-004: the runbook cannot reclaim a delete-laden data file — registry row `RDF-1`.

    Both CTAS files are inside Java's bin-pack band, and the six MERGEs delete every row in
    them. A file is a rewrite candidate only when it is outside that band or carries at least
    `delete_file_threshold` delete files, and the fork at pin `5e7b2e4` defers Java's third
    clause, `tooHighDeleteRatio`. So the two dead files are invisible to compaction, the
    delete files covering them survive, and `removed_delete_files_count` is 0.

    The answers stay correct. What is retained is dead bytes and a delete file every scan
    opens. The mechanism and the Spark oracle are pinned by MW-7 C-011
    (`test_mw7_scale_smoke.py::test_delete_laden_in_band_file_survives_the_runbook`); this
    clause holds the operator-visible half — a pass does not return the table to baseline.
    """
    low = BAND_LOW * RUNBOOK_TARGET_FILE_SIZE
    high = BAND_HIGH * RUNBOOK_TARGET_FILE_SIZE
    for path, size, records in cycle.seeded_files:
        assert low <= size <= high, (
            f"seeded file {size} B is outside the bin-pack band for {RUNBOOK_TARGET_FILE_SIZE}"
        )
        assert records > 0
        assert path in cycle.live_files_after, (
            "a 100 %-dead seeded file is still live: it was never a rewrite candidate"
        )
    rewrite = _step(cycle, "rewrite_data_files").result_first_row
    assert int(rewrite["removed_delete_files_count"]) == 0, (
        "RDF-1: compaction removes no delete file"
    )
    assert cycle.census_after.delete_files > 0, "delete files outlive the complete sequence"
    assert cycle.census_after.delete_records == RUNBOOK_MERGES * RUNBOOK_ROWS_PER_MERGE


def test_manifest_compaction_drops_the_manifest_count(cycle: RunbookCycle) -> None:
    """C-005: step 4 re-groups the manifests the first two steps churned.

    Every reader opens the manifest list first, so this is the cheapest step per byte saved.
    The result is Spark's two columns.
    """
    step = _step(cycle, "rewrite_manifests")
    before = _step(cycle, "rewrite_data_files").census_after.manifests
    after = step.census_after.manifests
    assert after < before, f"rewrite_manifests: {before} -> {after} manifests"
    assert step.census_after.manifest_list_bytes < cycle.census_before.manifest_list_bytes
    assert set(step.result_first_row) == {
        "rewritten_manifests_count",
        "added_manifests_count",
    }


def test_expire_snapshots_prunes_the_snapshots_and_deletes_what_they_held(
    cycle: RunbookCycle,
) -> None:
    """C-006: step 5 drops the snapshots, and with them the files the rewrites replaced.

    Nothing else deletes those files. Until this step runs, every rewritten data file and
    every folded delete file stays reachable from the snapshot that wrote it.
    """
    step = _step(cycle, "expire_snapshots")
    before = _step(cycle, "rewrite_manifests").census_after.snapshots
    assert before > EXPIRE_RETAIN_LAST
    assert step.census_after.snapshots == EXPIRE_RETAIN_LAST
    result = step.result_first_row
    assert int(result["deleted_data_files_count"]) > 0
    assert int(result["deleted_position_delete_files_count"]) == (
        RUNBOOK_MERGES * RUNBOOK_PARTITIONS
    ), "the delete files step 2 folded are exactly what expire reclaims"
    assert int(result["deleted_manifest_files_count"]) > 0
    assert int(result["deleted_manifest_lists_count"]) > 0


def test_the_orphan_step_is_a_lagging_net_and_the_armed_form_keeps_the_floor(
    cycle: RunbookCycle,
) -> None:
    """C-007: step 6 lists, step 7 deletes, and both stay behind the 24-hour floor.

    The dry run answers Spark's one column. It answers zero rows here, and that is not a
    clean bill of health: the floor means a cycle never sees the orphans the same cycle's
    `expire_snapshots` just created (registry `ORPHAN-1`). The armed form refuses inside the
    floor, so the one call that destroys data cannot be pointed at an in-flight commit.
    """
    dry_run = _step(cycle, "remove_orphan_files")
    assert cycle.dry_run_columns == ["orphan_file_location"]
    assert cycle.dry_run_rows == 0
    expired = _step(cycle, "expire_snapshots")
    assert dry_run.census_after.data_files == expired.census_after.data_files
    assert "less than 24 hours" in cycle.floor_refusal
    assert cycle.armed_columns == ["orphan_file_location"]
    assert cycle.armed_rows == 0, "a warehouse minutes old has nothing past the floor"


def test_the_runbook_never_changes_the_row_set(cycle: RunbookCycle) -> None:
    """C-008: `COUNT(*)` holds across the whole sequence, value and Arrow type.

    Maintenance rewrites which files hold the rows and which files mask them. It never
    changes which rows are live. This is the correctness control under every other clause.
    """
    assert cycle.rows_before == RUNBOOK_ROWS
    assert cycle.rows_after == RUNBOOK_ROWS
    assert cycle.row_count_type == "int64"


def test_the_guide_section_links_every_source_it_names() -> None:
    """C-009: every source the runbook section relies on is linked from it.

    The cadence and the budget belong to the MW-7 ledger; each difference from Apache Spark
    belongs to its registry row; the expire-cutoff measurements belong to this unit's ledger.
    This checks that each of those homes is LINKED. It does not, and cannot cheaply, detect a
    number in the prose that cites nothing — C-010 is the drift detector that reads the
    statements the section actually prints.
    """
    section = _guide_section()
    missing = [citation for citation in REQUIRED_CITATIONS if citation not in section]
    assert not missing, f"the runbook section links no home for: {', '.join(missing)}"


def test_the_printed_cycle_matches_the_sequence_the_engine_runs() -> None:
    """C-010: the SQL the guide prints is the SQL this unit measured, argument for argument.

    An operator copies the block, not the prose around it. A statement that drifts from the
    measured sequence is a runbook that documents one thing and proves another — and the
    first way it drifts is by losing an argument, because a `CALL` with a missing argument
    still runs and still answers a result shape that looks correct.

    Values are compared wherever the guide prints a literal; a placeholder is skipped.
    The guide passes a `TIMESTAMP` literal where the
    driver passes epoch milliseconds; both are correct and neither is the claim. Procedure
    names, their order, and the argument NAMES are the claim.
    """
    printed = _calls_of(_printed_cycle())
    driven = _calls_of(
        [sql for _procedure, sql in measure.maintenance_sequence(DRIVER_CATALOG, DRIVER_TABLE_ARG)]
    )
    assert [procedure for procedure, _arguments in printed] == RUNBOOK_PROCEDURES
    assert [procedure for procedure, _arguments in printed] == [
        procedure for procedure, _arguments in driven
    ]
    for (procedure, printed_arguments), (_same, driven_arguments) in zip(
        printed, driven, strict=True
    ):
        assert [name for name, _value in printed_arguments] == [
            name for name, _value in driven_arguments
        ], (
            f"the guide's {procedure} call has drifted from the sequence this unit measured:\n"
            f"  printed: {printed_arguments}\n  measured: {driven_arguments}"
        )
        # A value is comparable only where the guide prints a literal. `table` and `older_than`
        # are f-string placeholders there and a name and epoch milliseconds here; `retain_last`
        # is the same literal on both sides, and it decides how much history survives.
        for (name, printed_value), (_name, driven_value) in zip(
            printed_arguments, driven_arguments, strict=True
        ):
            if "{}" not in printed_value:
                assert printed_value == driven_value, (
                    f"the guide prints {procedure}({name} => {printed_value}) where this unit "
                    f"measured {driven_value}"
                )
    orphan_arguments = [name for name, _value in dict(printed)["remove_orphan_files"]]
    assert "dry_run" not in orphan_arguments, (
        "step 6 must print the dry-run default (registry ORPHAN-2); step 7 arms it in prose"
    )
