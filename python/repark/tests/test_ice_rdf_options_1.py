"""ICE-RDF-OPTIONS-1 options-map pins against the recorded Spark oracle."""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import (
    IllegalArgumentException,
    PySparkException,
    UnsupportedOperationException,
)

FIXTURE_PATH = Path(__file__).with_name("ice_rdf_options_1_spark_oracle.json")
GENERATOR_PATH = Path(__file__).with_name("_record_rdf_options_1_oracle.py")
_BANNER_SNIPPET = (
    "from pyspark.sql import SparkSession;"
    " s=SparkSession.builder.master('local[1]').appName('banner').getOrCreate();"
    " print(s.version); s.stop()"
)

_MOR = "'format-version' = '2', 'write.delete.mode' = 'merge-on-read'"
_CHUNK = "xxxxxxxxxxxxxxxxxxxx"


def _fixture() -> dict[str, object]:
    """Load the recorded Spark oracle cells."""
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """Memory catalog session for options-map pins."""
    session = ReparkSession.builder.appName("pytest-rdf-options-1").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _build_shape(
    spark: ReparkSession,
    table: str,
    parts: int = 2,
    files_per: int = 4,
    mor: bool = False,
    pre: tuple[str, ...] = (),
) -> None:
    """Create the oracle shape with one multi-row INSERT per file, then run pre steps."""
    props = _MOR if mor else "'format-version' = '2'"
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, p INT, v STRING) USING iceberg "
        f"PARTITIONED BY (p) TBLPROPERTIES ({props})"
    )
    for part in range(parts):
        for fileno in range(files_per):
            base = (part * files_per + fileno) * 50
            rows = ", ".join(f"({base + row}, {part}, '{_CHUNK}')" for row in range(50))
            spark.sql(f"INSERT INTO {table} VALUES {rows}")
    for stmt in pre:
        spark.sql(stmt.format(t=table))


def _call_sql(cell: dict[str, object], table: str) -> str:
    """Rewrite one fixture CALL onto the memory catalog and table."""
    sql = str(cell["sql"])
    return sql.replace("sc.system", "mem.system").replace("ns.t", table.split(".", 1)[1])


def _result_row(spark: ReparkSession, sql: str) -> dict[str, int]:
    """Run one CALL and return its result counts."""
    batch = spark.sql(sql).to_arrow()
    return {name: int(batch.column(name)[0].as_py()) for name in batch.schema.names}


def _snapshots(spark: ReparkSession, table: str) -> tuple[int, list[str]]:
    """Return snapshot count and operations in commit order."""
    batch = spark.sql(f"SELECT operation FROM {table}.snapshots ORDER BY committed_at").to_arrow()
    ops = batch.column("operation").to_pylist()
    return (len(ops), [str(op) for op in ops])


def _file_state(spark: ReparkSession, table: str) -> tuple[int, int, list[int]]:
    """Return .files row count, .delete_files count, and sorted spec ids."""
    files = spark.sql(f"SELECT count(*) c FROM {table}.files").to_arrow().column("c")[0].as_py()
    deletes = (
        spark.sql(f"SELECT count(*) c FROM {table}.delete_files").to_arrow().column("c")[0].as_py()
    )
    specs = spark.sql(f"SELECT spec_id FROM {table}.files").to_arrow().column("spec_id").to_pylist()
    return (int(files), int(deletes), sorted({int(spec) for spec in specs}))


def _live_rows(spark: ReparkSession, table: str) -> int:
    """Return the live row count of one table."""
    return int(spark.sql(f"SELECT count(*) c FROM {table}").to_arrow().column("c")[0].as_py())


def _file_sizes(spark: ReparkSession, table: str) -> dict[str, int]:
    """Map live file paths to their sizes in bytes."""
    batch = spark.sql(f"SELECT file_path, file_size_in_bytes FROM {table}.files").to_arrow()
    paths = batch.column("file_path").to_pylist()
    sizes = batch.column("file_size_in_bytes").to_pylist()
    return {str(path): int(size) for path, size in zip(paths, sizes, strict=True)}


def _delete_file_sizes(spark: ReparkSession, table: str) -> dict[str, int]:
    """Map live delete-file paths to their sizes in bytes."""
    batch = spark.sql(f"SELECT file_path, file_size_in_bytes FROM {table}.delete_files").to_arrow()
    paths = batch.column("file_path").to_pylist()
    sizes = batch.column("file_size_in_bytes").to_pylist()
    return {str(path): int(size) for path, size in zip(paths, sizes, strict=True)}


def _check_value_cell(
    spark: ReparkSession, name: str, table: str, live_rows: int, build: dict[str, object]
) -> None:
    """Run one oracle success cell and compare result, files, and rows to the fixture."""
    cells = _fixture()
    cell = cells[name]  # type: ignore[literal-required]
    assert isinstance(cell, dict)
    _build_shape(
        spark,
        table,
        parts=int(build.get("parts", 2)),
        files_per=int(build.get("files_per", 4)),
        mor=bool(build.get("mor", False)),
        pre=tuple(str(stmt) for stmt in build.get("pre", ())),  # type: ignore[arg-type]
    )
    before_sizes = _file_sizes(spark, table)
    before_deletes = _delete_file_sizes(spark, table)
    sql = _call_sql(cell, table)
    got = _result_row(spark, sql)
    want = cell["result"]
    assert isinstance(want, dict)
    for key in ("rewritten_data_files_count", "added_data_files_count", "failed_data_files_count"):
        if key in want:
            assert got[key] == int(want[key]), f"{name} {key}: {got} vs {want}"
    for key in ("rewritten_delete_files_count", "added_delete_files_count"):
        if key in want:
            assert got[key] == int(want[key]), f"{name} {key}: {got} vs {want}"
    if name.startswith("rpd_"):
        after_deletes = _delete_file_sizes(spark, table)
        vanished_deletes = [path for path in before_deletes if path not in after_deletes]
        assert got["rewritten_bytes_count"] == sum(
            before_deletes[path] for path in vanished_deletes
        ), f"{name} rewritten_bytes_count is the vanished delete files' size sum"
        added_deletes = [path for path in after_deletes if path not in before_deletes]
        assert got["added_bytes_count"] == sum(after_deletes[path] for path in added_deletes), (
            f"{name} added_bytes_count is the new delete files' size sum"
        )
    else:
        after_sizes = _file_sizes(spark, table)
        vanished = [path for path in before_sizes if path not in after_sizes]
        assert got["rewritten_bytes_count"] == sum(before_sizes[path] for path in vanished), (
            f"{name} rewritten_bytes_count is the vanished files' size sum"
        )
    after = cell["after"]
    assert isinstance(after, dict)
    files, deletes, specs = _file_state(spark, table)
    assert files == int(after["data_files"]), f"{name} .files rows"
    assert deletes == int(after["delete_files"]), f"{name} delete files"
    assert specs == [int(spec) for spec in after["spec_ids"]], f"{name} spec ids"  # type: ignore[union-attr]
    assert _live_rows(spark, table) == live_rows, f"{name} live rows"


def _check_snapshot_cell(
    spark: ReparkSession, name: str, table: str, build: dict[str, object]
) -> None:
    """Run one oracle success cell and compare snapshot count and ops to the fixture."""
    cells = _fixture()
    cell = cells[name]  # type: ignore[literal-required]
    assert isinstance(cell, dict)
    _build_shape(
        spark,
        table,
        parts=int(build.get("parts", 2)),
        files_per=int(build.get("files_per", 4)),
        mor=bool(build.get("mor", False)),
        pre=tuple(str(stmt) for stmt in build.get("pre", ())),  # type: ignore[arg-type]
    )
    spark.sql(_call_sql(cell, table)).to_arrow()
    after = cell["after"]
    assert isinstance(after, dict)
    count, ops = _snapshots(spark, table)
    assert count == int(after["snapshots"]), (
        f"{name} snapshot count: {count} vs {after['snapshots']}"
    )
    assert ops == [str(op) for op in after["ops"]], f"{name} snapshot ops"  # type: ignore[union-attr]


_VALUE_CELLS: list[tuple[str, dict[str, object], int]] = [
    ("baseline", {}, 400),
    ("min_input_files_1", {}, 400),
    ("min_input_files_9", {}, 400),
    ("rewrite_all", {}, 400),
    ("target_small", {}, 400),
    ("max_group_size", {}, 400),
    ("partial_progress", {}, 400),
    ("partial_progress_max1", {}, 400),
    ("partial_progress_groups", {}, 400),
    ("job_order_bytes_desc", {}, 400),
    ("job_order_files_asc", {}, 400),
    ("use_start_seq_false", {}, 400),
    ("concurrent", {}, 400),
    ("where_plus_options", {}, 400),
    ("delete_file_threshold", {"mor": True, "pre": ("DELETE FROM {t} WHERE id = 3",)}, 399),
    ("output_spec_id", {"pre": ("ALTER TABLE {t} DROP PARTITION FIELD p",)}, 400),
    ("output_spec_id_current", {"pre": ("ALTER TABLE {t} DROP PARTITION FIELD p",)}, 400),
    ("remove_dangling", {"mor": True, "pre": ("DELETE FROM {t} WHERE id < 30",)}, 370),
    ("err_bad_bool", {"parts": 1, "files_per": 2}, 100),
    ("rpd_baseline", {"mor": True, "pre": ("DELETE FROM {t} WHERE id % 2 = 0",)}, 200),
    ("rpd_rewrite_all", {"mor": True, "pre": ("DELETE FROM {t} WHERE id % 2 = 0",)}, 200),
    ("rpd_min_input_files_1", {"mor": True, "pre": ("DELETE FROM {t} WHERE id % 2 = 0",)}, 200),
    ("rpd_target_small", {"mor": True, "pre": ("DELETE FROM {t} WHERE id % 2 = 0",)}, 200),
    ("rpd_max_group_size", {"mor": True, "pre": ("DELETE FROM {t} WHERE id % 2 = 0",)}, 200),
    (
        "rpd_target_small_forced",
        {"mor": True, "pre": ("DELETE FROM {t} WHERE id % 2 = 0",)},
        200,
    ),
    (
        "rpd_max_group_size_forced",
        {"mor": True, "pre": ("DELETE FROM {t} WHERE id % 2 = 0",)},
        200,
    ),
]

_VALUE_XFAIL: dict[str, str] = {
    "target_small": "FORK-WRITE-GRANULARITY 2026-09-17: fork writes one file per group "
    "(RePark 8→2), Spark splits outputs to the target size (8→4)",
    "max_group_size": "FORK-GROUP-GRANULARITY 2026-09-17: RePark compacts 8→8 added, Spark 8→4",
    "partial_progress_groups": "FORK-GROUP-GRANULARITY 2026-09-17: RePark compacts 8→8 "
    "added, Spark 8→4",
    "rpd_rewrite_all": "FORK-RPD 2026-09-17: fork RPD untouched by #283, compacts 8→2 "
    "per-group commits, Spark rewrites 8→8 in one commit",
    "rpd_min_input_files_1": "FORK-RPD 2026-09-17: fork RPD untouched by #283, compacts "
    "8→2 per-group commits, Spark rewrites 8→8 in one commit",
}

_SNAPSHOT_XFAIL: dict[str, str] = {
    "partial_progress_groups": "FORK-GROUP-GRANULARITY 2026-09-17: 8 groups under "
    "max-commits 3 need 3 commits (11 snapshots), Spark compacts 4 groups in 2 (10)",
    "rpd_rewrite_all": "FORK-RPD 2026-09-17: per-group commits (11 snapshots), Spark one "
    "commit (10)",
    "rpd_min_input_files_1": "FORK-RPD 2026-09-17: per-group commits (11 snapshots), "
    "Spark one commit (10)",
}


def _value_params() -> list[object]:
    """Parametrize value cells with precise-reason marks on the still-red cells."""
    params = []
    for name, build, rows in _VALUE_CELLS:
        reason = _VALUE_XFAIL.get(name)
        marks = (pytest.mark.xfail(strict=True, reason=reason),) if reason else ()
        params.append(pytest.param(name, build, rows, marks=marks, id=name))
    return params


def _snapshot_params() -> list[object]:
    """Parametrize snapshot cells with precise-reason marks on the still-red cells."""
    params = []
    for name, build, _rows in _VALUE_CELLS:
        reason = _SNAPSHOT_XFAIL.get(name)
        marks = (pytest.mark.xfail(strict=True, reason=reason),) if reason else ()
        params.append(pytest.param(name, build, marks=marks, id=name))
    return params


@pytest.mark.parametrize(("name", "build", "rows"), _value_params())
def test_option_cell_values(
    spark: ReparkSession, name: str, build: dict[str, object], rows: int
) -> None:
    """Option cell result counts, file counts, and live rows match the oracle."""
    _check_value_cell(spark, name, "mem.ns.opts", rows, build)


@pytest.mark.parametrize(("name", "build"), _snapshot_params())
def test_option_cell_snapshots(spark: ReparkSession, name: str, build: dict[str, object]) -> None:
    """Option cell snapshot count and ops match the oracle."""
    _check_snapshot_cell(spark, name, "mem.ns.snaps", build)


def _check_keep_set(
    spark: ReparkSession, name: str, table: str, live_rows: int, build: dict[str, object]
) -> None:
    """Run one xfailed oracle cell and compare only its keep-set: live rows and rewritten counts."""
    cells = _fixture()
    cell = cells[name]  # type: ignore[literal-required]
    assert isinstance(cell, dict)
    _build_shape(
        spark,
        table,
        parts=int(build.get("parts", 2)),
        files_per=int(build.get("files_per", 4)),
        mor=bool(build.get("mor", False)),
        pre=tuple(str(stmt) for stmt in build.get("pre", ())),  # type: ignore[arg-type]
    )
    got = _result_row(spark, _call_sql(cell, table))
    want = cell["result"]
    assert isinstance(want, dict)
    for key in ("rewritten_data_files_count", "rewritten_delete_files_count"):
        if key in want:
            assert got[key] == int(want[key]), f"{name} {key}: {got} vs {want}"
    assert _live_rows(spark, table) == live_rows, f"{name} live rows"


@pytest.mark.parametrize(
    ("name", "build", "rows"),
    [pytest.param(n, b, r, id=n) for n, b, r in _VALUE_CELLS if n in _VALUE_XFAIL],
)
def test_option_cell_keep_set(
    spark: ReparkSession, name: str, build: dict[str, object], rows: int
) -> None:
    """Still-xfailed cells keep Spark's row set and rewritten counts."""
    _check_keep_set(spark, name, "mem.ns.keep", rows, build)


def _check_delete_counts(
    spark: ReparkSession, name: str, table: str, build: dict[str, object], key: str
) -> None:
    """Run one oracle cell and compare one delete-count column with the fixture."""
    cells = _fixture()
    cell = cells[name]  # type: ignore[literal-required]
    assert isinstance(cell, dict)
    _build_shape(
        spark,
        table,
        parts=int(build.get("parts", 2)),
        files_per=int(build.get("files_per", 4)),
        mor=bool(build.get("mor", False)),
        pre=tuple(str(stmt) for stmt in build.get("pre", ())),  # type: ignore[arg-type]
    )
    got = _result_row(spark, _call_sql(cell, table))
    want = cell["result"]
    assert isinstance(want, dict)
    assert key in want, f"{name} fixture has no {key}"
    assert got[key] == int(want[key]), f"{name} {key}: {got} vs {want}"


@pytest.mark.parametrize(
    ("name", "build"),
    [pytest.param(n, b, id=n) for n, b, _rows in _VALUE_CELLS if not n.startswith("rpd_")],
)
def test_option_cell_failed_counts(
    spark: ReparkSession, name: str, build: dict[str, object]
) -> None:
    """failed_data_files_count is 0 on every RDF cell, like every oracle cell."""
    _check_delete_counts(spark, name, "mem.ns.fail", build, "failed_data_files_count")


@pytest.mark.parametrize(
    ("name", "build"),
    [pytest.param(n, b, id=n) for n, b, _rows in _VALUE_CELLS if not n.startswith("rpd_")],
)
def test_option_cell_removed_counts(
    spark: ReparkSession, name: str, build: dict[str, object]
) -> None:
    """removed_delete_files_count matches the oracle on every RDF cell."""
    _check_delete_counts(spark, name, "mem.ns.removed", build, "removed_delete_files_count")


_ERROR_CELLS: list[tuple[str, dict[str, object]]] = [
    ("min_max", {}),
    ("err_unknown_key", {}),
    ("err_bad_int", {}),
    ("err_zero_min_input", {}),
    ("err_bad_job_order", {}),
    ("err_bad_spec", {}),
    ("err_max_commits_0", {}),
    ("err_target_le_min", {}),
    ("err_target_ge_max", {}),
    ("err_neg_target", {}),
    ("err_concurrent_0", {}),
    ("err_delete_ratio", {}),
    ("err_empty_key", {}),
    ("err_upper_key", {}),
    ("err_group_size_0", {}),
    ("err_delete_threshold_neg", {}),
    ("neg_min_file_size", {}),
    ("neg_max_file_size", {}),
    ("rpd_unknown_key", {"mor": True, "pre": ("DELETE FROM {t} WHERE id % 2 = 0",)}),
    ("rpd_bad_int", {"mor": True, "pre": ("DELETE FROM {t} WHERE id % 2 = 0",)}),
]


@pytest.mark.parametrize(("name", "build"), [pytest.param(n, b, id=n) for n, b in _ERROR_CELLS])
def test_option_cell_errors(spark: ReparkSession, name: str, build: dict[str, object]) -> None:
    """Option error cells raise IllegalArgumentException with Spark's message."""
    cells = _fixture()
    cell = cells[name]  # type: ignore[literal-required]
    assert isinstance(cell, dict)
    parts = int(build.get("parts", 1))
    files_per = int(build.get("files_per", 2))
    _build_shape(
        spark,
        "mem.ns.errs",
        parts=parts,
        files_per=files_per,
        mor=bool(build.get("mor", False)),
        pre=tuple(str(stmt) for stmt in build.get("pre", ())),  # type: ignore[arg-type]
    )
    with pytest.raises(IllegalArgumentException, match=re.escape(str(cell["error"]))):
        spark.sql(_call_sql(cell, "mem.ns.errs")).to_arrow()


def test_option_dup_key_matches_spark_map_text(spark: ReparkSession) -> None:
    """Duplicate map keys raise Spark's DUPLICATED_MAP_KEY text as a base PySparkException."""
    cells = _fixture()
    cell = cells["err_dup_key"]
    assert isinstance(cell, dict)
    _build_shape(spark, "mem.ns.dup", parts=1, files_per=2)
    with pytest.raises(PySparkException) as caught:
        spark.sql(_call_sql(cell, "mem.ns.dup")).to_arrow()
    assert not isinstance(caught.value, IllegalArgumentException)
    assert str(cell["error"]) in str(caught.value)


def test_number_format_maps_to_illegal_argument(spark: ReparkSession) -> None:
    """Bad integers surface as IllegalArgumentException, PySpark's NumberFormat superclass."""
    _build_shape(spark, "mem.ns.badint", parts=1, files_per=2)
    with pytest.raises(IllegalArgumentException, match=r'For input string: "abc"'):
        spark.sql(
            "CALL mem.system.rewrite_data_files(table => 'ns.badint', "
            "options => map('min-input-files','abc'))"
        ).to_arrow()


_RPD_UNSUPPORTED: list[tuple[str, str]] = [
    ("rewrite-job-order", "bytes-desc"),
    ("partial-progress.enabled", "true"),
    ("partial-progress.enabled", "false"),
    ("partial-progress.max-commits", "3"),
    ("max-concurrent-file-group-rewrites", "4"),
]


@pytest.mark.parametrize(
    ("key", "value"),
    [pytest.param(key, value, id=f"{key}={value}") for key, value in _RPD_UNSUPPORTED],
)
def test_rpd_unsupported_keys_refuse_loud(spark: ReparkSession, key: str, value: str) -> None:
    """RPD keys without a fork path refuse as UnsupportedOperationException naming the key."""
    _build_shape(
        spark,
        "mem.ns.rpdun",
        parts=2,
        files_per=4,
        mor=True,
        pre=("DELETE FROM {t} WHERE id % 2 = 0",),
    )
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(
            "CALL mem.system.rewrite_position_delete_files(table => 'ns.rpdun', "
            f"options => map('{key}','{value}'))"
        ).to_arrow()
    assert key in str(caught.value)
    assert "ICE-RDF-OPTIONS-1" in str(caught.value)


def test_rdf_max_failed_commits_accepted_without_effect(spark: ReparkSession) -> None:
    """partial-progress.max-failed-commits parses and changes nothing on a clean run."""
    _build_shape(spark, "mem.ns.mfc", parts=1, files_per=2)
    got = _result_row(
        spark,
        "CALL mem.system.rewrite_data_files(table => 'ns.mfc', "
        "options => map('rewrite-all','true', 'partial-progress.max-failed-commits','7'))",
    )
    assert got["rewritten_data_files_count"] == 2
    assert got["added_data_files_count"] == 1
    assert _live_rows(spark, "mem.ns.mfc") == 100
    count, _ops = _snapshots(spark, "mem.ns.mfc")
    assert count == 3


_RPD_IAE_FIRST: list[tuple[str, str]] = [
    ("map('max-concurrent-file-group-rewrites','0')", "err_concurrent_0"),
    ("map('rewrite-job-order','bogus')", "err_bad_job_order"),
    (
        "map('partial-progress.enabled','true', 'partial-progress.max-commits','0')",
        "err_max_commits_0",
    ),
]


@pytest.mark.parametrize(
    ("options_map", "error_cell"),
    [pytest.param(m, c, id=c) for m, c in _RPD_IAE_FIRST],
)
def test_rpd_invalid_values_report_illegal_argument_first(
    spark: ReparkSession, options_map: str, error_cell: str
) -> None:
    """RPD semantic-invalid values raise Spark's IAE text before the unwired-key refusal."""
    cells = _fixture()
    error = cells[error_cell]  # type: ignore[literal-required]
    assert isinstance(error, dict)
    _build_shape(
        spark,
        "mem.ns.rpdiae",
        parts=2,
        files_per=4,
        mor=True,
        pre=("DELETE FROM {t} WHERE id % 2 = 0",),
    )
    with pytest.raises(IllegalArgumentException, match=re.escape(str(error["error"]))):
        spark.sql(
            "CALL mem.system.rewrite_position_delete_files(table => 'ns.rpdiae', "
            f"options => {options_map})"
        ).to_arrow()


def test_remove_dangling_null_map_key_wins_over_flag(spark: ReparkSession) -> None:
    """A present NULL map key means Java's default (false), even with the legacy flag true."""
    _build_shape(
        spark,
        "mem.ns.dnull",
        parts=2,
        files_per=8,
        mor=True,
        pre=("DELETE FROM {t} WHERE id % 2 = 0",),
    )
    spark.sql("CALL mem.system.rewrite_position_delete_files(table => 'ns.dnull')").to_arrow()
    got = _result_row(
        spark,
        "CALL mem.system.rewrite_data_files(table => 'ns.dnull', "
        "options => map('remove-dangling-deletes', NULL), "
        "'remove-dangling-deletes' => true)",
    )
    assert got["removed_delete_files_count"] == 0
    _files, deletes, _specs = _file_state(spark, "mem.ns.dnull")
    assert deletes == 2
    assert _live_rows(spark, "mem.ns.dnull") == 400


def test_residue_repark_sequence_pins_current_shape(spark: ReparkSession) -> None:
    """RePark's own rpd-then-rdf sequence keeps rows and leaves two delete files behind."""
    _build_shape(
        spark,
        "mem.ns.res",
        parts=2,
        files_per=8,
        mor=True,
        pre=("DELETE FROM {t} WHERE id % 2 = 0",),
    )
    rpd = _result_row(spark, "CALL mem.system.rewrite_position_delete_files(table => 'ns.res')")
    assert rpd["rewritten_delete_files_count"] >= 0
    rdf = _result_row(spark, "CALL mem.system.rewrite_data_files(table => 'ns.res')")
    assert rdf["rewritten_data_files_count"] >= 0
    assert _live_rows(spark, "mem.ns.res") == 400
    _files, deletes, _specs = _file_state(spark, "mem.ns.res")
    assert deletes == 2


def test_residue_matches_spark_zero_delete_files(spark: ReparkSession) -> None:
    """Spark's sequence ends with zero delete files, and RePark's does too."""
    _build_shape(
        spark,
        "mem.ns.residue",
        parts=2,
        files_per=8,
        mor=True,
        pre=("DELETE FROM {t} WHERE id % 2 = 0",),
    )
    spark.sql("CALL mem.system.rewrite_position_delete_files(table => 'ns.residue')").to_arrow()
    spark.sql("CALL mem.system.rewrite_data_files(table => 'ns.residue')").to_arrow()
    _files, deletes, _specs = _file_state(spark, "mem.ns.residue")
    assert deletes == 0


_RPD_BYTE_CELLS = ("rpd_rewrite_all", "rpd_min_input_files_1")


def _normalise_rerun(cells: dict[str, object]) -> dict[str, object]:
    """Drop run-varying delete-rewrite byte counts; assert them positive instead."""
    pruned = json.loads(json.dumps(cells))
    for name in _RPD_BYTE_CELLS:
        result = pruned[name]["result"]
        assert isinstance(result, dict)
        for key in ("rewritten_bytes_count", "added_bytes_count"):
            assert int(result.pop(key)) > 0, f"{name} {key} must stay positive"
    step = pruned["residue_rpd_then_rdf"]["rewrite_position_delete_files"]["result"]
    assert isinstance(step, dict)
    for key in ("rewritten_bytes_count", "added_bytes_count"):
        assert int(step.pop(key)) > 0, f"residue rpd step {key} must stay positive"
    return pruned


def test_live_oracle_still_matches_spark(tmp_path: Path) -> None:
    """Live tier re-runs the generator and asserts the fixture is still Spark's answer."""
    if os.environ.get("REPARK_PARITY_LIVE") != "1":
        pytest.skip("REPARK_PARITY_LIVE unset")
    sparkenv = Path(os.environ.get("REPARK_SPARKENV_PYTHON", "/tmp/sparkenv/bin/python"))
    if not sparkenv.exists():
        pytest.skip(f"sparkenv interpreter missing: {sparkenv}")
    warehouse = tmp_path / "wh"
    rerun = tmp_path / "rerun.json"
    proc = subprocess.run(
        [
            str(sparkenv),
            str(GENERATOR_PATH),
            "--warehouse",
            str(warehouse),
            "--output",
            str(rerun),
        ],
        capture_output=True,
        text=True,
        timeout=1800,
        env={
            **os.environ,
            "JAVA_HOME": os.environ.get("JAVA_HOME", "/usr/lib/jvm/zulu-17-amd64"),
            "SPARK_LOCAL_IP": os.environ.get("SPARK_LOCAL_IP", "127.0.0.1"),
        },
    )
    assert proc.returncode == 0, f"generator failed: {proc.stderr[-3000:]}"
    want = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    got = json.loads(rerun.read_text(encoding="utf-8"))
    assert _normalise_rerun(got) == _normalise_rerun(want)
    banner = subprocess.run(
        [str(sparkenv), "-c", _BANNER_SNIPPET],
        capture_output=True,
        text=True,
        timeout=600,
        env={
            **os.environ,
            "JAVA_HOME": os.environ.get("JAVA_HOME", "/usr/lib/jvm/zulu-17-amd64"),
            "SPARK_LOCAL_IP": os.environ.get("SPARK_LOCAL_IP", "127.0.0.1"),
        },
    )
    assert banner.returncode == 0, f"banner failed: {banner.stderr[-1000:]}"
    print(f"oracle banner: {banner.stdout.strip()}")
    assert banner.stdout.strip().startswith("4.1.")
