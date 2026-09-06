"""Engine-free pins for the W-0 window-shape bench.

pins: w-0-window-bench/C-001, C-002, C-003, C-004, C-007, C-008, C-009, C-010, C-011
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parents[1] / "bench"
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from windows.classify import (  # noqa: E402
    OUTCOME_ABSENT,
    OUTCOME_ERROR,
    OUTCOME_OOM,
    OUTCOME_REFUSE,
    classify_exception_text,
    registry_heading,
)
from windows.datagen import (  # noqa: E402
    cleanup_scratch,
    seed_table,
    write_seed_parquet,
)
from windows.hardware import hardware_fields  # noqa: E402
from windows.models import RunResult  # noqa: E402
from windows.report import render_markdown  # noqa: E402
from windows.roster import (  # noqa: E402
    ABSENT_PLANNING_NAMES,
    FULL_UNPARTITIONED_ROWS,
    PROBE_NAMES,
    REFUSING_SLIDING_NAMES,
    RESCANNED_SLIDING_NAMES,
    RETRACT_NAMES,
    constant_select,
    lead_lag_select,
    retract_class,
    sliding_select,
    sliding_sum_select,
    spec_by_name,
    unpartitioned_select,
)

_WINDOWS_DIR = _BENCH_DIR / "windows"

CHARTER_NAMES: tuple[str, ...] = (
    "any",
    "any_value",
    "approx_count_distinct",
    "approx_percentile",
    "array_agg",
    "avg",
    "bit_and",
    "bit_or",
    "bit_xor",
    "bool_and",
    "bool_or",
    "collect_list",
    "collect_set",
    "corr",
    "count",
    "count_if",
    "covar_pop",
    "covar_samp",
    "every",
    "first",
    "first_value",
    "kurtosis",
    "last",
    "last_value",
    "max",
    "max_by",
    "mean",
    "median",
    "min",
    "min_by",
    "mode",
    "percentile",
    "percentile_approx",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "skewness",
    "some",
    "std",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "sum",
    "try_avg",
    "try_sum",
    "var_pop",
    "var_samp",
    "variance",
)


def test_probe_roster_matches_the_charter_enumeration() -> None:
    """C-002: the live roster is exactly the ledger's finite partition."""
    assert PROBE_NAMES == CHARTER_NAMES
    assert len(set(PROBE_NAMES)) == len(PROBE_NAMES)
    assert set(RETRACT_NAMES) <= set(PROBE_NAMES)


def test_frozen_refuse_set_is_the_measured_thirteen() -> None:
    """C-009 / WIN-SLIDE-1 C-008: the thirteen once-refusing names are the frozen finite list."""
    assert REFUSING_SLIDING_NAMES == ()
    assert RESCANNED_SLIDING_NAMES == (
        "approx_count_distinct",
        "approx_percentile",
        "bit_and",
        "bit_or",
        "bool_and",
        "bool_or",
        "collect_list",
        "collect_set",
        "corr",
        "covar_pop",
        "covar_samp",
        "percentile_approx",
        "try_sum",
    )
    assert set(RESCANNED_SLIDING_NAMES) <= set(PROBE_NAMES)
    assert spec_by_name("approx_count_distinct").sql_expr == "approx_count_distinct(vi)"
    assert ABSENT_PLANNING_NAMES == (
        "any",
        "any_value",
        "every",
        "first",
        "kurtosis",
        "last",
        "max_by",
        "min_by",
        "mode",
        "percentile",
        "skewness",
        "some",
        "std",
        "variance",
    )
    assert not (set(REFUSING_SLIDING_NAMES) & set(ABSENT_PLANNING_NAMES))
    classified = set(REFUSING_SLIDING_NAMES) | set(ABSENT_PLANNING_NAMES)
    assert classified <= set(PROBE_NAMES)


def test_registry_has_a_heading_per_sliding_refuse() -> None:
    """C-009: every frozen refuse name has a ``WIN-SLIDE-<name>`` registry row."""
    registry = Path(__file__).resolve().parents[3] / "docs" / "spark-sql-iceberg-parity.md"
    text = registry.read_text(encoding="utf-8")
    for name in REFUSING_SLIDING_NAMES:
        heading = registry_heading(name)
        assert heading in text, heading


def test_every_rescanned_name_has_a_fixed_registry_row() -> None:
    """WIN-SLIDE-1 C-008: each once-refusing name keeps its heading, now marked FIXED."""
    registry = Path(__file__).resolve().parents[3] / "docs" / "spark-sql-iceberg-parity.md"
    text = registry.read_text(encoding="utf-8")
    for name in RESCANNED_SLIDING_NAMES:
        heading = registry_heading(name)
        assert heading in text, heading
        row = text.split(heading, 1)[1].split("\n### ", 1)[0]
        assert "FIXED 2026-09-04 (WIN-SLIDE-1)" in row, name


def test_the_frozen_sliding_refuse_set_is_empty() -> None:
    """WIN-SLIDE-1 C-008: no built-in aggregate refuses a sliding frame any more."""
    assert REFUSING_SLIDING_NAMES == ()
    assert len(RESCANNED_SLIDING_NAMES) == 13


def test_full_unpartitioned_rows_is_ten_million() -> None:
    """C-004: full-scale unpartitioned ORDER BY is 1e7 rows."""
    assert FULL_UNPARTITIONED_ROWS == 10_000_000


def test_sql_shapes_are_count_wrapped_windows() -> None:
    """C-001 / C-003: sliding, constant, unpartitioned, and lead/lag SQL wrap the window."""
    sliding = sliding_select("sum(v)")
    assert "OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)" in sliding
    assert sliding.startswith("SELECT sum(v) OVER")
    assert "count(*)" not in sliding
    sunk = sliding_sum_select("sum(v)")
    assert sunk.startswith("SELECT sum(w)")
    assert "OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)" in sunk
    constant = constant_select("sum(v)")
    assert "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" in constant
    unpartitioned = unpartitioned_select("sum(v)")
    assert "OVER (ORDER BY ts)" in unpartitioned
    lead_lag = lead_lag_select()
    assert "lag(v, 1) OVER (ORDER BY ts)" in lead_lag
    assert "lead(v, 1) OVER (ORDER BY ts)" in lead_lag
    assert "count(*)" not in lead_lag
    assert spec_by_name("collect_list").sql_expr == "collect_list(v)"
    assert retract_class("sum") == "retract"
    assert retract_class("collect_list") == "nonretract"


def test_seed_table_is_deterministic_and_typed() -> None:
    """C-001: the same arguments rebuild an identical Arrow table."""
    first = seed_table(32, seed=42)
    second = seed_table(32, seed=42)
    other = seed_table(32, seed=43)
    assert first.schema == second.schema
    assert first.equals(second)
    assert not first.equals(other)
    names = set(first.schema.names)
    assert names == {"id", "ts", "v", "vi", "v2", "part"}
    assert str(first.schema.field("id").type) == "int64"
    assert str(first.schema.field("v").type) == "double"


def test_hardware_profile_has_required_keys() -> None:
    """C-001: machine profile carries cpu, cores, governor, ram_gib."""
    fields = hardware_fields()
    assert set(fields) >= {"cpu", "cores", "governor", "ram_gib"}
    assert fields["cores"].isdigit()


def test_requirements_pin_duckdb_and_pyspark() -> None:
    """C-007: bench-local pins match DuckDB 1.5.5 and PySpark 4.1.2."""
    text = (_WINDOWS_DIR / "requirements.txt").read_text(encoding="utf-8")
    assert "duckdb==1.5.5" in text
    assert "pyspark==4.1.2" in text


def test_classify_sliding_refuse_beats_generic_not_implemented() -> None:
    """C-010: a DataFusion sliding not_impl is refuse, not a rewritten query."""
    sliding = (
        "This feature is not implemented: Aggregate can not be used as a "
        "sliding accumulator because `retract_batch` is not implemented: collect_list"
    )
    assert classify_exception_text(sliding) == OUTCOME_REFUSE
    assert classify_exception_text("Error: invalid function foo") == OUTCOME_ABSENT
    assert classify_exception_text("ResourcesExhausted: memory limit") == OUTCOME_OOM
    assert classify_exception_text("something else went wrong") == OUTCOME_ERROR
    assert registry_heading("collect_list") == "### WIN-SLIDE-collect_list —"


def test_run_result_requires_version_and_machine_fields() -> None:
    """C-008: the result model cannot be built without versions and machine profile."""
    probe = [
        {
            "name": "sum",
            "sql_expr": "sum(v)",
            "intake_class": "retract",
            "outcome": "ok",
        }
    ]
    result = RunResult(
        scale="gate",
        seed=42,
        engine_version="repark-0.0.0",
        duckdb_version="1.5.5",
        pyspark_version="4.1.2",
        pyspark_skip_reason=None,
        duckdb_skip_reason=None,
        native_build="release_or_stripped size_bytes=1",
        machine={"cpu": "test", "cores": "1", "governor": "schedutil", "ram_gib": "1.0"},
        dataset_bytes={"seed.parquet": 12},
        probe=probe,
        cells=[],
        peak_rss_bytes=1,
        wall_seconds=0.1,
        scratch_deleted=True,
    )
    markdown = render_markdown(result)
    assert "repark-0.0.0" in markdown
    assert "1.5.5" in markdown
    assert "4.1.2" in markdown
    assert "schedutil" in markdown
    assert "seed.parquet" in markdown
    assert "process_hwm_rss_bytes" in markdown
    assert "UNMEASURED" in markdown
    cell_headers = [line for line in markdown.splitlines() if line.startswith("| engine |")]
    for header in cell_headers:
        assert "peak_rss" not in header
        assert "rss" not in header.lower()
    probe_with_newline = RunResult(
        scale="gate",
        seed=42,
        engine_version="repark-0.0.0",
        duckdb_version="1.5.5",
        pyspark_version=None,
        pyspark_skip_reason="n/a",
        duckdb_skip_reason=None,
        native_build="x",
        machine={"cpu": "t", "cores": "1", "governor": "schedutil", "ram_gib": "1.0"},
        dataset_bytes={"seed.parquet": 12},
        probe=[
            {
                "name": "collect_list",
                "sql_expr": "collect_list(v)",
                "intake_class": "nonretract",
                "outcome": "refuse",
                "message": "line one\nDid you mean 'avg'?",
            }
        ],
        cells=[],
        peak_rss_bytes=1,
        wall_seconds=0.1,
        scratch_deleted=True,
    )
    rendered = render_markdown(probe_with_newline)
    assert "line one Did you mean 'avg'?" in rendered
    assert "| `collect_list` |" in rendered
    for line in rendered.splitlines():
        if line.startswith("| `collect_list`"):
            assert "\n" not in line


def test_cleanup_scratch_deletes_unless_keep(tmp_path: Path) -> None:
    """C-011: generated files are deleted after the run unless keep is set."""
    scratch = tmp_path / "scratch"
    written = write_seed_parquet(scratch / "seed.parquet", 8)
    assert written > 0
    assert scratch.exists()
    kept = cleanup_scratch(scratch, keep=True)
    assert kept is False
    assert (scratch / "seed.parquet").is_file()
    gone = cleanup_scratch(scratch, keep=False)
    assert gone is True
    assert not scratch.exists()


def _function_named(tree: ast.AST, name: str) -> ast.FunctionDef:
    """Return the function definition ``name`` from ``tree``."""
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"missing function {name}")


def test_run_repark_sql_does_not_retry_a_different_query() -> None:
    """C-010: the error path records an outcome; it does not call make_session again."""
    source = (_WINDOWS_DIR / "measure.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    function = _function_named(tree, "run_repark_sql")
    calls = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "make_session"
    ]
    assert calls == []
    returns = [node for node in ast.walk(function) if isinstance(node, ast.Return)]
    assert len(returns) >= 2


def test_run_window_measurement_cleans_scratch_in_finally() -> None:
    """C-011: Spark-start abort still deletes generated datasets."""
    source = (_WINDOWS_DIR / "measure.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    function = _function_named(tree, "run_window_measurement")
    cleaned = False
    for node in ast.walk(function):
        if not isinstance(node, ast.Try):
            continue
        for statement in node.finalbody:
            for child in ast.walk(statement):
                if (
                    isinstance(child, ast.Call)
                    and isinstance(child.func, ast.Name)
                    and child.func.id == "cleanup_scratch"
                ):
                    cleaned = True
    assert cleaned is True
