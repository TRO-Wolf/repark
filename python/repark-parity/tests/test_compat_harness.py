"""Unit pins for the C2 / R-PYSPARK-COMPAT harness (no JVM, no Apache suite run).

Covers classify honesty, both denominators, tag mapping, and filter helpers — the
mutation surface for census correctness.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# compat/ lives next to src/ under python/repark-parity (not the wheel package).
_PARITY_ROOT = Path(__file__).resolve().parents[1]
if str(_PARITY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PARITY_ROOT))

from compat import runner as runner_module  # noqa: E402
from compat.classify import (  # noqa: E402
    CensusRow,
    classify_failure,
    classify_module_timeout,
    classify_skip,
    classify_success,
    denominators,
    rank_census,
)
from compat.fetch import tag_for_pyspark_version  # noqa: E402
from compat.runner import (  # noqa: E402
    _DEFAULT_SCRATCH_C2,
    _DEFAULT_SCRATCH_C3,
    _DEFAULT_SCRATCH_C4,
    _KNOWN_FATAL_TESTS,
    _SERIES_C2,
    _SERIES_C3,
    _SERIES_C4,
    C3_EXPAND_MODULES,
    C4_EXPAND_MODULES,
    CLASSIC_MODULES,
    EXIT_USAGE,
    NIGHT1_MODULES,
    STRETCH_MODULES,
    _census_row_from_dict,
    _deselect_known_fatal,
    _env_key_allowed,
    _filter_suite,
    _series_from_args_and_env,
    _test_method_name,
    _timeout_budget_from_error,
    _worker_env,
    resolve_census_modules,
    validate_module_short,
)


def _collect_suite_test_ids(items: list[object]) -> list[str]:
    """Flatten a unittest suite tree to test ids."""
    import unittest

    ids: list[str] = []
    for item in items:
        if isinstance(item, unittest.TestSuite):
            ids.extend(_collect_suite_test_ids(list(item)))
        else:
            ids.append(item.id())  # type: ignore[union-attr]
    return ids


# ---------------------------------------------------------------------------
# tag_for_pyspark_version
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("4.1.2", "v4.1.2"),
        ("v4.1.2", "v4.1.2"),
        (" 4.1.2 ", "v4.1.2"),
        ("4.1.2.dev0", "v4.1.2"),
        ("4.1.2+dev", "v4.1.2"),
        ("4.1.2rc1", "v4.1.2"),
        ("4.1.2.rc1", "v4.1.2"),
        ("4.1.2a1", "v4.1.2"),
        ("4.10.0", "v4.10.0"),
    ],
)
def test_tag_for_pyspark_version_numeric_core(version: str, expected: str) -> None:
    assert tag_for_pyspark_version(version) == expected


@pytest.mark.parametrize("version", ["../evil", "not-a-version", "", "rc1only"])
def test_tag_for_pyspark_version_rejects_junk(version: str) -> None:
    with pytest.raises(ValueError):
        tag_for_pyspark_version(version)


def test_tag_for_pyspark_version_strips_trailing_junk() -> None:
    # Leading numeric core only — shell-ish suffixes never reach --branch.
    assert tag_for_pyspark_version("4.1.2;rm -rf /") == "v4.1.2"


# ---------------------------------------------------------------------------
# denominators (both charter formulas)
# ---------------------------------------------------------------------------


def test_denominators_both_formulas() -> None:
    rows = [
        classify_success(test_id="a", module="m"),
        classify_success(test_id="b", module="m"),
        classify_skip(test_id="c", module="m", reason="upstream"),
        classify_failure(
            test_id="d",
            module="m",
            exc_type="AttributeError",
            exc="no attribute 'parallelize'",
            tb_text="",
        ),
        classify_failure(
            test_id="e",
            module="m",
            exc_type="RuntimeError",
            exc="install_redirect failed",
            tb_text='File "compat/bootstrap.py", line 1, in install_redirect\n',
        ),
        classify_module_timeout(module="m", budget_s=1200),
        classify_failure(
            test_id="g",
            module="m",
            exc_type="AssertionError",
            exc="1 != 2",
            tb_text="",
        ),
    ]
    denoms = denominators(rows)
    assert denoms["pass"] == 2
    assert denoms["all_collected"] == 7
    # engine-relevant = all - SKIP - NEEDS-JVM - HARNESS
    # excludes: skip, needs-jvm (parallelize msg), harness (bootstrap)
    # keeps: 2 pass + module-timeout + fail-value = 4
    assert denoms["excluded_skip_upstream"] == 1
    assert denoms["excluded_needs_jvm"] == 1
    assert denoms["excluded_harness"] == 1
    assert denoms["engine_relevant"] == 4
    assert denoms["pass_over_engine_relevant"] == pytest.approx(2 / 4)
    assert denoms["module_timeout_in_engine_relevant"] == 1
    ranked = dict(rank_census(rows))
    assert ranked["PASS"] == 2
    assert ranked["MODULE-TIMEOUT"] == 1


# ---------------------------------------------------------------------------
# classify honesty — message-first JVM, no source-line parallelize steal
# ---------------------------------------------------------------------------


def test_classify_fail_missing_not_stolen_by_source_line_parallelize() -> None:
    """C1-L1: source line mentioning parallelize must not force NEEDS-JVM."""
    tb = (
        'File "test_types.py", line 94, in test_apply_schema_to_row\n'
        '    df = self.spark.read.json(self.sc.parallelize(["""{"a":2}"""]))\n'
        "         ^^^^^^^^^^^^^^^^^^^^\n"
        "AttributeError: 'DataFrameReader' object has no attribute 'json'\n"
    )
    row = classify_failure(
        test_id="t",
        module="test_types",
        exc_type="AttributeError",
        exc="'DataFrameReader' object has no attribute 'json'",
        tb_text=tb,
    )
    assert row.status == "FAIL-MISSING"


def test_classify_needs_jvm_on_parallelize_attribute_message() -> None:
    row = classify_failure(
        test_id="t",
        module="m",
        exc_type="AttributeError",
        exc=(
            "repark SparkContext has no attribute 'parallelize' "
            "(only setLogLevel / applicationId / master are implemented; "
            "full SparkContext is out of scope)"
        ),
        tb_text="",
    )
    assert row.status == "NEEDS-JVM"


def test_classify_needs_jvm_on_active_spark_context_assert() -> None:
    tb = (
        'File "pyspark/sql/functions/builtin.py", line 110, in _invoke_function\n'
        "    assert SparkContext._active_spark_context is not None\n"
        "AssertionError\n"
    )
    row = classify_failure(
        test_id="t",
        module="m",
        exc_type="AssertionError",
        exc="",
        tb_text=tb,
    )
    assert row.status == "NEEDS-JVM"


def test_classify_harness_not_on_bare_setupclass() -> None:
    """C1-L2: Apache setUpClass frames are not HARNESS by themselves."""
    tb = (
        'File "test_functions.py", line 10, in setUpClass\n'
        "    raise AttributeError('x')\n"
        "AttributeError: x\n"
    )
    row = classify_failure(
        test_id="t",
        module="m",
        exc_type="AttributeError",
        exc="x",
        tb_text=tb,
    )
    assert row.status == "FAIL-MISSING"
    assert row.harness_justification == ""


def test_classify_harness_on_compat_bootstrap_frame() -> None:
    tb = (
        'File "compat/bootstrap.py", line 20, in install_redirect\n'
        "    raise RuntimeError('seam')\n"
        "RuntimeError: seam\n"
    )
    row = classify_failure(
        test_id="t",
        module="m",
        exc_type="RuntimeError",
        exc="seam",
        tb_text=tb,
    )
    assert row.status == "HARNESS"
    assert "compat" in row.harness_justification or row.harness_justification


def test_classify_range_missing_is_fail_missing() -> None:
    row = classify_failure(
        test_id="t",
        module="m",
        exc_type="AttributeError",
        exc="'ReparkSession' object has no attribute 'range'",
        tb_text="",
    )
    assert row.status == "FAIL-MISSING"


def test_classify_assertion_value() -> None:
    row = classify_failure(
        test_id="t",
        module="m",
        exc_type="AssertionError",
        exc="1 != 2",
        tb_text="",
    )
    assert row.status == "FAIL-VALUE"


def test_classify_error_class() -> None:
    row = classify_failure(
        test_id="t",
        module="m",
        exc_type="AssertionError",
        exc="Expected error class FOO but got BAR",
        tb_text="check_error",
    )
    assert row.status == "FAIL-ERROR-CLASS"


def test_classify_py4j_type_name_is_needs_jvm() -> None:
    row = classify_failure(
        test_id="t",
        module="m",
        exc_type="Py4JJavaError",
        exc="An error occurred while calling o1.x",
        tb_text="",
    )
    assert row.status == "NEEDS-JVM"


def test_classify_session_or_context_bare_token() -> None:
    row = classify_failure(
        test_id="t",
        module="m",
        exc_type="AnalysisException",
        exc="SESSION_OR_CONTEXT_NOT_EXISTS: create a session first",
        tb_text="",
    )
    assert row.status == "NEEDS-JVM"


def test_classify_third_party_import_is_harness() -> None:
    row = classify_failure(
        test_id="t",
        module="m",
        exc_type="ImportError",
        exc="cannot import name '_builtin_table' from 'pandas.core.common'",
        tb_text="",
    )
    assert row.status == "HARNESS"


def test_classify_pandas_import_with_cache_path_stays_harness() -> None:
    """X1: cache path ``repark-pyspark-tests`` must not steal pandas ImportError → FAIL-MISSING."""
    row = classify_failure(
        test_id="pyspark.sql.tests.test_functions.FunctionsTests.test_between_function",
        module="test_functions",
        exc_type="ImportError",
        exc="cannot import name '_builtin_table' from 'pandas.core.common'",
        tb_text=(
            'File "/home/ci/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/'
            'test_functions.py", line 969, in test_between_function\n'
            "    assertDataFrameEqual(\n"
            '  File ".../site-packages/pyspark/pandas/groupby.py", line 48, in <module>\n'
            "    from pandas.core.common import _builtin_table\n"
            "ImportError: cannot import name '_builtin_table'\n"
        ),
    )
    assert row.status == "HARNESS"
    assert "third-party" in (row.harness_justification or "").lower()


def test_classify_pandas_import_with_repark_install_frame_stays_harness() -> None:
    """Octo C1: site-packages/repark frames must not steal pandas ImportError → FAIL-MISSING."""
    row = classify_failure(
        test_id="pyspark.sql.tests.test_functions.FunctionsTests.test_between_function",
        module="test_functions",
        exc_type="ImportError",
        exc="cannot import name '_builtin_table' from 'pandas.core.common'",
        tb_text=(
            'File "/home/ci/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/'
            'test_functions.py", line 969, in test_between_function\n'
            "    assertDataFrameEqual(\n"
            '  File "/home/ci/.venv/lib/python3.12/site-packages/repark/dataframe.py", '
            "line 100, in collect\n"
            "    return self.to_arrow()\n"
            '  File ".../site-packages/pyspark/pandas/groupby.py", line 48, in <module>\n'
            "    from pandas.core.common import _builtin_table\n"
            "ImportError: cannot import name '_builtin_table' from 'pandas.core.common'\n"
        ),
    )
    assert row.status == "HARNESS"
    assert "third-party" in (row.harness_justification or "").lower()


def test_classify_product_repark_import_with_cache_path_is_fail_missing() -> None:
    """Octo C1 inverse: real repark ModuleNotFoundError stays FAIL-MISSING despite cache path."""
    row = classify_failure(
        test_id="pyspark.sql.tests.test_functions.FunctionsTests.test_x",
        module="test_functions",
        exc_type="ModuleNotFoundError",
        exc="No module named 'repark.missing_surface'",
        tb_text=(
            'File "/home/ci/.cache/repark-pyspark-tests/v4.1.2/python/pyspark/sql/tests/'
            'test_functions.py", line 10, in test_x\n'
            "    from repark.missing_surface import thing\n"
            "ModuleNotFoundError: No module named 'repark.missing_surface'\n"
        ),
    )
    assert row.status == "FAIL-MISSING"
    assert row.harness_justification in (None, "")


def test_classify_site_packages_traceback_import_is_harness() -> None:
    row = classify_failure(
        test_id="t",
        module="m",
        exc_type="ImportError",
        exc="No module named 'xyzlib'",
        tb_text='File "/usr/lib/python3.12/site-packages/foo.py", line 1',
    )
    assert row.status == "HARNESS"


def test_classify_timeout_error_is_module_timeout() -> None:
    row = classify_failure(
        test_id="t",
        module="m",
        exc_type="TimeoutError",
        exc="module wall exceeded 1200s",
        tb_text="",
    )
    assert row.status == "MODULE-TIMEOUT"


def test_rank_census_clamps_unknown_status() -> None:
    rows = [CensusRow(test_id="a", module="m", status="NOT-A-REAL-CLASS")]
    ranked = dict(rank_census(rows))
    assert ranked.get("FAIL-VALUE") == 1
    assert "NOT-A-REAL-CLASS" not in ranked


# ---------------------------------------------------------------------------
# runner helpers
# ---------------------------------------------------------------------------


def test_timeout_budget_from_error() -> None:
    assert _timeout_budget_from_error(TimeoutError("module wall exceeded 1200s")) == 1200.0
    assert _timeout_budget_from_error(TimeoutError("boom")) == 0.0


def test_recording_result_timeout_error_becomes_module_timeout() -> None:
    """C3-L1: unittest-absorbed TimeoutError must not stay FAIL-VALUE."""
    import unittest

    from compat.classify import classify_module_timeout
    from compat.runner import _RecordingResult

    class HangTests(unittest.TestCase):
        def test_hang(self) -> None:
            raise TimeoutError("module wall exceeded 12s")

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(HangTests)
    result = _RecordingResult(module_short="m")
    suite.run(result)
    assert result.module_timed_out is True
    # Simulate run_module_inprocess post-suite handling:
    rows = [*result.to_census_rows(), classify_module_timeout(module="m", budget_s=12.0)]
    assert any(row.status == "MODULE-TIMEOUT" for row in rows)
    assert not any(
        row.status == "FAIL-VALUE" and "wall exceeded" in row.cause
        for row in result.to_census_rows()
    )


def test_md_cell_escapes_pipes_and_newlines() -> None:
    from compat.runner import _md_cell

    assert "|" not in _md_cell("a|b") or "\\|" in _md_cell("a|b")
    assert "\n" not in _md_cell("a\nb")
    assert _md_cell("a\nb|c") == "a b\\|c"


def test_census_row_from_dict_ignores_unknown_keys() -> None:
    row = _census_row_from_dict(
        {
            "test_id": "a",
            "module": "m",
            "status": "PASS",
            "extra_future_field": 1,
        }
    )
    assert isinstance(row, CensusRow)
    assert row.test_id == "a"
    assert row.status == "PASS"


def test_census_row_from_dict_clamps_unknown_status() -> None:
    row = _census_row_from_dict(
        {
            "test_id": "a",
            "module": "m",
            "status": "NOT-A-CLASS",
        }
    )
    assert row.status == "FAIL-VALUE"
    assert "clamped-unknown-status" in row.tags


def test_env_key_secret_scrub() -> None:
    assert _env_key_allowed("PATH") is True
    assert _env_key_allowed("AWS_SECRET_ACCESS_KEY") is False
    assert _env_key_allowed("AWS_ACCESS_KEY_ID") is False
    assert _env_key_allowed("GOOGLE_APPLICATION_CREDENTIALS") is False
    assert _env_key_allowed("HOME") is True


def test_validate_module_short_rejects_path_injection() -> None:
    validate_module_short("test_functions")
    validate_module_short("pyspark.sql.tests.test_functions")
    with pytest.raises(ValueError):
        validate_module_short("../../evil")
    with pytest.raises(ValueError):
        validate_module_short("test_functions;rm")
    with pytest.raises(ValueError):
        validate_module_short("a/b")
    with pytest.raises(ValueError):
        validate_module_short("test_functions.")
    with pytest.raises(ValueError):
        validate_module_short("test..functions")


def test_stretch_modules_include_c3_expand_order() -> None:
    """C3 charter: expand stretch after classic column/readwriter; U8 adds test_udf."""
    assert NIGHT1_MODULES == ("test_functions", "test_dataframe", "test_types")
    assert STRETCH_MODULES[:2] == ("test_column", "test_readwriter")
    assert C3_EXPAND_MODULES == (
        "test_group",
        "test_session",
        "test_conf",
        "test_catalog",
        "test_sql",
        "test_udf",  # U8 DF half shipped — own expanded-census module
    )
    assert STRETCH_MODULES[2:] == C3_EXPAND_MODULES
    assert "test_udf" in STRETCH_MODULES
    assert "test_udf" in C3_EXPAND_MODULES
    # Classic night-1 modules must not appear in the C3 cohort (dual-denom isolation).
    assert not set(NIGHT1_MODULES) & set(C3_EXPAND_MODULES)


def test_resolve_census_modules_c3_expand_ignores_night1_and_stretch() -> None:
    """C3 dual-denom: --c3-expand is only C3_EXPAND_MODULES (never blend /345).

    Pins the CLI composition site (octo C3 C1-Q-002) — constant equality alone is not
    enough; a regression that appends night-1 under --c3-expand must fail this pin.
    """
    night1_csv = ",".join(NIGHT1_MODULES)
    only_c3 = resolve_census_modules(c3_expand=True, stretch=True, modules_csv=night1_csv)
    assert only_c3 == list(C3_EXPAND_MODULES)
    assert not set(NIGHT1_MODULES) & set(only_c3)
    # Hostile modules_csv with path-ish junk is ignored entirely under c3_expand.
    only_c3_hostile = resolve_census_modules(
        c3_expand=True,
        stretch=False,
        modules_csv="test_functions,../evil",
    )
    assert only_c3_hostile == list(C3_EXPAND_MODULES)
    # Classic stretch still appends C3 modules after night-1 (charter stretch list +=).
    stretched = resolve_census_modules(c3_expand=False, stretch=True, modules_csv=night1_csv)
    assert stretched[:3] == list(NIGHT1_MODULES)
    assert stretched[3:5] == ["test_column", "test_readwriter"]
    assert stretched[5:] == list(C3_EXPAND_MODULES)
    # Series / scratch labels stay distinct (octo C3 C1-L-001 / C1-SAF-001).
    assert _SERIES_C2 != _SERIES_C3
    assert "C3" in _SERIES_C3 and "C2" in _SERIES_C2
    assert _DEFAULT_SCRATCH_C2 != _DEFAULT_SCRATCH_C3
    assert _DEFAULT_SCRATCH_C3.endswith("c3-expand")


def test_default_markdown_report_path_is_under_target_census_reports() -> None:
    """C-2 / G-6: markdown defaults to gitignored ``target/census-reports/``, never ``task/``.

    Pins the resolved path only (no full census run). Aligns with ``scripts/run_census.sh``'s
    ``CENSUS_REPORT_DIR`` default; ``task/`` is opt-in via ``--markdown``.
    """
    from pathlib import Path

    from compat.runner import default_markdown_report_path

    worktree = Path("/tmp/fake-worktree")
    classic = default_markdown_report_path(worktree, date_stamp="2026-08-10")
    assert classic == worktree / "target" / "census-reports" / (
        "pyspark-compat-report-2026-08-10.md"
    )
    assert "task" not in classic.parts

    c3 = default_markdown_report_path(worktree, c3_expand=True, date_stamp="2026-08-10")
    assert c3.name == "pyspark-compat-report-c3-expand-2026-08-10.md"
    assert c3.parent == worktree / "target" / "census-reports"

    c4 = default_markdown_report_path(worktree, c4_expand=True, date_stamp="2026-08-10")
    assert c4.name == "pyspark-compat-report-c4-expand2-2026-08-10.md"
    assert c4.parent == worktree / "target" / "census-reports"


def test_runner_main_wires_default_markdown_through_helper() -> None:
    """Mutation pin: ``main``'s ``args.markdown is None`` branch calls the helper (not a raw path).

    Reverting the CLI default back to a hard-coded ``task/`` path while leaving the helper
    green would fail this pin.
    """
    from pathlib import Path

    from compat import runner as runner_mod

    source = Path(runner_mod.__file__).read_text(encoding="utf-8")
    assert "default_markdown_report_path(" in source
    assert 'worktree / "task" / f"pyspark-compat-report' not in source
    # The None-branch assignment must invoke the helper by name.
    assert "args.markdown = default_markdown_report_path(" in source


def test_c4_expand_modules_charter_order() -> None:
    """C4 charter: nine expand2 modules in fixed order; never blend with C3 or night-1."""
    assert C4_EXPAND_MODULES == (
        "test_subquery",
        "test_collection",
        "test_repartition",
        "test_utils",
        "test_errors",
        "test_stat",
        "test_creation",
        "test_conversion",
        "test_serde",
    )
    # Dual-denom isolation: C4 cohort must not intersect classic night-1 or C3 expand.
    assert not set(NIGHT1_MODULES) & set(C4_EXPAND_MODULES)
    assert not set(C3_EXPAND_MODULES) & set(C4_EXPAND_MODULES)
    # Permanently OUT modules must not sneak into the expand2 list.
    for forbidden in (
        "test_tvf",
        "test_functions",
        "test_dataframe",
        "test_types",
        "test_udf",
    ):
        assert forbidden not in C4_EXPAND_MODULES


def test_resolve_census_modules_c4_expand_ignores_night1_c3_and_stretch() -> None:
    """C4 dual-denom: --c4-expand is only C4_EXPAND_MODULES (never blend /345 or C3)."""
    night1_csv = ",".join(NIGHT1_MODULES)
    only_c4 = resolve_census_modules(
        c3_expand=True,
        stretch=True,
        modules_csv=night1_csv,
        c4_expand=True,
    )
    assert only_c4 == list(C4_EXPAND_MODULES)
    assert not set(NIGHT1_MODULES) & set(only_c4)
    assert not set(C3_EXPAND_MODULES) & set(only_c4)
    only_c4_hostile = resolve_census_modules(
        c3_expand=False,
        stretch=False,
        modules_csv="test_functions,../evil,test_group",
        c4_expand=True,
    )
    assert only_c4_hostile == list(C4_EXPAND_MODULES)
    # Series / scratch labels stay distinct from C2 and C3.
    assert _SERIES_C4 != _SERIES_C2 and _SERIES_C4 != _SERIES_C3
    assert "C4" in _SERIES_C4
    assert _DEFAULT_SCRATCH_C4 != _DEFAULT_SCRATCH_C2
    assert _DEFAULT_SCRATCH_C4 != _DEFAULT_SCRATCH_C3
    assert "c4-expand2" in _DEFAULT_SCRATCH_C4


def test_render_markdown_report_c3_series_never_c2_branding() -> None:
    """C3 octo C3: markdown series label must not claim classic C2 zero-fix /345."""
    from compat.runner import build_report, render_markdown_report

    class _Prov:
        pyspark_version = "4.1.2"
        tag = "v4.1.2"
        commit_sha = "deadbeef"

    # Empty cohort still labels C3 honestly (no FAIL findings needed).
    report = build_report([], provenance=_Prov(), series=_SERIES_C3)  # type: ignore[arg-type]
    markdown = render_markdown_report(report, series=_SERIES_C3)
    assert "C3 / R-CENSUS-EXPAND" in markdown
    assert "never blend classic /345" in markdown
    assert "C2 zero-fix" not in markdown
    assert "C2 / R-PYSPARK-COMPAT" not in markdown
    # C2 path retains classic branding for the night-1 series.
    c2_report = build_report([], provenance=_Prov(), series=_SERIES_C2)  # type: ignore[arg-type]
    c2_md = render_markdown_report(c2_report, series=_SERIES_C2)
    assert "C2 / R-PYSPARK-COMPAT" in c2_md


def test_render_markdown_report_c4_series_never_c2_or_c3_branding() -> None:
    """C4 Q11: expand2 markdown must not claim classic C2 /345 or C3 expand branding."""
    from compat.runner import build_report, render_markdown_report

    class _Prov:
        pyspark_version = "4.1.2"
        tag = "v4.1.2"
        commit_sha = "deadbeef"

    report = build_report([], provenance=_Prov(), series=_SERIES_C4)  # type: ignore[arg-type]
    markdown = render_markdown_report(report, series=_SERIES_C4)
    assert "C4 / R-CENSUS-EXPAND2" in markdown
    assert "C4 expand2 only" in markdown
    assert "test_subquery" in markdown and "test_serde" in markdown
    assert "C2 / R-PYSPARK-COMPAT" not in markdown
    assert "C3 / R-CENSUS-EXPAND" not in markdown
    assert "C2 zero-fix" not in markdown


def test_known_fatal_tests_exclude_c4_expand_modules() -> None:
    """C4 sole-writer: fatal deselect map must not swallow expand2 modules."""
    assert set(_KNOWN_FATAL_TESTS).isdisjoint(set(C4_EXPAND_MODULES))
    # Frozen sole entry: deliberate UDF segfault (C3/U8 surface, not C4) — octo C6.
    assert _KNOWN_FATAL_TESTS == {
        "test_udf": ("test_python_udf_segfault",),
    }


def test_render_markdown_c4_dual_denom_percentages_match_formula() -> None:
    """Dual-denom integrity: C4 markdown % must match denominators() arithmetic."""
    from compat.runner import ModuleCensus, build_report, render_markdown_report

    class _Prov:
        pyspark_version = "4.1.2"
        tag = "v4.1.2"
        commit_sha = "deadbeef"

    rows = [
        classify_success(test_id="m.T.a", module="test_serde"),
        classify_success(test_id="m.T.b", module="test_serde"),
        classify_failure(
            test_id="m.T.c",
            module="test_serde",
            exc_type="AssertionError",
            exc="value mismatch",
            tb_text="",
        ),
        classify_skip(test_id="m.T.d", module="test_serde", reason="upstream"),
        classify_failure(
            test_id="m.T.e",
            module="test_serde",
            exc_type="ImportError",
            exc="cannot import name '_builtin_table' from 'pandas.core.common'",
            tb_text='File "site-packages/pandas/core/common.py", line 1, in x',
        ),
    ]
    # Force HARNESS justification path for the pandas ImportError row.
    harness = rows[-1]
    assert harness.status == "HARNESS"
    module = ModuleCensus(
        module="test_serde",
        import_name="pyspark.sql.tests.test_serde",
        wall_s=0.1,
        timed_out=False,
        rows=rows,
    )
    report = build_report([module], provenance=_Prov(), series=_SERIES_C4)  # type: ignore[arg-type]
    denoms = denominators(report.all_rows())
    markdown = render_markdown_report(report, series=_SERIES_C4)
    # pass/all = 2/5 = 40.00%; engine-relevant = all - skip - harness = 3 → 2/3 = 66.67%
    assert denoms["pass"] == 2
    assert denoms["all_collected"] == 5
    assert denoms["engine_relevant"] == 3
    assert f"**{denoms['pass']} / {denoms['all_collected']}**" in markdown
    assert f"**{denoms['pass']} / {denoms['engine_relevant']}**" in markdown
    assert f"({denoms['pass_over_all'] * 100:.2f}%)" in markdown
    assert f"({denoms['pass_over_engine_relevant'] * 100:.2f}%)" in markdown
    assert "C4 / R-CENSUS-EXPAND2" in markdown


def test_deselect_known_fatal_exact_method_not_suffix() -> None:
    """Fatal deselect matches exact method name — not endswith prefix collision."""
    import unittest

    class Sample(unittest.TestCase):
        def test_python_udf_segfault(self) -> None:
            pass

        def test_python_udf_segfault_extra(self) -> None:
            pass

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(Sample)
    # Force ids through a module key that has the short fatal name.
    filtered, fatal_rows = _deselect_known_fatal(suite, "test_udf")
    kept_ids = [test.id() for test in filtered]
    assert len(fatal_rows) == 1
    assert fatal_rows[0].status == "NEEDS-JVM"
    assert _test_method_name(fatal_rows[0].test_id) == "test_python_udf_segfault"
    assert any(id_.endswith(".test_python_udf_segfault_extra") for id_ in kept_ids)
    assert not any(id_.endswith(".test_python_udf_segfault") for id_ in kept_ids)
    # C4 module short name → no deselect (empty fatal set).
    c4_suite = unittest.defaultTestLoader.loadTestsFromTestCase(Sample)
    kept_c4, fatal_c4 = _deselect_known_fatal(c4_suite, "test_repartition")
    assert fatal_c4 == []
    assert len(list(kept_c4)) == 2


def test_filter_matches_test_id_and_fatal_rows_under_filter() -> None:
    """--filter dual-denom: fatal NEEDS-JVM rows obey the same id match rules."""
    from compat.runner import _filter_matches_test_id

    fatal_id = "pyspark.sql.tests.test_udf.UDFTests.test_python_udf_segfault"
    other_id = "pyspark.sql.tests.test_udf.UDFTests.test_udf_with_callable"
    assert _filter_matches_test_id(fatal_id, "test_python_udf_segfault")
    assert not _filter_matches_test_id(other_id, "test_python_udf_segfault")
    assert _filter_matches_test_id(other_id, "*callable*")
    # Simulated post-deselect filter (octo C4 C2-S1-002).
    fatal_rows = [
        CensusRow(test_id=fatal_id, module="test_udf", status="NEEDS-JVM", cause="fatal"),
        CensusRow(test_id=other_id, module="test_udf", status="NEEDS-JVM", cause="x"),
    ]
    kept = [
        row
        for row in fatal_rows
        if _filter_matches_test_id(row.test_id, "test_python_udf_segfault")
    ]
    assert len(kept) == 1
    assert kept[0].test_id == fatal_id


def test_errors_overlay_rebinds_preimported_testing_utils() -> None:
    """C4 assert* honesty: overlay rebinds testing.utils even if it imported first.

    Uses fake modules so the parity harness stays free of pyspark/repark (py-test isolation).
    """
    import types

    from compat.bootstrap import _rebind_errors_on_loaded_testing_modules

    class FakePySparkBaseError(Exception):
        pass

    class FakePySparkAssertionError(FakePySparkBaseError, AssertionError):
        pass

    fake_repark_errors = types.SimpleNamespace(
        __all__=["PySparkAssertionError", "PySparkException"],
        PySparkAssertionError=FakePySparkAssertionError,
        PySparkException=FakePySparkBaseError,
    )
    module_name = "pyspark.testing.utils"
    prior = sys.modules.get(module_name)
    fake_utils = types.ModuleType(module_name)
    sentinel = type("SentinelAssertionError", (AssertionError,), {})
    fake_utils.PySparkAssertionError = sentinel  # type: ignore[attr-defined]
    fake_utils.PySparkException = Exception  # type: ignore[attr-defined]
    sys.modules[module_name] = fake_utils
    try:
        _rebind_errors_on_loaded_testing_modules(fake_repark_errors)
        assert fake_utils.PySparkAssertionError is FakePySparkAssertionError
        assert issubclass(fake_utils.PySparkAssertionError, FakePySparkBaseError)
    finally:
        if prior is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = prior


def test_patch_map_errors_overlay_documents_pyspark_assertion_error() -> None:
    """PATCH_MAP must name PySparkAssertionError (C4 check_error / assert* surface)."""
    from compat.bootstrap import PATCH_MAP

    errors_entries = [entry for entry in PATCH_MAP if "errors" in entry.target]
    assert errors_entries, "expected pyspark.errors overlay in PATCH_MAP"
    notes = " ".join(entry.notes for entry in errors_entries)
    assert "PySparkAssertionError" in notes
    assert "check_error" in notes or "assert" in notes.lower()


def test_install_redirect_patches_errors_before_test_factories() -> None:
    """Source-order pin: errors overlay must precede testing.utils factory import."""
    import inspect

    from compat import bootstrap as bootstrap_mod

    source = inspect.getsource(bootstrap_mod.install_redirect)
    errors_at = source.find("_patch_errors_overlay")
    factories_at = source.find("_patch_test_case_factories")
    assert errors_at != -1 and factories_at != -1
    assert errors_at < factories_at


def test_worker_env_propagates_c4_series_for_findings_branding() -> None:
    """Worker artifacts must not brand C4 expand as C2 zero-fix (octo C4-S1-001)."""
    root = Path("/tmp")
    env = _worker_env(worktree_root=root, series_short="C4")
    assert env["REPARK_COMPAT_SERIES"] == "C4"
    env_c2 = _worker_env(worktree_root=root, series_short="C2")
    assert env_c2["REPARK_COMPAT_SERIES"] == "C2"
    # CLI flags win regardless of worker bit.
    assert _series_from_args_and_env(c4_expand=True, c3_expand=True, worker=False)[1] == "C4"
    assert _series_from_args_and_env(c4_expand=False, c3_expand=True, worker=False)[1] == "C3"
    monkey_key = "REPARK_COMPAT_SERIES"
    old = os.environ.get(monkey_key)
    try:
        os.environ[monkey_key] = "C4"
        # Parent (non-worker) must ignore leaked env — classic modules stay C2 branded.
        series_parent, short_parent = _series_from_args_and_env(
            c4_expand=False, c3_expand=False, worker=False
        )
        assert short_parent == "C2" and series_parent == _SERIES_C2
        # Worker honors parent-stamped env for single-module C4 branding.
        series_worker, short_worker = _series_from_args_and_env(
            c4_expand=False, c3_expand=False, worker=True
        )
        assert short_worker == "C4" and series_worker == _SERIES_C4
        assert "C2 zero-fix" not in series_worker
    finally:
        if old is None:
            os.environ.pop(monkey_key, None)
        else:
            os.environ[monkey_key] = old


def test_tag_strips_leading_zeros() -> None:
    assert tag_for_pyspark_version("04.1.2") == "v4.1.2"


def test_filter_suite_method_name_not_prefix() -> None:
    import unittest

    class Sample(unittest.TestCase):
        def test_day(self) -> None:
            pass

        def test_dayofweek(self) -> None:
            pass

    loader = unittest.defaultTestLoader
    suite = loader.loadTestsFromTestCase(Sample)
    # Force module-like ids: unittest id is module.Class.method
    filtered = _filter_suite(suite, "test_day")
    ids = _collect_suite_test_ids(list(filtered))
    assert len(ids) == 1
    assert ids[0].endswith(".test_day")
    assert not any(item.endswith(".test_dayofweek") for item in ids)


# ---------------------------------------------------------------------------
# Phase-3 EC-8 (design §5 F1): the ADDITIVE classic cohort, and the pin that
# documents the --stretch blending trap it exists to avoid.
# ---------------------------------------------------------------------------


def test_classic_modules_charter_order() -> None:
    """EC-8: CLASSIC_MODULES is the /345 five-module cohort, in charter order."""
    assert CLASSIC_MODULES == (
        "test_functions",
        "test_dataframe",
        "test_types",
        "test_column",
        "test_readwriter",
    )
    # The classic cohort is night-1 plus exactly the two classic stretch modules — and
    # nothing from the expand cohorts (dual-denom isolation).
    assert CLASSIC_MODULES[:3] == NIGHT1_MODULES
    assert CLASSIC_MODULES[3:] == STRETCH_MODULES[:2]
    assert not set(CLASSIC_MODULES) & set(C3_EXPAND_MODULES)
    assert not set(CLASSIC_MODULES) & set(C4_EXPAND_MODULES)


def test_resolve_census_modules_classic_is_denominator_isolated() -> None:
    """--classic returns ONLY CLASSIC_MODULES; stretch / modules_csv cannot leak in."""
    only_classic = resolve_census_modules(
        c3_expand=False,
        stretch=True,
        modules_csv=",".join(NIGHT1_MODULES),
        classic=True,
    )
    assert only_classic == list(CLASSIC_MODULES)
    assert not set(only_classic) & set(C3_EXPAND_MODULES)
    # Hostile modules_csv is ignored entirely under --classic (same shape as --c3-expand).
    hostile = resolve_census_modules(
        c3_expand=False,
        stretch=False,
        modules_csv="test_functions,../evil",
        classic=True,
    )
    assert hostile == list(CLASSIC_MODULES)


def test_resolve_census_modules_classic_precedence_under_expand_flags() -> None:
    """Fixed precedence: c4 > c3 > classic — an expand cohort is never widened by --classic."""
    under_c4 = resolve_census_modules(
        c3_expand=False,
        stretch=True,
        modules_csv=",".join(NIGHT1_MODULES),
        c4_expand=True,
        classic=True,
    )
    assert under_c4 == list(C4_EXPAND_MODULES)
    under_c3 = resolve_census_modules(
        c3_expand=True,
        stretch=True,
        modules_csv=",".join(NIGHT1_MODULES),
        classic=True,
    )
    assert under_c3 == list(C3_EXPAND_MODULES)


def test_resolve_census_modules_classic_defaults_off_preserves_ported_behavior() -> None:
    """`classic` is keyword-only with default False — every ported call site is unchanged."""
    night1_csv = ",".join(NIGHT1_MODULES)
    assert resolve_census_modules(c3_expand=False, stretch=False, modules_csv=night1_csv) == list(
        NIGHT1_MODULES
    )


def test_stretch_blends_c3_into_the_classic_denominator() -> None:
    """EC-8 trap pin: --stretch is NOT the classic cohort — it appends the C3 modules.

    The ported `--stretch` flag is byte-identical to the pin on purpose. This test pins
    its blending behavior in executable form so the trap (design §5 F1: the census script
    ran the classic cohort with `--stretch` and produced an eleven-module run against a
    five-module denominator) is documented, not merely dodged. If someone "fixes"
    `STRETCH_MODULES` instead of using `--classic`, this test goes red and points at the
    ruling.
    """
    stretched = resolve_census_modules(
        c3_expand=False,
        stretch=True,
        modules_csv=",".join(NIGHT1_MODULES),
    )
    # Eleven modules, not five: night-1 + classic stretch + the whole C3 cohort.
    assert len(stretched) == len(NIGHT1_MODULES) + len(STRETCH_MODULES) == 11
    assert set(C3_EXPAND_MODULES) <= set(stretched)
    assert stretched != list(CLASSIC_MODULES)
    # And the isolated spelling is strictly narrower — the whole point of EC-8.
    classic = resolve_census_modules(
        c3_expand=False,
        stretch=True,
        modules_csv=",".join(NIGHT1_MODULES),
        classic=True,
    )
    assert set(classic) < set(stretched)


def test_cli_classic_flag_reaches_the_resolver() -> None:
    """CLI wiring pin: `--classic` must arrive at resolve_census_modules(classic=True).

    Constant equality is not enough (octo C3 C1-Q-002 precedent): the composition site is
    what runs. The resolver is stubbed to abort before any provenance fetch or census work.
    """
    captured: dict[str, object] = {}

    def spy(**kwargs: object) -> list[str]:  # nested-def: spy closes over captured kwargs
        captured.update(kwargs)
        raise ValueError("stubbed: abort before any census work")

    original = runner_module.resolve_census_modules
    runner_module.resolve_census_modules = spy  # type: ignore[assignment]
    try:
        assert runner_module.main(["--classic"]) == EXIT_USAGE
    finally:
        runner_module.resolve_census_modules = original  # type: ignore[assignment]
    assert captured["classic"] is True
    assert captured["stretch"] is False
    assert captured["c3_expand"] is False
    assert captured["c4_expand"] is False
