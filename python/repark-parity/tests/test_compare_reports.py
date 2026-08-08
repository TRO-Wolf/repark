"""Unit battery for the census report comparator (design §6.4).

The comparator is NEW code in the V2 port — it is the acceptance gate itself — so every
property the design states is pinned here over synthetic reports: manifest-first loud
failure, ledger-only subtraction, sorted-rendering byte comparison, both denominators,
the direction-grouped delta, junit mode with skips first-class, and the provoked
undeclared-subtraction attempt that proves the ledger is the only way out of the diff.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

# compat/ lives next to src/ under python/repark-parity (not the wheel package).
_PARITY_ROOT = Path(__file__).resolve().parents[1]
if str(_PARITY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PARITY_ROOT))

from compat import compare_reports  # noqa: E402
from compat.classify import CensusRow, denominators  # noqa: E402
from compat.compare_reports import (  # noqa: E402
    EXIT_DIFFERENT,
    EXIT_IDENTICAL,
    EXIT_LOUD_FAIL,
    FROZEN_OPTIONS,
    MANIFEST_KEYS,
    ComparatorError,
    build_parser,
    compute_delta,
    junit_node_id,
    load_ledger,
    main,
    render_side,
)

_MANIFEST: dict[str, str] = {
    "python_version": "3.12.7",
    "pyspark_version": "4.1.2",
    "spark_tag": "v4.1.2",
    "spark_commit_sha": "deadbeefcafe",
    "pandas_version": "2.2.3",
    "pyarrow_version": "25.0.0",
}


def _honest_denominators(rows: dict[str, str]) -> dict[str, Any]:
    """The denominator block a real runner would write for exactly these rows.

    The comparator validates a report's recorded block against its own rows, so a fixture
    that records a fictitious block is a malformed report — which is a different test.
    """
    return denominators(
        [CensusRow(test_id=test_id, module="", status=status) for test_id, status in rows.items()]
    )


def _report(rows: dict[str, str], **manifest_overrides: str) -> dict[str, Any]:
    """A minimal but shape-faithful compat.runner JSON report."""
    manifest = {**_MANIFEST, **manifest_overrides}
    by_module: dict[str, list[dict[str, Any]]] = {}
    for test_id, status in rows.items():
        module = test_id.split(".")[-3] if test_id.count(".") >= 2 else "test_functions"
        by_module.setdefault(module, []).append(
            {
                "test_id": test_id,
                "module": module,
                "status": status,
                "cause": "",
                "divergent_frame": "",
                "duration_s": 0.01,
                "harness_justification": "",
                "error_type": "",
                "raw_traceback": "",
                "tags": [],
            }
        )
    return {
        "generated_at": "2026-08-08T00:00:00+00:00",
        "repark_version": "0.0.0",
        **manifest,
        "denominators": _honest_denominators(rows),
        "ranked_census": [],
        "patch_log": [],
        "findings": [],
        "modules": [
            {
                "module": module,
                "import_name": f"pyspark.sql.tests.{module}",
                "wall_s": 1.0,
                "timed_out": False,
                "error": None,
                "rows": module_rows,
            }
            for module, module_rows in sorted(by_module.items())
        ],
    }


def _write(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


_BASE_ROWS: dict[str, str] = {
    "pyspark.sql.tests.test_functions.FunctionsTests.test_a": "PASS",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_b": "FAIL-VALUE",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_c": "NEEDS-JVM",
    "pyspark.sql.tests.test_types.TypesTests.test_d": "PASS",
}


def _pair(tmp_path: Path, left: dict[str, str], right: dict[str, str]) -> tuple[Path, Path]:
    return (
        _write(tmp_path / "v1.json", _report(left)),
        _write(tmp_path / "v2.json", _report(right)),
    )


def _run(argv: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    code = main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


# ---------------------------------------------------------------------------
# The happy path and the five difference directions
# ---------------------------------------------------------------------------


def test_identical_reports_exit_zero(tmp_path, capsys) -> None:
    v1, v2 = _pair(tmp_path, _BASE_ROWS, dict(_BASE_ROWS))
    code, out, err = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_IDENTICAL
    assert "byte comparison: IDENTICAL" in out
    assert "VERDICT: empty diff — exit 0" in out
    assert err == ""


def test_one_moved_cell_exits_one_and_names_it(tmp_path, capsys) -> None:
    moved = dict(_BASE_ROWS)
    moved["pyspark.sql.tests.test_functions.FunctionsTests.test_a"] = "FAIL-VALUE"
    v1, v2 = _pair(tmp_path, _BASE_ROWS, moved)
    code, out, _ = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_DIFFERENT
    assert "pass -> fail: 1" in out
    assert "pyspark.sql.tests.test_functions.FunctionsTests.test_a" in out
    assert "v1=PASS" in out and "v2=FAIL-VALUE" in out
    assert "byte comparison: DIFFERENT" in out


def test_fail_to_pass_and_class_change_are_grouped_separately(tmp_path, capsys) -> None:
    moved = dict(_BASE_ROWS)
    moved["pyspark.sql.tests.test_functions.FunctionsTests.test_b"] = "PASS"
    moved["pyspark.sql.tests.test_functions.FunctionsTests.test_c"] = "HARNESS"
    v1, v2 = _pair(tmp_path, _BASE_ROWS, moved)
    code, out, _ = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_DIFFERENT
    assert "fail -> pass: 1" in out
    assert "class change: 1" in out
    assert "pass -> fail: 0" in out


def test_vanished_row_exits_one_and_names_it(tmp_path, capsys) -> None:
    shrunk = dict(_BASE_ROWS)
    del shrunk["pyspark.sql.tests.test_types.TypesTests.test_d"]
    v1, v2 = _pair(tmp_path, _BASE_ROWS, shrunk)
    code, out, _ = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_DIFFERENT
    assert "vanished (only in v1): 1" in out
    assert "pyspark.sql.tests.test_types.TypesTests.test_d" in out
    assert "appeared (only in v2): 0" in out


def test_appeared_row_exits_one_and_names_it(tmp_path, capsys) -> None:
    grown = dict(_BASE_ROWS)
    grown["pyspark.sql.tests.test_types.TypesTests.test_new"] = "PASS"
    v1, v2 = _pair(tmp_path, _BASE_ROWS, grown)
    code, out, _ = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_DIFFERENT
    assert "appeared (only in v2): 1" in out
    assert "pyspark.sql.tests.test_types.TypesTests.test_new" in out


def test_denominator_change_is_re_asserted_and_named(tmp_path, capsys) -> None:
    """Both denominators are recomputed over the compared rows and must match exactly."""
    grown = dict(_BASE_ROWS)
    grown["pyspark.sql.tests.test_types.TypesTests.test_new"] = "PASS"
    v1, v2 = _pair(tmp_path, _BASE_ROWS, grown)
    code, out, _ = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_DIFFERENT
    assert "all_collected: v1=4  v2=5   <== DIFFERS" in out
    assert "engine_relevant: v1=3  v2=4   <== DIFFERS" in out
    assert "pass/all_collected:" in out
    assert "pass/engine_relevant:" in out


def test_denominators_are_reported_even_when_identical(tmp_path, capsys) -> None:
    v1, v2 = _pair(tmp_path, _BASE_ROWS, dict(_BASE_ROWS))
    code, out, _ = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_IDENTICAL
    # PASS=2, all=4, engine-relevant excludes NEEDS-JVM → 3.
    assert "pass/all_collected:     v1=2/4  v2=2/4" in out
    assert "pass/engine_relevant:   v1=2/3  v2=2/3" in out


# ---------------------------------------------------------------------------
# Ledgers: the ONLY subtraction inputs
# ---------------------------------------------------------------------------


def test_deferred_cell_present_on_one_side_only_passes(tmp_path, capsys) -> None:
    """A deferred v1 test is absent here by design: subtract from v1 only, and pass."""
    deferred_id = "pyspark.sql.tests.test_readwriter.ReadwriterTests.test_excel"
    left = {**_BASE_ROWS, deferred_id: "FAIL-MISSING"}
    v1, v2 = _pair(tmp_path, left, dict(_BASE_ROWS))
    ledger = tmp_path / "deferred.txt"
    ledger.write_text(f"# excel reader, post-milestone-one\n{deferred_id}\n", encoding="utf-8")
    code, out, _ = _run(
        ["--baseline", str(v1), "--candidate", str(v2), "--deferred", str(ledger)], capsys
    )
    assert code == EXIT_IDENTICAL
    # The subtraction is echoed so the reconciliation identity is visible.
    assert "deferred_subtracted: 1" in out
    assert deferred_id in out


def test_deferred_entry_absent_from_baseline_is_echoed_not_hidden(tmp_path, capsys) -> None:
    v1, v2 = _pair(tmp_path, _BASE_ROWS, dict(_BASE_ROWS))
    ledger = tmp_path / "deferred.txt"
    ledger.write_text("pyspark.sql.tests.test_x.X.test_nowhere\n", encoding="utf-8")
    code, out, _ = _run(
        ["--baseline", str(v1), "--candidate", str(v2), "--deferred", str(ledger)], capsys
    )
    assert code == EXIT_IDENTICAL
    assert "deferred_not_present_in_baseline: 1" in out
    assert "pyspark.sql.tests.test_x.X.test_nowhere" in out


def test_deferred_does_not_subtract_from_the_candidate_side(tmp_path, capsys) -> None:
    """A "deferred" test that shows up in v2 is a finding, not a free pass."""
    deferred_id = "pyspark.sql.tests.test_readwriter.ReadwriterTests.test_excel"
    right = {**_BASE_ROWS, deferred_id: "PASS"}
    v1, v2 = _pair(tmp_path, _BASE_ROWS, right)
    ledger = tmp_path / "deferred.txt"
    ledger.write_text(f"{deferred_id}\n", encoding="utf-8")
    code, out, _ = _run(
        ["--baseline", str(v1), "--candidate", str(v2), "--deferred", str(ledger)], capsys
    )
    assert code == EXIT_DIFFERENT
    assert "appeared (only in v2): 1" in out
    assert "deferred_present_in_candidate: 1" in out


def test_quarantined_rows_excluded_both_sides_and_reported_separately(tmp_path, capsys) -> None:
    flaky = "pyspark.sql.tests.test_functions.FunctionsTests.test_a"
    moved = dict(_BASE_ROWS)
    moved[flaky] = "FAIL-VALUE"  # the known-unstable row self-differs
    v1, v2 = _pair(tmp_path, _BASE_ROWS, moved)
    quarantine = tmp_path / "quarantine.txt"
    quarantine.write_text(f"# stability-run self-diff\n{flaky}\n", encoding="utf-8")
    code, out, _ = _run(
        ["--baseline", str(v1), "--candidate", str(v2), "--quarantine", str(quarantine)], capsys
    )
    assert code == EXIT_IDENTICAL
    assert "quarantined_baseline: 1" in out
    assert "quarantined_candidate: 1" in out


def test_missing_ledger_file_is_a_loud_failure(tmp_path, capsys) -> None:
    v1, v2 = _pair(tmp_path, _BASE_ROWS, dict(_BASE_ROWS))
    code, out, err = _run(
        ["--baseline", str(v1), "--candidate", str(v2), "--deferred", str(tmp_path / "nope.txt")],
        capsys,
    )
    assert code == EXIT_LOUD_FAIL
    assert "ledger file not found" in err
    assert out == ""


def test_ledger_parsing_ignores_comments_and_blanks(tmp_path) -> None:
    ledger = tmp_path / "l.txt"
    ledger.write_text("# header\n\n  a::b  \n\n# tail\nc::d\n", encoding="utf-8")
    assert load_ledger(ledger) == ["a::b", "c::d"]
    assert load_ledger(None) == []


def test_the_ledger_file_is_the_only_subtraction_input(tmp_path, capsys, monkeypatch) -> None:
    """PROVOKED undeclared subtraction: no flag and no environment variable can hide a row.

    A row moved from PASS to FAIL-VALUE. The test then tries every non-ledger escape hatch a
    future maintainer might reach for — plausible environment variables, and (structurally)
    a CLI option — and asserts the comparator still fails and still names the row.
    """
    moved_id = "pyspark.sql.tests.test_functions.FunctionsTests.test_a"
    moved = dict(_BASE_ROWS)
    moved[moved_id] = "FAIL-VALUE"
    v1, v2 = _pair(tmp_path, _BASE_ROWS, moved)

    for name in (
        "REPARK_CENSUS_EXCLUDE",
        "REPARK_CENSUS_ALLOWLIST",
        "REPARK_COMPAT_DEFERRED",
        "REPARK_COMPARE_IGNORE",
        "CENSUS_EXCLUDE",
    ):
        monkeypatch.setenv(name, moved_id)
    code, out, _ = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_DIFFERENT
    assert moved_id in out

    # Structural half of the property: the module reads no environment at all, so there is
    # no env-shaped subtraction path to find.
    source = Path(compare_reports.__file__).read_text(encoding="utf-8")
    assert "os.environ" not in source
    assert "os.getenv" not in source
    assert "getenv" not in source

    # And the option surface is frozen: an unknown "exclude" flag is rejected outright.
    with pytest.raises(SystemExit):
        main(["--baseline", str(v1), "--candidate", str(v2), "--exclude", moved_id])


def test_cli_option_set_is_frozen() -> None:
    """Any new option is a deliberate act — and none of them may subtract a row."""
    options: set[str] = set()
    # The parser object itself is the surface under test.
    for action in build_parser()._actions:
        options.update(action.option_strings)
    assert options == set(FROZEN_OPTIONS)
    # The only subtraction inputs are file paths to checked-in ledgers.
    assert {"--deferred", "--quarantine"} <= options


# ---------------------------------------------------------------------------
# Environment manifests — compared FIRST, loud failure before any diff
# ---------------------------------------------------------------------------


def test_mismatched_manifests_fail_loudly_before_any_diff(tmp_path, capsys) -> None:
    v1 = _write(tmp_path / "v1.json", _report(_BASE_ROWS))
    v2 = _write(tmp_path / "v2.json", _report(dict(_BASE_ROWS), pyspark_version="4.0.0"))
    code, out, err = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_LOUD_FAIL
    assert "ENVIRONMENT MANIFESTS DIFFER" in err
    assert "pyspark_version" in err
    # Nothing was diffed: no report body at all.
    assert out == ""
    assert "delta by direction" not in err


def test_manifest_mismatch_beats_an_otherwise_clean_comparison(tmp_path, capsys) -> None:
    v1 = _write(tmp_path / "v1.json", _report(_BASE_ROWS))
    v2 = _write(tmp_path / "v2.json", _report(dict(_BASE_ROWS), spark_commit_sha="feedfacefeed"))
    code, _, err = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_LOUD_FAIL
    assert "spark_commit_sha" in err


def test_generated_at_and_repark_version_are_not_gated(tmp_path, capsys) -> None:
    """The two keys that differ by construction must not red an otherwise clean run."""
    left = _report(_BASE_ROWS)
    right = _report(dict(_BASE_ROWS))
    right["generated_at"] = "2027-01-01T00:00:00+00:00"
    right["repark_version"] = "9.9.9"
    v1 = _write(tmp_path / "v1.json", left)
    v2 = _write(tmp_path / "v2.json", right)
    code, out, _ = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_IDENTICAL
    assert "(not gated) generated_at" in out
    assert "(not gated) repark_version" in out


def test_external_manifest_files_are_compared_too(tmp_path, capsys) -> None:
    v1, v2 = _pair(tmp_path, _BASE_ROWS, dict(_BASE_ROWS))
    left = tmp_path / "m1.json"
    right = tmp_path / "m2.json"
    left.write_text(json.dumps({"pandas": "2.2.3"}), encoding="utf-8")
    right.write_text(json.dumps({"pandas": "3.0.0"}), encoding="utf-8")
    code, _, err = _run(
        [
            "--baseline",
            str(v1),
            "--candidate",
            str(v2),
            "--manifest-baseline",
            str(left),
            "--manifest-candidate",
            str(right),
        ],
        capsys,
    )
    assert code == EXIT_LOUD_FAIL
    assert "pandas" in err


def test_manifest_keys_are_the_documented_set() -> None:
    assert MANIFEST_KEYS == (
        "python_version",
        "pyspark_version",
        "spark_tag",
        "spark_commit_sha",
        "pandas_version",
        "pyarrow_version",
    )


def _manifest_pair(tmp_path: Path, left: dict[str, str], right: dict[str, str]) -> list[str]:
    (tmp_path / "e1.json").write_text(json.dumps(left), encoding="utf-8")
    (tmp_path / "e2.json").write_text(json.dumps(right), encoding="utf-8")
    return [
        "--manifest-baseline",
        str(tmp_path / "e1.json"),
        "--manifest-candidate",
        str(tmp_path / "e2.json"),
    ]


def test_external_manifest_cannot_overwrite_a_key_the_report_records(tmp_path, capsys) -> None:
    """The first gate must not be defeatable from the CLI.

    Two reports from genuinely different environments, plus one shared external manifest
    handed to both sides: if the external file were allowed to overwrite, the manifests would
    render identical, the gate would print "identical — gate passed", and two incomparable
    runs would exit 0.
    """
    v1 = _write(tmp_path / "v1.json", _report(_BASE_ROWS))
    v2 = _write(tmp_path / "v2.json", _report(dict(_BASE_ROWS), pyspark_version="4.0.0"))
    shared = {"pyspark_version": "4.1.2", "pandas_version": "2.2.3"}
    code, out, err = _run(
        ["--baseline", str(v1), "--candidate", str(v2), *_manifest_pair(tmp_path, shared, shared)],
        capsys,
    )
    assert code == EXIT_LOUD_FAIL
    assert "EXTERNAL MANIFEST CONTRADICTS" in err
    assert "pyspark_version" in err
    assert out == ""
    assert "gate passed" not in out


def test_external_manifest_may_fill_a_key_the_report_does_not_record(tmp_path, capsys) -> None:
    """Augmenting is the legitimate use: the pip-freeze half the JSON report never carries."""
    v1, v2 = _pair(tmp_path, _BASE_ROWS, dict(_BASE_ROWS))
    freeze = {"pandas_version": "2.2.3", "polars_version": "1.35.0"}
    code, out, _ = _run(
        ["--baseline", str(v1), "--candidate", str(v2), *_manifest_pair(tmp_path, freeze, freeze)],
        capsys,
    )
    assert code == EXIT_IDENTICAL
    assert "polars_version: 1.35.0" in out


def test_restating_a_recorded_key_with_the_same_value_is_allowed(tmp_path, capsys) -> None:
    v1, v2 = _pair(tmp_path, _BASE_ROWS, dict(_BASE_ROWS))
    same = {"pyspark_version": "4.1.2"}
    code, _, _ = _run(
        ["--baseline", str(v1), "--candidate", str(v2), *_manifest_pair(tmp_path, same, same)],
        capsys,
    )
    assert code == EXIT_IDENTICAL


def test_a_pandas_major_difference_is_refused(tmp_path, capsys) -> None:
    """docs/port/census.md §1, made executable: the pandas major is a different measurement."""
    # The JSON report itself carries no pandas key — the freeze half arrives externally.
    v1 = _write(tmp_path / "v1.json", _report(_BASE_ROWS, pandas_version=""))
    v2 = _write(tmp_path / "v2.json", _report(dict(_BASE_ROWS), pandas_version=""))
    code, out, err = _run(
        [
            "--baseline",
            str(v1),
            "--candidate",
            str(v2),
            *_manifest_pair(tmp_path, {"pandas_version": "2.2.3"}, {"pandas_version": "3.0.5"}),
        ],
        capsys,
    )
    assert code == EXIT_LOUD_FAIL
    assert "ENVIRONMENT MANIFESTS DIFFER" in err
    assert "pandas_version" in err
    assert out == ""


def test_an_unrecorded_pandas_major_is_a_loud_failure(tmp_path, capsys) -> None:
    """A key nobody records compares equal by absence — so absence must be its own failure."""
    v1 = _write(tmp_path / "v1.json", _report(_BASE_ROWS, pandas_version=""))
    v2 = _write(tmp_path / "v2.json", _report(dict(_BASE_ROWS), pandas_version=""))
    code, out, err = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_LOUD_FAIL
    assert "ENVIRONMENT NOT RECORDED" in err
    assert "pandas_version" in err
    assert out == ""


def test_junit_mode_requires_the_pandas_major_but_not_pyspark(tmp_path, capsys) -> None:
    """The facade cohort is DEFINED by pyspark being absent, so only pandas is required."""
    v1 = _junit(tmp_path / "a.xml", _JUNIT_ROWS)
    v2 = _junit(tmp_path / "b.xml", dict(_JUNIT_ROWS))
    without_pandas = {"python_version": "3.12.7", "extras": "numpy,pandas,polars,ml-ext"}
    code, _, err = _run(
        [
            "--junit",
            "--baseline",
            str(v1),
            "--candidate",
            str(v2),
            *_manifest_pair(tmp_path, without_pandas, dict(without_pandas)),
        ],
        capsys,
    )
    assert code == EXIT_LOUD_FAIL
    assert "ENVIRONMENT NOT RECORDED" in err
    assert "pandas_version" in err
    assert "pyspark_version" not in err


# ---------------------------------------------------------------------------
# Recorded denominators — the half the byte comparison cannot imply
# ---------------------------------------------------------------------------


def test_recorded_denominators_are_validated_against_the_reports_own_rows(tmp_path, capsys) -> None:
    """The expand-baseline shape: a report claiming more collected rows than it carries."""
    payload = _report(_BASE_ROWS)
    payload["denominators"] = {"pass": 2, "all_collected": 171, "engine_relevant": 3}
    v1 = _write(tmp_path / "v1.json", payload)
    v2 = _write(tmp_path / "v2.json", _report(dict(_BASE_ROWS)))
    code, out, err = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_LOUD_FAIL
    assert "recorded denominators disagree with the rows it carries" in err
    assert "all_collected: recorded=171" in err
    assert out == ""


def test_the_recorded_denominator_gate_is_not_implied_by_the_byte_comparison(
    tmp_path, capsys
) -> None:
    """Both sides byte-identical AND identically wrong — the post-subtraction re-assert alone
    passes this, which is exactly why the recorded block is validated separately."""
    lying = _report(_BASE_ROWS)
    lying["denominators"] = {"pass": 999, "all_collected": 1, "engine_relevant": 0}
    v1 = _write(tmp_path / "v1.json", lying)
    v2 = _write(tmp_path / "v2.json", json.loads(json.dumps(lying)))
    code, out, err = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_LOUD_FAIL
    assert "malformed" in err
    assert out == ""


def test_a_report_without_a_recorded_denominator_block_still_compares(tmp_path, capsys) -> None:
    payload = _report(_BASE_ROWS)
    del payload["denominators"]
    v1 = _write(tmp_path / "v1.json", payload)
    v2 = _write(tmp_path / "v2.json", _report(dict(_BASE_ROWS)))
    code, _, _ = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_IDENTICAL


# ---------------------------------------------------------------------------
# Rendering / byte comparison
# ---------------------------------------------------------------------------


def test_render_side_is_sorted_and_byte_exact() -> None:
    rendered = render_side({"b": "PASS", "a": "FAIL-VALUE"})
    assert rendered == "a\tFAIL-VALUE\nb\tPASS\n"


def test_no_fuzzy_matching_on_class_strings(tmp_path, capsys) -> None:
    """A class string that only differs in case/whitespace is still a difference."""
    moved = dict(_BASE_ROWS)
    moved["pyspark.sql.tests.test_functions.FunctionsTests.test_b"] = "fail-value"
    v1, v2 = _pair(tmp_path, _BASE_ROWS, moved)
    code, out, _ = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_DIFFERENT
    assert "class change: 1" in out


def test_duplicate_test_ids_are_a_loud_failure(tmp_path, capsys) -> None:
    payload = _report(_BASE_ROWS)
    payload["modules"].append(dict(payload["modules"][0]))
    v1 = _write(tmp_path / "v1.json", payload)
    v2 = _write(tmp_path / "v2.json", _report(dict(_BASE_ROWS)))
    code, _, err = _run(["--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_LOUD_FAIL
    assert "duplicate test id" in err


def test_malformed_report_is_a_loud_failure(tmp_path, capsys) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    good = _write(tmp_path / "v2.json", _report(_BASE_ROWS))
    code, _, err = _run(["--baseline", str(bad), "--candidate", str(good)], capsys)
    assert code == EXIT_LOUD_FAIL
    assert "not valid JSON" in err


def test_compute_delta_is_deterministic_and_sorted() -> None:
    delta = compute_delta(
        {"z": "PASS", "a": "PASS"}, {"z": "FAIL-VALUE", "a": "FAIL-VALUE"}, junit=False
    )
    assert [test_id for test_id, _, _ in delta.pass_to_fail] == ["a", "z"]


# ---------------------------------------------------------------------------
# JUnit mode — skips are first-class outcomes
# ---------------------------------------------------------------------------


def _junit(path: Path, cases: dict[str, str]) -> Path:
    parts = ['<?xml version="1.0" encoding="utf-8"?>', "<testsuites><testsuite>"]
    for node_id, outcome in cases.items():
        classname, _, name = node_id.rpartition("::")
        body = {
            "passed": "",
            "failed": '<failure message="boom">tb</failure>',
            "error": '<error message="boom">tb</error>',
            "skipped": '<skipped type="pytest.skip" message="no jvm"/>',
            "xfailed": '<skipped type="pytest.xfail" message="known"/>',
        }[outcome]
        parts.append(f'<testcase classname="{classname}" name="{name}">{body}</testcase>')
    parts.append("</testsuite></testsuites>")
    path.write_text("".join(parts), encoding="utf-8")
    return path


def _junit_manifests(tmp_path: Path) -> tuple[Path, Path]:
    left = tmp_path / "jm1.json"
    right = tmp_path / "jm2.json"
    payload = json.dumps(
        {
            "python_version": "3.12.7",
            "pandas_version": "2.2.3",
            "extras": "numpy,pandas,polars,ml-ext",
        }
    )
    left.write_text(payload, encoding="utf-8")
    right.write_text(payload, encoding="utf-8")
    return left, right


_JUNIT_ROWS: dict[str, str] = {
    "tests.test_facade.TestX::test_one": "passed",
    "tests.test_facade.TestX::test_two": "skipped",
    "tests.test_facade.TestX::test_three": "xfailed",
    "tests.test_facade.TestX::test_four": "failed",
}


def test_junit_identical_exits_zero(tmp_path, capsys) -> None:
    left_manifest, right_manifest = _junit_manifests(tmp_path)
    v1 = _junit(tmp_path / "a.xml", _JUNIT_ROWS)
    v2 = _junit(tmp_path / "b.xml", dict(_JUNIT_ROWS))
    code, out, _ = _run(
        [
            "--junit",
            "--baseline",
            str(v1),
            "--candidate",
            str(v2),
            "--manifest-baseline",
            str(left_manifest),
            "--manifest-candidate",
            str(right_manifest),
        ],
        capsys,
    )
    assert code == EXIT_IDENTICAL
    assert "junit mode" in out


@pytest.mark.parametrize(
    ("collect_id", "expected"),
    [
        # plain module-level test — the facade cohort's only shape
        (
            "tests/test_excel_reader.py::test_excel_skip_rows",
            "tests.test_excel_reader::test_excel_skip_rows",
        ),
        # parametrized: the `[param]` suffix rides on `name` verbatim, as JUnit records it
        ("tests/test_types.py::test_cast[decimal]", "tests.test_types::test_cast[decimal]"),
        # class-based: every class segment joins the classname, the test name stays the name
        ("tests/test_facade.py::TestX::test_one", "tests.test_facade.TestX::test_one"),
        # nested directories collapse to dots
        ("tests/sub/dir/test_deep.py::test_x", "tests.sub.dir.test_deep::test_x"),
        # already JUnit-form / not a node id — returned unchanged (idempotent)
        ("tests.test_facade.TestX::test_one", "tests.test_facade.TestX::test_one"),
        ("tests.test_udf_oracle", "tests.test_udf_oracle"),
    ],
)
def test_junit_node_id_translates_ledger_ids_into_the_junit_id_space(
    collect_id: str, expected: str
) -> None:
    """EC-4: a ledger written in collect-only ids must still subtract in ``--junit`` mode."""
    assert junit_node_id(collect_id) == expected
    assert junit_node_id(junit_node_id(collect_id)) == expected


def test_junit_deferred_ledger_in_collect_only_form_actually_subtracts(tmp_path, capsys) -> None:
    """The regression itself: a collect-only ledger id must remove the matching JUnit row.

    Before the translation this echoed ``deferred_subtracted: 0`` and the run exited 1 on a
    phantom ``vanished`` row — a ledger that silently subtracts nothing.
    """
    left_manifest, right_manifest = _junit_manifests(tmp_path)
    deferred_row = "tests.test_excel_reader::test_excel_skip_rows"
    v1 = _junit(tmp_path / "a.xml", {**_JUNIT_ROWS, deferred_row: "passed"})
    v2 = _junit(tmp_path / "b.xml", dict(_JUNIT_ROWS))
    ledger = tmp_path / "deferred.txt"
    ledger.write_text(
        "# collect-only id form, as the checked-in ledger is written\n"
        "tests/test_excel_reader.py::test_excel_skip_rows\n",
        encoding="utf-8",
    )
    code, out, _ = _run(
        [
            "--junit",
            "--baseline",
            str(v1),
            "--candidate",
            str(v2),
            "--deferred",
            str(ledger),
            "--manifest-baseline",
            str(left_manifest),
            "--manifest-candidate",
            str(right_manifest),
        ],
        capsys,
    )
    assert code == EXIT_IDENTICAL
    assert "deferred_subtracted: 1" in out
    assert "deferred_not_present_in_baseline: 0" in out
    assert "vanished (only in v1): 0" in out


def test_junit_skip_state_change_is_detected(tmp_path, capsys) -> None:
    """A test that silently stops skipping is exactly as interesting as one that stops passing."""
    left_manifest, right_manifest = _junit_manifests(tmp_path)
    moved = dict(_JUNIT_ROWS)
    moved["tests.test_facade.TestX::test_two"] = "passed"
    v1 = _junit(tmp_path / "a.xml", _JUNIT_ROWS)
    v2 = _junit(tmp_path / "b.xml", moved)
    code, out, _ = _run(
        [
            "--junit",
            "--baseline",
            str(v1),
            "--candidate",
            str(v2),
            "--manifest-baseline",
            str(left_manifest),
            "--manifest-candidate",
            str(right_manifest),
        ],
        capsys,
    )
    assert code == EXIT_DIFFERENT
    assert "fail -> pass: 1" in out
    assert "tests.test_facade.TestX::test_two" in out
    # engine_relevant grows when a skip becomes a real execution.
    assert "engine_relevant: v1=2  v2=3   <== DIFFERS" in out


def test_junit_xfail_is_distinguished_from_skip(tmp_path, capsys) -> None:
    left_manifest, right_manifest = _junit_manifests(tmp_path)
    moved = dict(_JUNIT_ROWS)
    moved["tests.test_facade.TestX::test_three"] = "skipped"
    v1 = _junit(tmp_path / "a.xml", _JUNIT_ROWS)
    v2 = _junit(tmp_path / "b.xml", moved)
    code, out, _ = _run(
        [
            "--junit",
            "--baseline",
            str(v1),
            "--candidate",
            str(v2),
            "--manifest-baseline",
            str(left_manifest),
            "--manifest-candidate",
            str(right_manifest),
        ],
        capsys,
    )
    assert code == EXIT_DIFFERENT
    assert "class change: 1" in out


def test_junit_without_manifests_is_a_loud_failure(tmp_path, capsys) -> None:
    v1 = _junit(tmp_path / "a.xml", _JUNIT_ROWS)
    v2 = _junit(tmp_path / "b.xml", dict(_JUNIT_ROWS))
    code, _, err = _run(["--junit", "--baseline", str(v1), "--candidate", str(v2)], capsys)
    assert code == EXIT_LOUD_FAIL
    assert "--manifest-baseline" in err


def test_comparator_error_is_raised_not_swallowed(tmp_path) -> None:
    with pytest.raises(ComparatorError):
        load_ledger(tmp_path / "missing.txt")


def _inject_duplicate(payload: dict[str, Any], test_id: str, status: str) -> dict[str, Any]:
    """Append a second row for an EXISTING test_id (a dict-of-rows fixture cannot express a
    duplicate, so the raw modules list is edited the way the source runner actually emits it).
    The recorded denominator block is recomputed over the rows AS CARRIED — duplicates
    included — matching the real artifact, whose recorded counts cover every emitted row."""
    for module in payload["modules"]:
        for row in module["rows"]:
            if row["test_id"] == test_id:
                dup = dict(row)
                dup["status"] = status
                module["rows"].append(dup)
                carried = [
                    CensusRow(test_id=f"carried.{i}", module="", status=r["status"])
                    for m in payload["modules"]
                    for i, r in enumerate(m["rows"])
                ]
                payload["denominators"] = denominators(
                    [
                        CensusRow(test_id=f"{r.test_id}.{i}", module="", status=r.status)
                        for i, r in enumerate(carried)
                    ]
                )
                return payload
    raise AssertionError(f"fixture has no row {test_id!r}")


def test_duplicate_test_id_loads_when_quarantined(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The one escape from the duplicate-id refusal: ids named in the QUARANTINE ledger may
    repeat (the source runner emits a known duplicate pair with conflicting classes). The first
    row wins at load, the id is excluded from the gate and echoed under quarantined, and
    self-comparison exits 0."""
    dup_id = "pyspark.sql.tests.test_functions.FunctionsTests.test_a"
    payload = _inject_duplicate(_report(dict(_BASE_ROWS)), dup_id, "FAIL-MISSING")
    path = _write(tmp_path / "dup.json", payload)
    empty = tmp_path / "deferred.txt"
    empty.write_text("", encoding="utf-8")
    ledger = tmp_path / "quarantine.txt"
    ledger.write_text(f"{dup_id}\n", encoding="utf-8")
    code, out, _ = _run(
        [
            "--baseline",
            str(path),
            "--candidate",
            str(path),
            "--deferred",
            str(empty),
            "--quarantine",
            str(ledger),
        ],
        capsys,
    )
    assert code == 0
    assert dup_id in out


def test_duplicate_test_id_without_quarantine_still_refuses(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The contrast pin: the same duplicate WITHOUT a quarantine entry is a loud exit 2."""
    dup_id = "pyspark.sql.tests.test_functions.FunctionsTests.test_a"
    payload = _inject_duplicate(_report(dict(_BASE_ROWS)), dup_id, "FAIL-MISSING")
    path = _write(tmp_path / "dup.json", payload)
    empty = tmp_path / "empty.txt"
    empty.write_text("", encoding="utf-8")
    code, _, err = _run(
        [
            "--baseline",
            str(path),
            "--candidate",
            str(path),
            "--deferred",
            str(empty),
            "--quarantine",
            str(empty),
        ],
        capsys,
    )
    assert code == 2
    assert "duplicate test id" in err


def test_added_cell_present_on_candidate_side_only_passes(tmp_path, capsys) -> None:
    """A v2-only test is absent from the pin by design: subtract from the CANDIDATE only, pass.

    The mirror of `test_deferred_cell_present_on_one_side_only_passes`: the reconciliation
    identity `(candidate minus added) union deferred = baseline` holds when an added row is
    subtracted."""
    added_id = "pyspark.sql.tests.test_functions.FunctionsTests.test_v2_only"
    right = {**_BASE_ROWS, added_id: "PASS"}
    v1, v2 = _pair(tmp_path, dict(_BASE_ROWS), right)
    ledger = tmp_path / "added.txt"
    ledger.write_text(f"# tier-2 AWS guard, v2-only\n{added_id}\n", encoding="utf-8")
    code, out, _ = _run(
        ["--baseline", str(v1), "--candidate", str(v2), "--added", str(ledger)], capsys
    )
    assert code == EXIT_IDENTICAL
    assert "added_subtracted: 1" in out
    assert added_id in out


def test_added_does_not_subtract_from_the_baseline_side(tmp_path, capsys) -> None:
    """An "added" id that shows up at the pin is a finding, not a free pass — the mirror of the
    deferred-does-not-subtract-from-candidate guard."""
    added_id = "pyspark.sql.tests.test_functions.FunctionsTests.test_v2_only"
    left = {**_BASE_ROWS, added_id: "PASS"}
    v1, v2 = _pair(tmp_path, left, dict(_BASE_ROWS))
    ledger = tmp_path / "added.txt"
    ledger.write_text(f"{added_id}\n", encoding="utf-8")
    code, out, _ = _run(
        ["--baseline", str(v1), "--candidate", str(v2), "--added", str(ledger)], capsys
    )
    assert code == EXIT_DIFFERENT
    assert "vanished (only in v1): 1" in out
    assert "added_present_in_baseline: 1" in out
