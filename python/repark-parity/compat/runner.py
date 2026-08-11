"""Census runner: execute Apache pyspark.sql.tests modules under the repark redirect.

Subprocess isolation per MODULE; wall ~20 min/module → MODULE-TIMEOUT.
One run, no repeats (charter).

CLI::

    PYTHONPATH=python/repark-parity:python/repark/src \\
        python -m compat.runner --modules test_functions,test_dataframe,test_types
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import re
import signal
import subprocess
import sys
import time
import traceback
import unittest
from collections.abc import Sequence
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Allow `python -m compat.runner` when python/repark-parity is on PYTHONPATH.
from compat.bootstrap import (
    assert_no_jvm,
    install_redirect,
    is_redirect_installed,
    patch_map_as_markdown,
    redirect_log,
)
from compat.classify import (
    CENSUS_CLASSES,
    CensusRow,
    classify_failure,
    classify_module_timeout,
    classify_skip,
    classify_success,
    denominators,
    format_unittest_id,
    rank_census,
    traceback_from_err,
)
from compat.fetch import SparkTestsProvenance, ensure_spark_tests

LOGGER = logging.getLogger("compat.runner")

# Worker env keys that must not leak cloud credentials into Apache test processes.
_SECRET_ENV_PREFIXES: tuple[str, ...] = (
    "AWS_SECRET",
    "AWS_ACCESS",
    "AWS_SESSION",
    "AWS_SECURITY",
    "AZURE_",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GCM_KEY",
    "PRIVATE_KEY",
)
_SECRET_ENV_EXACT: frozenset[str] = frozenset(
    {
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_CONFIG_FILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
        "AWS_ROLE_SESSION_NAME",
    }
)

# Night-1 fixed order (charter). Stretch modules only after the three are censused.
NIGHT1_MODULES: tuple[str, ...] = (
    "test_functions",
    "test_dataframe",
    "test_types",
)
# Classic stretch (column/readwriter) then C3 expansion modules in charter order.
# U8: test_udf joins the expanded census as its own module (DF half shipped).
STRETCH_MODULES: tuple[str, ...] = (
    "test_column",
    "test_readwriter",
    "test_group",
    "test_session",
    "test_conf",
    "test_catalog",
    "test_sql",
    "test_udf",
)
# Classic C2 cohort — the /345 denominator, in charter order (phase-3 EC-8, design §5 F1).
# ADDITIVE: this constant and the `--classic` flag are new in the V2 port. `STRETCH_MODULES`
# above is left byte-identical on purpose — it *appends* C3 modules to night-1, which blends
# the C3 cohort into the classic denominator. `--stretch` therefore must never be used to run
# the classic cohort; `--classic` is the isolated spelling.
CLASSIC_MODULES: tuple[str, ...] = (
    "test_functions",
    "test_dataframe",
    "test_types",
    "test_column",
    "test_readwriter",
)
# C3 census expansion cohort only (own denominators; NEVER blend with classic /345).
# U8: test_udf is an expanded module with its own denominator when DF half ships.
C3_EXPAND_MODULES: tuple[str, ...] = (
    "test_group",
    "test_session",
    "test_conf",
    "test_catalog",
    "test_sql",
    "test_udf",
)
# === r20 C4: census expansion round 2 module lists + _KNOWN_FATAL_TESTS ===
# C4 expand2 cohort only (own denominators; NEVER blend with classic /345 or C3 expand).
# Charter order: subquery/collection/repartition/utils/errors/stat/creation/conversion/serde.
# test_tvf waits; streaming/connect/artifact/resources permanently OUT.
C4_EXPAND_MODULES: tuple[str, ...] = (
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

# Report / log series labels — never emit classic C2 branding for a C3/C4-only run.
_SERIES_C2 = "C2 / R-PYSPARK-COMPAT"
_SERIES_C3 = "C3 / R-CENSUS-EXPAND"
_SERIES_C4 = "C4 / R-CENSUS-EXPAND2"
_DEFAULT_SCRATCH_C2 = "/tmp/compat-quantile-sail-2026-08-01/c2"
_DEFAULT_SCRATCH_C3 = "/tmp/compat-quantile-sail-2026-08-01/c3-expand"
_DEFAULT_SCRATCH_C4 = "/tmp/r20-grouph-census5-2026-08-09/census/c4-expand2"

DEFAULT_MODULE_TIMEOUT_S = 20 * 60  # ~20 min wall per module
# Worker exit codes.
EXIT_OK = 0
EXIT_USAGE = 2
EXIT_WORKER_FAIL = 3
EXIT_TIMEOUT = 5


@dataclass
class ModuleCensus:
    """Per-module census block."""

    module: str
    import_name: str
    wall_s: float
    timed_out: bool
    rows: list[CensusRow] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "module": self.module,
            "import_name": self.import_name,
            "wall_s": self.wall_s,
            "timed_out": self.timed_out,
            "error": self.error,
            "rows": [row.to_dict() for row in self.rows],
        }


@dataclass
class CompatReport:
    """Full harness run payload (JSON-serializable)."""

    generated_at: str
    pyspark_version: str
    spark_tag: str
    spark_commit_sha: str
    repark_version: str
    python_version: str
    modules: list[ModuleCensus] = field(default_factory=list)
    patch_log: list[str] = field(default_factory=list)
    findings: list[str] = field(default_factory=list)

    def all_rows(self) -> list[CensusRow]:
        rows: list[CensusRow] = []
        for module in self.modules:
            rows.extend(module.rows)
        return rows

    def to_dict(self) -> dict[str, Any]:
        rows = self.all_rows()
        return {
            "generated_at": self.generated_at,
            "pyspark_version": self.pyspark_version,
            "spark_tag": self.spark_tag,
            "spark_commit_sha": self.spark_commit_sha,
            "repark_version": self.repark_version,
            "python_version": self.python_version,
            "denominators": denominators(rows),
            "ranked_census": rank_census(rows),
            "patch_log": self.patch_log,
            "findings": self.findings,
            "modules": [module.to_dict() for module in self.modules],
        }


# Module short names: no path separators / shell metacharacters (worker JSON path safety).
_MODULE_SHORT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")


def validate_module_short(module_short: str) -> str:
    """Return ``module_short`` or raise ``ValueError`` if it is not a safe identifier."""
    if not module_short or not _MODULE_SHORT_RE.match(module_short):
        raise ValueError(
            f"invalid module short name {module_short!r}: "
            f"expected dotted identifiers only (no path separators or trailing dots)"
        )
    if ".." in module_short:
        raise ValueError(f"invalid module short name {module_short!r}: '..' not allowed")
    return module_short


def import_name_for(module_short: str) -> str:
    """``test_functions`` → ``pyspark.sql.tests.test_functions``."""
    validate_module_short(module_short)
    if module_short.startswith("pyspark."):
        return module_short
    return f"pyspark.sql.tests.{module_short}"


def run_module_inprocess(
    module_short: str,
    *,
    provenance: SparkTestsProvenance,
    test_filter: str | None = None,
) -> ModuleCensus:
    """Load and run one Apache module under the redirect (same process)."""
    import_name = import_name_for(module_short)
    if not is_redirect_installed():
        install_redirect(spark_tests_root=provenance.cache_dir)
        assert_no_jvm()

    started = time.perf_counter()
    try:
        # Fresh import each call (worker is one-shot; in-process re-runs need reload).
        if import_name in sys.modules:
            module = importlib.reload(sys.modules[import_name])
        else:
            module = importlib.import_module(import_name)
    except Exception as error:
        wall = time.perf_counter() - started
        tb_text = traceback.format_exc()
        row = classify_failure(
            test_id=f"{import_name}::<import>",
            module=module_short,
            exc_type=type(error),
            exc=error,
            tb_text=tb_text,
            duration_s=wall,
        )
        # Import-time failures are almost always seam/injection (HARNESS), not engine gaps.
        # Exception: true JVM signals already classified NEEDS-JVM.
        if row.status != "NEEDS-JVM":
            row.status = "HARNESS"
            row.harness_justification = (
                "module import failed after redirect install — seam/path injection "
                f"(was {row.error_type or type(error).__name__})"
            )
        return ModuleCensus(
            module=module_short,
            import_name=import_name,
            wall_s=wall,
            timed_out=False,
            rows=[row],
            error=str(error),
        )

    loader = unittest.defaultTestLoader
    suite = loader.loadTestsFromModule(module)
    # loadTestsFromModule also picks up *imported* TestCase subclasses (e.g.
    # ReusedSQLTestCase sitting in the module globals after `from … import`).
    # Keep only cases whose defining module is the Apache test module itself.
    suite = _filter_suite_defined_in(suite, import_name)
    # Deliberate-crash tests (Apache proves its process-ISOLATED Python workers survive a
    # worker segfault). repark executes UDFs in-process — the crash would kill this worker
    # and take the whole module's census with it (r19 U8 finding: test_python_udf_segfault
    # ctypes.string_at(0) core-dumped the census). Deselect + classify NEEDS-JVM (needs
    # Spark's isolated-worker architecture); documented divergence in the U8 ledger.
    suite, fatal_rows = _deselect_known_fatal(suite, module_short)
    if test_filter:
        suite = _filter_suite(suite, test_filter)
        # Dual-denom honesty under --filter: only keep fatal NEEDS-JVM rows whose
        # test id matches the same filter (octo C4 C2-S1-002). Otherwise a debug
        # filter on one method would still inject every known-fatal into the census.
        fatal_rows = [
            row for row in fatal_rows if _filter_matches_test_id(row.test_id, test_filter)
        ]

    result = _RecordingResult(module_short=module_short)
    try:
        suite.run(result)
    except TimeoutError as error:
        # SIGALRM mid-suite escaping unittest — MODULE-TIMEOUT (denominator / wall honesty).
        wall = time.perf_counter() - started
        timeout_row = classify_module_timeout(
            module=module_short,
            budget_s=_timeout_budget_from_error(error),
        )
        rows = [*result.to_census_rows(), *fatal_rows, timeout_row]
        return ModuleCensus(
            module=module_short,
            import_name=import_name,
            wall_s=wall,
            timed_out=True,
            rows=rows,
            error=str(error),
        )
    wall = time.perf_counter() - started
    rows = [*result.to_census_rows(), *fatal_rows]
    # TimeoutError raised inside a test is absorbed by unittest as ERROR; detect via flag.
    if result.module_timed_out:
        error = result.module_timeout_error or TimeoutError("module wall exceeded")
        timeout_row = classify_module_timeout(
            module=module_short,
            budget_s=_timeout_budget_from_error(error),
        )
        rows = [*rows, timeout_row]
        return ModuleCensus(
            module=module_short,
            import_name=import_name,
            wall_s=wall,
            timed_out=True,
            rows=rows,
            error=str(error),
        )
    return ModuleCensus(
        module=module_short,
        import_name=import_name,
        wall_s=wall,
        timed_out=False,
        rows=rows,
    )


def _timeout_budget_from_error(error: TimeoutError) -> float:
    """Parse ``module wall exceeded NNs`` if present; else 0."""
    text = str(error)
    match = re.search(r"(\d+(?:\.\d+)?)\s*s", text)
    if match is None:
        return 0.0
    return float(match.group(1))


class _RecordingResult(unittest.TestResult):
    """Collect unittest outcomes as census rows."""

    def __init__(self, module_short: str) -> None:
        super().__init__(verbosity=0)
        self.module_short = module_short
        self._rows: list[CensusRow] = []
        self._started: dict[str, float] = {}
        # Set when SIGALRM TimeoutError is absorbed by unittest as an ERROR/FAILURE.
        self.module_timed_out: bool = False
        self.module_timeout_error: TimeoutError | None = None

    def startTest(self, test: unittest.case.TestCase) -> None:  # noqa: N802 — unittest API
        super().startTest(test)
        self._started[format_unittest_id(test)] = time.perf_counter()

    def addSuccess(self, test: unittest.case.TestCase) -> None:  # noqa: N802 — unittest API
        super().addSuccess(test)
        test_id = format_unittest_id(test)
        duration = time.perf_counter() - self._started.get(test_id, time.perf_counter())
        self._rows.append(
            classify_success(test_id=test_id, module=self.module_short, duration_s=duration)
        )

    def addSkip(self, test: unittest.case.TestCase, reason: str) -> None:  # noqa: N802
        super().addSkip(test, reason)
        test_id = format_unittest_id(test)
        duration = time.perf_counter() - self._started.get(test_id, time.perf_counter())
        self._rows.append(
            classify_skip(
                test_id=test_id,
                module=self.module_short,
                reason=reason,
                duration_s=duration,
            )
        )

    def addExpectedFailure(  # noqa: N802 — unittest API
        self, test: unittest.case.TestCase, err: Any
    ) -> None:
        # Treat expectedFailure as SKIP-UPSTREAM-like (upstream marks it).
        super().addExpectedFailure(test, err)
        test_id = format_unittest_id(test)
        duration = time.perf_counter() - self._started.get(test_id, time.perf_counter())
        self._rows.append(
            classify_skip(
                test_id=test_id,
                module=self.module_short,
                reason="unittest expectedFailure",
                duration_s=duration,
            )
        )

    def addUnexpectedSuccess(  # noqa: N802 — unittest API
        self, test: unittest.case.TestCase
    ) -> None:
        super().addUnexpectedSuccess(test)
        test_id = format_unittest_id(test)
        duration = time.perf_counter() - self._started.get(test_id, time.perf_counter())
        self._rows.append(
            classify_success(test_id=test_id, module=self.module_short, duration_s=duration)
        )

    def addError(self, test: unittest.case.TestCase, err: Any) -> None:  # noqa: N802
        if self._is_module_timeout_err(err):
            super().addError(test, err)
            self._mark_module_timeout(err)
            return
        super().addError(test, err)
        self._record_failure(test, err)

    def addFailure(self, test: unittest.case.TestCase, err: Any) -> None:  # noqa: N802
        if self._is_module_timeout_err(err):
            super().addFailure(test, err)
            self._mark_module_timeout(err)
            return
        super().addFailure(test, err)
        self._record_failure(test, err)

    def _is_module_timeout_err(self, err: Any) -> bool:
        if not err:
            return False
        exc_type = err[0]
        return exc_type is TimeoutError or (
            isinstance(exc_type, type) and issubclass(exc_type, TimeoutError)
        )

    def _mark_module_timeout(self, err: Any) -> None:
        self.module_timed_out = True
        exc = err[1] if err else None
        self.module_timeout_error = (
            exc if isinstance(exc, TimeoutError) else TimeoutError(str(exc) if exc else "timeout")
        )
        # Stop remaining tests in this module (wall already exceeded).
        self.stop()

    def _record_failure(self, test: unittest.case.TestCase, err: Any) -> None:
        test_id = format_unittest_id(test)
        duration = time.perf_counter() - self._started.get(test_id, time.perf_counter())
        tb_text = traceback_from_err(err)
        self._rows.append(
            classify_failure(
                test_id=test_id,
                module=self.module_short,
                exc_type=err[0] if err else None,
                exc=err[1] if err else None,
                tb_text=tb_text,
                duration_s=duration,
            )
        )

    def to_census_rows(self) -> list[CensusRow]:
        return list(self._rows)


# Apache tests that DELIBERATELY crash the executing process (proving Spark's isolated
# Python workers survive). In-process execution cannot run them; deselected + classified
# NEEDS-JVM (requires the isolated-worker architecture). Keyed by module short name.
# C4 owns this map (region banner above) — C4 expand2 modules must not appear as keys
# (no deliberate crash tests in that cohort; whole-module HARNESS on crash is only for
# unknown worker death, never a stand-in for missing deselect entries).
_KNOWN_FATAL_TESTS: dict[str, tuple[str, ...]] = {
    "test_udf": ("test_python_udf_segfault",),
}


def _test_method_name(test_id: str) -> str:
    """Last dotted segment of a unittest id (``module.Class.method`` → ``method``)."""
    return test_id.rsplit(".", 1)[-1]


def _deselect_known_fatal(
    suite: unittest.TestSuite, module_short: str
) -> tuple[unittest.TestSuite, list[CensusRow]]:
    """Remove deliberate-crash tests; return (filtered suite, their NEEDS-JVM rows).

    Match on the **exact method name** (last id segment), not ``endswith(.name)``, so a
    short fatal name cannot deselect a longer sibling (octo C4 C1-S2-005).
    """
    fatal_methods = _KNOWN_FATAL_TESTS.get(module_short, ())
    if not fatal_methods:
        return suite, []
    fatal_set = set(fatal_methods)
    kept = unittest.TestSuite()
    rows: list[CensusRow] = []

    def walk(items: Sequence[Any]) -> None:
        for item in items:
            if isinstance(item, unittest.TestSuite):
                walk(list(item))
            elif _test_method_name(item.id()) in fatal_set:
                rows.append(
                    CensusRow(
                        test_id=item.id(),
                        module=module_short,
                        status="NEEDS-JVM",
                        cause=(
                            "deliberate worker-crash test (ctypes segfault) — requires "
                            "Spark's process-isolated Python UDF workers; repark executes "
                            "UDFs in-process (documented divergence, U8 ledger)"
                        ),
                    )
                )
            else:
                kept.addTest(item)

    walk(list(suite))
    return kept, rows


def _filter_matches_test_id(test_id: str, test_filter: str) -> bool:
    """Whether ``test_id`` matches a ``--filter`` string (same rules as :func:`_filter_suite`)."""
    exact = test_filter
    method_only = ("." not in test_filter) and ("*" not in test_filter)
    substring = None
    if test_filter.startswith("*") and test_filter.endswith("*") and len(test_filter) > 2:
        substring = test_filter[1:-1]
    if test_id == exact:
        return True
    if method_only and test_id.endswith(f".{test_filter}"):
        return True
    return bool(substring is not None and substring in test_id)


def _filter_suite(suite: unittest.TestSuite, test_filter: str) -> unittest.TestSuite:
    """Keep tests matching ``test_filter``.

    Matching rules (first wins per test):
    * exact full id equality
    * id endswith ``.<filter>`` when filter has no dots (method name)
    * substring match only when filter contains ``*`` as a glob-ish marker
      (``*foo*`` → ``foo`` in id); plain substrings no longer match prefixes
    """
    kept = unittest.TestSuite()

    def walk(items: Sequence[Any]) -> None:
        for item in items:
            if isinstance(item, unittest.TestSuite):
                walk(list(item))
            elif _filter_matches_test_id(item.id(), test_filter):
                kept.addTest(item)

    walk(list(suite))
    return kept


def _filter_suite_defined_in(suite: unittest.TestSuite, import_name: str) -> unittest.TestSuite:
    """Drop imported base TestCase classes (ReusedSQLTestCase, …) from the suite."""
    kept = unittest.TestSuite()

    def walk(items: Sequence[Any]) -> None:
        for item in items:
            if isinstance(item, unittest.TestSuite):
                walk(list(item))
            else:
                defining = getattr(item, "__class__", type(item)).__module__
                if defining == import_name:
                    kept.addTest(item)

    walk(list(suite))
    return kept


def run_module_subprocess(
    module_short: str,
    *,
    timeout_s: float,
    python_executable: str,
    worktree_root: Path,
    output_json: Path,
    test_filter: str | None = None,
    series_short: str = "C2",
) -> ModuleCensus:
    """Run one module in an isolated subprocess with a hard wall timeout.

    ``series_short`` (``C2`` / ``C3`` / ``C4``) is passed via env so the worker's
    ``build_report`` findings use the same dual-denom series label as the parent
    (octo C4 C4-S1-001: bare ``--modules X`` must not brand C4 runs as C2 zero-fix).
    """
    worker = Path(__file__).resolve()
    env = _worker_env(worktree_root=worktree_root, series_short=series_short)

    cmd = [
        python_executable,
        str(worker),
        "--worker",
        "--modules",
        module_short,
        "--output",
        str(output_json),
    ]
    if test_filter:
        cmd.extend(["--filter", test_filter])

    started = time.perf_counter()
    try:
        completed = subprocess.run(
            cmd,
            env=env,
            cwd=str(worktree_root),
            capture_output=True,
            text=True,
            timeout=timeout_s + 30.0,  # grace beyond SIGALRM inside worker
            check=False,
        )
    except subprocess.TimeoutExpired:
        wall = time.perf_counter() - started
        row = classify_module_timeout(module=module_short, budget_s=timeout_s)
        return ModuleCensus(
            module=module_short,
            import_name=import_name_for(module_short),
            wall_s=wall,
            timed_out=True,
            rows=[row],
            error=f"subprocess hard-kill after {timeout_s}+30s",
        )

    wall = time.perf_counter() - started
    if output_json.is_file():
        payload = json.loads(output_json.read_text(encoding="utf-8"))
        modules = payload.get("modules") or []
        if modules:
            raw = modules[0]
            rows = [_census_row_from_dict(row) for row in raw.get("rows", [])]
            return ModuleCensus(
                module=raw.get("module", module_short),
                import_name=raw.get("import_name", import_name_for(module_short)),
                wall_s=float(raw.get("wall_s", wall)),
                timed_out=bool(raw.get("timed_out", False)),
                rows=rows,
                error=raw.get("error"),
            )

    # Worker crashed before writing output.
    stderr_tail = (completed.stderr or completed.stdout or "")[-2000:]
    row = classify_failure(
        test_id=f"{import_name_for(module_short)}::<worker>",
        module=module_short,
        exc_type="WorkerCrash",
        exc=stderr_tail or f"exit {completed.returncode}",
        tb_text=stderr_tail,
        duration_s=wall,
    )
    row.status = "HARNESS"
    row.harness_justification = "worker subprocess exited without JSON output"
    return ModuleCensus(
        module=module_short,
        import_name=import_name_for(module_short),
        wall_s=wall,
        timed_out=False,
        rows=[row],
        error=stderr_tail or f"exit {completed.returncode}",
    )


def _install_worker_alarm(timeout_s: float) -> None:
    """SIGALRM wall for the worker process (Unix)."""
    if timeout_s <= 0:
        return
    if not hasattr(signal, "SIGALRM"):
        return

    def _handler(signum: int, frame: Any) -> None:
        raise TimeoutError(f"module wall exceeded {timeout_s:.0f}s")

    signal.signal(signal.SIGALRM, _handler)
    signal.alarm(int(timeout_s))


def _worker_env(*, worktree_root: Path, series_short: str = "C2") -> dict[str, str]:
    """Build a subprocess env with worktree PYTHONPATH and cloud secrets stripped."""
    env = {key: value for key, value in os.environ.items() if _env_key_allowed(key)}
    pythonpath_parts = [
        str(worktree_root / "python" / "repark-parity"),
        str(worktree_root / "python" / "repark" / "src"),
        str(worktree_root / "python" / "repark-parity" / "src"),
    ]
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts + ([existing] if existing else []))
    env["REPARK_PYSPARK_COMPAT"] = "1"
    # Dual-denom series for worker findings (does not change module resolution).
    env["REPARK_COMPAT_SERIES"] = series_short if series_short in {"C2", "C3", "C4"} else "C2"
    env.pop("PYSPARK_SUBMIT_ARGS", None)
    return env


def _series_from_args_and_env(
    *,
    c4_expand: bool,
    c3_expand: bool,
    worker: bool = False,
) -> tuple[str, str]:
    """Return ``(series_label, series_short)`` for report branding.

    CLI ``--c4-expand`` / ``--c3-expand`` always win. ``REPARK_COMPAT_SERIES`` is honored
    **only when** ``worker`` is true (parent deliberately stamps the child env) so a
    leaked shell export cannot rebrand a classic parent run as C4 (octo C4 C5-S1-001).
    """
    env_series = ""
    if worker:
        env_series = os.environ.get("REPARK_COMPAT_SERIES", "").strip().upper()
    if c4_expand or env_series == "C4":
        return _SERIES_C4, "C4"
    if c3_expand or env_series == "C3":
        return _SERIES_C3, "C3"
    return _SERIES_C2, "C2"


def _env_key_allowed(key: str) -> bool:
    if key in _SECRET_ENV_EXACT:
        return False
    return not any(key.startswith(prefix) for prefix in _SECRET_ENV_PREFIXES)


def _census_row_from_dict(row: dict[str, Any]) -> CensusRow:
    """Reconstruct a CensusRow ignoring unknown JSON keys (forward-compatible).

    Unknown ``status`` values are clamped to ``FAIL-VALUE`` (same as rank/denominators).
    """
    allowed = {item.name for item in fields(CensusRow)}
    cleaned = {key: value for key, value in row.items() if key in allowed}
    status = cleaned.get("status")
    if isinstance(status, str) and status not in CENSUS_CLASSES:
        cleaned["status"] = "FAIL-VALUE"
        tags = list(cleaned.get("tags") or [])
        if "clamped-unknown-status" not in tags:
            tags.append("clamped-unknown-status")
        cleaned["tags"] = tags
    return CensusRow(**cleaned)


def _md_cell(text: str) -> str:
    """Collapse whitespace and escape pipes for a single markdown table cell."""
    cleaned = " ".join(str(text).split())
    return cleaned.replace("|", "\\|")


def resolve_census_modules(
    *,
    c3_expand: bool,
    stretch: bool,
    modules_csv: str,
    c4_expand: bool = False,
    classic: bool = False,
) -> list[str]:
    """Resolve the module short-name list for a census run (dual-denom isolation).

    When ``c4_expand`` is true, returns **only** :data:`C4_EXPAND_MODULES` (charter order)
    and ignores ``modules_csv`` / ``stretch`` / ``c3_expand`` so classic /345 and the C3
    expand cohort are never blended into the C4 expand2 denominator. When ``c3_expand`` is
    true (and not c4), returns **only** :data:`C3_EXPAND_MODULES`. When ``classic`` is true
    (and neither expand flag is), returns **only** :data:`CLASSIC_MODULES` — the /345 cohort
    with no C3 blending. Otherwise starts from ``modules_csv`` and optionally appends
    :data:`STRETCH_MODULES`.

    Precedence (fixed): ``c4_expand`` > ``c3_expand`` > ``classic`` > ``modules_csv``
    (+ ``stretch``). ``classic`` is ADDITIVE in the V2 port (phase-3 EC-8); the
    ``stretch`` branch below is unchanged and still appends, which is exactly the blending
    trap ``--classic`` exists to avoid.
    """
    if c4_expand:
        return [validate_module_short(name) for name in C4_EXPAND_MODULES]
    if c3_expand:
        return [validate_module_short(name) for name in C3_EXPAND_MODULES]
    if classic:
        return [validate_module_short(name) for name in CLASSIC_MODULES]
    module_names = [
        validate_module_short(name.strip()) for name in modules_csv.split(",") if name.strip()
    ]
    if stretch:
        for name in STRETCH_MODULES:
            if name not in module_names:
                # Validate constants too (defense-in-depth — octo C3 C5).
                module_names.append(validate_module_short(name))
    return module_names


def default_markdown_report_path(
    worktree: Path,
    *,
    c4_expand: bool = False,
    c3_expand: bool = False,
    date_stamp: str | None = None,
) -> Path:
    """Default markdown report path — C-2 policy: gitignored ``target/census-reports/``.

    Aligns with ``scripts/run_census.sh``'s ``CENSUS_REPORT_DIR`` default. Reports land under
    the gitignored tree so an uncommitted local run cannot look like committed evidence under
    ``task/census/<run>/``. Promote to evidence with an explicit copy into ``task/``; pass
    ``--markdown`` to opt into any other path (including ``task/``).
    """
    stamp = date_stamp if date_stamp is not None else datetime.now(UTC).strftime("%Y-%m-%d")
    report_dir = worktree / "target" / "census-reports"
    if c4_expand:
        name = f"pyspark-compat-report-c4-expand2-{stamp}.md"
    elif c3_expand:
        name = f"pyspark-compat-report-c3-expand-{stamp}.md"
    else:
        name = f"pyspark-compat-report-{stamp}.md"
    return report_dir / name


def render_markdown_report(report: CompatReport, *, series: str = _SERIES_C2) -> str:
    """Human-readable census report (default body under ``target/census-reports/``).

    ``series`` labels the scoreboard unit (C2 classic vs C3 expand) so dual denominators
    are never misread as the same /345 series (octo C3 C1-L-001).
    """
    rows = report.all_rows()
    denoms = denominators(rows)
    ranked = rank_census(rows)
    lines: list[str] = []
    lines.append(f"# PySpark-suite compatibility report ({series})")
    lines.append("")
    lines.append(f"- **Generated:** {report.generated_at}")
    lines.append(f"- **pyspark version:** `{report.pyspark_version}`")
    lines.append(
        f"- **Spark test source tag:** `{report.spark_tag}` (commit `{report.spark_commit_sha}`)"
    )
    lines.append(f"- **repark version:** `{report.repark_version}`")
    lines.append(f"- **Python:** `{report.python_version}`")
    if series == _SERIES_C3:
        lines.append(
            "- **Cohort:** C3 expand only "
            f"(`{'`, `'.join(C3_EXPAND_MODULES)}`) — **own denominators; never blend classic /345**"
        )
    elif series == _SERIES_C4:
        lines.append(
            "- **Cohort:** C4 expand2 only "
            f"(`{'`, `'.join(C4_EXPAND_MODULES)}`) — **own denominators; never blend classic "
            "/345 or prior C3 expand**"
        )
    lines.append("")
    lines.append("## Denominators (charter — both required)")
    lines.append("")
    lines.append(
        f"- **pass / all_collected** = "
        f"**{denoms['pass']} / {denoms['all_collected']}** "
        f"({denoms['pass_over_all'] * 100:.2f}%)"
    )
    lines.append(
        f"- **pass / engine-relevant** = "
        f"**{denoms['pass']} / {denoms['engine_relevant']}** "
        f"({denoms['pass_over_engine_relevant'] * 100:.2f}%) "
        f"where engine-relevant = all - SKIP-UPSTREAM - NEEDS-JVM - HARNESS"
    )
    lines.append(
        f"  - excluded SKIP-UPSTREAM={denoms['excluded_skip_upstream']}, "
        f"NEEDS-JVM={denoms['excluded_needs_jvm']}, "
        f"HARNESS={denoms['excluded_harness']}"
    )
    lines.append(
        "  - note: `MODULE-TIMEOUT` stays in engine-relevant (charter formula; wall is "
        "ops/harness but not listed among the three exclusions)"
    )
    lines.append("")
    lines.append("## Ranked census (class, count)")
    lines.append("")
    lines.append("| Class | Count |")
    lines.append("|---|---:|")
    for name, count in ranked:
        lines.append(f"| `{name}` | {count} |")
    lines.append("")
    lines.append("## Per-module totals")
    lines.append("")
    lines.append("| Module | Tests | PASS | Wall (s) | Timed out |")
    lines.append("|---|---:|---:|---:|:---:|")
    for module in report.modules:
        n_pass = sum(1 for row in module.rows if row.status == "PASS")
        lines.append(
            f"| `{module.module}` | {len(module.rows)} | {n_pass} | "
            f"{module.wall_s:.1f} | {'yes' if module.timed_out else 'no'} |"
        )
    lines.append("")
    lines.append("## Patch map (redirect seam)")
    lines.append("")
    lines.append(patch_map_as_markdown())
    lines.append("")
    lines.append("### Patch log (runtime)")
    lines.append("")
    for entry in report.patch_log:
        lines.append(f"- {entry}")
    if not report.patch_log:
        lines.append("- _(empty — worker logs not merged)_")
    lines.append("")
    lines.append("## Non-PASS rows")
    lines.append("")
    lines.append("Every non-PASS: test id, class, one-line cause, first divergent frame.")
    lines.append("")
    lines.append("| Test id | Class | Cause | Frame |")
    lines.append("|---|---|---|---|")
    for row in rows:
        if row.status == "PASS":
            continue
        cause = _md_cell(row.cause)
        frame = _md_cell(row.divergent_frame)
        extra = ""
        if row.status == "HARNESS" and row.harness_justification:
            extra = f" _(justification: {_md_cell(row.harness_justification)})_"
        lines.append(
            f"| `{_md_cell(row.test_id)}` | `{_md_cell(row.status)}` | {cause}{extra} | `{frame}` |"
        )
    lines.append("")
    if series == _SERIES_C4:
        findings_heading = (
            "## Findings (engine-relevant; C4 expand2 cohort — dual denoms own series)"
        )
    elif series == _SERIES_C3:
        findings_heading = (
            "## Findings (engine-relevant; C3 expand cohort — dual denoms own 78-row series)"
        )
    else:
        findings_heading = "## Findings (engine-relevant; zero mid-unit fixes)"
    lines.append(findings_heading)
    lines.append("")
    if report.findings:
        for finding in report.findings:
            lines.append(f"- {finding}")
    else:
        if series == _SERIES_C4:
            fallback = (
                "- Auto-derived: see FAIL-* ranks above. C4 facade upside is optional; "
                "engine walls are seeds (not blended with classic /345 or C3 expand)."
            )
        elif series == _SERIES_C3:
            fallback = (
                "- Auto-derived: see FAIL-* ranks above. C3 facade upside is optional; "
                "engine walls are seeds (not blended with classic /345)."
            )
        else:
            fallback = (
                "- Auto-derived: see FAIL-* ranks above. Product fixes are out of scope for C2."
            )
        lines.append(fallback)
    lines.append("")
    lines.append("## Provenance")
    lines.append("")
    lines.append(
        f"Apache tests loaded from cache tag `{report.spark_tag}` @ "
        f"`{report.spark_commit_sha}` (runtime fetch; **not** vendored in git). "
        f"Installed pyspark `{report.pyspark_version}` provides the runtime package; "
        f"only `pyspark.sql.tests` is injected from the cache."
    )
    lines.append("")
    return "\n".join(lines)


def _default_worktree_root() -> Path:
    # python/repark-parity/compat/runner.py → repo root is parents[3]
    return Path(__file__).resolve().parents[3]


def _repark_version() -> str:
    try:
        import repark

        return getattr(repark, "__version__", "0.0.0")
    except Exception:
        return "unknown"


def build_report(
    modules: list[ModuleCensus],
    *,
    provenance: SparkTestsProvenance,
    patch_log: list[str] | None = None,
    series: str = _SERIES_C2,
) -> CompatReport:
    """Assemble the top-level report object + auto findings.

    ``series`` selects the finding-line unit label so C3 expand reports never claim
    ``C2 zero-fix`` (dual-denom series honesty — octo C3 C1-L-001).
    """
    report = CompatReport(
        generated_at=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        pyspark_version=provenance.pyspark_version,
        spark_tag=provenance.tag,
        spark_commit_sha=provenance.commit_sha,
        repark_version=_repark_version(),
        python_version=sys.version.split()[0],
        modules=modules,
        patch_log=list(patch_log or []),
    )
    ranked = rank_census(report.all_rows())
    if series == _SERIES_C4:
        measure_tag = "measurement only (C4 expand2 cohort; own denoms, never /345 or C3)"
    elif series == _SERIES_C3:
        measure_tag = "measurement only (C3 expand cohort; own denoms, never /345)"
    else:
        measure_tag = "measurement only (C2 zero-fix)"
    for name, count in ranked:
        if name.startswith("FAIL") or name in {"NEEDS-JVM", "MODULE-TIMEOUT"}:
            report.findings.append(f"{name}: {count} test(s) — {measure_tag}")
    return report


def main(argv: list[str] | None = None) -> int:
    """CLI entry."""
    parser = argparse.ArgumentParser(
        description=(
            "PySpark-compat census runner (C2 night-1/stretch, C3 expand via --c3-expand, "
            "or C4 expand2 via --c4-expand)"
        )
    )
    parser.add_argument(
        "--modules",
        default=",".join(NIGHT1_MODULES),
        help="Comma-separated module short names (default: night-1 trio)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_MODULE_TIMEOUT_S,
        help="Per-module wall seconds (default 1200)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Write JSON report to this path",
    )
    parser.add_argument(
        "--markdown",
        type=Path,
        default=None,
        help="Write markdown report to this path",
    )
    parser.add_argument(
        "--filter",
        default=None,
        help="Optional substring filter on test ids (debug)",
    )
    parser.add_argument(
        "--inprocess",
        action="store_true",
        help="Run modules in-process (default for --worker; parent uses subprocess)",
    )
    parser.add_argument(
        "--worker",
        action="store_true",
        help="Internal: worker mode (single module, in-process, writes --output)",
    )
    parser.add_argument(
        "--stretch",
        action="store_true",
        help=(
            "Append stretch modules after night-1: classic test_column/test_readwriter "
            "then C3 expand (test_group, test_session, test_conf, test_catalog, test_sql). "
            "For C3 dual-denom measurement run --modules with C3_EXPAND_MODULES only "
            "(do not blend with classic /345)."
        ),
    )
    parser.add_argument(
        "--classic",
        action="store_true",
        help=(
            "Run only the classic C2 cohort (test_functions, test_dataframe, test_types, "
            "test_column, test_readwriter) with the /345 denominator — never blended with "
            "C3 expand or C4 expand2. Use this, never --stretch, for the classic cohort."
        ),
    )
    parser.add_argument(
        "--c3-expand",
        action="store_true",
        help=(
            "Run only the C3 expansion cohort (test_group, test_session, test_conf, "
            "test_catalog, test_sql, test_udf) with their own denominators — never blended "
            "with classic night-1 /345"
        ),
    )
    parser.add_argument(
        "--c4-expand",
        action="store_true",
        help=(
            "Run only the C4 expand2 cohort (test_subquery, test_collection, "
            "test_repartition, test_utils, test_errors, test_stat, test_creation, "
            "test_conversion, test_serde) with their own denominators — never blended with "
            "classic /345 or C3 expand"
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="DEBUG logging",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        module_names = resolve_census_modules(
            c3_expand=args.c3_expand,
            stretch=args.stretch,
            modules_csv=args.modules,
            c4_expand=args.c4_expand,
            classic=args.classic,
        )
    except ValueError as error:
        LOGGER.error("%s", error)
        return EXIT_USAGE

    if not module_names:
        LOGGER.error("no modules specified")
        return EXIT_USAGE

    series, series_short = _series_from_args_and_env(
        c4_expand=args.c4_expand,
        c3_expand=args.c3_expand,
        worker=bool(args.worker),
    )

    provenance = ensure_spark_tests()
    LOGGER.info(
        "provenance tag=%s sha=%s pyspark=%s cache=%s series=%s",
        provenance.tag,
        provenance.commit_sha,
        provenance.pyspark_version,
        provenance.cache_dir,
        series_short,
    )

    worktree = _default_worktree_root()
    if args.worker or args.inprocess:
        install_redirect(spark_tests_root=provenance.cache_dir)
        assert_no_jvm()
        modules: list[ModuleCensus] = []
        for name in module_names:
            # Re-arm per module so a prior timeout cannot leave the worker unwatched.
            if args.worker:
                _install_worker_alarm(args.timeout)
            LOGGER.info("in-process module %s", name)
            try:
                modules.append(
                    run_module_inprocess(
                        name,
                        provenance=provenance,
                        test_filter=args.filter,
                    )
                )
            except TimeoutError as error:
                modules.append(
                    ModuleCensus(
                        module=name,
                        import_name=import_name_for(name),
                        wall_s=float(args.timeout),
                        timed_out=True,
                        rows=[classify_module_timeout(module=name, budget_s=args.timeout)],
                        error=str(error),
                    )
                )
            finally:
                if args.worker and hasattr(signal, "alarm"):
                    signal.alarm(0)
        report = build_report(
            modules, provenance=provenance, patch_log=redirect_log(), series=series
        )
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(
                json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        if args.markdown:
            args.markdown.parent.mkdir(parents=True, exist_ok=True)
            args.markdown.write_text(
                render_markdown_report(report, series=series), encoding="utf-8"
            )
        if not args.output and not args.markdown:
            print(json.dumps(report.to_dict()["denominators"], indent=2))
        return EXIT_OK

    # Parent: one subprocess per module.
    # C3/C4 expand use distinct default scratch so worker JSON never clobbers classic C2
    # artifacts (dual-denom isolation — octo C3 C1-SAF-001; C4 Q11 own scratch).
    if args.c4_expand:
        default_scratch = _DEFAULT_SCRATCH_C4
    elif args.c3_expand:
        default_scratch = _DEFAULT_SCRATCH_C3
    else:
        default_scratch = _DEFAULT_SCRATCH_C2
    scratch = Path(os.environ.get("REPARK_COMPAT_SCRATCH", default_scratch))
    scratch.mkdir(parents=True, exist_ok=True)
    modules = []
    for name in module_names:
        out_json = scratch / f"worker-{name}.json"
        if out_json.exists():
            out_json.unlink()
        LOGGER.info("subprocess module %s timeout=%ss series=%s", name, args.timeout, series_short)
        modules.append(
            run_module_subprocess(
                name,
                timeout_s=args.timeout,
                python_executable=sys.executable,
                worktree_root=worktree,
                output_json=out_json,
                test_filter=args.filter,
                series_short=series_short,
            )
        )

    # Merge patch log from first successful worker if present.
    patch_log: list[str] = []
    for name in module_names:
        worker_json = scratch / f"worker-{name}.json"
        if worker_json.is_file():
            payload = json.loads(worker_json.read_text(encoding="utf-8"))
            patch_log = list(payload.get("patch_log") or [])
            if patch_log:
                break

    report = build_report(modules, provenance=provenance, patch_log=patch_log, series=series)

    if args.output is None:
        args.output = scratch / "compat-report.json"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    LOGGER.info("wrote JSON %s", args.output)

    if args.markdown is None:
        # C-2: default into gitignored target/census-reports/ (matches scripts/run_census.sh).
        args.markdown = default_markdown_report_path(
            worktree,
            c4_expand=bool(args.c4_expand),
            c3_expand=bool(args.c3_expand),
        )
    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    args.markdown.write_text(render_markdown_report(report, series=series), encoding="utf-8")
    LOGGER.info("wrote markdown %s", args.markdown)

    denoms = denominators(report.all_rows())
    own_note = ""
    if args.c4_expand:
        own_note = " (own denoms; never /345 or C3)"
    elif args.c3_expand:
        own_note = " (own denoms; never /345)"
    print(
        f"{series_short} census: pass/all={denoms['pass']}/{denoms['all_collected']} "
        f"pass/engine={denoms['pass']}/{denoms['engine_relevant']}" + own_note
    )
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
