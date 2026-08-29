"""Redirect seam: install repark into the pyspark namespace before suite imports.

Replace **session / bootstrap layer only** (base-class session factories → repark);
never rewrite Apache test bodies. The patch map (:data:`PATCH_MAP`) is a charter
deliverable — it measures how drop-in "change one import line" really is when the
suite still imports from ``pyspark.*``.
"""

from __future__ import annotations

import contextlib
import logging
import os
import shutil
import sys
import tempfile
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

LOGGER = logging.getLogger(__name__)

_REDIRECT_INSTALLED = False
_SQL_TESTS_INJECTED = False
_PATCH_LOG: list[str] = []


class PatchEntry(BaseModel):
    """One documented redirect from a pyspark symbol to a repark object."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    target: str
    source: str
    kind: str  # "replace" | "overlay" | "factory" | "inject-package"
    notes: str


# Exact patch map (charter deliverable). Updated only when the bootstrap changes.
PATCH_MAP: tuple[PatchEntry, ...] = (
    PatchEntry(
        target="pyspark.sql.SparkSession",
        source="repark.spark.session.SparkSession (= ReparkSession)",
        kind="replace",
        notes="Builder + session type used by suite factories after ReusedSQLTestCase patch.",
    ),
    PatchEntry(
        target="pyspark.sql.session.SparkSession",
        source="repark.spark.session.SparkSession",
        kind="replace",
        notes="Same class object as pyspark.sql.SparkSession.",
    ),
    PatchEntry(
        target="pyspark.sql.classic.session.SparkSession",
        source="repark.spark.session.SparkSession",
        kind="replace",
        notes="Spark 4 classic submodule (when importable).",
    ),
    PatchEntry(
        target="pyspark.sql.DataFrame",
        source="repark.spark.dataframe.DataFrame",
        kind="replace",
        notes="So `from pyspark.sql import DataFrame` binds repark's type.",
    ),
    PatchEntry(
        target="pyspark.sql.classic.dataframe.DataFrame",
        source="repark.spark.dataframe.DataFrame",
        kind="replace",
        notes="Spark 4 classic dataframe submodule (when importable).",
    ),
    PatchEntry(
        target="pyspark.sql.Row",
        source="repark.spark.row.Row",
        kind="replace",
        notes="Row identity for createDataFrame / collect comparisons.",
    ),
    PatchEntry(
        target="pyspark.sql.column.Column",
        source="repark.spark.column.Column",
        kind="replace",
        notes="Column type shared with repark.spark.functions; submodule + package attribute.",
    ),
    PatchEntry(
        target="pyspark.sql.dataframe.DataFrame",
        source="repark.spark.dataframe.DataFrame",
        kind="replace",
        notes="Submodule path for `from pyspark.sql.dataframe import DataFrame`.",
    ),
    PatchEntry(
        target="pyspark.sql.classic.column.Column",
        source="repark.spark.column.Column",
        kind="replace",
        notes="Spark 4 classic column submodule (when importable).",
    ),
    PatchEntry(
        target="pyspark.sql.types.* (overlay)",
        source="repark.spark.types public names",
        kind="overlay",
        notes="Only names repark implements; missing Spark types stay pyspark (often NEEDS-JVM).",
    ),
    PatchEntry(
        target="pyspark.sql.functions.* (overlay)",
        source="repark.spark.functions public names",
        kind="overlay",
        notes="Overlay onto classic functions module + package __init__; submodules "
        "(avro, builtin extras) stay pyspark and usually FAIL-MISSING / NEEDS-JVM.",
    ),
    PatchEntry(
        target="pyspark.errors.* (overlay)",
        source="repark.errors public exception classes",
        kind="overlay",
        notes=(
            "AnalysisException / PySparkTypeError / PySparkAssertionError / … identity "
            "for except clauses and check_error isinstance; testing.utils names rebound "
            "when already imported (C4 expand2 assert* / assertSchemaEqual)."
        ),
    ),
    PatchEntry(
        target="pyspark.testing.sqlutils.ReusedSQLTestCase.setUpClass",
        source="compat bootstrap factory → ReparkSession.builder.getOrCreate()",
        kind="factory",
        notes="Does NOT start SparkContext / JVM. cls.sc is repark's minimal SparkContext.",
    ),
    PatchEntry(
        target="pyspark.testing.sqlutils.ReusedSQLTestCase.tearDownClass",
        source="compat bootstrap (session.stop + tempdir cleanup)",
        kind="factory",
        notes="Skips JVM sc.stop().",
    ),
    PatchEntry(
        target="pyspark.testing.sqlutils.ReusedSQLTestCase.tearDown",
        source="compat bootstrap no-op (skip _jsparkSession cleanup)",
        kind="factory",
        notes="Original tearDown calls JVM-only cleanupPythonWorkerLogs.",
    ),
    PatchEntry(
        target="pyspark.testing.utils.ReusedPySparkTestCase.setUpClass",
        source="compat bootstrap (no SparkContext)",
        kind="factory",
        notes="Prevents parent setUpClass from launching a gateway if a subclass chains super.",
    ),
    PatchEntry(
        target="pyspark.sql.tests (package)",
        source="~/.cache/repark-pyspark-tests/<tag>/python/pyspark/sql/tests",
        kind="inject-package",
        notes="Installed pyspark wheel does not ship sql/tests; injected from cache only.",
    ),
)


def patch_map_as_markdown() -> str:
    """Render the patch map as a markdown table for the census report."""
    lines = [
        "| Target | Source | Kind | Notes |",
        "|---|---|---|---|",
    ]
    for entry in PATCH_MAP:
        notes = entry.notes.replace("|", "\\|")
        lines.append(f"| `{entry.target}` | `{entry.source}` | {entry.kind} | {notes} |")
    return "\n".join(lines)


def install_redirect(*, spark_tests_root: Path | None = None) -> list[str]:
    """Install the repark→pyspark redirect seam. Idempotent.

    Args:
        spark_tests_root: path to the cached Spark tree (the directory containing
            ``python/``), required so ``pyspark.sql.tests`` can be injected. If the
            first call omits it, a later call with a root still injects the package.

    Returns:
        Human-readable log of patches applied (also stored for the report).
    """
    global _REDIRECT_INSTALLED, _SQL_TESTS_INJECTED
    if _REDIRECT_INSTALLED:
        # Allow a later call to inject sql.tests if the first call lacked a root.
        if spark_tests_root is not None and not _SQL_TESTS_INJECTED:
            inject_sql_tests_package(spark_tests_root)
            _SQL_TESTS_INJECTED = True
        return list(_PATCH_LOG)

    # No JVM markers repark must not rely on; find_spark_home resolving to the
    # wheel path is fine — no Java gateway may start (asserted below).
    os.environ.pop("PYSPARK_SUBMIT_ARGS", None)
    # Refuse accidental real cluster masters in the harness process.
    os.environ.setdefault("REPARK_PYSPARK_COMPAT", "1")

    _patch_session_and_types()
    _patch_functions_overlay()
    _patch_errors_overlay()
    _patch_test_case_factories()
    if spark_tests_root is not None:
        inject_sql_tests_package(spark_tests_root)
        _SQL_TESTS_INJECTED = True
    else:
        LOGGER.warning("install_redirect called without spark_tests_root; sql.tests not injected")

    _assert_no_jvm_started()
    _REDIRECT_INSTALLED = True
    LOGGER.info("repark→pyspark redirect installed (%d log lines)", len(_PATCH_LOG))
    return list(_PATCH_LOG)


def inject_sql_tests_package(spark_tests_root: Path) -> None:
    """Make ``import pyspark.sql.tests.*`` resolve into the cached Apache tree.

    Uses a package module with ``__path__`` pointing at the cache directory so test
    modules load from disk without shadowing the installed ``pyspark`` package.
    """
    tests_dir = Path(spark_tests_root) / "python" / "pyspark" / "sql" / "tests"
    if not tests_dir.is_dir():
        raise FileNotFoundError(f"Apache sql tests directory missing: {tests_dir}")

    import pyspark.sql as pyspark_sql

    existing = sys.modules.get("pyspark.sql.tests")
    if existing is not None and getattr(existing, "__path__", None):
        # Already a package — ensure cache path is first.
        path_list = list(existing.__path__)
        tests_str = str(tests_dir)
        if tests_str not in path_list:
            existing.__path__.insert(0, tests_str)  # type: ignore[attr-defined]
            _log(f"inject-package: prepended {tests_str} on pyspark.sql.tests.__path__")
        return

    package = types.ModuleType("pyspark.sql.tests")
    package.__path__ = [str(tests_dir)]  # type: ignore[attr-defined]
    package.__file__ = str(tests_dir / "__init__.py")
    package.__package__ = "pyspark.sql.tests"
    # Load the real __init__ if present so package attributes match Apache.
    init_file = tests_dir / "__init__.py"
    if init_file.is_file():
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "pyspark.sql.tests",
            init_file,
            submodule_search_locations=[str(tests_dir)],
        )
        if spec is not None and spec.loader is not None:
            package = importlib.util.module_from_spec(spec)
            sys.modules["pyspark.sql.tests"] = package
            spec.loader.exec_module(package)
            package.__path__ = [str(tests_dir)]  # type: ignore[attr-defined]
            pyspark_sql.tests = package  # type: ignore[attr-defined]
            _log(f"inject-package: loaded pyspark.sql.tests from {tests_dir}")
            return

    sys.modules["pyspark.sql.tests"] = package
    pyspark_sql.tests = package  # type: ignore[attr-defined]
    _log(f"inject-package: registered empty pyspark.sql.tests → {tests_dir}")


def is_redirect_installed() -> bool:
    """Return True after :func:`install_redirect` has completed successfully."""
    return _REDIRECT_INSTALLED


def redirect_log() -> list[str]:
    """Copy of the human-readable patch log."""
    return list(_PATCH_LOG)


def assert_no_jvm() -> None:
    """Public meta-pin: no Java gateway / SparkContext JVM may be live."""
    _assert_no_jvm_started()


# Internal patch helpers


def _log(message: str) -> None:
    _PATCH_LOG.append(message)
    LOGGER.debug("%s", message)


def _patch_session_and_types() -> None:
    import pyspark.sql as pyspark_sql
    import pyspark.sql.session as pyspark_session
    import pyspark.sql.types as pyspark_types

    import repark
    from repark.spark.column import Column
    from repark.spark.dataframe import DataFrame
    from repark.spark.row import Row
    from repark.spark.session import ReparkSession, SparkSession
    from repark.spark.window import Window, WindowSpec

    pyspark_sql.SparkSession = SparkSession  # type: ignore[misc, assignment]
    pyspark_session.SparkSession = SparkSession  # type: ignore[misc, assignment]
    _log("replace: pyspark.sql.SparkSession → repark.spark.session.SparkSession")

    pyspark_sql.DataFrame = DataFrame  # type: ignore[misc, assignment]
    pyspark_sql.Row = Row  # type: ignore[misc, assignment]
    pyspark_sql.Column = Column  # type: ignore[misc, assignment]
    if hasattr(pyspark_sql, "Window"):
        pyspark_sql.Window = Window  # type: ignore[misc, assignment]
    if hasattr(pyspark_sql, "WindowSpec"):
        pyspark_sql.WindowSpec = WindowSpec  # type: ignore[misc, assignment]
    _log("replace: pyspark.sql.{DataFrame,Row,Column,Window} → repark")

    # Submodules used by `from pyspark.sql.column import Column` (and peers).
    try:
        import pyspark.sql.column as pyspark_column

        pyspark_column.Column = Column  # type: ignore[misc, assignment]
        _log("replace: pyspark.sql.column.Column → repark")
    except ImportError:
        pass
    try:
        import pyspark.sql.dataframe as pyspark_dataframe

        pyspark_dataframe.DataFrame = DataFrame  # type: ignore[misc, assignment]
        _log("replace: pyspark.sql.dataframe.DataFrame → repark")
    except ImportError:
        pass

    # classic submodule (Spark 4) also exposes DataFrame / SparkSession.
    try:
        import pyspark.sql.classic.session as classic_session

        classic_session.SparkSession = SparkSession  # type: ignore[misc, assignment]
        _log("replace: pyspark.sql.classic.session.SparkSession → repark")
    except ImportError:
        pass
    try:
        import pyspark.sql.classic.dataframe as classic_dataframe

        classic_dataframe.DataFrame = DataFrame  # type: ignore[misc, assignment]
        _log("replace: pyspark.sql.classic.dataframe.DataFrame → repark")
    except ImportError:
        pass
    try:
        import pyspark.sql.classic.column as classic_column

        classic_column.Column = Column  # type: ignore[misc, assignment]
        _log("replace: pyspark.sql.classic.column.Column → repark")
    except ImportError:
        pass

    import repark.spark.types as repark_types

    overlaid = _overlay_public_names(pyspark_types, repark_types)
    _log(f"overlay: pyspark.sql.types ← repark.spark.types ({overlaid} names)")
    # Apache test_types imports private helpers by name (`from pyspark.sql.types
    # import _merge_type, _make_type_verifier`). Public-name overlay skips
    # underscore attrs; pin the repark implementations so check_error sees
    # repark's PySparkTypeError / parameter keys.
    for private_name in ("_merge_type", "_make_type_verifier"):
        if hasattr(repark_types, private_name):
            setattr(pyspark_types, private_name, getattr(repark_types, private_name))
            _log(f"replace: pyspark.sql.types.{private_name} → repark.spark.types.{private_name}")

    # Apache cache tests compare pyspark.storagelevel.StorageLevel to
    # DataFrame.storageLevel (repark); replace the class so identity matches.
    try:
        import pyspark.storagelevel as pyspark_storagelevel

        from repark.spark.storage import StorageLevel

        pyspark_storagelevel.StorageLevel = StorageLevel  # type: ignore[misc, assignment]
        _log("replace: pyspark.storagelevel.StorageLevel → repark.spark.storage.StorageLevel")
    except ImportError:
        pass

    # Keep a handle for debugging / meta-pins.
    pyspark_sql._repark_redirect = repark  # type: ignore[attr-defined]
    _ = ReparkSession  # silence lint: re-exported via SparkSession alias


def _patch_functions_overlay() -> None:
    import repark.spark.functions as repark_functions

    # Spark 4 ships functions as a package; overlay public callables onto it.
    try:
        import pyspark.sql.functions as pyspark_functions
    except ImportError:
        _log("overlay: pyspark.sql.functions missing — skipped")
        return

    overlaid = _overlay_public_names(pyspark_functions, repark_functions)
    _log(f"overlay: pyspark.sql.functions ← repark.spark.functions ({overlaid} names)")

    # classic.functions module (used by some internal imports)
    try:
        import pyspark.sql.classic.functions as classic_functions

        n = _overlay_public_names(classic_functions, repark_functions)
        _log(f"overlay: pyspark.sql.classic.functions ← repark.spark.functions ({n} names)")
    except ImportError:
        pass


def _patch_errors_overlay() -> None:
    import pyspark.errors as pyspark_errors

    import repark.errors as repark_errors

    overlaid = _overlay_public_names(pyspark_errors, repark_errors)
    _log(f"overlay: pyspark.errors ← repark.errors ({overlaid} names)")

    # exceptions.base is where some suites import PySparkException from.
    try:
        import pyspark.errors.exceptions.base as base_errors

        n = _overlay_public_names(base_errors, repark_errors)
        _log(f"overlay: pyspark.errors.exceptions.base ← repark.errors ({n} names)")
    except ImportError:
        pass

    # pyspark.testing.utils imports error classes from pyspark.errors at import
    # time; module-level overlay alone does not rebind them if testing.utils was
    # already imported. Rebind already-loaded testing helpers so check_error /
    # assertSchemaEqual raise and isinstance-check repark's tree (not a silent
    # FAIL-ERROR-CLASS regression).
    _rebind_errors_on_loaded_testing_modules(repark_errors)


def _rebind_errors_on_loaded_testing_modules(repark_errors: Any) -> None:
    """Rebind repark exception classes onto already-imported pyspark.testing* modules."""
    exported = getattr(repark_errors, "__all__", None)
    if not isinstance(exported, (list, tuple)) or not exported:
        return
    rebound = 0
    for module_name, module in list(sys.modules.items()):
        if module is None:
            continue
        if not (
            module_name == "pyspark.testing.utils" or module_name.startswith("pyspark.testing.")
        ):
            continue
        for name in exported:
            if not isinstance(name, str) or name.startswith("_"):
                continue
            if not hasattr(module, name) or not hasattr(repark_errors, name):
                continue
            try:
                setattr(module, name, getattr(repark_errors, name))
                rebound += 1
            except Exception:
                continue
    if rebound:
        _log(f"rebind: pyspark.testing* error names ← repark.errors ({rebound} attrs)")


def _overlay_public_names(target: Any, source: Any) -> int:
    """Copy public attributes from source onto target; return count copied.

    Prefer ``source.__all__`` when present so private helpers and import residue
    (``re``, ``json``, ``Any``, …) do not pollute the target module.
    """
    count = 0
    names: list[str]
    exported = getattr(source, "__all__", None)
    if isinstance(exported, (list, tuple)) and exported:
        names = [name for name in exported if isinstance(name, str) and not name.startswith("_")]
    else:
        names = [name for name in dir(source) if not name.startswith("_")]
    for name in names:
        try:
            value = getattr(source, name)
        except Exception:
            continue
        try:
            setattr(target, name, value)
            count += 1
        except Exception:
            continue
    return count


def _reused_pyspark_setup(cls: type) -> None:
    """No-op parent setup — do not construct a JVM SparkContext."""
    cls.sc = None  # type: ignore[attr-defined]
    _log(f"factory: ReusedPySparkTestCase.setUpClass skipped JVM for {cls.__name__}")


def _reused_pyspark_teardown(cls: type) -> None:
    sc = getattr(cls, "sc", None)
    if sc is not None and hasattr(sc, "stop"):
        with contextlib.suppress(Exception):
            sc.stop()


def _reused_sql_setup(cls: type) -> None:
    """Build a repark session; mirror Apache's cls.spark / cls.df fixtures."""
    from repark.spark.catalog import DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME
    from repark.spark.row import Row
    from repark.spark.session import ReparkSession, _reset_active_session_for_tests

    _reset_active_session_for_tests()
    # Parent would start SparkContext — skip it intentionally.
    _reused_pyspark_setup(cls)

    cls.spark = (  # type: ignore[attr-defined]
        ReparkSession.builder.master("local[4]").appName(cls.__name__).getOrCreate()
    )
    # Apache sqlutils bare names (`saveAsTable("t")`, `DROP TABLE t`) need a
    # registered default catalog + namespace, matching Spark's always-present
    # spark_catalog.default.
    warehouse = Path(tempfile.mkdtemp(prefix="repark-compat-warehouse-"))
    cls._repark_compat_warehouse = warehouse  # type: ignore[attr-defined]
    try:
        cls.spark.register_memory_catalog(DEFAULT_CATALOG_NAME, warehouse)  # type: ignore[attr-defined]
        # Keep current catalog/database at the Spark oracle defaults; create the
        # default namespace (Spark's default database always exists).
        state = cls.spark._catalog_state()  # type: ignore[attr-defined]
        state["current_catalog"] = DEFAULT_CATALOG_NAME
        state["current_database"] = DEFAULT_DATABASE_NAME
        with contextlib.suppress(Exception):
            cls.spark.create_namespace(  # type: ignore[attr-defined]
                DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME
            )
    except Exception as error:
        LOGGER.warning("compat: default spark_catalog memory registration failed: %s", error)
    # repark's minimal SparkContext (no JVM); tests touching _jvm / parallelize
    # classify as NEEDS-JVM at runtime.
    cls.sc = cls.spark.sparkContext  # type: ignore[attr-defined]
    cls._legacy_sc = cls.sc  # type: ignore[attr-defined]

    # Mirror Apache: NamedTemporaryFile(delete=False) + unlink leaves a path name
    # for tests that write under cls.tempdir.name.
    temp_handle = tempfile.NamedTemporaryFile(delete=False)  # noqa: SIM115
    temp_handle.close()
    Path(temp_handle.name).unlink(missing_ok=True)
    cls.tempdir = temp_handle  # type: ignore[attr-defined]
    cls.testData = [Row(key=index, value=str(index)) for index in range(100)]  # type: ignore[attr-defined]
    try:
        cls.df = cls.spark.createDataFrame(cls.testData)  # type: ignore[attr-defined]
    except Exception as error:
        LOGGER.warning("createDataFrame(testData) failed in setUpClass: %s", error)
        cls.df = None  # type: ignore[attr-defined]
    _log(f"factory: ReusedSQLTestCase.setUpClass → ReparkSession for {cls.__name__}")


def _reused_sql_teardown(cls: type) -> None:
    spark = getattr(cls, "spark", None)
    if spark is not None and hasattr(spark, "stop"):
        with contextlib.suppress(Exception):
            spark.stop()
    tempdir = getattr(cls, "tempdir", None)
    if tempdir is not None:
        shutil.rmtree(getattr(tempdir, "name", tempdir), ignore_errors=True)
    warehouse = getattr(cls, "_repark_compat_warehouse", None)
    if warehouse is not None:
        shutil.rmtree(warehouse, ignore_errors=True)
    from repark.spark.session import _reset_active_session_for_tests

    _reset_active_session_for_tests()


def _reused_sql_instance_teardown(self: Any) -> None:
    """Skip JVM worker-log cleanup; keep unittest tearDown chain minimal."""
    # Intentionally empty: Apache calls self.spark._jsparkSession.cleanupPythonWorkerLogs().


def _patch_test_case_factories() -> None:
    """Replace ReusedSQLTestCase / ReusedPySparkTestCase session factories."""
    import pyspark.testing.sqlutils as sqlutils
    import pyspark.testing.utils as testing_utils

    testing_utils.ReusedPySparkTestCase.setUpClass = classmethod(_reused_pyspark_setup)  # type: ignore[method-assign, assignment]
    testing_utils.ReusedPySparkTestCase.tearDownClass = classmethod(_reused_pyspark_teardown)  # type: ignore[method-assign, assignment]
    sqlutils.ReusedSQLTestCase.setUpClass = classmethod(_reused_sql_setup)  # type: ignore[method-assign, assignment]
    sqlutils.ReusedSQLTestCase.tearDownClass = classmethod(_reused_sql_teardown)  # type: ignore[method-assign, assignment]
    sqlutils.ReusedSQLTestCase.tearDown = _reused_sql_instance_teardown  # type: ignore[method-assign, assignment]
    _log("factory: patched ReusedSQLTestCase + ReusedPySparkTestCase session lifecycle")


def _assert_no_jvm_started() -> None:
    """Raise if a Py4J / Spark JVM gateway is already live in this process.

    Best-effort (no OS process-tree walk): SparkContext active singleton,
    class-level gateway handle, and py4j ``JavaGateway._gateway_client`` when the
    module is loaded.
    """
    context_mod = sys.modules.get("pyspark.context")
    if context_mod is not None:
        spark_context_cls = getattr(context_mod, "SparkContext", None)
        if spark_context_cls is not None:
            active = getattr(spark_context_cls, "_active_spark_context", None)
            if active is not None:
                raise RuntimeError(
                    "JVM SparkContext is active after redirect install — NEEDS-JVM leak"
                )
            gateway = getattr(spark_context_cls, "_gateway", None)
            if gateway is not None:
                raise RuntimeError(
                    "JVM SparkContext._gateway is set after redirect install — NEEDS-JVM leak"
                )

    java_gateway_mod = sys.modules.get("py4j.java_gateway")
    if java_gateway_mod is not None:
        # pyspark may stash a module-level gateway; any connected client is a leak.
        for attr_name in ("java_gateway", "gateway", "_gateway"):
            candidate = getattr(java_gateway_mod, attr_name, None)
            if candidate is None:
                continue
            client = getattr(candidate, "_gateway_client", None) or getattr(
                candidate, "gateway_client", None
            )
            if client is not None and getattr(client, "is_connected", lambda: False)():
                raise RuntimeError(
                    f"live py4j gateway ({attr_name}) detected after redirect — NEEDS-JVM leak"
                )


def build_repark_session(app_name: str = "repark-pyspark-compat") -> Any:
    """Helper for smoke/meta tests: a fresh repark session via the public builder."""
    from repark.spark.session import ReparkSession, _reset_active_session_for_tests

    _reset_active_session_for_tests()
    return ReparkSession.builder.appName(app_name).getOrCreate()


# Type alias for factory callables (documentation / tests).
SessionFactory = Callable[[type], None]
