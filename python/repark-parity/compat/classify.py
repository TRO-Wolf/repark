"""Per-test census classification for the PySpark-compat harness.

Schema (charter, fixed)::

    PASS | FAIL-VALUE | FAIL-ERROR-CLASS | FAIL-MISSING | NEEDS-JVM
    | HARNESS | SKIP-UPSTREAM | MODULE-TIMEOUT
"""

from __future__ import annotations

import re
import traceback
import unittest
from dataclasses import asdict, dataclass, field
from typing import Any

CENSUS_CLASSES: tuple[str, ...] = (
    "PASS",
    "FAIL-VALUE",
    "FAIL-ERROR-CLASS",
    "FAIL-MISSING",
    "NEEDS-JVM",
    "HARNESS",
    "SKIP-UPSTREAM",
    "MODULE-TIMEOUT",
)

# Message/type signals that the failure is a real JVM / SparkContext RDD surface need.
# Applied primarily to (type_name + message), NOT full traceback source lines — a traceback
# line like ``self.spark.read.json(self.sc.parallelize(...))`` must not become NEEDS-JVM
# when the actual exception is ``no attribute 'json'`` (FAIL-MISSING).
_JVM_MESSAGE_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"Java gateway",
        r"\bpy4j\b",
        r"launch_gateway",
        r"\bJvmView\b",
        r"\bJVM_",
        r"no attribute ['\"]_jvm['\"]",
        r"no attribute ['\"]_jsparkSession['\"]",
        r"no attribute ['\"]_j[A-Za-z]",
        r"no attribute ['\"]parallelize['\"]",
        r"no attribute ['\"]textFile['\"]",
        r"only setLogLevel / applicationId / master",
        r"full SparkContext is out of scope",
        r"repark SparkContext has no attribute",
        r"\[?SESSION_OR_CONTEXT_NOT_EXISTS\]?",
        r"java\.lang\.",
        r"org\.apache\.spark\.",
        # pyspark builtin path: assert SparkContext._active_spark_context is not None
        r"SparkContext\._active_spark_context",
        r"SPARK_HOME",
    )
)

# Strong traceback-only JVM frames (final assert / py4j), checked when message is empty.
_JVM_TRACE_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"assert SparkContext\._active_spark_context is not None",
        r"py4j[/\\]",
        r"java_gateway",
        r"launch_gateway",
    )
)

# Surface not implemented on repark (loud gaps).
_MISSING_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"has no attribute",
        r"AttributeError",
        r"NotImplementedError",
        r"not (yet )?implemented",
        r"unsupported",
        r"UnsupportedOperation",
        r"is not supported",
        r"no attribute ['\"]range['\"]",
        r"no attribute ['\"]conf['\"]",
        r"module '.*' has no attribute",
        r"cannot import name",
        r"ImportError",
        r"ModuleNotFoundError",
    )
)

# Error-class / check_error style failures.
_ERROR_CLASS_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"Expected error class",
        r"check_error",
        r"checkError",
        r"error class",
        r"errorClass",
        r"getCondition",
        r"PySparkTypeError",
        r"PySparkValueError",
        r"AssertionError: .*Exception",
        r"not raised",
        r"raised .* unexpectedly",
    )
)

# Harness / redirect-seam artifacts (justify each in the report).
# Do NOT match bare setUpClass/tearDownClass — Apache suites legitimately fail there.
_HARNESS_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"install_redirect",
        r"compat\.bootstrap",
        r"compat/bootstrap\.py",
        r"compat\.runner",
        r"compat/runner\.py",
        r"REPARK_PYSPARK_COMPAT",
        r"redirect seam",
        r"repark-parity/compat",
    )
)


@dataclass
class CensusRow:
    """One Apache test result in the census."""

    test_id: str
    module: str
    status: str
    cause: str = ""
    divergent_frame: str = ""
    duration_s: float | None = None
    # Free-form justification required for HARNESS rows.
    harness_justification: str = ""
    error_type: str = ""
    raw_traceback: str = ""
    tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def classify_success(*, test_id: str, module: str, duration_s: float | None = None) -> CensusRow:
    """Build a PASS row."""
    return CensusRow(
        test_id=test_id,
        module=module,
        status="PASS",
        duration_s=duration_s,
    )


def classify_skip(
    *,
    test_id: str,
    module: str,
    reason: str,
    duration_s: float | None = None,
) -> CensusRow:
    """Upstream unittest skip → SKIP-UPSTREAM."""
    return CensusRow(
        test_id=test_id,
        module=module,
        status="SKIP-UPSTREAM",
        cause=_one_line(reason) or "unittest skip",
        duration_s=duration_s,
    )


def classify_failure(
    *,
    test_id: str,
    module: str,
    exc_type: type[BaseException] | str | None,
    exc: BaseException | str | None,
    tb_text: str,
    duration_s: float | None = None,
) -> CensusRow:
    """Map an exception + traceback to a census class."""
    type_name = (
        exc_type
        if isinstance(exc_type, str)
        else (exc_type.__name__ if exc_type is not None else "")
    )
    message = _one_line(str(exc) if exc is not None else "")
    blob = "\n".join([type_name, message, tb_text])
    frame = first_divergent_frame(tb_text)
    status, justification = _decide_status(type_name=type_name, message=message, blob=blob)
    return CensusRow(
        test_id=test_id,
        module=module,
        status=status,
        cause=message or type_name or "unknown failure",
        divergent_frame=frame,
        duration_s=duration_s,
        harness_justification=justification,
        error_type=type_name,
        raw_traceback=tb_text[-4000:],  # cap
    )


def classify_module_timeout(*, module: str, budget_s: float) -> CensusRow:
    """Whole-module wall exceeded."""
    return CensusRow(
        test_id=f"{module}::<module>",
        module=module,
        status="MODULE-TIMEOUT",
        cause=f"module wall exceeded ~{budget_s:.0f}s (SIGALRM / subprocess kill)",
        divergent_frame="",
    )


def first_divergent_frame(tb_text: str) -> str:
    """Return a one-line summary of the first useful traceback frame."""
    if not tb_text:
        return ""
    # Prefer the last "File ..." frame that is not unittest internals.
    frames = re.findall(r'File "([^"]+)", line (\d+), in (\S+)', tb_text)
    if not frames:
        # Fallback: first non-empty line after Traceback
        for line in tb_text.splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("Traceback"):
                return stripped[:240]
        return ""
    for path, line_no, func in frames:
        if "unittest" in path.replace("\\", "/"):
            continue
        if path.endswith("classify.py") or path.endswith("runner.py"):
            continue
        return f"{path}:{line_no} in {func}"
    path, line_no, func = frames[-1]
    return f"{path}:{line_no} in {func}"


def _decide_status(*, type_name: str, message: str, blob: str) -> tuple[str, str]:
    """Return (status, harness_justification).

    JVM detection is **message-first** so traceback source lines that merely *mention*
    ``parallelize`` / ``SparkContext`` do not steal FAIL-MISSING rows into NEEDS-JVM
    (denominator honesty).
    """
    if type_name in {"SkipTest", "unittest.case.SkipTest"} or "SkipTest" in blob[:200]:
        return "SKIP-UPSTREAM", ""

    message_blob = "\n".join([type_name, message])
    is_jvm = _is_needs_jvm(
        type_name=type_name, message=message, message_blob=message_blob, blob=blob
    )

    if _matches(_HARNESS_PATTERNS, blob) and not is_jvm:
        return (
            "HARNESS",
            "traceback references compat bootstrap / redirect seam rather than engine SQL",
        )

    if is_jvm:
        return "NEEDS-JVM", ""

    # Third-party / env ImportErrors are harness/env, not engine surface gaps.
    # Full blob: traceback may be the only place site-packages appears.
    if type_name in {"ImportError", "ModuleNotFoundError"} and _is_third_party_import(blob):
        return (
            "HARNESS",
            "third-party or environment import failure (not repark engine surface)",
        )

    # Module wall TimeoutError (when not routed through RecordingResult flag).
    if type_name == "TimeoutError":
        return "MODULE-TIMEOUT", ""

    if type_name in {"AttributeError", "NotImplementedError", "ImportError", "ModuleNotFoundError"}:
        return "FAIL-MISSING", ""

    if _matches(_MISSING_PATTERNS, blob) and type_name not in {"AssertionError"}:
        return "FAIL-MISSING", ""

    if type_name in {"AssertionError"}:
        if _matches(_ERROR_CLASS_PATTERNS, blob):
            return "FAIL-ERROR-CLASS", ""
        return "FAIL-VALUE", ""

    if _matches(_ERROR_CLASS_PATTERNS, blob):
        return "FAIL-ERROR-CLASS", ""

    # TypeError/ValueError often mean API mismatch (missing surface).
    if type_name in {"TypeError", "ValueError"} and _matches(_MISSING_PATTERNS, blob):
        return "FAIL-MISSING", ""

    if type_name in {"TypeError", "ValueError"}:
        return "FAIL-ERROR-CLASS", ""

    # Default engine-relevant failure bucket for unexpected exceptions.
    if _matches(_MISSING_PATTERNS, blob):
        return "FAIL-MISSING", ""

    return "FAIL-VALUE", ""


def _is_needs_jvm(*, type_name: str, message: str, message_blob: str, blob: str) -> bool:
    """True when the failure is a real JVM / full SparkContext requirement."""
    # Py4JJavaError / Py4JError: type name embeds "py4j" without a word boundary.
    type_lower = type_name.lower()
    if type_lower.startswith("py4j") or "py4j" in type_lower:
        return True
    if _matches(_JVM_MESSAGE_PATTERNS, message_blob):
        return True
    # Empty AssertionError from pyspark's ``assert SparkContext._active_spark_context`` path.
    empty_assert = type_name == "AssertionError" and (not message or message == "AssertionError")
    if empty_assert and _matches(_JVM_TRACE_PATTERNS, blob):
        return True
    # Non-assertion failures: only strong trace patterns (py4j frames), never bare source tokens.
    non_attr = type_name not in {
        "AssertionError",
        "AttributeError",
        "NotImplementedError",
    }
    return bool(non_attr and _matches(_JVM_TRACE_PATTERNS, blob))


def _is_third_party_import(text: str) -> bool:
    """True when an ImportError points at env/third-party packages, not repark/pyspark gaps.

    X1 note: Apache test cache lives under ``~/.cache/repark-pyspark-tests/`` — a bare
    ``"repark" in text`` match would false-negative every suite traceback that only
    *runs* a cached test while failing on pandas/numpy (e.g. ``assertDataFrameEqual``
    → ``pyspark.pandas`` → missing ``pandas.core.common._builtin_table``).

    Octo C1: product detection must key off the **imported module / error message**, not
    stack-frame paths. Frames under ``site-packages/repark`` or ``python/repark/`` are
    normal after redirect (or when repark is installed in the venv); using them as a
    short-circuit stole pandas ImportErrors into FAIL-MISSING and polluted the
    engine-relevant denominator.
    """
    lowered = text.lower()
    # Product repark import gaps stay FAIL-MISSING (not env HARNESS).
    # Match the missing/imported module name — including quoted forms used by CPython:
    #   No module named 'repark.foo'
    #   cannot import name 'X' from 'repark.functions'
    if re.search(
        r"no module named ['\"]?repark"
        r"|cannot import name .+ from ['\"]repark"
        r"|\bfrom repark[\.\s'\"]"
        r"|import repark\b"
        r"|module ['\"]repark",
        lowered,
    ):
        return False
    third_party_markers = (
        "pandas",
        "numpy",
        "pyarrow",
        "site-packages",
    )
    return any(marker in lowered for marker in third_party_markers)


def _matches(patterns: tuple[re.Pattern[str], ...], text: str) -> bool:
    return any(pattern.search(text) for pattern in patterns)


def _one_line(text: str) -> str:
    cleaned = " ".join(text.split())
    if len(cleaned) > 240:
        return cleaned[:237] + "..."
    return cleaned


def rank_census(rows: list[CensusRow]) -> list[tuple[str, int]]:
    """Ranked (class, count) pairs — highest count first, stable by name.

    Unknown status strings are counted as ``FAIL-VALUE`` so they stay engine-relevant
    and cannot invent a new denominator bucket.
    """
    counts: dict[str, int] = dict.fromkeys(CENSUS_CLASSES, 0)
    for row in rows:
        status = _normalize_status(row.status)
        counts[status] = counts.get(status, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [(name, count) for name, count in ranked if count > 0]


def _normalize_status(status: str) -> str:
    """Map unknown census labels to FAIL-VALUE (engine-relevant catch-all)."""
    return status if status in CENSUS_CLASSES else "FAIL-VALUE"


def denominators(rows: list[CensusRow]) -> dict[str, Any]:
    """Both charter denominators.

    * ``pass/all_collected``
    * ``pass/(all - SKIP-UPSTREAM - NEEDS-JVM - HARNESS)``  (engine-relevant)

    ``MODULE-TIMEOUT`` is **not** excluded (charter lists only the three classes above).
    Unknown status strings are treated as ``FAIL-VALUE`` (same as :func:`rank_census`).
    """
    normalized = [_normalize_status(row.status) for row in rows]
    total = len(normalized)
    n_pass = sum(1 for status in normalized if status == "PASS")
    exclude = {"SKIP-UPSTREAM", "NEEDS-JVM", "HARNESS"}
    engine_relevant = [status for status in normalized if status not in exclude]
    n_engine = len(engine_relevant)
    n_pass_engine = sum(1 for status in engine_relevant if status == "PASS")
    return {
        "pass": n_pass,
        "all_collected": total,
        "pass_over_all": (n_pass / total) if total else 0.0,
        "engine_relevant": n_engine,
        "pass_over_engine_relevant": (n_pass_engine / n_engine) if n_engine else 0.0,
        "excluded_skip_upstream": sum(1 for status in normalized if status == "SKIP-UPSTREAM"),
        "excluded_needs_jvm": sum(1 for status in normalized if status == "NEEDS-JVM"),
        "excluded_harness": sum(1 for status in normalized if status == "HARNESS"),
        "module_timeout_in_engine_relevant": sum(
            1 for status in normalized if status == "MODULE-TIMEOUT"
        ),
    }


def format_unittest_id(test: unittest.case.TestCase) -> str:
    """Stable test id: ``module.Class.method``."""
    return test.id()


def traceback_from_err(err: tuple[type[BaseException], BaseException, Any] | None) -> str:
    """Format a unittest err triple to text."""
    if err is None:
        return ""
    return "".join(traceback.format_exception(err[0], err[1], err[2]))
