"""repark.errors — the PySpark-shaped exception taxonomy raised by the engine.

Mirrors the names in ``pyspark.errors`` for near-drop-in migration: ``from repark.errors import
AnalysisException`` replaces ``from pyspark.errors import AnalysisException``, no other change.

The classes are defined in the native module (``repark._native``) and re-exported here, so an
exception raised by the Rust engine is the *same* class object caught here — ``except
AnalysisException`` catches an engine analysis error by class identity, never by message-matching.

Every class here subclasses :class:`RuntimeError` (via :class:`PySparkException`), so code that
already does ``except RuntimeError`` on engine failures keeps working after migrating from
PySpark — the near-drop-in contract. The mapping (engine error → exception type) lives in Rust
(``repark_core::Error::exception_class`` → ``repark-python`` ``to_py_err``):

- a SQL/expression **syntax** error → :class:`ParseException` (a subclass of
  :class:`AnalysisException` — PySpark parity, ``pyspark.errors`` defines
  ``ParseException(AnalysisException)`` — so ``except AnalysisException`` also catches it)
- a **planning/analysis** error (unresolved table/column, type error, and the iceberg
  not-found / already-exists catalog kinds — Spark's ``NoSuchTableException`` /
  ``TableAlreadyExistsException`` families are ``AnalysisException`` subclasses) →
  :class:`AnalysisException`
- a deterministically **unsupported operation** (the documented scope gates — an unrecognised
  ``write.merge.mode``, merge-on-read MERGE on a non-V2 table, non-Parquet write format, … — and
  unsupported iceberg features) →
  :class:`UnsupportedOperationException` (what PySpark raises for a JVM
  ``UnsupportedOperationException``)
- an invalid ``.config(...)`` key/value the session cannot map to a valid engine/catalog
  configuration → :class:`IllegalArgumentException` (what PySpark raises for a JVM
  ``IllegalArgumentException`` — live pyspark 4.0.0 raises it for an invalid ``SQLConf`` value)
- everything else (execution, IO, iceberg commit/data errors — the latter with the
  structured iceberg kind name leading ``str(exc)``, e.g. ``"CatalogCommitConflicts => …"``) →
  the base :class:`PySparkException`

**Python-argument validation** (a bad type or value passed to a facade method — ``df.select(123)``,
``df.sort()`` with no columns, ``df.nosuchattr``) is raised by the pure-Python facade, not the
engine, and mirrors PySpark's wrapper classes: :class:`PySparkTypeError`,
:class:`PySparkValueError`, :class:`PySparkAttributeError`. Each inherits BOTH
:class:`PySparkException` and the builtin it wraps, exactly as ``pyspark.errors`` does
(``class PySparkTypeError(PySparkException, TypeError)``), so pre-existing ``except TypeError`` /
``except ValueError`` / ``except AttributeError`` code keeps working *and* a migrated
``except PySparkException`` now catches them too (before, it silently missed).

.. note::
   Exception-class hierarchy vs Spark is registry row FA-3
   (``docs/spark-sql-iceberg-parity.md`` §5). Pin:
   ``tests/test_errors.py::test_python_arg_errors_runtime_error_divergence_is_deliberate``.

They are defined here in Python rather than in the native module because they need **multiple
bases** (``PySparkException`` + the builtin), which ``pyo3::create_exception!`` cannot express —
and nothing in the Rust engine raises them, so there is no cross-boundary identity to preserve.
"""

from __future__ import annotations

from typing import Any

from repark._native import (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)


# Native exception surface shim (E1 / G4-a): check_error calls getCondition /
# getErrorClass / getMessageParameters / getQueryContext on engine-raised leaves.
# Full native errorClass wiring is OUT; degrade cleanly to None / empty list so
# AttributeError is never the failure mode.
def _native_get_condition(self: object) -> None:
    """Spark 4.x error condition — unset on native engine raises (E1 shim)."""
    return None


def _native_get_message_parameters(self: object) -> None:
    """Spark 4.x message parameters — unset on native engine raises (E1 shim)."""
    return None


def _native_get_query_context(self: object) -> list[Any]:
    """Spark 4.x query context list — empty seed on native engine raises (E1 shim)."""
    return []


for _native_exception_type in (
    PySparkException,
    AnalysisException,
    ParseException,
    UnsupportedOperationException,
    IllegalArgumentException,
):
    if not hasattr(_native_exception_type, "getCondition"):
        _native_exception_type.getCondition = _native_get_condition  # type: ignore[attr-defined]
    if not hasattr(_native_exception_type, "getErrorClass"):
        _native_exception_type.getErrorClass = _native_get_condition  # type: ignore[attr-defined]
    if not hasattr(_native_exception_type, "getMessageParameters"):
        _native_exception_type.getMessageParameters = (  # type: ignore[attr-defined]
            _native_get_message_parameters
        )
    if not hasattr(_native_exception_type, "getQueryContext"):
        _native_exception_type.getQueryContext = (  # type: ignore[attr-defined]
            _native_get_query_context
        )
del _native_exception_type


def _format_error_message(
    message: str | None,
    error_class: str | None,
    message_parameters: dict[str, str | None] | None,
) -> str:
    """Build a display message for a Python-side PySpark*Error.

    When ``message`` is provided it is kept (existing call sites). When only
    ``errorClass`` / ``messageParameters`` are provided (Apache-suite style), render a
    bracketed condition string so ``str(exc)`` is non-empty for logs and asserts.
    Parameter values may be bare ``None`` (C4: assertDataFrameEqual null-arg pins).
    """
    if message is not None:
        return message
    if error_class is None:
        return ""
    params = message_parameters or {}
    if params:
        rendered = ", ".join(f"{key}={value!r}" for key, value in params.items())
        return f"[{error_class}] {rendered}"
    return f"[{error_class}]"


class _PySparkErrorMixin:
    """Spark 4.x error-condition surface used by ``pyspark.testing.utils.check_error``.

    Charter seed for deep kwargs: ``errorClass`` / ``messageParameters`` + empty
    :meth:`getQueryContext` so Apache tests that only pin condition + parameters can PASS
    without a full QueryContext implementation (X3 census).
    """

    _error_class: str | None
    # Values may be bare None (C4: assertDataFrameEqual actual_type/expected_type).
    _message_parameters: dict[str, str | None] | None
    _contexts: list[Any]

    def _init_error_condition(
        self,
        message: str | None = None,
        *,
        errorClass: str | None = None,  # noqa: N803 — PySpark kwarg name
        messageParameters: dict[str, str | None] | None = None,  # noqa: N803
        contexts: list[Any] | None = None,
    ) -> str:
        self._error_class = errorClass
        # Copy so caller mutation / returned-dict mutation cannot corrupt the error
        # (octo X3 C2 — check_error pins identity of messageParameters). Coerce values
        # to str so Apache-style equality on messageParameters stays type-stable (C5).
        # Preserve bare ``None`` values (C4: Apache ``assertDataFrameEqual`` passes
        # ``actual_type: None`` for a missing arg — ``str(None)`` would become ``"None"``
        # and fail check_error equality against the test's expected dict).
        self._message_parameters = (
            None
            if messageParameters is None
            else {
                str(key): (None if value is None else str(value))
                for key, value in messageParameters.items()
            }
        )
        self._contexts = list(contexts or [])
        return _format_error_message(message, errorClass, self._message_parameters)

    def getCondition(self) -> str | None:  # noqa: N802 — PySpark method name
        """Return the error condition / errorClass (PySpark 4.0+)."""
        return self._error_class

    def getErrorClass(self) -> str | None:  # noqa: N802 — PySpark method name
        """Deprecated alias of :meth:`getCondition` (PySpark 3.4+)."""
        return self._error_class

    def getMessageParameters(  # noqa: N802 — PySpark method name
        self,
    ) -> dict[str, str | None] | None:
        """Return a copy of the message parameter map for structured error checks.

        Values may be bare ``None`` (C4 expand2 — Apache assert* null-arg pins).
        """
        if self._message_parameters is None:
            return None
        return dict(self._message_parameters)

    def getQueryContext(self) -> list[Any]:  # noqa: N802 — PySpark method name
        """Return query contexts (empty unless a caller supplied ``contexts=``)."""
        return list(self._contexts)

    def getSqlState(self) -> str | None:  # noqa: N802 — PySpark method name
        """Python-raised errors have no SQLSTATE (PySpark parity → ``None``)."""
        return None

    def getMessage(self) -> str:  # noqa: N802 — PySpark method name
        """Full error message (bracketed condition when set)."""
        body = self.args[0] if self.args else ""
        if self._error_class is not None and not str(body).startswith(f"[{self._error_class}]"):
            return f"[{self._error_class}] {body}"
        return str(body)


class PySparkValueError(_PySparkErrorMixin, PySparkException, ValueError):
    """Wrapper class for :class:`ValueError` — a bad *value* passed to a facade method.

    PySpark parity: ``pyspark.errors.PySparkValueError(PySparkException, ValueError)``. Raised for
    e.g. ``df.sort()`` with no columns, ``df.dropna(how="bogus")``,
    ``spark.createDataFrame([])`` with no schema.

    Accepts Apache-style ``errorClass`` / ``messageParameters`` kwargs (X3 census seed).
    """

    def __init__(
        self,
        message: str | None = None,
        *,
        errorClass: str | None = None,  # noqa: N803 — PySpark kwarg name
        messageParameters: dict[str, str | None] | None = None,  # noqa: N803
        contexts: list[Any] | None = None,
    ) -> None:
        text = self._init_error_condition(
            message,
            errorClass=errorClass,
            messageParameters=messageParameters,
            contexts=contexts,
        )
        ValueError.__init__(self, text)
        self.args = (text,)


class PySparkTypeError(_PySparkErrorMixin, PySparkException, TypeError):
    """Wrapper class for :class:`TypeError` — a wrong-typed argument to a facade method.

    PySpark parity: ``pyspark.errors.PySparkTypeError(PySparkException, TypeError)``. Raised for
    e.g. ``df.select(123)``, ``df.filter(123)``, ``F.sum(123)``.

    Accepts Apache-style ``errorClass`` / ``messageParameters`` kwargs (X3 census seed).
    """

    def __init__(
        self,
        message: str | None = None,
        *,
        errorClass: str | None = None,  # noqa: N803 — PySpark kwarg name
        messageParameters: dict[str, str | None] | None = None,  # noqa: N803
        contexts: list[Any] | None = None,
    ) -> None:
        text = self._init_error_condition(
            message,
            errorClass=errorClass,
            messageParameters=messageParameters,
            contexts=contexts,
        )
        TypeError.__init__(self, text)
        self.args = (text,)


class PySparkAttributeError(_PySparkErrorMixin, PySparkException, AttributeError):
    """Wrapper class for :class:`AttributeError` — an unsupported attribute on a facade object.

    PySpark parity: ``pyspark.errors.PySparkAttributeError(PySparkException, AttributeError)``.
    Raised for e.g. ``df.nosuchattr`` (repark already emitted PySpark's exact
    ``[ATTRIBUTE_NOT_SUPPORTED]`` message here — Group X gives it PySpark's class too).

    Accepts Apache-style ``errorClass`` / ``messageParameters`` kwargs (X3 census seed).
    """

    def __init__(
        self,
        message: str | None = None,
        *,
        errorClass: str | None = None,  # noqa: N803 — PySpark kwarg name
        messageParameters: dict[str, str | None] | None = None,  # noqa: N803
        contexts: list[Any] | None = None,
    ) -> None:
        text = self._init_error_condition(
            message,
            errorClass=errorClass,
            messageParameters=messageParameters,
            contexts=contexts,
        )
        AttributeError.__init__(self, text)
        self.args = (text,)


class PySparkRuntimeError(_PySparkErrorMixin, PySparkException, RuntimeError):
    """Wrapper class for :class:`RuntimeError` with Spark error-class kwargs (E1).

    PySpark parity: ``pyspark.errors.PySparkRuntimeError(PySparkException, RuntimeError)``.
    In repark, :class:`PySparkException` already subclasses :class:`RuntimeError`, so this
    leaf stays under the repark exception tree for ``check_error`` isinstance checks
    (Apache ``test_daytime_interval_type_constructor`` /
    ``test_yearmonth_interval_type_constructor``).
    """

    def __init__(
        self,
        message: str | None = None,
        *,
        errorClass: str | None = None,  # noqa: N803 — PySpark kwarg name
        messageParameters: dict[str, str | None] | None = None,  # noqa: N803
        contexts: list[Any] | None = None,
    ) -> None:
        text = self._init_error_condition(
            message,
            errorClass=errorClass,
            messageParameters=messageParameters,
            contexts=contexts,
        )
        RuntimeError.__init__(self, text)
        self.args = (text,)


class PySparkNotImplementedError(_PySparkErrorMixin, PySparkException, NotImplementedError):
    """Wrapper class for :class:`NotImplementedError` with Spark error-class kwargs (E1).

    PySpark parity: ``pyspark.errors.PySparkNotImplementedError(PySparkException,
    NotImplementedError)``. Seeded under the repark tree so ``assertRaises`` /
    ``check_error`` identity matches after the errors overlay.
    """

    def __init__(
        self,
        message: str | None = None,
        *,
        errorClass: str | None = None,  # noqa: N803 — PySpark kwarg name
        messageParameters: dict[str, str | None] | None = None,  # noqa: N803
        contexts: list[Any] | None = None,
    ) -> None:
        text = self._init_error_condition(
            message,
            errorClass=errorClass,
            messageParameters=messageParameters,
            contexts=contexts,
        )
        NotImplementedError.__init__(self, text)
        self.args = (text,)


# === r20 C4: PySparkAssertionError under repark tree (check_error / assert*Equal) ===
class PySparkAssertionError(_PySparkErrorMixin, PySparkException, AssertionError):
    """Wrapper class for :class:`AssertionError` with Spark error-class kwargs (C4 expand2).

    PySpark parity: ``pyspark.errors.PySparkAssertionError(PySparkException, AssertionError)``.
    Apache ``assertDataFrameEqual`` / ``assertSchemaEqual`` raise this class; after the
    errors overlay, ``check_error`` requires ``isinstance(..., repark.errors.PySparkException)``
    — without this leaf, pyspark's stock class still subclasses the *pre-overlay* base and
    fails that isinstance (C4 hour-0: 5x FAIL-ERROR-CLASS in ``test_utils``).
    """

    def __init__(
        self,
        message: str | None = None,
        *,
        errorClass: str | None = None,  # noqa: N803 — PySpark kwarg name
        messageParameters: dict[str, str | None] | None = None,  # noqa: N803
        contexts: list[Any] | None = None,
        data: list[Any] | None = None,
    ) -> None:
        text = self._init_error_condition(
            message,
            errorClass=errorClass,
            messageParameters=messageParameters,
            contexts=contexts,
        )
        AssertionError.__init__(self, text)
        self.args = (text,)
        # Spark's assert* helpers attach optional row-diff payload under ``data``.
        self.data = data


# The native exception types are heap types whose ``__module__`` names where they are *defined*
# (``repark._native``); re-home them under this module so reprs and tracebacks name the public
# surface users import from (``repark.errors.AnalysisException``). Identity is unchanged — these are
# the exact classes the engine raises.
for _exception_type in (
    PySparkException,
    AnalysisException,
    ParseException,
    UnsupportedOperationException,
    IllegalArgumentException,
):
    _exception_type.__module__ = __name__
del _exception_type

__all__ = [
    "AnalysisException",
    "IllegalArgumentException",
    "ParseException",
    "PySparkAssertionError",
    "PySparkAttributeError",
    "PySparkException",
    "PySparkNotImplementedError",
    "PySparkRuntimeError",
    "PySparkTypeError",
    "PySparkValueError",
    "UnsupportedOperationException",
]
