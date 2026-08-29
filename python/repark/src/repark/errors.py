"""PySpark-shaped exceptions raised by the engine and Python facade.

Native classes are re-exported unchanged, so catches use class identity. Engine
errors map to ``ParseException`` for syntax, ``AnalysisException`` for planning,
``UnsupportedOperationException`` for refused operations, and
``IllegalArgumentException`` for invalid configuration, ML parameters, schemas,
values, or stream inputs. Other execution, IO, and
Iceberg failures use ``PySparkException``.

Facade argument checks use ``PySparkTypeError``, ``PySparkValueError``, and
``PySparkAttributeError``. Each combines ``PySparkException`` with its builtin
counterpart, preserving both exception contracts. The Python-only classes also
provide Spark's structured error methods and require multiple inheritance that
the native exception macro cannot express.
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


# Provide the structured error methods expected by Spark error-checking helpers.
def _native_get_condition(self: object) -> None:
    """Return no condition for a native engine exception."""
    return None


def _native_get_message_parameters(self: object) -> None:
    """Return no message parameters for a native engine exception."""
    return None


def _native_get_query_context(self: object) -> list[Any]:
    """Return an empty query context for a native engine exception."""
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
    """Build a display message from text or a structured error condition."""
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
    """Provide Spark's structured error-condition methods."""

    _error_class: str | None
    # Values may be bare None in structured error checks.
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
        # Copy and normalize values so callers cannot mutate stored error details.
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

        Values may be bare ``None`` in structured error checks.
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
    """Wrap a bad facade value while preserving Spark's structured error API."""

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
    """Wrap a wrong-typed facade argument while preserving Spark's error API."""

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
    """Wrap an unsupported facade attribute while preserving Spark's error API."""

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
    """Wrap a runtime error with Spark's structured error API."""

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
    """Wrap an unsupported implementation with Spark's structured error API."""

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


class PySparkAssertionError(_PySparkErrorMixin, PySparkException, AssertionError):
    """Wrap assertion failures with Spark's structured error API and optional diff data."""

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
        # Spark's assertion helpers attach optional row-diff payload under ``data``.
        self.data = data


# Re-home native types for public reprs while preserving class identity.
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
