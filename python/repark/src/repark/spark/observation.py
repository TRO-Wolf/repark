"""PySpark ``Observation`` — named metrics filled by ``DataFrame.observe``."""

from __future__ import annotations

import uuid
from typing import Any

from repark.errors import PySparkAssertionError, PySparkTypeError, PySparkValueError


class Observation:
    """A once-attached metrics handle filled by the first action on the observed frame."""

    def __init__(self, name: str | None = None) -> None:
        """Build an Observation with ``name``, or a generated name when omitted."""
        if name is not None:
            if not isinstance(name, str):
                raise PySparkTypeError(
                    f"[NOT_STR] Argument `name` should be a str, got {type(name).__name__}.",
                    errorClass="NOT_STR",
                    messageParameters={
                        "arg_name": "name",
                        "arg_type": type(name).__name__,
                    },
                )
            if name == "":
                raise PySparkValueError(
                    "[VALUE_NOT_NON_EMPTY_STR] Value for `name` must be a non-empty string, "
                    "got ''.",
                    errorClass="VALUE_NOT_NON_EMPTY_STR",
                    messageParameters={"arg_name": "name", "arg_value": ""},
                )
        self._name = uuid.uuid4().hex if name is None else name
        self._used = False
        self._attached = False
        self._metrics: dict[str, Any] | None = None

    @property
    def get(self) -> dict[str, Any]:
        """Return the filled metric dict, or raise if observe has not run an action."""
        if not self._attached or self._metrics is None:
            raise PySparkAssertionError(
                "[NO_OBSERVE_BEFORE_GET] Should observe by calling `DataFrame.observe` "
                "before `get`.",
                errorClass="NO_OBSERVE_BEFORE_GET",
                messageParameters={},
            )
        return dict(self._metrics)
