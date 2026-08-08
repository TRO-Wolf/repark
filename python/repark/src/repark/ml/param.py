"""Param system — :class:`Param`, :class:`Params`, type converters (PySpark ``ml.param``).

Oracle surface: ``getOrDefault`` / ``explainParams`` / ``copy(extra)`` / param doc strings.
"""

from __future__ import annotations

import copy as copy_module
from collections.abc import Callable
from typing import Any, Generic, TypeVar, cast

from repark.errors import IllegalArgumentException, PySparkTypeError
from repark.ml.util import Identifiable

T = TypeVar("T")
P = TypeVar("P", bound="Params")


class Param(Generic[T]):
    """A documented hyperparameter on a :class:`Params` parent."""

    def __init__(
        self,
        parent: Identifiable,
        name: str,
        doc: str,
        typeConverter: Callable[[Any], T] | None = None,  # noqa: N803 — Spark name
    ) -> None:
        """Bind ``name`` / ``doc`` to ``parent.uid``; optional converter validates sets."""
        if not isinstance(parent, Identifiable):
            raise PySparkTypeError(
                f"Param parent must be Identifiable, got {type(parent).__name__}"
            )
        if not isinstance(name, str):
            raise PySparkTypeError(f"Param name must be str, got {type(name).__name__}")
        if not isinstance(doc, str):
            raise PySparkTypeError(f"Param doc must be str, got {type(doc).__name__}")
        self.parent = parent.uid
        self.name = name
        self.doc = doc
        self.typeConverter: Callable[[Any], T] = (
            typeConverter
            if typeConverter is not None
            else cast(Callable[[Any], T], TypeConverters.identity)
        )

    def __str__(self) -> str:
        """Spark form ``parent__name``."""
        return f"{self.parent}__{self.name}"

    def __repr__(self) -> str:
        """Debug form with doc."""
        return f"Param(parent={self.parent!r}, name={self.name!r}, doc={self.doc!r})"

    def __hash__(self) -> int:
        """Hash on parent uid + name."""
        return hash((self.parent, self.name))

    def __eq__(self, other: object) -> bool:
        """Equality on parent uid + name."""
        if not isinstance(other, Param):
            return NotImplemented
        return self.parent == other.parent and self.name == other.name


class TypeConverters:
    """Static converters used by shared Param mixins (Spark ``TypeConverters``)."""

    @staticmethod
    def identity(value: Any) -> Any:
        """Pass-through converter."""
        return value

    @staticmethod
    def toList(value: Any) -> list[Any]:
        """Convert a sequence to ``list``."""
        if isinstance(value, list):
            return list(value)
        if isinstance(value, tuple):
            return list(value)
        raise TypeError(f"Could not convert {value!r} to list")

    @staticmethod
    def toListFloat(value: Any) -> list[float]:
        """Convert a sequence to ``list[float]``."""
        return [float(item) for item in TypeConverters.toList(value)]

    @staticmethod
    def toListInt(value: Any) -> list[int]:
        """Convert a sequence to ``list[int]``."""
        return [int(item) for item in TypeConverters.toList(value)]

    @staticmethod
    def toListString(value: Any) -> list[str]:
        """Convert a sequence to ``list[str]``."""
        return [str(item) for item in TypeConverters.toList(value)]

    @staticmethod
    def toFloat(value: Any) -> float:
        """Convert to ``float``."""
        if isinstance(value, bool):
            raise TypeError(f"Could not convert {value!r} to float")
        return float(value)

    @staticmethod
    def toInt(value: Any) -> int:
        """Convert to ``int``."""
        if isinstance(value, bool):
            raise TypeError(f"Could not convert {value!r} to int")
        return int(value)

    @staticmethod
    def toString(value: Any) -> str:
        """Convert to ``str``."""
        if isinstance(value, str):
            return value
        raise TypeError(f"Could not convert {value!r} to string")

    @staticmethod
    def toBoolean(value: Any) -> bool:
        """Convert to ``bool``."""
        if isinstance(value, bool):
            return value
        raise TypeError(f"Could not convert {value!r} to boolean")


class Params(Identifiable):
    """Base for objects that own a :class:`Param` map (Spark ``Params``)."""

    def __init__(self) -> None:
        """Initialize empty param maps; subclasses register params as attributes."""
        super().__init__()
        self._paramMap: dict[Param[Any], Any] = {}
        self._defaultParamMap: dict[Param[Any], Any] = {}

    @property
    def params(self) -> list[Param[Any]]:
        """All :class:`Param` attributes on this instance (Spark ``params``)."""
        found: dict[str, Param[Any]] = {}
        for cls in type(self).mro():
            for name, value in vars(cls).items():
                if isinstance(value, Param) and name not in found:
                    found[name] = value
        # Instance-level Params (created in ``__init__``) win.
        for name, value in vars(self).items():
            if isinstance(value, Param):
                found[name] = value
        return sorted(found.values(), key=lambda param: param.name)

    def explainParam(self, param: str | Param[Any]) -> str:
        """One-line explanation for a single param (Spark ``explainParam``)."""
        resolved = self._resolve_param(param)
        value_note = self._value_note(resolved)
        return f"{resolved.name}: {resolved.doc} ({value_note})"

    def explainParams(self) -> str:
        """Multi-line explanation of all params (Spark ``explainParams``)."""
        return "\n".join(self.explainParam(param) for param in self.params)

    def _value_note(self, param: Param[Any]) -> str:
        """Render default/current/undefined note for explainParams."""
        has_user = param in self._paramMap
        has_default = param in self._defaultParamMap
        if has_user and has_default:
            return f"default: {self._defaultParamMap[param]}, current: {self._paramMap[param]}"
        if has_user:
            return f"current: {self._paramMap[param]}"
        if has_default:
            return f"default: {self._defaultParamMap[param]}"
        return "undefined"

    def isSet(self, param: str | Param[Any]) -> bool:
        """Whether the user has set this param."""
        return self._resolve_param(param) in self._paramMap

    def hasDefault(self, param: str | Param[Any]) -> bool:
        """Whether a default exists."""
        return self._resolve_param(param) in self._defaultParamMap

    def isDefined(self, param: str | Param[Any]) -> bool:
        """Whether a value (user or default) exists."""
        resolved = self._resolve_param(param)
        return resolved in self._paramMap or resolved in self._defaultParamMap

    def getOrDefault(self, param: str | Param[Any]) -> Any:
        """Return user value or default; raise if undefined."""
        resolved = self._resolve_param(param)
        if resolved in self._paramMap:
            return self._paramMap[resolved]
        if resolved in self._defaultParamMap:
            return self._defaultParamMap[resolved]
        raise IllegalArgumentException(
            f"Param {resolved.name} does not have a default or set value"
        )

    def getParam(self, param_name: str) -> Param[Any]:
        """Look up a Param by name."""
        for param in self.params:
            if param.name == param_name:
                return param
        raise IllegalArgumentException(f"Param {param_name} does not exist")

    def _resolve_param(self, param: str | Param[Any]) -> Param[Any]:
        """Resolve a name or Param to the instance's Param object."""
        if isinstance(param, str):
            return self.getParam(param)
        if not isinstance(param, Param):
            raise PySparkTypeError(f"expected Param or str, got {type(param).__name__}")
        # Match by name against our params (parent uids may differ after copy).
        for candidate in self.params:
            if candidate.name == param.name:
                return candidate
        raise IllegalArgumentException(
            f"Param {param.name} does not exist on {type(self).__name__}"
        )

    def _set(self: P, **kwargs: Any) -> P:
        """Set params by keyword (internal)."""
        for name, value in kwargs.items():
            param = self.getParam(name)
            try:
                converted = param.typeConverter(value)
            except TypeError as error:
                raise IllegalArgumentException(
                    f"Invalid value for param {name}: {error}"
                ) from error
            self._paramMap[param] = converted
        return self

    def _setDefault(self: P, **kwargs: Any) -> P:
        """Set default param values."""
        for name, value in kwargs.items():
            param = self.getParam(name)
            try:
                converted = param.typeConverter(value)
            except TypeError as error:
                raise IllegalArgumentException(
                    f"Invalid default for param {name}: {error}"
                ) from error
            self._defaultParamMap[param] = converted
        return self

    def clear(self, param: Param[Any]) -> None:
        """Clear a user-set param (defaults remain)."""
        resolved = self._resolve_param(param)
        self._paramMap.pop(resolved, None)

    def copy(self: P, extra: dict[Param[Any], Any] | None = None) -> P:
        """Deep-ish copy with optional extra Param map (Spark ``copy``)."""
        # Create a fresh instance without calling subclass __init__ side effects twice
        # when possible; fall back to copy_module for simple stages.
        that = copy_module.copy(self)
        that._paramMap = dict(self._paramMap)
        that._defaultParamMap = dict(self._defaultParamMap)
        # Fresh uid like Spark's copy? Spark keeps same class; uid is typically new on
        # new instances. Spark Params.copy keeps the same uid. Match Spark: keep uid.
        if extra:
            for param, value in extra.items():
                name = param.name if isinstance(param, Param) else str(param)
                that._set(**{name: value})
        return that

    def extractParamMap(self, extra: dict[Param[Any], Any] | None = None) -> dict[Param[Any], Any]:
        """Defaults + user + extra (Spark ``extractParamMap``)."""
        result = dict(self._defaultParamMap)
        result.update(self._paramMap)
        if extra:
            for param, value in extra.items():
                resolved = self._resolve_param(param)
                result[resolved] = (
                    resolved.typeConverter(value) if hasattr(resolved, "typeConverter") else value
                )
        return result


# Shared mixins (subset used by M1/M2 feature transformers).
# Pure mixins: cooperative ``super().__init__()``; must be combined with :class:`Params`
# (do not inherit Params here — avoids diamond double-init of Identifiable.uid).


class HasInputCol:
    """Mixin: ``inputCol`` param (requires :class:`Params` in the MRO)."""

    def __init__(self) -> None:
        """Register ``inputCol``."""
        super().__init__()
        self.inputCol: Param[str] = Param(
            self,  # type: ignore[arg-type]
            "inputCol",
            "input column name.",
            TypeConverters.toString,
        )
        self._setDefault(inputCol=self.uid + "__input")  # type: ignore[attr-defined]

    def setInputCol(self: P, value: str) -> P:
        """Set input column name."""
        return self._set(inputCol=value)  # type: ignore[attr-defined,return-value]

    def getInputCol(self) -> str:
        """Get input column name."""
        return self.getOrDefault(self.inputCol)  # type: ignore[attr-defined,no-any-return]


class HasOutputCol:
    """Mixin: ``outputCol`` param (requires :class:`Params` in the MRO)."""

    def __init__(self) -> None:
        """Register ``outputCol``."""
        super().__init__()
        self.outputCol: Param[str] = Param(
            self,  # type: ignore[arg-type]
            "outputCol",
            "output column name.",
            TypeConverters.toString,
        )
        self._setDefault(outputCol=self.uid + "__output")  # type: ignore[attr-defined]

    def setOutputCol(self: P, value: str) -> P:
        """Set output column name."""
        return self._set(outputCol=value)  # type: ignore[attr-defined,return-value]

    def getOutputCol(self) -> str:
        """Get output column name."""
        return self.getOrDefault(self.outputCol)  # type: ignore[attr-defined,no-any-return]


class HasInputCols:
    """Mixin: ``inputCols`` param (requires :class:`Params` in the MRO)."""

    def __init__(self) -> None:
        """Register ``inputCols``."""
        super().__init__()
        self.inputCols: Param[list[str]] = Param(
            self,  # type: ignore[arg-type]
            "inputCols",
            "input column names.",
            TypeConverters.toListString,
        )

    def setInputCols(self: P, value: list[str]) -> P:
        """Set input column names."""
        return self._set(inputCols=value)  # type: ignore[attr-defined,return-value]

    def getInputCols(self) -> list[str]:
        """Get input column names."""
        return self.getOrDefault(self.inputCols)  # type: ignore[attr-defined,no-any-return]


class HasOutputCols:
    """Mixin: ``outputCols`` param (requires :class:`Params` in the MRO)."""

    def __init__(self) -> None:
        """Register ``outputCols``."""
        super().__init__()
        self.outputCols: Param[list[str]] = Param(
            self,  # type: ignore[arg-type]
            "outputCols",
            "output column names.",
            TypeConverters.toListString,
        )

    def setOutputCols(self: P, value: list[str]) -> P:
        """Set output column names."""
        return self._set(outputCols=value)  # type: ignore[attr-defined,return-value]

    def getOutputCols(self) -> list[str]:
        """Get output column names."""
        return self.getOrDefault(self.outputCols)  # type: ignore[attr-defined,no-any-return]


class HasHandleInvalid:
    """Mixin: ``handleInvalid`` param (requires :class:`Params` in the MRO)."""

    def __init__(self) -> None:
        """Register ``handleInvalid`` (default ``error``)."""
        super().__init__()
        self.handleInvalid: Param[str] = Param(
            self,  # type: ignore[arg-type]
            "handleInvalid",
            "how to handle invalid data. Options are 'skip', 'error', or 'keep'.",
            TypeConverters.toString,
        )
        self._setDefault(handleInvalid="error")  # type: ignore[attr-defined]

    def setHandleInvalid(self: P, value: str) -> P:
        """Set invalid-handling strategy."""
        return self._set(handleInvalid=value)  # type: ignore[attr-defined,return-value]

    def getHandleInvalid(self) -> str:
        """Get invalid-handling strategy."""
        return self.getOrDefault(self.handleInvalid)  # type: ignore[attr-defined,no-any-return]


class HasFeaturesCol:
    """Mixin: ``featuresCol`` param (Spark ML estimators)."""

    def __init__(self) -> None:
        """Register ``featuresCol`` (default ``features``)."""
        super().__init__()
        self.featuresCol: Param[str] = Param(
            self,  # type: ignore[arg-type]
            "featuresCol",
            "features column name.",
            TypeConverters.toString,
        )
        self._setDefault(featuresCol="features")  # type: ignore[attr-defined]

    def setFeaturesCol(self: P, value: str) -> P:
        """Set features column name."""
        return self._set(featuresCol=value)  # type: ignore[attr-defined,return-value]

    def getFeaturesCol(self) -> str:
        """Get features column name."""
        return self.getOrDefault(self.featuresCol)  # type: ignore[attr-defined,no-any-return]


class HasLabelCol:
    """Mixin: ``labelCol`` param (Spark ML estimators)."""

    def __init__(self) -> None:
        """Register ``labelCol`` (default ``label``)."""
        super().__init__()
        self.labelCol: Param[str] = Param(
            self,  # type: ignore[arg-type]
            "labelCol",
            "label column name.",
            TypeConverters.toString,
        )
        self._setDefault(labelCol="label")  # type: ignore[attr-defined]

    def setLabelCol(self: P, value: str) -> P:
        """Set label column name."""
        return self._set(labelCol=value)  # type: ignore[attr-defined,return-value]

    def getLabelCol(self) -> str:
        """Get label column name."""
        return self.getOrDefault(self.labelCol)  # type: ignore[attr-defined,no-any-return]


class HasPredictionCol:
    """Mixin: ``predictionCol`` param (Spark ML models)."""

    def __init__(self) -> None:
        """Register ``predictionCol`` (default ``prediction``)."""
        super().__init__()
        self.predictionCol: Param[str] = Param(
            self,  # type: ignore[arg-type]
            "predictionCol",
            "prediction column name.",
            TypeConverters.toString,
        )
        self._setDefault(predictionCol="prediction")  # type: ignore[attr-defined]

    def setPredictionCol(self: P, value: str) -> P:
        """Set prediction column name."""
        return self._set(predictionCol=value)  # type: ignore[attr-defined,return-value]

    def getPredictionCol(self) -> str:
        """Get prediction column name."""
        return self.getOrDefault(self.predictionCol)  # type: ignore[attr-defined,no-any-return]


__all__ = [
    "HasFeaturesCol",
    "HasHandleInvalid",
    "HasInputCol",
    "HasInputCols",
    "HasLabelCol",
    "HasOutputCol",
    "HasOutputCols",
    "HasPredictionCol",
    "Param",
    "Params",
    "TypeConverters",
]
