"""Spark-shaped parameter objects, converters, and shared parameter mixins."""

from __future__ import annotations

import copy as copy_module
from collections.abc import Callable
from typing import Any, Generic, TypeVar, cast

from repark.errors import IllegalArgumentException, PySparkTypeError
from repark.spark.ml.util import Identifiable

T = TypeVar("T")
P = TypeVar("P", bound="Params")


class Param(Generic[T]):
    """A documented hyperparameter owned by a ``Params`` instance."""

    def __init__(
        self,
        parent: Identifiable,
        name: str,
        doc: str,
        typeConverter: Callable[[Any], T] | None = None,  # noqa: N803 — Spark name
    ) -> None:
        """Bind a name and converter to ``parent.uid``."""
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
        """Return the Spark parameter name."""
        return f"{self.parent}__{self.name}"

    def __repr__(self) -> str:
        """Return a diagnostic representation."""
        return f"Param(parent={self.parent!r}, name={self.name!r}, doc={self.doc!r})"

    def __hash__(self) -> int:
        """Hash by parent uid and name."""
        return hash((self.parent, self.name))

    def __eq__(self, other: object) -> bool:
        """Compare parent uid and name."""
        if not isinstance(other, Param):
            return NotImplemented
        return self.parent == other.parent and self.name == other.name


class TypeConverters:
    """Converters used by Spark-shaped parameter mixins."""

    @staticmethod
    def identity(value: Any) -> Any:
        """Return ``value`` unchanged."""
        return value

    @staticmethod
    def toList(value: Any) -> list[Any]:
        """Convert a list or tuple to a new list."""
        if isinstance(value, list):
            return list(value)
        if isinstance(value, tuple):
            return list(value)
        raise TypeError(f"Could not convert {value!r} to list")

    @staticmethod
    def toListFloat(value: Any) -> list[float]:
        """Convert a list or tuple to float values."""
        return [float(item) for item in TypeConverters.toList(value)]

    @staticmethod
    def toListInt(value: Any) -> list[int]:
        """Convert a list or tuple to integer values."""
        return [int(item) for item in TypeConverters.toList(value)]

    @staticmethod
    def toListString(value: Any) -> list[str]:
        """Convert a list or tuple to string values."""
        return [str(item) for item in TypeConverters.toList(value)]

    @staticmethod
    def toFloat(value: Any) -> float:
        """Convert a non-boolean value to ``float``."""
        if isinstance(value, bool):
            raise TypeError(f"Could not convert {value!r} to float")
        return float(value)

    @staticmethod
    def toInt(value: Any) -> int:
        """Convert a non-boolean value to ``int``."""
        if isinstance(value, bool):
            raise TypeError(f"Could not convert {value!r} to int")
        return int(value)

    @staticmethod
    def toString(value: Any) -> str:
        """Require a string value."""
        if isinstance(value, str):
            return value
        raise TypeError(f"Could not convert {value!r} to string")

    @staticmethod
    def toBoolean(value: Any) -> bool:
        """Require a boolean value."""
        if isinstance(value, bool):
            return value
        raise TypeError(f"Could not convert {value!r} to boolean")


class Params(Identifiable):
    """Base class for objects that own parameter maps."""

    def __init__(self) -> None:
        """Initialize uid, user values, and default values."""
        super().__init__()
        self._paramMap: dict[Param[Any], Any] = {}
        self._defaultParamMap: dict[Param[Any], Any] = {}

    @property
    def params(self) -> list[Param[Any]]:
        """Return all declared parameters in name order."""
        found: dict[str, Param[Any]] = {}
        for cls in type(self).mro():
            for name, value in vars(cls).items():
                if isinstance(value, Param) and name not in found:
                    found[name] = value
        for name, value in vars(self).items():
            if isinstance(value, Param):
                found[name] = value
        return sorted(found.values(), key=lambda param: param.name)

    def explainParam(self, param: str | Param[Any]) -> str:
        """Explain one parameter and its current value state."""
        resolved = self._resolve_param(param)
        value_note = self._value_note(resolved)
        return f"{resolved.name}: {resolved.doc} ({value_note})"

    def explainParams(self) -> str:
        """Explain all parameters, one per line."""
        return "\n".join(self.explainParam(param) for param in self.params)

    def _value_note(self, param: Param[Any]) -> str:
        """Return the default and current value note for ``param``."""
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
        """Return whether the user set ``param``."""
        return self._resolve_param(param) in self._paramMap

    def hasDefault(self, param: str | Param[Any]) -> bool:
        """Return whether ``param`` has a default."""
        return self._resolve_param(param) in self._defaultParamMap

    def isDefined(self, param: str | Param[Any]) -> bool:
        """Return whether ``param`` has a user or default value."""
        resolved = self._resolve_param(param)
        return resolved in self._paramMap or resolved in self._defaultParamMap

    def getOrDefault(self, param: str | Param[Any]) -> Any:
        """Return the user or default value, or raise when undefined."""
        resolved = self._resolve_param(param)
        if resolved in self._paramMap:
            return self._paramMap[resolved]
        if resolved in self._defaultParamMap:
            return self._defaultParamMap[resolved]
        raise IllegalArgumentException(
            f"Param {resolved.name} does not have a default or set value"
        )

    def getParam(self, param_name: str) -> Param[Any]:
        """Return the parameter named ``param_name``."""
        for param in self.params:
            if param.name == param_name:
                return param
        raise IllegalArgumentException(f"Param {param_name} does not exist")

    def _resolve_param(self, param: str | Param[Any]) -> Param[Any]:
        """Resolve a name or copied parameter by name across parent UIDs."""
        if isinstance(param, str):
            return self.getParam(param)
        if not isinstance(param, Param):
            raise PySparkTypeError(f"expected Param or str, got {type(param).__name__}")
        for candidate in self.params:
            if candidate.name == param.name:
                return candidate
        raise IllegalArgumentException(
            f"Param {param.name} does not exist on {type(self).__name__}"
        )

    def _set(self: P, **kwargs: Any) -> P:
        """Set parameters by name after conversion."""
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
        """Set default values after conversion."""
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
        """Clear the user value for ``param`` while keeping its default."""
        resolved = self._resolve_param(param)
        self._paramMap.pop(resolved, None)

    def copy(self: P, extra: dict[Param[Any], Any] | None = None) -> P:
        """Copy parameters, preserve the UID, and apply optional name-based overrides."""
        that = copy_module.copy(self)
        that._paramMap = dict(self._paramMap)
        that._defaultParamMap = dict(self._defaultParamMap)
        if extra:
            for param, value in extra.items():
                name = param.name if isinstance(param, Param) else str(param)
                that._set(**{name: value})
        return that

    def extractParamMap(self, extra: dict[Param[Any], Any] | None = None) -> dict[Param[Any], Any]:
        """Return defaults merged with user values and optional overrides."""
        result = dict(self._defaultParamMap)
        result.update(self._paramMap)
        if extra:
            for param, value in extra.items():
                resolved = self._resolve_param(param)
                result[resolved] = (
                    resolved.typeConverter(value) if hasattr(resolved, "typeConverter") else value
                )
        return result


class HasInputCol:
    """Mixin providing ``inputCol``.

    Requires ``Params`` in the MRO so ``super()`` reaches ``_setDefault``.
    """

    def __init__(self) -> None:
        """Register the input column parameter."""
        super().__init__()
        self.inputCol: Param[str] = Param(
            self,  # type: ignore[arg-type]
            "inputCol",
            "input column name.",
            TypeConverters.toString,
        )
        self._setDefault(inputCol=self.uid + "__input")  # type: ignore[attr-defined]

    def setInputCol(self: P, value: str) -> P:
        """Set the input column name."""
        return self._set(inputCol=value)  # type: ignore[attr-defined,return-value]

    def getInputCol(self) -> str:
        """Return the input column name."""
        return self.getOrDefault(self.inputCol)  # type: ignore[attr-defined,no-any-return]


class HasOutputCol:
    """Mixin providing ``outputCol``.

    Requires ``Params`` in the MRO so ``super()`` reaches ``_setDefault``.
    """

    def __init__(self) -> None:
        """Register the output column parameter."""
        super().__init__()
        self.outputCol: Param[str] = Param(
            self,  # type: ignore[arg-type]
            "outputCol",
            "output column name.",
            TypeConverters.toString,
        )
        self._setDefault(outputCol=self.uid + "__output")  # type: ignore[attr-defined]

    def setOutputCol(self: P, value: str) -> P:
        """Set the output column name."""
        return self._set(outputCol=value)  # type: ignore[attr-defined,return-value]

    def getOutputCol(self) -> str:
        """Return the output column name."""
        return self.getOrDefault(self.outputCol)  # type: ignore[attr-defined,no-any-return]


class HasInputCols:
    """Mixin providing ``inputCols``.

    Requires ``Params`` in the MRO for inherited ``uid``, ``_set``, and ``getOrDefault``.
    """

    def __init__(self) -> None:
        """Register the input columns parameter."""
        super().__init__()
        self.inputCols: Param[list[str]] = Param(
            self,  # type: ignore[arg-type]
            "inputCols",
            "input column names.",
            TypeConverters.toListString,
        )

    def setInputCols(self: P, value: list[str]) -> P:
        """Set the input column names."""
        return self._set(inputCols=value)  # type: ignore[attr-defined,return-value]

    def getInputCols(self) -> list[str]:
        """Return the input column names."""
        return self.getOrDefault(self.inputCols)  # type: ignore[attr-defined,no-any-return]


class HasOutputCols:
    """Mixin providing ``outputCols``.

    Requires ``Params`` in the MRO for inherited ``uid``, ``_set``, and ``getOrDefault``.
    """

    def __init__(self) -> None:
        """Register the output columns parameter."""
        super().__init__()
        self.outputCols: Param[list[str]] = Param(
            self,  # type: ignore[arg-type]
            "outputCols",
            "output column names.",
            TypeConverters.toListString,
        )

    def setOutputCols(self: P, value: list[str]) -> P:
        """Set the output column names."""
        return self._set(outputCols=value)  # type: ignore[attr-defined,return-value]

    def getOutputCols(self) -> list[str]:
        """Return the output column names."""
        return self.getOrDefault(self.outputCols)  # type: ignore[attr-defined,no-any-return]


class HasHandleInvalid:
    """Mixin providing ``handleInvalid``.

    Requires ``Params`` in the MRO so ``super()`` reaches ``_setDefault``.
    """

    def __init__(self) -> None:
        """Register ``handleInvalid`` with default ``error``."""
        super().__init__()
        self.handleInvalid: Param[str] = Param(
            self,  # type: ignore[arg-type]
            "handleInvalid",
            "how to handle invalid data. Options are 'skip', 'error', or 'keep'.",
            TypeConverters.toString,
        )
        self._setDefault(handleInvalid="error")  # type: ignore[attr-defined]

    def setHandleInvalid(self: P, value: str) -> P:
        """Set the invalid-data strategy."""
        return self._set(handleInvalid=value)  # type: ignore[attr-defined,return-value]

    def getHandleInvalid(self) -> str:
        """Return the invalid-data strategy."""
        return self.getOrDefault(self.handleInvalid)  # type: ignore[attr-defined,no-any-return]


class HasFeaturesCol:
    """Mixin providing ``featuresCol``.

    Requires ``Params`` in the MRO so ``super()`` reaches ``_setDefault``.
    """

    def __init__(self) -> None:
        """Register ``featuresCol`` with default ``features``."""
        super().__init__()
        self.featuresCol: Param[str] = Param(
            self,  # type: ignore[arg-type]
            "featuresCol",
            "features column name.",
            TypeConverters.toString,
        )
        self._setDefault(featuresCol="features")  # type: ignore[attr-defined]

    def setFeaturesCol(self: P, value: str) -> P:
        """Set the features column name."""
        return self._set(featuresCol=value)  # type: ignore[attr-defined,return-value]

    def getFeaturesCol(self) -> str:
        """Return the features column name."""
        return self.getOrDefault(self.featuresCol)  # type: ignore[attr-defined,no-any-return]


class HasLabelCol:
    """Mixin providing ``labelCol``.

    Requires ``Params`` in the MRO so ``super()`` reaches ``_setDefault``.
    """

    def __init__(self) -> None:
        """Register ``labelCol`` with default ``label``."""
        super().__init__()
        self.labelCol: Param[str] = Param(
            self,  # type: ignore[arg-type]
            "labelCol",
            "label column name.",
            TypeConverters.toString,
        )
        self._setDefault(labelCol="label")  # type: ignore[attr-defined]

    def setLabelCol(self: P, value: str) -> P:
        """Set the label column name."""
        return self._set(labelCol=value)  # type: ignore[attr-defined,return-value]

    def getLabelCol(self) -> str:
        """Return the label column name."""
        return self.getOrDefault(self.labelCol)  # type: ignore[attr-defined,no-any-return]


class HasPredictionCol:
    """Mixin providing ``predictionCol``.

    Requires ``Params`` in the MRO so ``super()`` reaches ``_setDefault``.
    """

    def __init__(self) -> None:
        """Register ``predictionCol`` with default ``prediction``."""
        super().__init__()
        self.predictionCol: Param[str] = Param(
            self,  # type: ignore[arg-type]
            "predictionCol",
            "prediction column name.",
            TypeConverters.toString,
        )
        self._setDefault(predictionCol="prediction")  # type: ignore[attr-defined]

    def setPredictionCol(self: P, value: str) -> P:
        """Set the prediction column name."""
        return self._set(predictionCol=value)  # type: ignore[attr-defined,return-value]

    def getPredictionCol(self) -> str:
        """Return the prediction column name."""
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
