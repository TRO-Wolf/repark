"""Delegated ML backends with lazy optional-dependency imports.

Bare package imports succeed without ``repark[ml-ext]``. Touching a backend
class raises ``ImportError`` naming the extra when its dependency is missing.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "EXT_SAVE_UNSUPPORTED",
    "PICKLE_FORBIDDEN_REASON",
    "SKLEARN_SAVE_UNSUPPORTED",
    "LightGBMClassifier",
    "LightGBMClassifierModel",
    "LightGBMRegressor",
    "LightGBMRegressorModel",
    "RandomForestClassifier",
    "RandomForestClassifierModel",
    "RandomForestRegressor",
    "RandomForestRegressorModel",
    "XGBoostClassifier",
    "XGBoostClassifierModel",
    "XGBoostRegressor",
    "XGBoostRegressorModel",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "EXT_SAVE_UNSUPPORTED": ("repark.spark.ml.ext._persist", "EXT_SAVE_UNSUPPORTED"),
    "PICKLE_FORBIDDEN_REASON": ("repark.spark.ml.ext._persist", "PICKLE_FORBIDDEN_REASON"),
    "SKLEARN_SAVE_UNSUPPORTED": ("repark.spark.ml.ext._persist", "SKLEARN_SAVE_UNSUPPORTED"),
    "XGBoostRegressor": ("repark.spark.ml.ext._xgboost", "XGBoostRegressor"),
    "XGBoostRegressorModel": ("repark.spark.ml.ext._xgboost", "XGBoostRegressorModel"),
    "XGBoostClassifier": ("repark.spark.ml.ext._xgboost", "XGBoostClassifier"),
    "XGBoostClassifierModel": ("repark.spark.ml.ext._xgboost", "XGBoostClassifierModel"),
    "LightGBMRegressor": ("repark.spark.ml.ext._lightgbm", "LightGBMRegressor"),
    "LightGBMRegressorModel": ("repark.spark.ml.ext._lightgbm", "LightGBMRegressorModel"),
    "LightGBMClassifier": ("repark.spark.ml.ext._lightgbm", "LightGBMClassifier"),
    "LightGBMClassifierModel": ("repark.spark.ml.ext._lightgbm", "LightGBMClassifierModel"),
    "RandomForestRegressor": ("repark.spark.ml.ext._sklearn", "RandomForestRegressor"),
    "RandomForestRegressorModel": ("repark.spark.ml.ext._sklearn", "RandomForestRegressorModel"),
    "RandomForestClassifier": ("repark.spark.ml.ext._sklearn", "RandomForestClassifier"),
    "RandomForestClassifierModel": ("repark.spark.ml.ext._sklearn", "RandomForestClassifierModel"),
}


def __getattr__(name: str) -> Any:
    """Load an exported backend lazily so package import needs no extra."""
    if name not in _EXPORT_MAP:
        raise AttributeError(f"module 'repark.ml.ext' has no attribute {name!r}")
    module_name, attr_name = _EXPORT_MAP[name]
    import importlib

    module = importlib.import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Expose public names for dir()/autocomplete."""
    return sorted(__all__)
