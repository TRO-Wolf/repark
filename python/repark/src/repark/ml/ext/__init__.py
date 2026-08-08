"""``repark.ml.ext`` — delegated ML backends (M4).

Bare import of this package **succeeds without** the optional extra installed::

    import repark.ml.ext  # always OK

Touching a concrete class (``XGBoostRegressor``, …) without
``pip install 'repark[ml-ext]'`` raises ``ImportError`` naming the extra.

Native estimators under ``repark.ml`` remain under the M3 Rust rule; only this
subpackage may call optional external libraries on Arrow/pandas frames derived
from ``to_arrow()``.
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
    "EXT_SAVE_UNSUPPORTED": ("repark.ml.ext._persist", "EXT_SAVE_UNSUPPORTED"),
    "PICKLE_FORBIDDEN_REASON": ("repark.ml.ext._persist", "PICKLE_FORBIDDEN_REASON"),
    "SKLEARN_SAVE_UNSUPPORTED": ("repark.ml.ext._persist", "SKLEARN_SAVE_UNSUPPORTED"),
    "XGBoostRegressor": ("repark.ml.ext._xgboost", "XGBoostRegressor"),
    "XGBoostRegressorModel": ("repark.ml.ext._xgboost", "XGBoostRegressorModel"),
    "XGBoostClassifier": ("repark.ml.ext._xgboost", "XGBoostClassifier"),
    "XGBoostClassifierModel": ("repark.ml.ext._xgboost", "XGBoostClassifierModel"),
    "LightGBMRegressor": ("repark.ml.ext._lightgbm", "LightGBMRegressor"),
    "LightGBMRegressorModel": ("repark.ml.ext._lightgbm", "LightGBMRegressorModel"),
    "LightGBMClassifier": ("repark.ml.ext._lightgbm", "LightGBMClassifier"),
    "LightGBMClassifierModel": ("repark.ml.ext._lightgbm", "LightGBMClassifierModel"),
    "RandomForestRegressor": ("repark.ml.ext._sklearn", "RandomForestRegressor"),
    "RandomForestRegressorModel": ("repark.ml.ext._sklearn", "RandomForestRegressorModel"),
    "RandomForestClassifier": ("repark.ml.ext._sklearn", "RandomForestClassifier"),
    "RandomForestClassifierModel": ("repark.ml.ext._sklearn", "RandomForestClassifierModel"),
}


def __getattr__(name: str) -> Any:
    """Lazy-load ext classes so bare ``import repark.ml.ext`` needs no extra."""
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
