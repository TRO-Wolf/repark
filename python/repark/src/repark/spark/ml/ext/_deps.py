"""Lazy optional-extra imports for ``repark.ml.ext`` (M4).

Product surface: missing library → loud ``ImportError`` naming
``pip install repark[ml-ext]``. Tests use ``pytest.importorskip``.

Never import numpy / pandas / xgboost / lightgbm / sklearn at ``repark.ml``
top level — only through this module (or after its guards) inside ``ext``.
"""

from __future__ import annotations

from typing import Any

_ML_EXT_HINT = (
    "repark.ml.ext requires the optional extra 'repark[ml-ext]' "
    "(xgboost, lightgbm, scikit-learn, numpy, pandas). "
    "Install with: pip install 'repark[ml-ext]'"
)


def require_xgboost() -> Any:
    """Import ``xgboost`` or raise ``ImportError`` naming ``repark[ml-ext]``."""
    try:
        import xgboost as xgb
    except ImportError as error:
        raise ImportError(_ML_EXT_HINT) from error
    return xgb


def require_lightgbm() -> Any:
    """Import ``lightgbm`` or raise ``ImportError`` naming ``repark[ml-ext]``."""
    try:
        import lightgbm as lgb
    except ImportError as error:
        raise ImportError(_ML_EXT_HINT) from error
    return lgb


def require_sklearn() -> Any:
    """Import ``sklearn`` or raise ``ImportError`` naming ``repark[ml-ext]``."""
    try:
        import sklearn
    except ImportError as error:
        raise ImportError(_ML_EXT_HINT) from error
    return sklearn


def require_numpy() -> Any:
    """Import ``numpy`` (library-required; only behind ext path)."""
    try:
        import numpy as np
    except ImportError as error:
        raise ImportError(_ML_EXT_HINT) from error
    return np


def require_pandas() -> Any:
    """Import ``pandas`` (library-required; only where the lib demands DataFrame)."""
    try:
        import pandas as pd
    except ImportError as error:
        raise ImportError(_ML_EXT_HINT) from error
    return pd


__all__ = [
    "require_lightgbm",
    "require_numpy",
    "require_pandas",
    "require_sklearn",
    "require_xgboost",
]
