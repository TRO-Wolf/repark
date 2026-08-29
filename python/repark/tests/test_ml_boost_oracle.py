"""M4 R-ML-BOOST oracles — XGBoostRegressor + ParamGrid/CV + OHE plural + ext gates.

Booster parity is vs the library run **directly** on the same frame (not Spark).
CrossValidator live-pyspark differentials importorskip when JVM unavailable.
No bit-exact claim beyond library determinism with seed.
"""

from __future__ import annotations

import builtins
import gc
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    UnsupportedOperationException,
)
from repark.spark._temp_views import local_view_name
from repark.spark.ml.evaluation import (
    MULTICLASS_F1_SEED,
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
)
from repark.spark.ml.feature import OneHotEncoder, VectorAssembler
from repark.spark.ml.pipeline import Pipeline
from repark.spark.ml.regression import LinearRegression
from repark.spark.ml.tuning import CrossValidator, ParamGridBuilder


def _session() -> ReparkSession:
    return ReparkSession.builder.appName("ml-boost-test").getOrCreate()


def _rel_close(a: float, b: float, tol: float = 1e-5) -> bool:
    if a == b:
        return True
    denom = max(abs(a), abs(b), 1e-15)
    return abs(a - b) / denom <= tol


def _block_module_import(monkeypatch: pytest.MonkeyPatch, module_root: str) -> None:
    """Force ``import module_root`` to raise so require_* rewrites stay always-on."""
    real_import = builtins.__import__

    def _blocked(
        name: str,
        globals_dict: dict | None = None,
        locals_dict: dict | None = None,
        fromlist: tuple = (),
        level: int = 0,
    ):
        if name == module_root or name.startswith(f"{module_root}."):
            raise ImportError(f"No module named {module_root!r}")
        return real_import(name, globals_dict, locals_dict, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _blocked)
    # Drop cached package + submodules so the next import re-enters builtins.
    prefix = f"{module_root}."
    doomed = [key for key in list(sys.modules) if key == module_root or key.startswith(prefix)]
    for key in doomed:
        monkeypatch.delitem(sys.modules, key, raising=False)


def _looks_like_training_row_batch(
    value: Any, *, expected_num_rows: int, depth: int = 0
) -> str | None:
    """Return a reason string if ``value`` looks like a re-held training batch.

    Catches ndarray, list/tuple, Arrow-table, and nested dict holds; depth-limited.
    """
    if expected_num_rows <= 0 or depth > 4:
        return None
    shape = getattr(value, "shape", None)
    if shape is not None and hasattr(value, "dtype"):
        try:
            if len(shape) >= 1 and int(shape[0]) == expected_num_rows:
                return f"array-like shape={shape!r}"
        except (TypeError, ValueError):
            pass
    # Module-agnostic: no pyarrow import required.
    num_rows_attr = getattr(value, "num_rows", None)
    if num_rows_attr is not None and hasattr(value, "column_names"):
        try:
            if int(num_rows_attr) == expected_num_rows:
                return f"Arrow-like table num_rows={num_rows_attr}"
        except (TypeError, ValueError):
            pass
    if isinstance(value, (list, tuple)) and len(value) == expected_num_rows:
        return f"sequence len={len(value)}"
    if isinstance(value, dict):
        for key, nested in value.items():
            reason = _looks_like_training_row_batch(
                nested, expected_num_rows=expected_num_rows, depth=depth + 1
            )
            if reason is not None:
                return f"dict[{key!r}] -> {reason}"
    return None


def _assert_no_training_row_rehold(model: Any, *, expected_num_rows: int) -> None:
    """Pin: fitted ext model shell must not re-hold training rows (octo C5-Q-002).

    Name denylist alone is insufficient; list/Arrow holds, including fit_params,
    must fail too.
    """
    forbidden_names = (
        "_training_rows",
        "training_data",
        "_X",
        "_y",
        "X_train",
        "y_train",
        "_feature_matrix",
        "_label_vector",
        "_train_table",
        "_hold",
        "_rows",
        "_train_frame",
    )
    for name in forbidden_names:
        assert not hasattr(model, name), f"forbidden training-row attr present: {name}"

    # Skip only the external booster handle + scalar bookkeeping — NOT fit_params.
    skip_names = {"_booster", "uid", "num_features", "num_rows"}
    for name, value in vars(model).items():
        if name in skip_names:
            continue
        reason = _looks_like_training_row_batch(value, expected_num_rows=expected_num_rows)
        if reason is not None:
            raise AssertionError(f"training-row re-hold suspect: model.{name}: {reason}")


def _maybe_live_spark():
    """Return a live SparkSession or skip."""
    pytest.importorskip("pyspark")
    java_home = os.environ.get("JAVA_HOME", "")
    if not java_home or "11" in java_home:
        for candidate in (
            "/usr/lib/jvm/zulu-17-amd64",
            "/usr/lib/jvm/java-17-openjdk-amd64",
            "/usr/lib/jvm/java-21-openjdk-amd64",
        ):
            if Path(candidate).is_dir():
                os.environ["JAVA_HOME"] = candidate
                os.environ["PATH"] = f"{candidate}/bin:" + os.environ.get("PATH", "")
                break
    try:
        from pyspark.sql import SparkSession

        return (
            SparkSession.builder.master("local[1]")
            .appName("repark-ml-boost-oracle")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    except Exception as error:
        pytest.skip(f"live pyspark unavailable: {error}")


# Package surface — bare import + loud ImportError naming the extra


def test_ext_package_import_succeeds_bare() -> None:
    """``import repark.ml.ext`` must succeed without ml-ext installed."""
    import repark.spark.ml.ext as ext

    assert ext is not None
    assert hasattr(ext, "XGBoostRegressor") or "XGBoostRegressor" in getattr(ext, "__all__", [])


@pytest.mark.parametrize(
    ("module_root", "require_name"),
    [
        ("xgboost", "require_xgboost"),
        ("lightgbm", "require_lightgbm"),
        ("sklearn", "require_sklearn"),
        ("numpy", "require_numpy"),
        ("pandas", "require_pandas"),
    ],
)
def test_ext_require_names_extra_when_missing(
    monkeypatch: pytest.MonkeyPatch, module_root: str, require_name: str
) -> None:
    """Every require_* must rewrite ImportError to name repark[ml-ext] (octo C5-Q-001).

    Mutation-proof (C3-Q-001 + C5): only pinning require_xgboost left the other
    rewrites untested; force missing-dep via import monkeypatch.
    """
    _block_module_import(monkeypatch, module_root)
    import repark.spark.ml.ext._deps as deps

    require_fn = getattr(deps, require_name)
    with pytest.raises(ImportError, match=r"repark\[ml-ext\]"):
        require_fn()


@pytest.mark.parametrize(
    ("module_root", "import_path", "class_name"),
    [
        ("xgboost", "repark.spark.ml.ext._xgboost", "XGBoostRegressor"),
        ("xgboost", "repark.spark.ml.ext._xgboost", "XGBoostClassifier"),
        ("lightgbm", "repark.spark.ml.ext._lightgbm", "LightGBMRegressor"),
        ("lightgbm", "repark.spark.ml.ext._lightgbm", "LightGBMClassifier"),
        ("sklearn", "repark.spark.ml.ext._sklearn", "RandomForestRegressor"),
        ("sklearn", "repark.spark.ml.ext._sklearn", "RandomForestClassifier"),
    ],
)
def test_ext_class_touch_names_extra_when_missing(
    monkeypatch: pytest.MonkeyPatch,
    module_root: str,
    import_path: str,
    class_name: str,
) -> None:
    """Class-touch for XGB/LGBM/RF (regressor + classifier) must name repark[ml-ext].

    Octo C7-Q-003: a regressor-only enum left classifier ``__init__`` free to drop
    ``_ensure_*_loaded`` while the suite stayed green.
    """
    _block_module_import(monkeypatch, module_root)
    import importlib

    module = importlib.import_module(import_path)
    estimator_cls = getattr(module, class_name)
    with pytest.raises(ImportError, match=r"repark\[ml-ext\]"):
        estimator_cls()


# ParamGridBuilder + CrossValidator (merge bar; CV over LR counts)


def test_param_grid_builder_cartesian() -> None:
    """ParamGridBuilder builds the cartesian product of axes."""
    lr = LinearRegression()
    grid = (
        ParamGridBuilder()
        .addGrid(lr.maxIter, [1, 2])
        .addGrid(lr.fitIntercept, [True, False])
        .build()
    )
    assert len(grid) == 4
    names_sets = [{param.name for param in param_map} for param_map in grid]
    assert all(names == {"maxIter", "fitIntercept"} for names in names_sets)


def test_cross_validator_over_linear_regression() -> None:
    """CV over small LR grid selects a model and exposes avgMetrics (merge bar)."""
    spark = _session()
    try:
        rows = [(float(x), 1.0 + 2.0 * float(x) + (0.01 * (x % 3))) for x in range(24)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        lr = LinearRegression(featuresCol="features", labelCol="label", predictionCol="prediction")
        grid = ParamGridBuilder().addGrid(lr.fitIntercept, [True, False]).build()
        evaluator = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="rmse"
        )
        cv = CrossValidator(
            estimator=lr,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42,
        )
        model = cv.fit(assembled)
        assert model.bestModel is not None
        assert len(model.avgMetrics) == len(grid)
        assert all(isinstance(value, float) for value in model.avgMetrics)
        # Smaller RMSE is better — best avg should be the min (isLargerBetter=False path).
        best_index = min(range(len(model.avgMetrics)), key=lambda index: model.avgMetrics[index])
        assert model.avgMetrics[best_index] == min(model.avgMetrics)
        # Mutation-proof: bestModel must match the param map at best_index (not
        # always [0]); y≈1+2x with intercept means fitIntercept=True wins on RMSE.
        best_map = model.estimatorParamMaps[best_index]
        fit_intercept = None
        for param, value in best_map.items():
            if getattr(param, "name", None) == "fitIntercept":
                fit_intercept = value
        assert fit_intercept is True, (
            f"expected fitIntercept=True as best map, got {best_map!r} "
            f"avgMetrics={model.avgMetrics!r}"
        )
        assert bool(model.bestModel.fit_intercept) is True
        # Metrics must not be identical placeholders (catches always-zero / inverted-only).
        assert min(model.avgMetrics) < max(model.avgMetrics)
        preds = model.transform(assembled).collect()
        assert len(preds) == 24
        assert "prediction" in preds[0].asDict()
    finally:
        spark.stop()


def test_cross_validator_parallelism_determinism() -> None:
    """M6: parallelism>1 matches sequential avgMetrics and bestModel selection.

    MUTATION: racey non-atomic metrics[map_index] += score without lock / wrong reduce
    → metrics differ across parallelism; or fold re-assignment under concurrent view
    registration → non-deterministic bestModel.
    """
    spark = _session()
    try:
        rows = [(float(x), 1.0 + 2.0 * float(x) + (0.01 * (x % 3))) for x in range(24)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        lr = LinearRegression(featuresCol="features", labelCol="label", predictionCol="prediction")
        grid = ParamGridBuilder().addGrid(lr.fitIntercept, [True, False]).build()
        evaluator = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="rmse"
        )
        common = {
            "estimator": lr,
            "estimatorParamMaps": grid,
            "evaluator": evaluator,
            "numFolds": 3,
            "seed": 42,
        }
        sequential = CrossValidator(**common, parallelism=1).fit(assembled)
        parallel = CrossValidator(**common, parallelism=4).fit(assembled)
        assert len(sequential.avgMetrics) == len(parallel.avgMetrics) == len(grid)
        for left, right in zip(sequential.avgMetrics, parallel.avgMetrics, strict=True):
            assert abs(left - right) < 1e-9, (sequential.avgMetrics, parallel.avgMetrics)
        assert sequential.bestModel is not None and parallel.bestModel is not None
        assert bool(sequential.bestModel.fit_intercept) == bool(parallel.bestModel.fit_intercept)
    finally:
        spark.stop()


def test_cross_validator_parallelism_ctor_refuses_non_positive() -> None:
    """M6 octo C1: constructor parallelism must not silently clamp via getParallelism max(1,…).

    MUTATION: ctor ``_set(parallelism=value)`` without setParallelism validation → raw -3
    stored; getParallelism returns 1 and fit proceeds without a loud refuse.
    """
    with pytest.raises(IllegalArgumentException, match=r"parallelism.*>= 1"):
        CrossValidator(parallelism=0)
    with pytest.raises(IllegalArgumentException, match=r"parallelism.*>= 1"):
        CrossValidator(parallelism=-3)
    with pytest.raises(IllegalArgumentException, match=r"parallelism.*>= 1"):
        CrossValidator().setParallelism(0)


def test_cross_validator_live_pyspark_shape() -> None:
    """When JVM available: Spark CrossValidator also accepts ParamGrid + numFolds shape."""
    spark = _maybe_live_spark()
    try:
        from pyspark.ml.evaluation import RegressionEvaluator as SparkRE
        from pyspark.ml.feature import VectorAssembler as SparkVA
        from pyspark.ml.regression import LinearRegression as SparkLR
        from pyspark.ml.tuning import CrossValidator as SparkCV
        from pyspark.ml.tuning import ParamGridBuilder as SparkPGB

        rows = [(float(x), 1.0 + 2.0 * float(x)) for x in range(20)]
        sdf = spark.createDataFrame(rows, ["x", "label"])
        assembled = SparkVA(inputCols=["x"], outputCol="features").transform(sdf)
        lr = SparkLR(featuresCol="features", labelCol="label")
        grid = SparkPGB().addGrid(lr.fitIntercept, [True, False]).build()
        evaluator = SparkRE(labelCol="label", predictionCol="prediction", metricName="rmse")
        cv = SparkCV(
            estimator=lr,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=3,
            seed=7,
        )
        model = cv.fit(assembled)
        assert model.bestModel is not None
        assert len(model.avgMetrics) == 2
    finally:
        spark.stop()


# OneHotEncoder plural inputCols/outputCols (merge bar)


def test_one_hot_encoder_plural_cols() -> None:
    """OHE plural inputCols/outputCols produces one sparse col per input."""
    spark = _session()
    try:
        df = spark.createDataFrame([(0.0, 1.0), (1.0, 0.0), (0.0, 1.0)], ["a", "b"])
        model = OneHotEncoder(
            inputCols=["a", "b"],
            outputCols=["a_oh", "b_oh"],
            dropLast=True,
        ).fit(df)
        assert len(model.category_sizes) == 2
        out = model.transform(df).collect()
        row0 = out[0].asDict()
        assert "a_oh" in row0 and "b_oh" in row0
        assert "size" in row0["a_oh"] and "indices" in row0["a_oh"]
        assert "size" in row0["b_oh"] and "indices" in row0["b_oh"]
    finally:
        spark.stop()


def test_one_hot_encoder_singular_still_works() -> None:
    """Singular inputCol path remains green."""
    spark = _session()
    try:
        df = spark.createDataFrame([(0.0,), (1.0,), (0.0,)], ["idx"])
        model = OneHotEncoder(inputCol="idx", outputCol="oh", dropLast=True).fit(df)
        assert model.category_size == 2
        row = model.transform(df).collect()[0].asDict()["oh"]
        assert row["size"] == 1
    finally:
        spark.stop()


def test_one_hot_encoder_handle_invalid_rejects_illegal_mode() -> None:
    """Illegal handleInvalid must fail loud — not silently act as keep (octo C3-L-001).

    Mutation-proof: deleting the {error,keep,skip} membership check lets illegal
    modes emit empty sparse vectors without exception.
    """
    spark = _session()
    try:
        df = spark.createDataFrame([(0.0,), (1.0,)], ["idx"])
        model = OneHotEncoder(
            inputCol="idx", outputCol="oh", dropLast=True, handleInvalid="Error"
        ).fit(df)
        with pytest.raises(IllegalArgumentException, match=r"handleInvalid.*error\|keep\|skip"):
            model.transform(df)
        # Plural path must also validate.
        model_p = OneHotEncoder(
            inputCols=["idx"],
            outputCols=["oh"],
            dropLast=True,
            handleInvalid="bogus",
        ).fit(df)
        with pytest.raises(IllegalArgumentException, match=r"handleInvalid"):
            model_p.transform(df)
    finally:
        spark.stop()


def test_one_hot_encoder_refuses_existing_output_col() -> None:
    """OHE transform must refuse pre-existing outputCols (octo C3-L-002).

    Mutation-proof: without _refuse_output_collision, SELECT view.*, expr AS oh
    silently overwrites an existing column name.
    """
    spark = _session()
    try:
        df = spark.createDataFrame([(0.0, 1.0), (1.0, 2.0)], ["idx", "oh"])
        model = OneHotEncoder(inputCol="idx", outputCol="oh", dropLast=True).fit(df)
        with pytest.raises(AnalysisException, match=r"already exists|outputCol"):
            model.transform(df)
        # Plural: collision on any of the outputCols.
        df2 = spark.createDataFrame([(0.0, 1.0, 9.0), (1.0, 0.0, 8.0)], ["a", "b", "b_oh"])
        model2 = OneHotEncoder(
            inputCols=["a", "b"],
            outputCols=["a_oh", "b_oh"],
            dropLast=True,
        ).fit(df2)
        with pytest.raises(AnalysisException, match=r"already exists|outputCol"):
            model2.transform(df2)
    finally:
        spark.stop()


# XGBoostRegressor E2E + lib-direct parity (merge bar)


def test_xgboost_regressor_e2e_and_lib_parity() -> None:
    """XGBoostRegressor fit/transform green; preds match library on same matrix."""
    pytest.importorskip("xgboost")
    pytest.importorskip("numpy")
    import numpy as np
    import xgboost as xgb

    from repark.spark.ml.ext import XGBoostRegressor

    spark = _session()
    try:
        rows = [(float(x), float(x * x) * 0.1 + 0.5) for x in range(40)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        estimator = XGBoostRegressor(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            maxDepth=3,
            nEstimators=20,
            learningRate=0.2,
            seed=42,
        )
        model = estimator.fit(assembled)
        assert model.num_rows == 40
        assert model.num_features == 1
        assert model.booster is not None
        # C5-Q-002: shell is booster + scalars only; scan fit_params + list/Arrow.
        _assert_no_training_row_rehold(model, expected_num_rows=40)
        # Mutation-proof the pin itself: fit_params list hold and Arrow-like
        # non-denylist attr must go red.
        injected_rows = [[float(x)] for x, _ in rows]
        model.fit_params["_rows"] = injected_rows
        with pytest.raises(AssertionError, match=r"training-row re-hold"):
            _assert_no_training_row_rehold(model, expected_num_rows=40)
        del model.fit_params["_rows"]
        try:
            import pyarrow as pa

            # Non-denylist attr: content scan must catch Arrow table (not name-only).
            model.cached_batch = pa.table({"x": [float(x) for x, _ in rows]})
            with pytest.raises(AssertionError, match=r"training-row re-hold"):
                _assert_no_training_row_rehold(model, expected_num_rows=40)
        finally:
            if hasattr(model, "cached_batch"):
                delattr(model, "cached_batch")
        # Name denylist still covers explicit _hold attrs.
        model._hold = injected_rows
        with pytest.raises(AssertionError, match=r"forbidden training-row"):
            _assert_no_training_row_rehold(model, expected_num_rows=40)
        delattr(model, "_hold")
        _assert_no_training_row_rehold(model, expected_num_rows=40)

        out = model.transform(assembled).collect()
        repark_preds = [float(row.asDict()["prediction"]) for row in out]
        assert len(repark_preds) == 40

        # Lib-direct oracle on the same dense matrix + same hyperparams.
        matrix = np.asarray([[float(x)] for x, _ in rows], dtype=np.float64)
        labels = np.asarray([float(y) for _, y in rows], dtype=np.float64)
        direct = xgb.XGBRegressor(
            max_depth=3,
            n_estimators=20,
            learning_rate=0.2,
            random_state=42,
            objective="reg:squarederror",
            n_jobs=1,
            verbosity=0,
        )
        direct.fit(matrix, labels)
        lib_preds = [float(value) for value in direct.predict(matrix)]
        for left, right in zip(repark_preds, lib_preds, strict=True):
            assert _rel_close(left, right, tol=1e-6), (left, right)
    finally:
        spark.stop()


def test_xgboost_regressor_load_requires_params_parquet() -> None:
    """Missing/empty params.parquet must refuse — not zero num_features (octo M5 C2).

    MUTATION: optional params.parquet + ``if table.num_rows > 0`` → missing/0-row file
    loads with num_features=0 / empty fit_params, skipping transform width checks while
    still predicting from booster.raw (silent integrity loss).
    """
    pytest.importorskip("xgboost")
    from repark.spark.ml.ext import XGBoostRegressor, XGBoostRegressorModel

    spark = _session()
    try:
        rows = [(float(x), float(x) * 2.0 + 1.0) for x in range(12)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = XGBoostRegressor(
            featuresCol="features",
            labelCol="label",
            nEstimators=4,
            maxDepth=2,
            seed=3,
        ).fit(assembled)
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "xgb"
            model.save(str(target))
            params_path = target / "fitted" / "params.parquet"
            params_path.unlink()
            with pytest.raises(IllegalArgumentException, match=r"params\.parquet|missing"):
                XGBoostRegressorModel.load(str(target))
            # Rewrite then empty (0-row) params.parquet
            model.write().overwrite().save(str(target))
            import pyarrow as pa
            import pyarrow.parquet as pq

            pq.write_table(
                pa.table({"num_features": pa.array([], type=pa.int64())}),
                params_path,
            )
            with pytest.raises(IllegalArgumentException, match=r"params\.parquet|1 row|exactly"):
                XGBoostRegressorModel.load(str(target))
            model.write().overwrite().save(str(target))
            (target / "fitted" / "booster.raw").write_bytes(b"")
            with pytest.raises(IllegalArgumentException, match=r"empty booster|booster blob"):
                XGBoostRegressorModel.load(str(target))
    finally:
        spark.stop()


def test_xgboost_regressor_load_refuses_booster_blob_path_escape() -> None:
    """Hostile metadata ``booster_blob`` must not escape the model root (octo M5 C1).

    MUTATION: load joins ``target / blob_rel`` without ``..`` / absolute confinement →
    ``../evil/booster.raw`` and absolute paths load a real booster outside the model tree.
    """
    pytest.importorskip("xgboost")
    from repark.spark.ml.ext import XGBoostRegressor, XGBoostRegressorModel

    spark = _session()
    try:
        rows = [(float(x), float(x) * 2.0 + 1.0) for x in range(12)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = XGBoostRegressor(
            featuresCol="features",
            labelCol="label",
            nEstimators=4,
            maxDepth=2,
            seed=3,
        ).fit(assembled)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "xgb"
            model.save(str(target))
            outside = root / "evil" / "booster.raw"
            outside.parent.mkdir()
            outside.write_bytes((target / "fitted" / "booster.raw").read_bytes())
            (target / "fitted" / "booster.raw").unlink()
            meta_path = target / "metadata.json"
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            meta["booster_blob"] = "../evil/booster.raw"
            meta_path.write_text(json.dumps(meta), encoding="utf-8")
            with pytest.raises(
                IllegalArgumentException,
                match=r"booster_blob|\.\.|escape|relative|absolute",
            ):
                XGBoostRegressorModel.load(str(target))
            meta["booster_blob"] = str(outside.resolve())
            meta_path.write_text(json.dumps(meta), encoding="utf-8")
            with pytest.raises(
                IllegalArgumentException,
                match=r"booster_blob|absolute|relative|escape",
            ):
                XGBoostRegressorModel.load(str(target))
    finally:
        spark.stop()


def test_xgboost_regressor_booster_bytes_save_load_predict_parity() -> None:
    """M5 booster-bytes: save_raw + M1 envelope; load restores predict-parity (lib-direct)."""
    pytest.importorskip("xgboost")
    import numpy as np

    from repark.spark.ml.ext import XGBoostRegressor, XGBoostRegressorModel
    from repark.spark.ml.pipeline import REPARK_ML_FORMAT

    spark = _session()
    try:
        rows = [(float(x), float(x) * 2.0 + 0.5) for x in range(16)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = XGBoostRegressor(
            featuresCol="features",
            labelCol="label",
            nEstimators=8,
            maxDepth=3,
            seed=7,
            learningRate=0.2,
        ).fit(assembled)
        before = [
            float(row.prediction)
            for row in model.transform(assembled).select("prediction").collect()
        ]
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "xgb-booster-bytes"
            model.save(str(target))
            assert (target / "metadata.json").is_file()
            meta = json.loads((target / "metadata.json").read_text(encoding="utf-8"))
            assert meta["format"] == REPARK_ML_FORMAT
            assert meta["kind"] == "XGBoostRegressorModel"
            assert (target / "fitted" / "booster.raw").is_file()
            assert (target / "fitted" / "params.parquet").is_file()
            # Mutation-proof layout: non-empty booster bytes + confined blob path
            # (octo M5 C8: empty blob or num_features=0 soft-load must go red).
            booster_bytes = (target / "fitted" / "booster.raw").read_bytes()
            assert len(booster_bytes) > 0
            assert meta.get("booster_blob") == "fitted/booster.raw"
            assert ".." not in str(meta.get("booster_blob", ""))
            # No training feature/label rows in the tree (M1 hard test class).
            tree_text = " ".join(
                path.read_text(encoding="utf-8", errors="ignore")
                for path in target.rglob("*")
                if path.is_file() and path.suffix in {".json", ".txt", ".csv"}
            )
            assert "1.5" not in tree_text  # sample label values not dumped as text
            loaded = XGBoostRegressorModel.load(str(target))
            assert loaded.num_features == model.num_features == 1
            assert loaded.fit_params.get("n_estimators") == model.fit_params.get("n_estimators")
            after = [
                float(row.prediction)
                for row in loaded.transform(assembled).select("prediction").collect()
            ]
        assert len(before) == len(after) == 16
        for left, right in zip(before, after, strict=True):
            assert _rel_close(left, right, tol=1e-6), (left, right)
        # Library-direct: raw booster predict matches repark path on the same matrix.
        matrix = np.asarray([[float(x)] for x in range(16)], dtype=np.float64)
        lib_before = [float(value) for value in model.booster.predict(matrix)]
        lib_after = [float(value) for value in loaded.booster.predict(matrix)]
        for left, right in zip(lib_before, lib_after, strict=True):
            assert _rel_close(left, right, tol=1e-9), (left, right)
        with tempfile.TemporaryDirectory() as tmp2:
            target2 = Path(tmp2) / "xgb-ow"
            model.write().save(str(target2))
            model.write().overwrite().save(str(target2))
            loaded2 = XGBoostRegressorModel.load(str(target2))
            assert loaded2.num_features == model.num_features
    finally:
        spark.stop()


def test_pipeline_model_save_with_xgb_stage_stop_loud() -> None:
    """PipelineModel must not hollow-publish ext stages as empty fitted parquet (octo C1-Q-001)."""
    pytest.importorskip("xgboost")
    from repark.spark.ml.ext import XGBoostRegressor

    spark = _session()
    try:
        rows = [(float(x), float(x) * 2.0) for x in range(12)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembler = VectorAssembler(inputCols=["x"], outputCol="features")
        xgb = XGBoostRegressor(
            featuresCol="features",
            labelCol="label",
            nEstimators=5,
            maxDepth=2,
            seed=2,
        )
        pipeline_model = Pipeline(stages=[assembler, xgb]).fit(df)
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "pipe-xgb"
            with pytest.raises(
                UnsupportedOperationException,
                match=r"save not supported for ext estimators|cannot be persisted",
            ):
                pipeline_model.write().save(str(target))
            # Must not leave a valid-looking repark-ml tree (silent soft save).
            assert not target.exists() or not (target / "metadata.json").is_file()
    finally:
        spark.stop()


def test_ext_transform_temp_view_owned_and_dropped() -> None:
    """Success-path re-entry must own and GC-drop __repark_ml_ext_* views (octo C1-SAF-001)."""
    pytest.importorskip("xgboost")
    from repark.spark.ml.ext import XGBoostRegressor

    spark = _session()
    try:
        rows = [(float(x), float(x) + 1.0) for x in range(10)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = XGBoostRegressor(
            featuresCol="features",
            labelCol="label",
            nEstimators=5,
            maxDepth=2,
            seed=3,
        ).fit(assembled)

        # Instrument via session proxy (PyO3 methods are read-only on the native handle).
        events: list[tuple[str, str]] = []
        real_session = assembled._session

        class _SessionProxy:
            def register_ipc_stream_as_temp_view(self, view_name: str, ipc_bytes: bytes) -> None:
                events.append(("register", view_name))
                real_session.register_ipc_stream_as_temp_view(view_name, ipc_bytes)

            def drop_temp_view(self, view_name: str) -> object:
                events.append(("drop", view_name))
                return real_session.drop_temp_view(view_name)

            def __getattr__(self, name: str) -> object:
                return getattr(real_session, name)

        assembled._session = _SessionProxy()  # type: ignore[assignment]
        predicted = model.transform(assembled)
        registered = [name for action, name in events if action == "register"]
        assert registered, "transform must register an __repark_ml_ext_* MemTable"
        # R7-1: the registered name is the scratch view's HOME-qualified spelling.
        assert all(local_view_name(name).startswith("__repark_ml_ext_") for name in registered)
        rows_out = predicted.collect()
        assert len(rows_out) == 10
        # Success path must not eager-drop while the DF is live (plan still needs the view).
        dropped_while_live = {name for action, name in events if action == "drop"}
        assert not any(name in dropped_while_live for name in registered), events
        del predicted
        gc.collect()
        dropped = {name for action, name in events if action == "drop"}
        assert set(registered) <= dropped, (
            f"ext MemTables not GC-dropped: registered={registered} events={events}"
        )
    finally:
        spark.stop()


def test_sparse_feature_size_capped() -> None:
    """Sparse densify must refuse size > MAX_EXT_FEATURES (octo C1-SAF-002)."""
    pytest.importorskip("numpy")
    import pyarrow as pa

    from repark.spark.ml.ext._arrow_util import MAX_EXT_FEATURES, features_matrix_from_arrow

    huge = MAX_EXT_FEATURES + 1
    # Sparse struct cell with absurd size — must not allocate dense=[0.0]*huge.
    cell = {"size": huge, "indices": [0], "values": [1.0]}
    table = pa.table({"features": pa.array([cell])})
    with pytest.raises(IllegalArgumentException, match=r"exceeds hard limit|p≤"):
        features_matrix_from_arrow(table, "features")


def test_sparse_feature_nnz_capped() -> None:
    """Sparse densify must refuse nnz > MAX_EXT_FEATURES even when size is small (C3-SAF-001).

    Mutation-proof: size/width oracles stay green if only list(indices)/list(values)
    materialize is unbounded. Hostile shape size=1 + huge nnz must refuse before densify.
    """
    pytest.importorskip("numpy")
    import pyarrow as pa

    from repark.spark.ml.ext._arrow_util import MAX_EXT_FEATURES, features_matrix_from_arrow

    huge_nnz = MAX_EXT_FEATURES + 1
    # size within cap but nnz exceeds — must not list()-materialize then densify.
    cell = {
        "size": 1,
        "indices": [0] * huge_nnz,
        "values": [1.0] * huge_nnz,
    }
    table = pa.table({"features": pa.array([cell])})
    with pytest.raises(
        IllegalArgumentException,
        match=r"nnz=|exceeds hard limit|exceeds size|p≤",
    ):
        features_matrix_from_arrow(table, "features")


def test_dense_feature_width_capped() -> None:
    """Dense list width > MAX_EXT_FEATURES must refuse (octo C2-Q-002 / C1-SAF-002 dense path).

    Mutation-proof: deleting the dense ``len(values) > MAX_EXT_FEATURES`` check leaves
    sparse-only tests green — this pin goes red on that deletion.
    """
    pytest.importorskip("numpy")
    import pyarrow as pa

    from repark.spark.ml.ext._arrow_util import MAX_EXT_FEATURES, features_matrix_from_arrow

    huge = MAX_EXT_FEATURES + 1
    dense_row = [0.0] * huge
    dense_row[0] = 1.0
    table = pa.table({"features": pa.array([dense_row])})
    with pytest.raises(IllegalArgumentException, match=r"exceeds hard limit|p≤|width="):
        features_matrix_from_arrow(table, "features")


def test_xgboost_regressor_transform_wrong_width_refuses() -> None:
    """Fitted XGBoostRegressorModel must refuse wrong feature width (octo C3-Q-002).

    Mutation-proof: deleting the num_features check in ``_transform`` leaves the
    suite green — this oracle goes red on that deletion.
    """
    pytest.importorskip("xgboost")
    from repark.spark.ml.ext import XGBoostRegressor

    spark = _session()
    try:
        rows = [(float(x), float(x) * 2.0) for x in range(12)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = XGBoostRegressor(
            featuresCol="features",
            labelCol="label",
            nEstimators=5,
            maxDepth=2,
            seed=7,
        ).fit(assembled)
        assert model.num_features == 1
        # Transform frame with width 2 (x,y assembled) against model fitted on width 1.
        wide = spark.createDataFrame(
            [(float(x), float(x + 1), float(x)) for x in range(4)],
            ["x", "y", "label"],
        )
        wide_features = VectorAssembler(inputCols=["x", "y"], outputCol="features").transform(wide)
        with pytest.raises(
            IllegalArgumentException,
            match=r"feature width|num_features",
        ):
            model.transform(wide_features)
    finally:
        spark.stop()


def test_multiclass_f1_is_loud_not_accuracy() -> None:
    """Default/f1 metric must not silently return accuracy (octo C1-L-002)."""
    spark = _session()
    try:
        df = spark.createDataFrame(
            [(0.0, 0.0), (1.0, 1.0), (0.0, 1.0), (1.0, 0.0)],
            ["label", "prediction"],
        )
        # Default metricName is f1 (Spark shape) — must refuse, not return 0.5 accuracy.
        with pytest.raises(UnsupportedOperationException, match=r"metricName='f1'|F1"):
            MulticlassClassificationEvaluator().evaluate(df)
        with pytest.raises(UnsupportedOperationException, match=r"accuracy"):
            MulticlassClassificationEvaluator(metricName="f1").evaluate(df)
        assert "accuracy" in MULTICLASS_F1_SEED
        acc = MulticlassClassificationEvaluator(metricName="accuracy").evaluate(df)
        assert acc == pytest.approx(0.5)
    finally:
        spark.stop()


def test_xgboost_classifier_stretch() -> None:
    """Stretch: XGBoostClassifier binary labels + lib-direct prediction parity.

    Octo C6-Q-001: row-count / column-presence alone stays green under
    predict→zeros mutation; pin must equal library on the same matrix.
    Octo C7-Q-001: re-hold pin on classifier model (not XGBRegressor-only).
    """
    pytest.importorskip("xgboost")
    pytest.importorskip("numpy")
    import numpy as np
    import xgboost as xgb

    from repark.spark.ml.ext import XGBoostClassifier

    spark = _session()
    try:
        rows = [(float(x), 1.0 if x >= 5 else 0.0) for x in range(16)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        # Defaults match wrapper: learningRate=0.3, objective=binary:logistic.
        model = XGBoostClassifier(
            featuresCol="features",
            labelCol="label",
            nEstimators=10,
            maxDepth=2,
            seed=0,
        ).fit(assembled)
        _assert_no_training_row_rehold(model, expected_num_rows=16)
        out = model.transform(assembled).collect()
        repark_preds = [float(row.asDict()["prediction"]) for row in out]
        assert len(repark_preds) == 16

        matrix = np.asarray([[float(x)] for x, _ in rows], dtype=np.float64)
        labels = np.asarray([float(y) for _, y in rows], dtype=np.float64)
        direct = xgb.XGBClassifier(
            max_depth=2,
            n_estimators=10,
            learning_rate=0.3,
            random_state=0,
            objective="binary:logistic",
            n_jobs=1,
            verbosity=0,
        )
        direct.fit(matrix, labels)
        lib_preds = [float(value) for value in direct.predict(matrix)]
        for left, right in zip(repark_preds, lib_preds, strict=True):
            assert _rel_close(left, right, tol=1e-6), (left, right)
    finally:
        spark.stop()


def test_lightgbm_regressor_stretch() -> None:
    """Stretch: LightGBMRegressor E2E + lib-direct prediction parity (C6-Q-001)."""
    pytest.importorskip("lightgbm")
    pytest.importorskip("numpy")
    import lightgbm as lgb
    import numpy as np

    from repark.spark.ml.ext import LightGBMRegressor

    spark = _session()
    try:
        rows = [(float(x), 2.0 * float(x) + 1.0) for x in range(20)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        # Defaults match wrapper: learningRate=0.1, verbosity=-1, n_jobs=1.
        model = LightGBMRegressor(
            featuresCol="features",
            labelCol="label",
            nEstimators=15,
            maxDepth=3,
            seed=3,
        ).fit(assembled)
        # C7-Q-001: re-hold not only on XGBoostRegressorModel.
        _assert_no_training_row_rehold(model, expected_num_rows=20)
        out = model.transform(assembled).collect()
        repark_preds = [float(row.asDict()["prediction"]) for row in out]
        assert len(repark_preds) == 20

        matrix = np.asarray([[float(x)] for x, _ in rows], dtype=np.float64)
        labels = np.asarray([float(y) for _, y in rows], dtype=np.float64)
        direct = lgb.LGBMRegressor(
            max_depth=3,
            n_estimators=15,
            learning_rate=0.1,
            random_state=3,
            verbosity=-1,
            n_jobs=1,
        )
        direct.fit(matrix, labels)
        lib_preds = [float(value) for value in direct.predict(matrix)]
        for left, right in zip(repark_preds, lib_preds, strict=True):
            assert _rel_close(left, right, tol=1e-6), (left, right)
    finally:
        spark.stop()


def test_sklearn_random_forest_regressor_stretch() -> None:
    """Stretch: sklearn RandomForestRegressor + lib-direct prediction parity (C6-Q-001)."""
    pytest.importorskip("sklearn")
    pytest.importorskip("numpy")
    import numpy as np
    from sklearn.ensemble import RandomForestRegressor as SKRandomForestRegressor

    from repark.spark.ml.ext import RandomForestRegressor

    spark = _session()
    try:
        rows = [(float(x), 3.0 * float(x) - 1.0) for x in range(20)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = RandomForestRegressor(
            featuresCol="features",
            labelCol="label",
            numTrees=10,
            maxDepth=3,
            seed=5,
        ).fit(assembled)
        # C7-Q-001: re-hold not only on XGBoostRegressorModel.
        _assert_no_training_row_rehold(model, expected_num_rows=20)
        out = model.transform(assembled).collect()
        repark_preds = [float(row.asDict()["prediction"]) for row in out]
        assert len(repark_preds) == 20

        matrix = np.asarray([[float(x)] for x, _ in rows], dtype=np.float64)
        labels = np.asarray([float(y) for _, y in rows], dtype=np.float64)
        direct = SKRandomForestRegressor(
            n_estimators=10,
            max_depth=3,
            random_state=5,
            n_jobs=1,
        )
        direct.fit(matrix, labels)
        lib_preds = [float(value) for value in direct.predict(matrix)]
        for left, right in zip(repark_preds, lib_preds, strict=True):
            assert _rel_close(left, right, tol=1e-6), (left, right)
    finally:
        spark.stop()


def test_lightgbm_classifier_stretch() -> None:
    """Stretch: LightGBMClassifier transform + lib-direct parity (octo C7-Q-002).

    Mutation-proof: save/write-only classifier pins stay green if ``_transform``
    returns zeros; pin equals library on the same matrix + hyperparams.
    """
    pytest.importorskip("lightgbm")
    pytest.importorskip("numpy")
    import lightgbm as lgb
    import numpy as np

    from repark.spark.ml.ext import LightGBMClassifier

    spark = _session()
    try:
        rows = [(float(x), 1.0 if x >= 5 else 0.0) for x in range(16)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        # Defaults match wrapper: learningRate=0.1, verbosity=-1, n_jobs=1.
        model = LightGBMClassifier(
            featuresCol="features",
            labelCol="label",
            nEstimators=10,
            maxDepth=2,
            seed=0,
        ).fit(assembled)
        _assert_no_training_row_rehold(model, expected_num_rows=16)
        out = model.transform(assembled).collect()
        repark_preds = [float(row.asDict()["prediction"]) for row in out]
        assert len(repark_preds) == 16

        matrix = np.asarray([[float(x)] for x, _ in rows], dtype=np.float64)
        labels = np.asarray([float(y) for _, y in rows], dtype=np.float64)
        direct = lgb.LGBMClassifier(
            max_depth=2,
            n_estimators=10,
            learning_rate=0.1,
            random_state=0,
            verbosity=-1,
            n_jobs=1,
        )
        direct.fit(matrix, labels)
        lib_preds = [float(value) for value in direct.predict(matrix)]
        for left, right in zip(repark_preds, lib_preds, strict=True):
            assert _rel_close(left, right, tol=1e-6), (left, right)
    finally:
        spark.stop()


def test_sklearn_random_forest_classifier_stretch() -> None:
    """Stretch: sklearn RandomForestClassifier lib-direct parity (octo C7-Q-002).

    Mutation-proof: save/write-only pins stay green under zeros-predict transform.
    """
    pytest.importorskip("sklearn")
    pytest.importorskip("numpy")
    import numpy as np
    from sklearn.ensemble import RandomForestClassifier as SKRandomForestClassifier

    from repark.spark.ml.ext import RandomForestClassifier

    spark = _session()
    try:
        rows = [(float(x), 1.0 if x >= 5 else 0.0) for x in range(16)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            numTrees=10,
            maxDepth=3,
            seed=5,
        ).fit(assembled)
        _assert_no_training_row_rehold(model, expected_num_rows=16)
        out = model.transform(assembled).collect()
        repark_preds = [float(row.asDict()["prediction"]) for row in out]
        assert len(repark_preds) == 16

        matrix = np.asarray([[float(x)] for x, _ in rows], dtype=np.float64)
        labels = np.asarray([float(y) for _, y in rows], dtype=np.float64)
        direct = SKRandomForestClassifier(
            n_estimators=10,
            max_depth=3,
            random_state=5,
            n_jobs=1,
        )
        direct.fit(matrix, labels)
        lib_preds = [float(value) for value in direct.predict(matrix)]
        for left, right in zip(repark_preds, lib_preds, strict=True):
            assert _rel_close(left, right, tol=1e-6), (left, right)
    finally:
        spark.stop()


# Grep-style gates: numpy not at repark.ml top-level; no crates numpy


def test_numpy_not_imported_at_repark_ml_toplevel() -> None:
    """numpy must not appear as a top-level import of repark.ml (only behind ext)."""
    ml_init = (
        Path(__file__).resolve().parents[1] / "src" / "repark" / "spark" / "ml" / "__init__.py"
    )
    text = ml_init.read_text(encoding="utf-8")
    assert "import numpy" not in text
    assert "from numpy" not in text
    # ext package may lazy-load; top-level repark.ml.regression etc. must stay clean.
    for relative in (
        "regression.py",
        "classification.py",
        "clustering.py",
        "evaluation.py",
        "base.py",
        "pipeline.py",
        "tuning.py",
    ):
        path = ml_init.parent / relative
        body = path.read_text(encoding="utf-8")
        assert not re.search(r"^\s*(import numpy|from numpy)\b", body, re.M), relative


def test_cv_over_small_xgb_grid() -> None:
    """CV over a tiny XGB grid also satisfies merge bar (when xgboost present)."""
    pytest.importorskip("xgboost")
    from repark.spark.ml.ext import XGBoostRegressor

    spark = _session()
    try:
        rows = [(float(x), 1.5 * float(x) + 0.2) for x in range(30)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        xgb_est = XGBoostRegressor(
            featuresCol="features",
            labelCol="label",
            nEstimators=8,
            maxDepth=2,
            seed=9,
        )
        grid = ParamGridBuilder().addGrid(xgb_est.learningRate, [0.2, 0.4]).build()
        evaluator = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="rmse"
        )
        cv = CrossValidator(
            estimator=xgb_est,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=2,
            seed=1,
        )
        model = cv.fit(assembled)
        assert model.bestModel is not None
        assert len(model.avgMetrics) == 2
        # bestModel learning rate must match argmin avgMetrics (not hardcoded index 0).
        best_index = min(range(len(model.avgMetrics)), key=lambda index: model.avgMetrics[index])
        best_map = model.estimatorParamMaps[best_index]
        rates = [
            value
            for param, value in best_map.items()
            if getattr(param, "name", None) == "learningRate"
        ]
        assert len(rates) == 1
        # Fitted shell stores hyperparams in fit_params (no learningRate Param on the model).
        assert float(model.bestModel.fit_params["learning_rate"]) == float(rates[0])
        preds = model.transform(assembled).collect()
        assert len(preds) == 30
    finally:
        spark.stop()


# Octo cycle-2 mutation-proof pins (S1)


def test_cross_validator_materializes_fold_labels() -> None:
    """CV must materialize fold labels *and use* the mat_view (octo C2-Q-001 / C4-Q-001).

    Mutation-proof: call-only oracles stay green when materialize is called then
    ignored; SQL after materialize must read ``__repark_cv_mat_*``.
    """
    spark = _session()
    try:
        rows = [(float(x), 1.0 + 2.0 * float(x)) for x in range(18)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        lr = LinearRegression(featuresCol="features", labelCol="label", predictionCol="prediction")
        grid = ParamGridBuilder().addGrid(lr.fitIntercept, [True, False]).build()
        evaluator = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="rmse"
        )
        cv = CrossValidator(
            estimator=lr,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=3,
            seed=11,
        )

        materialize_calls: list[str] = []
        sql_queries: list[str] = []
        real_session = assembled._session

        class _SessionProxy:
            def materialize_as_temp_view(self, view_name: str, plan: object) -> object:
                materialize_calls.append(str(view_name))
                return real_session.materialize_as_temp_view(view_name, plan)

            def sql(self, query: str) -> object:
                sql_queries.append(str(query))
                return real_session.sql(query)

            def __getattr__(self, name: str) -> object:
                return getattr(real_session, name)

        assembled._session = _SessionProxy()  # type: ignore[assignment]
        model = cv.fit(assembled)
        assert model.bestModel is not None
        assert materialize_calls, (
            "CrossValidator.fit must materialize fold labels via materialize_as_temp_view "
            "(octo C1-L-001 / C2-Q-001); calls empty → materialization removed"
        )
        mat_names = [
            name
            for name in materialize_calls
            if local_view_name(name).startswith("__repark_cv_mat_")  # R7-1 home spelling
        ]
        assert mat_names, materialize_calls
        used = any(any(mat_name in query for mat_name in mat_names) for query in sql_queries)
        assert used, (
            "materialize_as_temp_view was called but no SQL read a __repark_cv_mat_* view "
            f"(hollow pin: mat_view ignored). mat={mat_names!r} sql={sql_queries!r}"
        )
    finally:
        spark.stop()


def test_regression_evaluator_r2_is_larger_better_case_insensitive() -> None:
    """metricName='R2' must be larger-better (octo C2-L-001).

    evaluate() lowercases; isLargerBetter must too — else CV takes min of R2 (worst model).
    """
    evaluator_lower = RegressionEvaluator(metricName="r2")
    evaluator_upper = RegressionEvaluator(metricName="R2")
    evaluator_mixed = RegressionEvaluator(metricName="R2")
    assert evaluator_lower.isLargerBetter() is True
    assert evaluator_upper.isLargerBetter() is True
    assert evaluator_mixed.isLargerBetter() is True
    assert RegressionEvaluator(metricName="rmse").isLargerBetter() is False
    assert RegressionEvaluator(metricName="RMSE").isLargerBetter() is False

    # Composition pin: larger-better path must select max avgMetrics for R2.
    spark = _session()
    try:
        # Strong linear signal so R2 is well-defined and intercept True wins.
        rows = [(float(x), 3.0 + 1.5 * float(x) + 0.02 * (x % 2)) for x in range(24)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        lr = LinearRegression(featuresCol="features", labelCol="label", predictionCol="prediction")
        grid = ParamGridBuilder().addGrid(lr.fitIntercept, [True, False]).build()
        evaluator = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="R2"
        )
        assert evaluator.isLargerBetter() is True
        cv = CrossValidator(
            estimator=lr,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=3,
            seed=5,
        )
        model = cv.fit(assembled)
        best_index = max(range(len(model.avgMetrics)), key=lambda index: model.avgMetrics[index])
        assert model.avgMetrics[best_index] == max(model.avgMetrics)
        best_map = model.estimatorParamMaps[best_index]
        fit_intercept = None
        for param, value in best_map.items():
            if getattr(param, "name", None) == "fitIntercept":
                fit_intercept = value
        assert fit_intercept is True, (
            f"R2 larger-better must pick fitIntercept=True; avgMetrics={model.avgMetrics!r}"
        )
        assert bool(model.bestModel.fit_intercept) is True
    finally:
        spark.stop()


def test_cross_validator_refuses_nan_fold_metrics() -> None:
    """NaN fold scores must not poison bestModel to param_maps[0] (octo C2-L-002)."""
    spark = _session()
    try:
        # Constant labels → R2 SS_tot=0 → nullif → NaN on every fold.
        rows = [(float(x), 1.0) for x in range(12)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        lr = LinearRegression(featuresCol="features", labelCol="label", predictionCol="prediction")
        grid = ParamGridBuilder().addGrid(lr.fitIntercept, [True, False]).build()
        evaluator = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="r2"
        )
        cv = CrossValidator(
            estimator=lr,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=2,
            seed=3,
        )
        with pytest.raises(IllegalArgumentException, match=r"NaN|zero-variance|fold evaluation"):
            cv.fit(assembled)
    finally:
        spark.stop()


def test_pipeline_load_refuses_path_traversal() -> None:
    """relative_path with .. must not escape model root (octo C2-SEC-001)."""
    import json
    import shutil

    from repark.spark.ml.pipeline import REPARK_ML_FORMAT, REPARK_ML_VERSION, PipelineModel

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        model_dir = root / "model"
        model_dir.mkdir()
        outside = root / "outside_secret.txt"
        outside.write_text("SECRET_PAYLOAD_SHOULD_NOT_READ\n", encoding="utf-8")
        metadata = {
            "format": REPARK_ML_FORMAT,
            "version": REPARK_ML_VERSION,
            "kind": "PipelineModel",
            "uid": "hostile_pipe",
            "stages": [
                {
                    "index": 0,
                    "uid": "evil",
                    "class": "repark.spark.ml.pipeline._ConstantColumnModel",
                    "relative_path": "../",
                }
            ],
        }
        (model_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
        )
        with pytest.raises(IllegalArgumentException, match=r"relative_path|\.\.|escape|unsafe"):
            PipelineModel.load(str(model_dir))

        metadata["stages"][0]["relative_path"] = "stages/../../outside_secret.txt"
        (model_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
        )
        with pytest.raises(IllegalArgumentException, match=r"relative_path|\.\.|escape|unsafe"):
            PipelineModel.load(str(model_dir))

        # Save path: hostile uid with separators must not write outside stages/.
        from repark.spark.ml.pipeline import Pipeline, _ConstantColumnEstimator

        spark = _session()
        try:
            df = spark.createDataFrame([(1,), (2,)], ["id"])
            estimator = _ConstantColumnEstimator(output_col="c", value=1.0)
            estimator.uid = "../escape_uid"  # type: ignore[assignment]
            model = Pipeline(stages=[estimator]).fit(df)
            save_path = root / "safe_save"
            model.write().overwrite().save(str(save_path))
            # All stage dirs must remain under save_path/stages/.
            for path in save_path.rglob("*"):
                if path.is_file():
                    path.resolve().relative_to(save_path.resolve())
            stage_dirs = list((save_path / "stages").iterdir())
            assert stage_dirs, "expected sanitized stage directory under stages/"
            assert all(".." not in p.name for p in stage_dirs)
            # No sibling escape dir next to save_path from uid traversal.
            assert not (root / "escape_uid").exists()
        finally:
            spark.stop()
            shutil.rmtree(root / "safe_save", ignore_errors=True)


def test_pipeline_instantiate_stage_allowlists_repark_ml() -> None:
    """importlib class_path must be under repark.ml (octo C2-SEC-002)."""
    from repark.spark.ml.pipeline import _assert_allowed_stage_class_path, _instantiate_stage

    with pytest.raises(UnsupportedOperationException, match=r"allowlist|repark\.ml"):
        _assert_allowed_stage_class_path("os.system")
    with pytest.raises(UnsupportedOperationException, match=r"allowlist|repark\.ml|denied"):
        _assert_allowed_stage_class_path("repark.spark.ml.ext._xgboost.XGBoostRegressorModel")
    with pytest.raises(UnsupportedOperationException, match=r"allowlist|repark\.ml"):
        _instantiate_stage(
            "json.loads",
            {},
            fitted=True,
            fitted_state={},
        )
    # Legitimate repark.ml path is allowed (import may still fail on a missing
    # _ml_from_save — that is a different refuse).
    module_name, class_name = _assert_allowed_stage_class_path(
        "repark.spark.ml.regression.LinearRegressionModel"
    )
    assert module_name == "repark.spark.ml.regression"
    assert class_name == "LinearRegressionModel"


def test_stretch_ext_write_stop_loud() -> None:
    """M8: LightGBM regressor save/load works; sklearn still pin-refuses (pickle).

    sklearn refuses with the exact pickle-forbidden reason (octo C2-L-003 residual).
    """
    import importlib.util

    spark = _session()
    try:
        rows = [(float(x), float(x) + 1.0) for x in range(12)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)

        if importlib.util.find_spec("lightgbm") is not None:
            from repark.spark.ml.ext import LightGBMRegressor, LightGBMRegressorModel

            lgb_model = LightGBMRegressor(
                featuresCol="features",
                labelCol="label",
                nEstimators=5,
                maxDepth=2,
                seed=1,
            ).fit(assembled)
            before = [
                float(row.prediction)
                for row in lgb_model.transform(assembled).select("prediction").collect()
            ]
            with tempfile.TemporaryDirectory() as tmp:
                target = Path(tmp) / "lgb-reg"
                lgb_model.save(str(target))
                loaded = LightGBMRegressorModel.load(str(target))
                after = [
                    float(row.prediction)
                    for row in loaded.transform(assembled).select("prediction").collect()
                ]
            assert len(before) == len(after)
            for left, right in zip(before, after, strict=True):
                assert _rel_close(left, right, tol=1e-6), (left, right)

        if importlib.util.find_spec("sklearn") is not None:
            from repark.spark.ml.ext import PICKLE_FORBIDDEN_REASON, RandomForestRegressor

            rf = RandomForestRegressor(
                featuresCol="features",
                labelCol="label",
                numTrees=5,
                maxDepth=2,
                seed=1,
            ).fit(assembled)
            with tempfile.TemporaryDirectory() as tmp:
                target = str(Path(tmp) / "rf-should-not-write")
                with pytest.raises(
                    UnsupportedOperationException,
                    match=re.escape(PICKLE_FORBIDDEN_REASON),
                ):
                    rf.save(target)
                with pytest.raises(
                    UnsupportedOperationException,
                    match=re.escape(PICKLE_FORBIDDEN_REASON),
                ):
                    rf.write()
                assert not Path(target).exists()
        elif importlib.util.find_spec("lightgbm") is None:
            pytest.skip("neither lightgbm nor sklearn installed")
    finally:
        spark.stop()


# Octo cycle-4 mutation-proof pins (S1) — M8 update for classifier save matrix


def test_classifier_models_save_write_stop_loud() -> None:
    """M8 matrix: XGB/LGBM classifiers save/load; sklearn RF classifier pin-refuses.

    Every fitted classifier model is either round-trip-pinned or refuse-pinned
    (no silent third state).
    """
    import importlib.util

    spark = _session()
    try:
        rows = [(float(x), 1.0 if x >= 6 else 0.0) for x in range(14)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        saw_any = False

        if importlib.util.find_spec("xgboost") is not None:
            saw_any = True
            from repark.spark.ml.ext import XGBoostClassifier, XGBoostClassifierModel

            model = XGBoostClassifier(
                featuresCol="features",
                labelCol="label",
                nEstimators=5,
                maxDepth=2,
                seed=1,
            ).fit(assembled)
            before = [
                float(row.prediction)
                for row in model.transform(assembled).select("prediction").collect()
            ]
            with tempfile.TemporaryDirectory() as tmp:
                target = Path(tmp) / "xgb-clf"
                model.save(str(target))
                loaded = XGBoostClassifierModel.load(str(target))
                after = [
                    float(row.prediction)
                    for row in loaded.transform(assembled).select("prediction").collect()
                ]
            assert before == after

        if importlib.util.find_spec("lightgbm") is not None:
            saw_any = True
            from repark.spark.ml.ext import LightGBMClassifier, LightGBMClassifierModel

            model = LightGBMClassifier(
                featuresCol="features",
                labelCol="label",
                nEstimators=5,
                maxDepth=2,
                seed=1,
            ).fit(assembled)
            before = [
                float(row.prediction)
                for row in model.transform(assembled).select("prediction").collect()
            ]
            with tempfile.TemporaryDirectory() as tmp:
                target = Path(tmp) / "lgb-clf"
                model.save(str(target))
                loaded = LightGBMClassifierModel.load(str(target))
                after = [
                    float(row.prediction)
                    for row in loaded.transform(assembled).select("prediction").collect()
                ]
            assert before == after

        if importlib.util.find_spec("sklearn") is not None:
            saw_any = True
            from repark.spark.ml.ext import PICKLE_FORBIDDEN_REASON, RandomForestClassifier

            model = RandomForestClassifier(
                featuresCol="features",
                labelCol="label",
                numTrees=5,
                maxDepth=2,
                seed=1,
            ).fit(assembled)
            with tempfile.TemporaryDirectory() as tmp:
                target = str(Path(tmp) / "rf-clf-should-not-write")
                with pytest.raises(
                    UnsupportedOperationException,
                    match=re.escape(PICKLE_FORBIDDEN_REASON),
                ):
                    model.save(target)
                with pytest.raises(
                    UnsupportedOperationException,
                    match=re.escape(PICKLE_FORBIDDEN_REASON),
                ):
                    model.write()
                assert not Path(target).exists()

        if not saw_any:
            pytest.skip("no classifier extras installed (xgboost/lightgbm/sklearn)")
    finally:
        spark.stop()


def test_features_matrix_caps_before_as_py() -> None:
    """Null probe + dense width must refuse without as_py materialize (octo C4-SAF-001).

    Mutation-proof: an as_py-first implementation reintroduces hostile materialize
    before MAX_EXT_FEATURES; this pin raises if as_py runs first.
    """
    pytest.importorskip("numpy")
    from repark.spark.ml.ext._arrow_util import MAX_EXT_FEATURES, features_matrix_from_arrow

    class _NullCell:
        is_valid = False

        def as_py(self) -> None:
            raise AssertionError("null probe must use is_valid, not as_py (C4-SAF-001)")

    class _HugeListCell:
        is_valid = True

        def __len__(self) -> int:
            return MAX_EXT_FEATURES + 1

        def as_py(self) -> list[float]:
            raise AssertionError("dense width must cap via len before as_py (C4-SAF-001)")

    class _Column:
        def __init__(self, cell: object) -> None:
            self._cell = cell

        def __len__(self) -> int:
            return 1

        def __getitem__(self, index: int) -> object:
            return self._cell

    class _Table:
        def __init__(self, cell: object) -> None:
            self.column_names = ["features"]
            self._cell = cell

        def column(self, name: str) -> _Column:
            assert name == "features"
            return _Column(self._cell)

    with pytest.raises(IllegalArgumentException, match=r"null"):
        features_matrix_from_arrow(_Table(_NullCell()), "features")

    with pytest.raises(IllegalArgumentException, match=r"exceeds hard limit|width="):
        features_matrix_from_arrow(_Table(_HugeListCell()), "features")


def test_one_hot_encoder_keep_drop_last_false_extra_invalid_bucket() -> None:
    """handleInvalid=keep + dropLast=False → size=category_size+1 (octo C4-L-001).

    Spark reserves an invalid category *before* dropLast. Mutation: size=category_size
    and empty-for-invalid leaves plural/singular dropLast=True oracles green.
    """
    spark = _session()
    try:
        train = spark.createDataFrame([(0.0,), (1.0,)], ["idx"])
        model = OneHotEncoder(
            inputCol="idx",
            outputCol="oh",
            dropLast=False,
            handleInvalid="keep",
        ).fit(train)
        assert model.category_size == 2
        held = spark.createDataFrame([(0.0,), (1.0,), (2.0,), (None,)], ["idx"])
        rows = model.transform(held).collect()
        encoded = [row.asDict()["oh"] for row in rows]
        # Valid indices keep their one-hot; invalid/null at last bucket index 2.
        assert encoded[0]["size"] == 3
        assert list(encoded[0]["indices"]) == [0]
        assert encoded[1]["size"] == 3
        assert list(encoded[1]["indices"]) == [1]
        assert encoded[2]["size"] == 3
        assert list(encoded[2]["indices"]) == [2]
        assert encoded[3]["size"] == 3
        assert list(encoded[3]["indices"]) == [2]
        # keep + dropLast=True: expanded=3, size=2; invalid bucket dropped → empty.
        model_drop = OneHotEncoder(
            inputCol="idx",
            outputCol="oh",
            dropLast=True,
            handleInvalid="keep",
        ).fit(train)
        drop_rows = model_drop.transform(held).collect()
        drop_enc = [row.asDict()["oh"] for row in drop_rows]
        assert drop_enc[0]["size"] == 2
        assert list(drop_enc[0]["indices"]) == [0]
        assert drop_enc[1]["size"] == 2
        assert list(drop_enc[1]["indices"]) == [1]
        assert list(drop_enc[2]["indices"]) == []
        assert list(drop_enc[3]["indices"]) == []
    finally:
        spark.stop()


def test_model_copy_extra_applies_prediction_col_override() -> None:
    """Model.copy(extra) must apply Param overrides (octo C4-L-002).

    Mutation-proof: discarding extra leaves transform(df, {predictionCol: pred_out})
    still writing ``prediction``; pin LR, Logit, XGB regressor, and CV model.
    """
    import importlib.util

    from repark.spark.ml.classification import LogisticRegression
    from repark.spark.ml.regression import LinearRegression

    spark = _session()
    try:
        rows = [(float(x), 1.0 + 2.0 * float(x)) for x in range(16)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)

        lr_model = LinearRegression(
            featuresCol="features", labelCol="label", predictionCol="prediction"
        ).fit(assembled)
        out = lr_model.transform(assembled, {lr_model.predictionCol: "pred_out"}).collect()
        assert all("pred_out" in row.asDict() for row in out)
        assert all("prediction" not in row.asDict() for row in out)

        class_rows = [(float(x), 1.0 if x >= 8 else 0.0) for x in range(16)]
        cdf = spark.createDataFrame(class_rows, ["x", "label"])
        cassembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(cdf)
        logit = LogisticRegression(
            featuresCol="features", labelCol="label", predictionCol="prediction"
        ).fit(cassembled)
        lout = logit.transform(cassembled, {logit.predictionCol: "pred_out"}).collect()
        assert all("pred_out" in row.asDict() for row in lout)
        assert all("prediction" not in row.asDict() for row in lout)

        # CrossValidatorModel must forward extra onto bestModel.
        lr_est = LinearRegression(
            featuresCol="features", labelCol="label", predictionCol="prediction"
        )
        grid = ParamGridBuilder().addGrid(lr_est.fitIntercept, [True, False]).build()
        evaluator = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="rmse"
        )
        cv_model = CrossValidator(
            estimator=lr_est,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=2,
            seed=3,
        ).fit(assembled)
        assert cv_model.bestModel is not None
        best_pred = cv_model.bestModel.predictionCol
        cv_out = cv_model.transform(assembled, {best_pred: "pred_out"}).collect()
        assert all("pred_out" in row.asDict() for row in cv_out)
        assert all("prediction" not in row.asDict() for row in cv_out)

        if importlib.util.find_spec("xgboost") is not None:
            from repark.spark.ml.ext import XGBoostRegressor

            xgb_model = XGBoostRegressor(
                featuresCol="features",
                labelCol="label",
                predictionCol="prediction",
                nEstimators=5,
                maxDepth=2,
                seed=2,
            ).fit(assembled)
            xout = xgb_model.transform(assembled, {xgb_model.predictionCol: "pred_out"}).collect()
            assert all("pred_out" in row.asDict() for row in xout)
            assert all("prediction" not in row.asDict() for row in xout)
    finally:
        spark.stop()


# M8 — ext estimator save/load matrix (booster-bytes / pin-refuse)


def test_m8_xgboost_regressor_atomic_overwrite_and_version_guard() -> None:
    """M8: atomic overwrite (no rmtree-before-write) + library major version guard."""
    pytest.importorskip("xgboost")
    import xgboost as xgb

    from repark.spark.ml.ext import XGBoostRegressor, XGBoostRegressorModel
    from repark.spark.ml.pipeline import REPARK_ML_FORMAT

    spark = _session()
    try:
        rows = [(float(x), float(x) * 2.0 + 0.25) for x in range(14)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = XGBoostRegressor(
            featuresCol="features",
            labelCol="label",
            nEstimators=6,
            maxDepth=2,
            seed=11,
        ).fit(assembled)
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "xgb-atomic"
            model.save(str(target))
            meta = json.loads((target / "metadata.json").read_text(encoding="utf-8"))
            assert meta["format"] == REPARK_ML_FORMAT
            assert meta["library"] == "xgboost"
            assert meta["library_version"] == xgb.__version__
            # Atomic overwrite must leave a valid tree (M7 pattern).
            marker = target / "fitted" / "booster.raw"
            assert marker.is_file()
            old_bytes = marker.read_bytes()
            model.write().overwrite().save(str(target))
            assert (target / "metadata.json").is_file()
            assert (target / "fitted" / "booster.raw").is_file()
            assert len((target / "fitted" / "booster.raw").read_bytes()) > 0
            # Staging siblings must not leak after successful commit.
            siblings = list(target.parent.glob(f".{target.name}.repark-ml-*"))
            assert siblings == [], siblings
            loaded = XGBoostRegressorModel.load(str(target))
            assert loaded.num_features == model.num_features
            # Hostile major version mismatch refuses loud.
            meta["library_version"] = "99.0.0"
            (target / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
            with pytest.raises(
                IllegalArgumentException,
                match=r"major version mismatch|xgboost",
            ):
                XGBoostRegressorModel.load(str(target))
            # Same-major metadata still loads after restore.
            meta["library_version"] = xgb.__version__
            (target / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
            assert len(old_bytes) > 0
            again = XGBoostRegressorModel.load(str(target))
            assert again.num_features == model.num_features
    finally:
        spark.stop()


def test_m8_xgboost_classifier_booster_bytes_predict_parity() -> None:
    """M8: XGBoostClassifierModel save_raw round-trip equals pre-save predict."""
    pytest.importorskip("xgboost")
    from repark.spark.ml.ext import XGBoostClassifier, XGBoostClassifierModel

    spark = _session()
    try:
        rows = [(float(x), 1.0 if x % 2 == 0 else 0.0) for x in range(20)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = XGBoostClassifier(
            featuresCol="features",
            labelCol="label",
            nEstimators=8,
            maxDepth=3,
            seed=5,
        ).fit(assembled)
        before = [
            float(row.prediction)
            for row in model.transform(assembled).select("prediction").collect()
        ]
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "xgb-clf-m8"
            model.save(str(target))
            meta = json.loads((target / "metadata.json").read_text(encoding="utf-8"))
            assert meta["kind"] == "XGBoostClassifierModel"
            assert (target / "fitted" / "booster.raw").is_file()
            loaded = XGBoostClassifierModel.load(str(target))
            after = [
                float(row.prediction)
                for row in loaded.transform(assembled).select("prediction").collect()
            ]
        assert before == after
    finally:
        spark.stop()


def test_m8_lightgbm_regressor_and_classifier_predict_parity() -> None:
    """M8: LightGBM model_to_string save→load→predict equality (reg + clf)."""
    pytest.importorskip("lightgbm")
    from repark.spark.ml.ext import (
        LightGBMClassifier,
        LightGBMClassifierModel,
        LightGBMRegressor,
        LightGBMRegressorModel,
    )

    spark = _session()
    try:
        reg_rows = [(float(x), float(x) * 1.5 + 0.1) for x in range(16)]
        reg_df = spark.createDataFrame(reg_rows, ["x", "label"])
        reg_assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(reg_df)
        reg = LightGBMRegressor(
            featuresCol="features",
            labelCol="label",
            nEstimators=8,
            maxDepth=3,
            seed=4,
        ).fit(reg_assembled)
        reg_before = [
            float(row.prediction)
            for row in reg.transform(reg_assembled).select("prediction").collect()
        ]
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "lgb-reg-m8"
            reg.save(str(target))
            meta = json.loads((target / "metadata.json").read_text(encoding="utf-8"))
            assert meta["kind"] == "LightGBMRegressorModel"
            assert meta["library"] == "lightgbm"
            assert (target / "fitted" / "booster.txt").is_file()
            # Text blob must not be pickle protocol.
            blob = (target / "fitted" / "booster.txt").read_bytes()
            assert not blob.startswith(b"\x80")  # pickle protocol magic
            assert b"tree" in blob.lower() or b"Tree" in blob or len(blob) > 100
            loaded_reg = LightGBMRegressorModel.load(str(target))
            reg_after = [
                float(row.prediction)
                for row in loaded_reg.transform(reg_assembled).select("prediction").collect()
            ]
        for left, right in zip(reg_before, reg_after, strict=True):
            assert _rel_close(left, right, tol=1e-6), (left, right)

        clf_rows = [(float(x), 1.0 if x >= 8 else 0.0) for x in range(20)]
        clf_df = spark.createDataFrame(clf_rows, ["x", "label"])
        clf_assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(clf_df)
        clf = LightGBMClassifier(
            featuresCol="features",
            labelCol="label",
            nEstimators=8,
            maxDepth=3,
            seed=4,
        ).fit(clf_assembled)
        clf_before = [
            float(row.prediction)
            for row in clf.transform(clf_assembled).select("prediction").collect()
        ]
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "lgb-clf-m8"
            clf.save(str(target))
            loaded_clf = LightGBMClassifierModel.load(str(target))
            clf_after = [
                float(row.prediction)
                for row in loaded_clf.transform(clf_assembled).select("prediction").collect()
            ]
        assert clf_before == clf_after
    finally:
        spark.stop()


def test_m8_sklearn_random_forest_pickle_forbidden_pin() -> None:
    """M8: sklearn RF* refuse with exact pickle-forbidden reason (no third state)."""
    pytest.importorskip("sklearn")
    from repark.spark.ml.ext import (
        PICKLE_FORBIDDEN_REASON,
        RandomForestClassifier,
        RandomForestClassifierModel,
        RandomForestRegressor,
        RandomForestRegressorModel,
    )

    spark = _session()
    try:
        rows = [(float(x), float(x) + 0.5, 1.0 if x >= 6 else 0.0) for x in range(12)]
        df = spark.createDataFrame(rows, ["x", "y", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        reg = RandomForestRegressor(
            featuresCol="features", labelCol="y", numTrees=4, maxDepth=2, seed=1
        ).fit(assembled)
        clf = RandomForestClassifier(
            featuresCol="features", labelCol="label", numTrees=4, maxDepth=2, seed=1
        ).fit(assembled)
        with tempfile.TemporaryDirectory() as tmp:
            for name, model in (("reg", reg), ("clf", clf)):
                target = str(Path(tmp) / f"rf-{name}")
                with pytest.raises(
                    UnsupportedOperationException,
                    match=re.escape(PICKLE_FORBIDDEN_REASON),
                ):
                    model.save(target)
                with pytest.raises(
                    UnsupportedOperationException,
                    match=re.escape(PICKLE_FORBIDDEN_REASON),
                ):
                    model.write()
                assert not Path(target).exists()
            # Load/read also pin-refuse (octo M8 C1 — no AttributeError third state).
            for load_cls in (RandomForestRegressorModel, RandomForestClassifierModel):
                with pytest.raises(
                    UnsupportedOperationException,
                    match=re.escape(PICKLE_FORBIDDEN_REASON),
                ):
                    load_cls.load(str(Path(tmp) / "missing"))
                with pytest.raises(
                    UnsupportedOperationException,
                    match=re.escape(PICKLE_FORBIDDEN_REASON),
                ):
                    load_cls.read()
        # Constant is the charter exact string.
        assert PICKLE_FORBIDDEN_REASON == ("pickle forbidden (arbitrary code execution on load)")
    finally:
        spark.stop()


def test_m8_every_ext_model_has_save_or_pin_refuse() -> None:
    """M8 completeness: every public fitted model class is save/load XOR pin-refuse.

    Mutation: drop save/write on a landed model (AttributeError / silent no-op) or
    soft-success sklearn pickle path → red.
    """
    import importlib.util

    from repark.spark.ml.ext import PICKLE_FORBIDDEN_REASON

    spark = _session()
    try:
        reg_rows = [(float(x), float(x) * 2.0) for x in range(12)]
        clf_rows = [(float(x), 1.0 if x >= 6 else 0.0) for x in range(14)]
        reg_df = spark.createDataFrame(reg_rows, ["x", "label"])
        clf_df = spark.createDataFrame(clf_rows, ["x", "label"])
        reg_assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(reg_df)
        clf_assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(clf_df)

        matrix: list[tuple[str, object, str]] = []  # name, model, expected: save|refuse

        if importlib.util.find_spec("xgboost") is not None:
            from repark.spark.ml.ext import XGBoostClassifier, XGBoostRegressor

            matrix.append(
                (
                    "XGBoostRegressorModel",
                    XGBoostRegressor(
                        featuresCol="features",
                        labelCol="label",
                        nEstimators=4,
                        maxDepth=2,
                        seed=1,
                    ).fit(reg_assembled),
                    "save",
                )
            )
            matrix.append(
                (
                    "XGBoostClassifierModel",
                    XGBoostClassifier(
                        featuresCol="features",
                        labelCol="label",
                        nEstimators=4,
                        maxDepth=2,
                        seed=1,
                    ).fit(clf_assembled),
                    "save",
                )
            )
        if importlib.util.find_spec("lightgbm") is not None:
            from repark.spark.ml.ext import LightGBMClassifier, LightGBMRegressor

            matrix.append(
                (
                    "LightGBMRegressorModel",
                    LightGBMRegressor(
                        featuresCol="features",
                        labelCol="label",
                        nEstimators=4,
                        maxDepth=2,
                        seed=1,
                    ).fit(reg_assembled),
                    "save",
                )
            )
            matrix.append(
                (
                    "LightGBMClassifierModel",
                    LightGBMClassifier(
                        featuresCol="features",
                        labelCol="label",
                        nEstimators=4,
                        maxDepth=2,
                        seed=1,
                    ).fit(clf_assembled),
                    "save",
                )
            )
        if importlib.util.find_spec("sklearn") is not None:
            from repark.spark.ml.ext import RandomForestClassifier, RandomForestRegressor

            matrix.append(
                (
                    "RandomForestRegressorModel",
                    RandomForestRegressor(
                        featuresCol="features",
                        labelCol="label",
                        numTrees=4,
                        maxDepth=2,
                        seed=1,
                    ).fit(reg_assembled),
                    "refuse",
                )
            )
            matrix.append(
                (
                    "RandomForestClassifierModel",
                    RandomForestClassifier(
                        featuresCol="features",
                        labelCol="label",
                        numTrees=4,
                        maxDepth=2,
                        seed=1,
                    ).fit(clf_assembled),
                    "refuse",
                )
            )

        if not matrix:
            pytest.skip("no ml-ext backends installed")

        with tempfile.TemporaryDirectory() as tmp:
            for name, model, expected in matrix:
                target = Path(tmp) / name
                if expected == "save":
                    model.save(str(target))  # type: ignore[attr-defined]
                    assert (target / "metadata.json").is_file(), name
                    load_cls = type(model)
                    loaded = load_cls.load(str(target))  # type: ignore[attr-defined]
                    before = [
                        float(row.prediction)
                        for row in model.transform(  # type: ignore[attr-defined]
                            reg_assembled if "Regressor" in name else clf_assembled
                        )
                        .select("prediction")
                        .collect()
                    ]
                    after = [
                        float(row.prediction)
                        for row in loaded.transform(
                            reg_assembled if "Regressor" in name else clf_assembled
                        )
                        .select("prediction")
                        .collect()
                    ]
                    assert len(before) == len(after), name
                    for left, right in zip(before, after, strict=True):
                        assert _rel_close(left, right, tol=1e-5), (name, left, right)
                else:
                    with pytest.raises(
                        UnsupportedOperationException,
                        match=re.escape(PICKLE_FORBIDDEN_REASON),
                    ):
                        model.save(str(target))  # type: ignore[attr-defined]
                    assert not target.exists(), name
    finally:
        spark.stop()


def test_m8_no_pickle_import_in_ext_persist_sources() -> None:
    """M8 hygiene: ext persistence modules must not import pickle/joblib."""
    root = Path(__file__).resolve().parents[1] / "src" / "repark" / "spark" / "ml" / "ext"
    forbidden = re.compile(
        r"^\s*(import\s+pickle|from\s+pickle\s+import|import\s+joblib|from\s+joblib\s+import)"
    )
    offenders: list[str] = []
    for path in sorted(root.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            if forbidden.search(line):
                offenders.append(f"{path.name}:{line_no}:{line.strip()}")
    assert offenders == [], offenders


def test_m8_load_refuses_classifier_flag_mismatch_and_nonpositive_num_features() -> None:
    """Octo M8 C1: hostile task-type relabel + num_features<=0 soft-load must refuse.

    MUTATION: rewrite metadata.kind + fitted.classifier to the opposite task → silent
    cross-load with wrong predict semantics. MUTATION: num_features=0 → width guard skip.
    """
    pytest.importorskip("xgboost")
    import pyarrow as pa
    import pyarrow.parquet as pq

    from repark.spark.ml.ext import XGBoostClassifierModel, XGBoostRegressor, XGBoostRegressorModel

    spark = _session()
    try:
        rows = [(float(x), float(x) * 2.0 + 0.5) for x in range(14)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = XGBoostRegressor(
            featuresCol="features",
            labelCol="label",
            nEstimators=4,
            maxDepth=2,
            seed=9,
        ).fit(assembled)
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "xgb-c1"
            model.save(str(target))
            # --- kind spoof only: fitted.classifier stays False (honest regressor tree) ---
            meta = json.loads((target / "metadata.json").read_text(encoding="utf-8"))
            table = pq.read_table(target / "fitted" / "params.parquet")
            row = {name: table.column(name)[0].as_py() for name in table.column_names}
            assert row.get("classifier") in (False, 0)
            meta["kind"] = "XGBoostClassifierModel"
            (target / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
            with pytest.raises(
                IllegalArgumentException,
                match=r"classifier flag|task type|classifier=",
            ):
                XGBoostClassifierModel.load(str(target))
            # Restore honest tree then pin num_features<=0.
            model.write().overwrite().save(str(target))
            table = pq.read_table(target / "fitted" / "params.parquet")
            row = {name: table.column(name)[0].as_py() for name in table.column_names}
            row["num_features"] = 0
            out: dict[str, list[Any]] = {}
            for key, value in row.items():
                if isinstance(value, (list, dict)):
                    out[key] = [json.dumps(value)]
                else:
                    out[key] = [value]
            pq.write_table(pa.table(out), target / "fitted" / "params.parquet")
            with pytest.raises(
                IllegalArgumentException,
                match=r"num_features must be > 0",
            ):
                XGBoostRegressorModel.load(str(target))
    finally:
        spark.stop()
