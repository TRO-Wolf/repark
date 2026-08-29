"""M3 R-ML-ESTIMATORS oracles — LinearRegression + evaluators + divergence pins.

Live-pyspark differentials importorskip when JVM unavailable. EXPECTED-ERROR never skip.
Parity bar: 1e-6 relative on coefficients/intercept for well-conditioned fixtures.
No bit-exact claim. Divergence pins for solver / elastic net / init.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException, UnsupportedOperationException
from repark.spark.ml.clustering import KMeans
from repark.spark.ml.evaluation import (
    AUC_PR_SEED,
    BinaryClassificationEvaluator,
    RegressionEvaluator,
)
from repark.spark.ml.feature import VectorAssembler
from repark.spark.ml.regression import (
    ELASTIC_NET_SEED,
    SOLVER_DIVERGENCE,
    LinearRegression,
    LinearRegressionModel,
)


def _session() -> ReparkSession:
    return ReparkSession.builder.appName("ml-estimators-test").getOrCreate()


def _rel_close(a: float, b: float, tol: float = 1e-6) -> bool:
    if a == b:
        return True
    denom = max(abs(a), abs(b), 1e-15)
    return abs(a - b) / denom <= tol


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
            .appName("repark-ml-est-oracle")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    except Exception as error:
        pytest.skip(f"live pyspark unavailable: {error}")


# LinearRegression


def test_linear_regression_perfect_line() -> None:
    """y = 2 + 3x recovers intercept/slope within 1e-6 rel."""
    spark = _session()
    try:
        rows = [(float(x), 2.0 + 3.0 * float(x)) for x in range(10)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = LinearRegression(featuresCol="features", labelCol="label").fit(assembled)
        assert _rel_close(model.intercept, 2.0), model.intercept
        assert len(model.coefficients) == 1
        assert _rel_close(model.coefficients[0], 3.0), model.coefficients
        preds = model.transform(assembled).collect()
        for row in preds:
            d = row.asDict()
            assert _rel_close(float(d["prediction"]), float(d["label"]), tol=1e-5)
    finally:
        spark.stop()


def test_linear_regression_multi_feature() -> None:
    """y = 1 + 2*x0 - 0.5*x1 well-conditioned recovery."""
    spark = _session()
    try:
        data = [
            (1.0, 0.0, 3.0),
            (0.0, 1.0, 0.5),
            (1.0, 1.0, 2.5),
            (2.0, 1.0, 4.5),
            (1.0, 2.0, 2.0),
            (3.0, 2.0, 6.0),
            (4.0, 1.0, 8.5),
            (2.0, 3.0, 3.5),
        ]
        df = spark.createDataFrame(data, ["x0", "x1", "label"])
        assembled = VectorAssembler(inputCols=["x0", "x1"], outputCol="features").transform(df)
        model = LinearRegression().fit(assembled)
        assert _rel_close(model.intercept, 1.0), model.intercept
        assert _rel_close(model.coefficients[0], 2.0), model.coefficients
        assert _rel_close(model.coefficients[1], -0.5), model.coefficients
    finally:
        spark.stop()


def test_linear_regression_no_intercept() -> None:
    """fitIntercept=False forces through origin."""
    spark = _session()
    try:
        rows = [(float(x), 2.0 * float(x)) for x in range(1, 8)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = LinearRegression(fitIntercept=False).fit(assembled)
        assert model.intercept == 0.0
        assert _rel_close(model.coefficients[0], 2.0)
    finally:
        spark.stop()


def test_linear_regression_singular_loud() -> None:
    """Collinear features → loud singular (EXPECTED-ERROR; Spark may pinv)."""
    spark = _session()
    try:
        # x1 == x0 always → rank deficient with intercept
        rows = [(float(x), float(x), float(x)) for x in range(1, 6)]
        df = spark.createDataFrame(rows, ["x0", "x1", "label"])
        assembled = VectorAssembler(inputCols=["x0", "x1"], outputCol="features").transform(df)
        with pytest.raises(IllegalArgumentException, match=r"singular|ill-conditioned|Cholesky"):
            LinearRegression().fit(assembled)
    finally:
        spark.stop()


def test_linear_regression_elastic_net_unsupported() -> None:
    """elasticNetParam != 0 → loud M4 seed (EXPECTED-ERROR)."""
    spark = _session()
    try:
        df = spark.createDataFrame([(1.0, 1.0)], ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        with pytest.raises(UnsupportedOperationException, match=r"elasticNetParam|M4"):
            LinearRegression(elasticNetParam=0.5).fit(assembled)
        assert "M4" in ELASTIC_NET_SEED
    finally:
        spark.stop()


def test_linear_regression_standardization_unsupported() -> None:
    """standardization=True → loud unsupported (EXPECTED-ERROR)."""
    spark = _session()
    try:
        df = spark.createDataFrame([(1.0, 1.0)], ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        with pytest.raises(UnsupportedOperationException, match=r"standardization"):
            LinearRegression(standardization=True).fit(assembled)
    finally:
        spark.stop()


def test_linear_regression_params_only_no_training_rows_on_save() -> None:
    """Persistence holds coefficients only — save path never contains training labels."""
    spark = _session()
    try:
        # Distinctive training values that must not appear under save.
        secret = 987654.321
        rows = [(1.0, secret), (2.0, secret + 1.0), (3.0, secret + 2.0)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = LinearRegression().fit(assembled)
        from repark.spark.ml import PipelineModel

        pm = PipelineModel(stages=[model])
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "lr_model")
            pm.write().overwrite().save(path)
            # Grep-gate: no training-row secrets under stages/
            for file_path in Path(path).rglob("*"):
                if file_path.is_file():
                    text = file_path.read_bytes()
                    assert b"987654.321" not in text
                    assert str(secret).encode() not in text
            # Fitted state has coefficients, not labels
            meta = json.loads((Path(path) / "metadata.json").read_text(encoding="utf-8"))
            assert meta["format"] == "repark-ml"
            loaded = PipelineModel.load(path)
            assert isinstance(loaded.stages[0], LinearRegressionModel)
            assert _rel_close(loaded.stages[0].coefficients[0], model.coefficients[0])
    finally:
        spark.stop()


def test_solver_divergence_pin_documented() -> None:
    """Divergence pin string is stable for oracles / docs."""
    assert "Cholesky" in SOLVER_DIVERGENCE
    lowered = SOLVER_DIVERGENCE.lower()
    assert "pinv" in lowered or "pseudo" in lowered or "ridge" in lowered


# Evaluators


def test_regression_evaluator_empty_dataset_loud() -> None:
    """Empty evaluation frame must not silently return NaN."""
    spark = _session()
    try:
        df = spark.createDataFrame([(1.0, 1.0)], ["label", "prediction"]).filter("label > 10")
        with pytest.raises(IllegalArgumentException, match=r"empty dataset|0 rows"):
            RegressionEvaluator(metricName="mse").evaluate(df)
    finally:
        spark.stop()


def test_regression_evaluator_rmse() -> None:
    """RMSE via plan aggregate matches hand formula."""
    spark = _session()
    try:
        # predictions vs labels
        df = spark.createDataFrame(
            [(1.0, 1.0), (2.0, 3.0), (3.0, 3.0)],
            ["label", "prediction"],
        )
        # errors: 0, -1, 0 → mse = 1/3 → rmse = sqrt(1/3)
        metric = RegressionEvaluator(metricName="rmse").evaluate(df)
        expected = (1.0 / 3.0) ** 0.5
        assert _rel_close(metric, expected, tol=1e-9), (metric, expected)
    finally:
        spark.stop()


def test_regression_evaluator_mse_mae_r2() -> None:
    """MSE / MAE / R2 plan aggregates match hand formulas (mutation resistance)."""
    spark = _session()
    try:
        # labels 1,2,3 ; preds 1,3,3 → errors 0,-1,0
        df = spark.createDataFrame(
            [(1.0, 1.0), (2.0, 3.0), (3.0, 3.0)],
            ["label", "prediction"],
        )
        mse = RegressionEvaluator(metricName="mse").evaluate(df)
        mae = RegressionEvaluator(metricName="mae").evaluate(df)
        r2 = RegressionEvaluator(metricName="r2").evaluate(df)
        assert _rel_close(mse, 1.0 / 3.0, tol=1e-12), mse
        assert _rel_close(mae, 1.0 / 3.0, tol=1e-12), mae
        # SS_res = 1; mean label = 2; SS_tot = (1-2)^2+(2-2)^2+(3-2)^2 = 2; r2 = 1 - 1/2 = 0.5
        assert _rel_close(r2, 0.5, tol=1e-12), r2
    finally:
        spark.stop()


def test_binary_area_under_roc_rank_sum() -> None:
    """M5: areaUnderROC via window RANK + aggregate Mann-Whitney (perfect ranking → 1.0)."""
    spark = _session()
    try:
        # Higher score → positive label: perfect ranking.
        df = spark.createDataFrame(
            [
                (0.0, 0.1),
                (0.0, 0.2),
                (1.0, 0.7),
                (1.0, 0.9),
            ],
            ["label", "score"],
        )
        auc = BinaryClassificationEvaluator(
            metricName="areaUnderROC",
            labelCol="label",
            rawPredictionCol="score",
        ).evaluate(df)
        assert _rel_close(auc, 1.0, tol=1e-12), auc
        # Inverted ranking → 0.0
        inv = spark.createDataFrame(
            [
                (0.0, 0.9),
                (0.0, 0.7),
                (1.0, 0.2),
                (1.0, 0.1),
            ],
            ["label", "score"],
        )
        auc_inv = BinaryClassificationEvaluator(
            metricName="areaUnderROC",
            labelCol="label",
            rawPredictionCol="score",
        ).evaluate(inv)
        assert _rel_close(auc_inv, 0.0, tol=1e-12), auc_inv
    finally:
        spark.stop()


def test_binary_area_under_pr_average_precision() -> None:
    """M6: areaUnderPR via plan-built average precision (perfect ranking → 1.0)."""
    spark = _session()
    try:
        # Higher score → positive: perfect ranking → AP = 1.0
        df = spark.createDataFrame(
            [
                (0.0, 0.1),
                (0.0, 0.2),
                (1.0, 0.7),
                (1.0, 0.9),
            ],
            ["label", "score"],
        )
        ap = BinaryClassificationEvaluator(
            metricName="areaUnderPR",
            labelCol="label",
            rawPredictionCol="score",
        ).evaluate(df)
        assert _rel_close(ap, 1.0, tol=1e-12), ap
        # All positives ranked after negatives → low AP
        inv = spark.createDataFrame(
            [
                (0.0, 0.9),
                (0.0, 0.7),
                (1.0, 0.2),
                (1.0, 0.1),
            ],
            ["label", "score"],
        )
        ap_inv = BinaryClassificationEvaluator(
            metricName="areaUnderPR",
            labelCol="label",
            rawPredictionCol="score",
        ).evaluate(inv)
        assert ap_inv < 0.5, ap_inv
        assert "areaUnderPR" in AUC_PR_SEED  # historical seed string still exported
    finally:
        spark.stop()


def test_binary_area_under_pr_ties_order_independent() -> None:
    """M6 octo C3: areaUnderPR must not depend on physical row order among tied scores.

    MUTATION: per-row ``ROW_NUMBER() OVER (ORDER BY score DESC)`` without score-group
    aggregation → same multiset of (label, score) yields AP ∈ {≈0.42, 0.5, ≈0.83, 1.0}
    depending on insert order (silently wrong / non-deterministic).
    """
    spark = _session()
    try:
        orders = [
            [(0.0, 0.5), (0.0, 0.5), (1.0, 0.5), (1.0, 0.5)],
            [(1.0, 0.5), (1.0, 0.5), (0.0, 0.5), (0.0, 0.5)],
            [(0.0, 0.5), (1.0, 0.5), (0.0, 0.5), (1.0, 0.5)],
            [(1.0, 0.5), (0.0, 0.5), (1.0, 0.5), (0.0, 0.5)],
        ]
        aps: list[float] = []
        for rows in orders:
            frame = spark.createDataFrame(rows, ["label", "score"])
            aps.append(
                BinaryClassificationEvaluator(
                    metricName="areaUnderPR",
                    labelCol="label",
                    rawPredictionCol="score",
                ).evaluate(frame)
            )
        # All-tie balanced 2pos/2neg → group precision 0.5, order-independent.
        for value in aps:
            assert _rel_close(value, 0.5, tol=1e-12), (value, aps)
        # Mixed-score ties: two distinct scores, order of rows within score must not matter.
        mixed_orders = [
            [(0.0, 0.5), (1.0, 0.5), (0.0, 0.9), (1.0, 0.9)],
            [(1.0, 0.9), (0.0, 0.9), (1.0, 0.5), (0.0, 0.5)],
            [(0.0, 0.9), (1.0, 0.5), (1.0, 0.9), (0.0, 0.5)],
        ]
        mixed_aps = [
            BinaryClassificationEvaluator(
                metricName="areaUnderPR",
                labelCol="label",
                rawPredictionCol="score",
            ).evaluate(spark.createDataFrame(rows, ["label", "score"]))
            for rows in mixed_orders
        ]
        for value in mixed_aps[1:]:
            assert _rel_close(value, mixed_aps[0], tol=1e-12), mixed_aps
    finally:
        spark.stop()


def test_binary_area_under_roc_vector_raw_prediction() -> None:
    """M6: dense list/array rawPrediction extracts positive-class score at index 1."""
    spark = _session()
    try:
        # [neg_logit, pos_score] — pos scores perfect-rank positives above negatives.
        df = spark.createDataFrame(
            [
                (0.0, [0.9, 0.1]),
                (0.0, [0.8, 0.2]),
                (1.0, [0.3, 0.7]),
                (1.0, [0.1, 0.9]),
            ],
            ["label", "rawPrediction"],
        )
        auc = BinaryClassificationEvaluator(
            metricName="areaUnderROC",
            labelCol="label",
            rawPredictionCol="rawPrediction",
        ).evaluate(df)
        assert _rel_close(auc, 1.0, tol=1e-12), auc
        ap = BinaryClassificationEvaluator(
            metricName="areaUnderPR",
            labelCol="label",
            rawPredictionCol="rawPrediction",
        ).evaluate(df)
        assert _rel_close(ap, 1.0, tol=1e-12), ap
    finally:
        spark.stop()


def test_binary_area_under_roc_sparse_vector_raw_prediction() -> None:
    """M7: sparse VectorUDT rawPrediction extracts positive-class (index 1) via plan.

    Sparse zeros omitted: missing index 1 → score 0.0. Perfect ranking still → AUC/PR 1.0.
    """
    from repark.spark.ml.linalg import Vectors

    spark = _session()
    try:
        df = spark.createDataFrame(
            [
                (0.0, Vectors.sparse(2, [1], [0.1])),
                (0.0, Vectors.sparse(2, [1], [0.2])),
                (1.0, Vectors.sparse(2, [1], [0.7])),
                (1.0, Vectors.sparse(2, [1], [0.9])),
            ],
            ["label", "rawPrediction"],
        )
        auc = BinaryClassificationEvaluator(
            metricName="areaUnderROC",
            labelCol="label",
            rawPredictionCol="rawPrediction",
        ).evaluate(df)
        assert _rel_close(auc, 1.0, tol=1e-12), auc
        ap = BinaryClassificationEvaluator(
            metricName="areaUnderPR",
            labelCol="label",
            rawPredictionCol="rawPrediction",
        ).evaluate(df)
        assert _rel_close(ap, 1.0, tol=1e-12), ap
        # Missing positive index densifies to 0.0 — negatives with only index 0 rank lowest.
        missing_pos = spark.createDataFrame(
            [
                (0.0, Vectors.sparse(2, [0], [0.9])),  # pos=0.0
                (1.0, Vectors.sparse(2, [1], [0.8])),
            ],
            ["label", "rawPrediction"],
        )
        auc_m = BinaryClassificationEvaluator(
            metricName="areaUnderROC",
            labelCol="label",
            rawPredictionCol="rawPrediction",
        ).evaluate(missing_pos)
        assert _rel_close(auc_m, 1.0, tol=1e-12), auc_m
        # MUTATION (octo M7 C4): both indices present — must read index 1 (pos), not 0.
        # Scores [0.1,0.2,0.8,0.9] → AUC 1.0; if index 0 were used → inverted AUC 0.0.
        both = spark.createDataFrame(
            [
                (0.0, Vectors.sparse(2, [0, 1], [0.9, 0.1])),
                (0.0, Vectors.sparse(2, [0, 1], [0.8, 0.2])),
                (1.0, Vectors.sparse(2, [0, 1], [0.2, 0.8])),
                (1.0, Vectors.sparse(2, [0, 1], [0.1, 0.9])),
            ],
            ["label", "rawPrediction"],
        )
        auc_both = BinaryClassificationEvaluator(
            metricName="areaUnderROC",
            labelCol="label",
            rawPredictionCol="rawPrediction",
        ).evaluate(both)
        assert _rel_close(auc_both, 1.0, tol=1e-12), auc_both
        # Inverted ranking → AUC 0.0 (mutation: always-return-1.0 would pass perfect-only).
        inverted = spark.createDataFrame(
            [
                (0.0, Vectors.sparse(2, [1], [0.9])),
                (0.0, Vectors.sparse(2, [1], [0.8])),
                (1.0, Vectors.sparse(2, [1], [0.2])),
                (1.0, Vectors.sparse(2, [1], [0.1])),
            ],
            ["label", "rawPrediction"],
        )
        auc_inv = BinaryClassificationEvaluator(
            metricName="areaUnderROC",
            labelCol="label",
            rawPredictionCol="rawPrediction",
        ).evaluate(inverted)
        assert _rel_close(auc_inv, 0.0, tol=1e-12), auc_inv
        # Null rawPrediction cells must not densify to score 0.0 (would collapse AUC to 0.5).
        with_null = spark.createDataFrame(
            [
                (0.0, Vectors.sparse(2, [1], [0.1])),
                (1.0, None),
                (1.0, Vectors.sparse(2, [1], [0.9])),
                (0.0, Vectors.sparse(2, [1], [0.2])),
            ],
            ["label", "rawPrediction"],
        )
        auc_null = BinaryClassificationEvaluator(
            metricName="areaUnderROC",
            labelCol="label",
            rawPredictionCol="rawPrediction",
        ).evaluate(with_null)
        assert _rel_close(auc_null, 1.0, tol=1e-12), auc_null
    finally:
        spark.stop()


def test_binary_area_under_roc_non_vector_score_refuses_loud() -> None:
    """M7 octo C3: map/non-vector nested score refuses with AUC_VECTOR_RAW_GAP (not CAST)."""
    spark = _session()
    try:
        # MapType score column (createDataFrame dict without sparse keys).
        df = spark.createDataFrame(
            [(0.0, {"a": 0.1}), (1.0, {"a": 0.9})],
            ["label", "rawPrediction"],
        )
        with pytest.raises(UnsupportedOperationException, match=r"sparse VectorUDT|rawPrediction"):
            BinaryClassificationEvaluator(
                metricName="areaUnderROC",
                labelCol="label",
                rawPredictionCol="rawPrediction",
            ).evaluate(df)
    finally:
        spark.stop()


def test_native_estimator_sparse_features_densify_disclosure() -> None:
    """M7 octo C2: native fit on sparse VectorUDT names densify/sparseOutput boundary."""
    from repark.spark.ml.linalg import Vectors

    spark = _session()
    try:
        df = spark.createDataFrame(
            [
                (0.0, Vectors.sparse(2, [0], [1.0])),
                (1.0, Vectors.sparse(2, [1], [1.0])),
            ],
            ["label", "features"],
        )
        with pytest.raises(
            IllegalArgumentException,
            match=r"sparse VectorUDT|sparseOutput|densify",
        ):
            LinearRegression(featuresCol="features", labelCol="label").fit(df)
    finally:
        spark.stop()


def test_binary_area_under_roc_short_sparse_vector_refuses_loud() -> None:
    """M7: sparse size < 2 must refuse like short dense (not degenerate-labels)."""
    from repark.spark.ml.linalg import Vectors

    spark = _session()
    try:
        df = spark.createDataFrame(
            [
                (0.0, Vectors.sparse(1, [0], [0.1])),
                (1.0, Vectors.sparse(1, [0], [0.9])),
            ],
            ["label", "rawPrediction"],
        )
        with pytest.raises(IllegalArgumentException, match=r"rawPrediction|length|size|sparse"):
            BinaryClassificationEvaluator(
                metricName="areaUnderROC",
                labelCol="label",
                rawPredictionCol="rawPrediction",
            ).evaluate(df)
        with pytest.raises(IllegalArgumentException, match=r"rawPrediction|length|size|sparse"):
            BinaryClassificationEvaluator(
                metricName="areaUnderPR",
                labelCol="label",
                rawPredictionCol="rawPrediction",
            ).evaluate(df)
    finally:
        spark.stop()


def test_binary_area_under_roc_short_vector_refuses_loud() -> None:
    """M6 octo C4: length-1 dense rawPrediction must not look like 'degenerate labels'.

    MUTATION: array_element(col, 1) → NULL for len-1 arrays; all scores filtered →
    generic degenerate-label message (misleads; labels were fine).
    """
    spark = _session()
    try:
        df = spark.createDataFrame(
            [(0.0, [0.1]), (1.0, [0.9]), (0.0, [0.2])],
            "label DOUBLE, rawPrediction ARRAY<DOUBLE>",
        )
        with pytest.raises(IllegalArgumentException, match=r"array_element|length|rawPrediction"):
            BinaryClassificationEvaluator(
                metricName="areaUnderROC",
                labelCol="label",
                rawPredictionCol="rawPrediction",
            ).evaluate(df)
        with pytest.raises(IllegalArgumentException, match=r"array_element|length|rawPrediction"):
            BinaryClassificationEvaluator(
                metricName="areaUnderPR",
                labelCol="label",
                rawPredictionCol="rawPrediction",
            ).evaluate(df)
    finally:
        spark.stop()


def test_binary_area_under_roc_refuses_non_binary_labels() -> None:
    """Non-0/1 labels must refuse loud — not return AUC outside [0,1] (octo M5 C1).

    MUTATION: drop ``n_other`` check → labels (0, 2, 1) with scores ascending yield
    Mann-Whitney midranks contaminated by the non-binary row; n_pos/n_neg omit it and
    AUC can be 2.0 (silently wrong, outside [0,1]).
    """
    spark = _session()
    try:
        df = spark.createDataFrame(
            [
                (0.0, 0.1),
                (2.0, 0.5),  # non-binary
                (1.0, 0.9),
            ],
            ["label", "score"],
        )
        with pytest.raises(IllegalArgumentException, match=r"binary|non-binary|0/1"):
            BinaryClassificationEvaluator(
                metricName="areaUnderROC",
                labelCol="label",
                rawPredictionCol="score",
            ).evaluate(df)
        # sklearn-style -1/1 must also refuse (not "degenerate" alone).
        df_neg = spark.createDataFrame(
            [(-1.0, 0.1), (-1.0, 0.2), (1.0, 0.8), (1.0, 0.9)],
            ["label", "score"],
        )
        with pytest.raises(IllegalArgumentException, match=r"binary|non-binary|0/1"):
            BinaryClassificationEvaluator(
                metricName="areaUnderROC",
                labelCol="label",
                rawPredictionCol="score",
            ).evaluate(df_neg)
    finally:
        spark.stop()


def test_binary_area_under_roc_ties_midrank() -> None:
    """Tied scores use midranks → AUC 0.875 on classic 2x2 tie fixture (octo M5)."""
    spark = _session()
    try:
        df = spark.createDataFrame(
            [
                (0.0, 0.1),
                (0.0, 0.5),
                (1.0, 0.5),
                (1.0, 0.9),
            ],
            ["label", "score"],
        )
        auc = BinaryClassificationEvaluator(
            metricName="areaUnderROC",
            labelCol="label",
            rawPredictionCol="score",
        ).evaluate(df)
        assert _rel_close(auc, 0.875, tol=1e-12), auc
        # All scores identical → midranks equal → AUC 0.5 (not NaN / not 0/1 bias).
        all_tie = spark.createDataFrame(
            [(0.0, 0.5), (0.0, 0.5), (1.0, 0.5), (1.0, 0.5)],
            ["label", "score"],
        )
        auc_tie = BinaryClassificationEvaluator(
            metricName="areaUnderROC",
            labelCol="label",
            rawPredictionCol="score",
        ).evaluate(all_tie)
        assert _rel_close(auc_tie, 0.5, tol=1e-12), auc_tie
    finally:
        spark.stop()


def test_binary_area_under_roc_prefers_raw_prediction_col() -> None:
    """When both rawPrediction and prediction exist, rank raw (octo M5 C3).

    MUTATION: prefer predictionCol when both present → inverted raw ignored; AUC=1.
    """
    spark = _session()
    try:
        # raw inverted (AUC 0); prediction perfect (AUC 1) — must choose raw.
        df = spark.createDataFrame(
            [
                (0.0, 0.9, 0.1),
                (1.0, 0.1, 0.9),
            ],
            ["label", "rawPrediction", "prediction"],
        )
        auc = BinaryClassificationEvaluator(
            metricName="areaUnderROC",
            labelCol="label",
        ).evaluate(df)
        assert _rel_close(auc, 0.0, tol=1e-12), auc
    finally:
        spark.stop()


def test_binary_accuracy() -> None:
    """Binary accuracy aggregate."""
    spark = _session()
    try:
        df = spark.createDataFrame(
            [(0.0, 0.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)],
            ["label", "prediction"],
        )
        acc = BinaryClassificationEvaluator(metricName="accuracy").evaluate(df)
        assert _rel_close(acc, 0.75, tol=1e-12)
    finally:
        spark.stop()


# Logistic + KMeans


def test_logistic_regression_separable() -> None:
    """Binary logistic learns positive slope on 1-d separable data."""
    spark = _session()
    try:
        from repark.spark.ml.classification import LogisticRegression

        rows = []
        for x in range(-5, 6):
            rows.append((float(x), 1.0 if x > 0 else 0.0))
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = LogisticRegression(maxIter=50, tol=1e-8).fit(assembled)
        assert model.coefficients[0] > 0.0
        preds = model.transform(assembled).select("x", "label", "prediction").collect()
        # Extreme points should classify correctly
        by_x = {row.asDict()["x"]: row.asDict() for row in preds}
        assert by_x[5.0]["prediction"] == 1.0
        assert by_x[-5.0]["prediction"] == 0.0
    finally:
        spark.stop()


def test_kmeans_default_init_mode_errors() -> None:
    """Default initMode (k-means||) fails loud — set initMode=random (EXPECTED-ERROR)."""
    spark = _session()
    try:
        df = spark.createDataFrame([(0.0,), (1.0,), (10.0,), (11.0,)], ["x"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        with pytest.raises(UnsupportedOperationException, match=r"initMode|random|k-means\|\|"):
            KMeans(k=2).fit(assembled)
    finally:
        spark.stop()


def test_kmeans_random_init_two_blobs() -> None:
    """Lloyd with initMode=random separates two 1-d blobs."""
    spark = _session()
    try:
        rows = [(0.0,), (0.1,), (-0.1,), (10.0,), (10.1,), (9.9,)]
        df = spark.createDataFrame(rows, ["x"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = KMeans(k=2, initMode="random", seed=7, maxIter=20).fit(assembled)
        centers = sorted(c[0] for c in model.clusterCenters())
        assert centers[0] < 1.0
        assert centers[1] > 9.0
        labeled = model.transform(assembled).collect()
        # Two distinct prediction labels present
        preds = {row.asDict()["prediction"] for row in labeled}
        assert len(preds) == 2
    finally:
        spark.stop()


# Live PySpark parity (1e-6 rel) — skip without JVM


def test_linear_regression_live_pyspark_parity() -> None:
    """Well-conditioned coefficients within 1e-6 rel of live Spark 4.x OLS."""
    spark_jvm = _maybe_live_spark()
    try:
        from pyspark.ml.feature import VectorAssembler as SparkVA
        from pyspark.ml.regression import LinearRegression as SparkLR

        data = [
            (1.0, 0.0, 3.0),
            (0.0, 1.0, 0.5),
            (1.0, 1.0, 2.5),
            (2.0, 1.0, 4.5),
            (1.0, 2.0, 2.0),
            (3.0, 2.0, 6.0),
            (4.0, 1.0, 8.5),
            (2.0, 3.0, 3.5),
        ]
        sdf = spark_jvm.createDataFrame(data, ["x0", "x1", "label"])
        s_assembled = SparkVA(inputCols=["x0", "x1"], outputCol="features").transform(sdf)
        s_model = (
            SparkLR(featuresCol="features", labelCol="label", standardization=False)
            .setElasticNetParam(0.0)
            .setRegParam(0.0)
            .fit(s_assembled)
        )
        s_intercept = float(s_model.intercept)
        s_coefs = [float(c) for c in s_model.coefficients.toArray()]
    finally:
        spark_jvm.stop()

    spark = _session()
    try:
        df = spark.createDataFrame(data, ["x0", "x1", "label"])
        assembled = VectorAssembler(inputCols=["x0", "x1"], outputCol="features").transform(df)
        model = LinearRegression(standardization=False, elasticNetParam=0.0, regParam=0.0).fit(
            assembled
        )
        assert _rel_close(model.intercept, s_intercept), (model.intercept, s_intercept)
        for left, right in zip(model.coefficients, s_coefs, strict=True):
            assert _rel_close(left, right), (left, right, model.coefficients, s_coefs)
    finally:
        spark.stop()


# Grep-gate helpers (import surface)


def test_no_numpy_import_in_ml_fit_modules() -> None:
    """numpy must not appear in native repark.ml fit modules.

    M4 exception: ``repark.ml.ext`` may import numpy only behind the optional
    ``repark[ml-ext]`` path (lazy ``require_numpy``). Native estimators remain
    under the M3 Rust rule — no numpy in non-ext modules.
    """
    root = Path(__file__).resolve().parents[1] / "src" / "repark" / "spark" / "ml"
    banned = re.compile(r"^\s*(import numpy|from numpy)")
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        # M4: ext package is the only sanctioned numpy/pandas import site.
        try:
            path.relative_to(root / "ext")
            continue
        except ValueError:
            pass
        text = path.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            if banned.search(line):
                offenders.append(f"{path}:{line_no}:{line.strip()}")
    assert not offenders, "numpy import in native ml package:\n" + "\n".join(offenders)


def test_model_copy_isolates_fitted_params() -> None:
    """copy() deep-copies coefficients / centers — mutating the copy must not alias."""
    spark = _session()
    try:
        from repark.spark.ml.classification import LogisticRegression
        from repark.spark.ml.clustering import KMeans

        rows = [(-2.0, 0.0), (-1.0, 0.0), (1.0, 1.0), (2.0, 1.0)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        logit = LogisticRegression(maxIter=30).fit(assembled)
        logit_copy = logit.copy()
        assert logit.coefficients is not logit_copy.coefficients
        logit_copy.coefficients[0] = 999.0
        assert logit.coefficients[0] != 999.0

        blobs = [(0.0,), (0.1,), (10.0,), (10.1,)]
        kdf = VectorAssembler(inputCols=["x"], outputCol="features").transform(
            spark.createDataFrame(blobs, ["x"])
        )
        km = KMeans(k=2, initMode="random", seed=3).fit(kdf)
        km_copy = km.copy()
        assert km.centers is not km_copy.centers
        assert km.centers[0] is not km_copy.centers[0]
        km_copy.centers[0][0] = -123.0
        assert km.centers[0][0] != -123.0

        lr = LinearRegression().fit(assembled)
        lr_copy = lr.copy()
        assert lr.coefficients is not lr_copy.coefficients
        lr_copy.coefficients[0] = 999.0
        assert lr.coefficients[0] != 999.0
    finally:
        spark.stop()


def test_transform_refuses_prediction_col_collision() -> None:
    """predictionCol already present → loud AnalysisException (no silent overwrite)."""
    spark = _session()
    try:
        rows = [(float(x), 2.0 + 3.0 * float(x)) for x in range(5)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        model = LinearRegression().fit(assembled)
        once = model.transform(assembled)
        with pytest.raises(AnalysisException, match=r"already exists|prediction"):
            model.transform(once).collect()
    finally:
        spark.stop()


def test_transform_refuses_num_features_desync() -> None:
    """coefficients length must match num_features — else loud refuse (no silent NULL)."""
    model = LinearRegressionModel(
        coefficients=[1.0, 2.0],
        intercept=0.0,
        featuresCol="features",
        predictionCol="prediction",
        num_features=1,
    )
    spark = _session()
    try:
        narrow = VectorAssembler(inputCols=["x"], outputCol="features").transform(
            spark.createDataFrame([(1.0,), (2.0,)], ["x"])
        )
        with pytest.raises(IllegalArgumentException, match=r"desynced|coefficients"):
            model.transform(narrow).collect()
    finally:
        spark.stop()


def test_transform_refuses_feature_width_mismatch() -> None:
    """Wrong-width features must not silently NULL-out predictions via array_element."""
    spark = _session()
    try:
        rows = [(1.0, 0.0, 3.0), (0.0, 1.0, 0.5), (1.0, 1.0, 2.5), (2.0, 1.0, 4.5)]
        df = spark.createDataFrame(rows, ["x0", "x1", "label"])
        assembled = VectorAssembler(inputCols=["x0", "x1"], outputCol="features").transform(df)
        model = LinearRegression().fit(assembled)
        narrow = VectorAssembler(inputCols=["x0"], outputCol="features").transform(
            spark.createDataFrame([(1.0,), (2.0,)], ["x0"])
        )
        with pytest.raises(IllegalArgumentException, match=r"width|num_features|array_length"):
            model.transform(narrow).collect()
    finally:
        spark.stop()


def test_empty_feature_vector_intercept_only() -> None:
    """Width-0 dense features + fitIntercept recover mean(y) (empty make_array path)."""
    spark = _session()
    try:
        df = spark.sql(
            "SELECT make_array() AS features, CAST(1.0 AS DOUBLE) AS label "
            "UNION ALL SELECT make_array(), CAST(3.0 AS DOUBLE) "
            "UNION ALL SELECT make_array(), CAST(5.0 AS DOUBLE)"
        )
        model = LinearRegression(fitIntercept=True).fit(df)
        assert model.num_features == 0
        assert model.coefficients == []
        assert _rel_close(model.intercept, 3.0)
    finally:
        spark.stop()


def test_max_iter_zero_no_optimization_steps() -> None:
    """maxIter=0 → zero iterations; params stay cold-start / init centers; num_rows counted."""
    spark = _session()
    try:
        from repark.spark.ml.classification import LogisticRegression
        from repark.spark.ml.clustering import KMeans

        rows = [(-2.0, 0.0), (-1.0, 0.0), (1.0, 1.0), (2.0, 1.0)]
        df = spark.createDataFrame(rows, ["x", "label"])
        assembled = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
        logit = LogisticRegression(maxIter=0).fit(assembled)
        assert logit.iterations == 0
        assert logit.coefficients == [0.0]
        assert logit.intercept == 0.0
        assert logit.num_rows == 4

        blobs = [(0.0,), (0.1,), (10.0,), (10.1,)]
        kdf = VectorAssembler(inputCols=["x"], outputCol="features").transform(
            spark.createDataFrame(blobs, ["x"])
        )
        km = KMeans(k=2, initMode="random", seed=3, maxIter=0).fit(kdf)
        assert km.iterations == 0
        assert km.num_rows == 4
        assert len(km.clusterCenters()) == 2
    finally:
        spark.stop()
