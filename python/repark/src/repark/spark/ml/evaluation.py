"""Spark ML evaluators implemented as aggregate SQL plans."""

from __future__ import annotations

import contextlib
from typing import Any

from repark.errors import IllegalArgumentException, UnsupportedOperationException
from repark.spark._idents import quote_ident as _quote_ident
from repark.spark._temp_views import scratch_view_name
from repark.spark.ml.base import _require_repark_dataframe
from repark.spark.ml.param import HasLabelCol, HasPredictionCol, Param, Params, TypeConverters

AUC_PR_SEED = (
    "areaUnderPR is plan-built average precision (M6): precision-at-each-positive-hit "
    "averaged over n_pos, via window RANK on score DESC. areaUnderROC remains Mann-Whitney."
)

AUC_VECTOR_RAW_GAP = (
    "areaUnderROC/areaUnderPR: rawPrediction score column is not a scalar DOUBLE, not a "
    "dense list/FixedSizeList of length >= 2 (positive-class index 1), and not a sparse "
    "VectorUDT struct {size,indices,values} with size >= 2. Unsupported nested layouts "
    "remain a plan gap — provide a scalar score, a dense [neg, pos] array, or a sparse "
    "struct via rawPredictionCol."
)


def _sparse_positive_class_score_sql(score_quoted: str) -> str:
    """Build SQL for sparse positive-class score extraction at zero-based index 1.

    Logical class index 1 is zero-based. SQL ``array_position`` and ``element_at`` positions
    are one-based. Guard null or short vectors before ``COALESCE``. Missing indices produce zero.
    """
    return (
        f"CASE WHEN {score_quoted} IS NULL OR {score_quoted}.size IS NULL "
        f"OR {score_quoted}.size < 2 THEN CAST(NULL AS DOUBLE) "
        f"ELSE CAST(COALESCE("
        f"element_at({score_quoted}.values, array_position({score_quoted}.indices, 1)), "
        f"0.0) AS DOUBLE) END"
    )


def _collect_scalar(frame: Any, sql: str) -> float:
    """Run aggregate SQL and return its first cell as a float."""
    rows = list(frame._spawn(frame._session.sql(sql)).collect())
    if not rows:
        raise IllegalArgumentException("evaluator query returned no rows")
    row = rows[0]
    values = list(row.asDict().values()) if hasattr(row, "asDict") else list(row)
    if not values:
        raise IllegalArgumentException("evaluator query returned empty row")
    value = values[0]
    if value is None:
        return float("nan")
    return float(value)


def _require_nonempty_eval(frame: Any, view: str, *, verb: str) -> None:
    """Refuse empty frames because aggregate metrics are undefined."""
    rows = list(frame._spawn(frame._session.sql(f"SELECT COUNT(*) AS n FROM {view}")).collect())
    if not rows:
        raise IllegalArgumentException(f"{verb}: count query returned no rows")
    values = list(rows[0].asDict().values()) if hasattr(rows[0], "asDict") else list(rows[0])
    count = int(values[0] or 0)
    if count == 0:
        raise IllegalArgumentException(f"{verb}: empty dataset (0 rows) — cannot compute metric")


class Evaluator(Params):
    """Base class for metric evaluators."""

    def evaluate(self, dataset: Any) -> float:
        """Compute a metric on ``dataset``."""
        raise NotImplementedError

    def isLargerBetter(self) -> bool:
        """Return whether larger metric values are better."""
        return True


class RegressionEvaluator(HasLabelCol, HasPredictionCol, Evaluator):
    """Evaluate RMSE, MSE, MAE, or R2 with aggregate SQL.

    Metric names match case-insensitively. R2 is larger-is-better. Other metrics are not.
    """

    def __init__(
        self,
        *,
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        metricName: str | None = None,  # noqa: N803
    ) -> None:
        """Initialize evaluator parameters."""
        super().__init__()
        self.metricName: Param[str] = Param(
            self,
            "metricName",
            "metric: rmse | mse | mae | r2",
            TypeConverters.toString,
        )
        self._setDefault(metricName="rmse")
        if labelCol is not None:
            self.setLabelCol(labelCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)
        if metricName is not None:
            self._set(metricName=metricName)

    def setMetricName(self, value: str) -> RegressionEvaluator:
        """Set the metric name."""
        return self._set(metricName=value)

    def getMetricName(self) -> str:
        """Return the metric name."""
        return str(self.getOrDefault(self.metricName))

    def isLargerBetter(self) -> bool:
        """Return whether this metric is larger-is-better."""
        return self.getMetricName().lower() == "r2"

    def evaluate(self, dataset: Any) -> float:
        """Evaluate the selected metric with a lazy aggregate plan."""
        frame = _require_repark_dataframe(dataset, verb="RegressionEvaluator.evaluate")
        label = _quote_ident(self.getLabelCol())
        pred = _quote_ident(self.getPredictionCol())
        metric = self.getMetricName().lower()
        view = scratch_view_name(frame._session, "__repark_reval_")
        frame.createOrReplaceTempView(view)
        try:
            _require_nonempty_eval(frame, view, verb="RegressionEvaluator.evaluate")
            if metric == "rmse":
                sql = f"SELECT sqrt(avg(power({pred} - {label}, 2))) AS metric FROM {view}"
            elif metric == "mse":
                sql = f"SELECT avg(power({pred} - {label}, 2)) AS metric FROM {view}"
            elif metric == "mae":
                sql = f"SELECT avg(abs({pred} - {label})) AS metric FROM {view}"
            elif metric == "r2":
                sql = (
                    f"SELECT 1.0 - (sum(power({pred} - {label}, 2)) / "
                    f"nullif(sum(power({label} - avg_label, 2)), 0.0)) AS metric "
                    f"FROM (SELECT {label}, {pred}, "
                    f"(SELECT avg({label}) FROM {view}) AS avg_label FROM {view})"
                )
            else:
                raise IllegalArgumentException(
                    f"RegressionEvaluator.metricName must be rmse|mse|mae|r2, got {metric!r}"
                )
            return _collect_scalar(frame, sql)
        finally:
            with contextlib.suppress(Exception):
                frame._session.drop_temp_view(view)


class BinaryClassificationEvaluator(HasLabelCol, HasPredictionCol, Evaluator):
    """Evaluate binary accuracy, areaUnderROC, or areaUnderPR.

    Defaults are ``areaUnderROC`` and ``rawPredictionCol="rawPrediction"``. AUC score
    resolution falls back to ``predictionCol`` when the raw score column is absent.
    """

    def __init__(
        self,
        *,
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        metricName: str | None = None,  # noqa: N803
        rawPredictionCol: str | None = None,  # noqa: N803
    ) -> None:
        """Initialize evaluator parameters."""
        super().__init__()
        self.metricName: Param[str] = Param(
            self,
            "metricName",
            "metric: areaUnderROC | areaUnderPR | accuracy",
            TypeConverters.toString,
        )
        self.rawPredictionCol: Param[str] = Param(
            self,
            "rawPredictionCol",
            "score column for ranking metrics (scalar DOUBLE or dense list [neg, pos]).",
            TypeConverters.toString,
        )
        self._setDefault(metricName="areaUnderROC", rawPredictionCol="rawPrediction")
        if labelCol is not None:
            self.setLabelCol(labelCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)
        if metricName is not None:
            self._set(metricName=metricName)
        if rawPredictionCol is not None:
            self._set(rawPredictionCol=rawPredictionCol)

    def setMetricName(self, value: str) -> BinaryClassificationEvaluator:
        """Set the metric name."""
        return self._set(metricName=value)

    def getMetricName(self) -> str:
        """Return the metric name."""
        return str(self.getOrDefault(self.metricName))

    def setRawPredictionCol(self, value: str) -> BinaryClassificationEvaluator:
        """Set the raw prediction or score column."""
        return self._set(rawPredictionCol=value)

    def getRawPredictionCol(self) -> str:
        """Return the raw prediction or score column."""
        return str(self.getOrDefault(self.rawPredictionCol))

    def evaluate(self, dataset: Any) -> float:
        """Evaluate the selected metric with a lazy aggregate plan."""
        frame = _require_repark_dataframe(dataset, verb="BinaryClassificationEvaluator.evaluate")
        metric = self.getMetricName()
        if metric == "areaUnderPR":
            return self._evaluate_area_under_pr(frame)
        if metric == "areaUnderROC":
            return self._evaluate_area_under_roc(frame)
        if metric != "accuracy":
            raise IllegalArgumentException(
                f"BinaryClassificationEvaluator.metricName must be "
                f"areaUnderROC|areaUnderPR|accuracy, got {metric!r}"
            )
        label = _quote_ident(self.getLabelCol())
        pred = _quote_ident(self.getPredictionCol())
        view = scratch_view_name(frame._session, "__repark_beval_")
        frame.createOrReplaceTempView(view)
        try:
            _require_nonempty_eval(frame, view, verb="BinaryClassificationEvaluator.evaluate")
            sql = (
                f"SELECT avg(CASE WHEN {pred} = {label} THEN 1.0 ELSE 0.0 END) AS metric "
                f"FROM {view}"
            )
            return _collect_scalar(frame, sql)
        finally:
            with contextlib.suppress(Exception):
                frame._session.drop_temp_view(view)

    def _resolve_score_sql(self, frame: Any, *, verb: str) -> tuple[str, str, Any | None]:
        """Resolve binary ranking score metadata and SQL expression.

        Return ``(label_name, score_sql, score_view_or_none)``. Prefer ``rawPredictionCol``
        over ``predictionCol``. Cast scalars to ``DOUBLE``. Use zero-based vector index 1 for
        the positive class. Sparse vectors use ``indices``/``values`` with null and short-vector
        guards before missing entries become zero. Unsupported nested layouts raise
        ``UnsupportedOperationException`` with the gap contract.
        """
        label_name = self.getLabelCol()
        raw_name = self.getRawPredictionCol()
        pred_name = self.getPredictionCol()
        columns = list(frame.columns) if hasattr(frame, "columns") else []
        if not columns:
            try:
                columns = [field.name for field in frame.schema.fields]
            except Exception:
                columns = []
        if raw_name in columns:
            score_name = raw_name
        elif pred_name in columns:
            score_name = pred_name
        else:
            raise IllegalArgumentException(
                f"{verb} needs a score column: "
                f"neither rawPredictionCol={raw_name!r} nor predictionCol={pred_name!r} "
                f"is present (columns={columns})"
            )

        score_quoted = _quote_ident(score_name)
        score_sql = f"CAST({score_quoted} AS DOUBLE)"
        score_type = None
        type_name = ""
        try:
            field_types = {field.name: field.dataType for field in frame.schema.fields}
            score_type = field_types.get(score_name)
            type_name = type(score_type).__name__ if score_type is not None else ""
        except (AttributeError, TypeError, ValueError):
            type_name = ""
        if "Array" in type_name or "List" in type_name or "Vector" in type_name:
            score_sql = f"CAST(array_element({score_quoted}, 1) AS DOUBLE)"
        elif "Struct" in type_name:
            nested_names: set[str] = set()
            try:
                nested_names = {field.name for field in score_type.fields}  # type: ignore[union-attr]
            except (AttributeError, TypeError):
                nested_names = set()
            if nested_names and not {"size", "indices", "values"}.issubset(nested_names):
                fields_list = sorted(nested_names)
                raise UnsupportedOperationException(
                    f"{verb}: score column {score_name!r} has struct type without "
                    f"sparse VectorUDT fields size/indices/values "
                    f"(fields={fields_list}). {AUC_VECTOR_RAW_GAP}"
                )
            score_sql = _sparse_positive_class_score_sql(score_quoted)
        elif type_name and any(
            token in type_name for token in ("Map", "Dict", "Binary", "String", "Boolean")
        ):
            raise UnsupportedOperationException(
                f"{verb}: score column {score_name!r} has type {type_name} which is not a "
                f"scalar DOUBLE, dense list, or sparse VectorUDT. {AUC_VECTOR_RAW_GAP}"
            )
        return label_name, score_sql, None

    def _evaluate_area_under_roc(self, frame: Any) -> float:
        """Evaluate areaUnderROC with a Mann-Whitney rank-sum plan.

        Refuse empty, non-binary, or degenerate labels. Project scores to scalars before ordering.
        DataFusion window ordering cannot safely use complex array or struct expressions. Average
        tied ranks, independent of input order. Unusable vectors fail loudly.
        """
        label_name, score_sql, _ = self._resolve_score_sql(
            frame, verb="BinaryClassificationEvaluator.areaUnderROC"
        )
        label = _quote_ident(label_name)
        view = scratch_view_name(frame._session, "__repark_auc_")
        scored = scratch_view_name(frame._session, "__repark_auc_sc_")
        frame.createOrReplaceTempView(view)
        try:
            _require_nonempty_eval(frame, view, verb="BinaryClassificationEvaluator.areaUnderROC")
            scored_sql = (
                f"SELECT CAST({label} AS DOUBLE) AS label, ({score_sql}) AS score FROM {view}"
            )
            scored_frame = frame._spawn(frame._session.sql(scored_sql))
            scored_frame.createOrReplaceTempView(scored)
            sql = f"""
            WITH ordered AS (
              SELECT
                label,
                score,
                ROW_NUMBER() OVER (ORDER BY score ASC) AS row_num
              FROM {scored}
              WHERE label IS NOT NULL AND score IS NOT NULL
            ),
            ranked AS (
              SELECT
                label,
                score,
                AVG(row_num) OVER (PARTITION BY score) AS rnk
              FROM ordered
            ),
            stats AS (
              SELECT
                SUM(CASE WHEN label = 1.0 THEN rnk ELSE 0.0 END) AS sum_ranks_pos,
                SUM(CASE WHEN label = 1.0 THEN 1.0 ELSE 0.0 END) AS n_pos,
                SUM(CASE WHEN label = 0.0 THEN 1.0 ELSE 0.0 END) AS n_neg,
                SUM(
                  CASE
                    WHEN label <> 0.0 AND label <> 1.0 THEN 1.0
                    ELSE 0.0
                  END
                ) AS n_other
              FROM ranked
            )
            SELECT
              sum_ranks_pos,
              n_pos,
              n_neg,
              n_other,
              (sum_ranks_pos - n_pos * (n_pos + 1.0) / 2.0)
                / NULLIF(n_pos * n_neg, 0.0) AS metric
            FROM stats
            """
            rows = list(frame._spawn(frame._session.sql(sql)).collect())
            if not rows:
                raise IllegalArgumentException("evaluator query returned no rows")
            row = rows[0]
            cells = row.asDict() if hasattr(row, "asDict") else {}
            if cells:
                n_other = float(cells.get("n_other") or 0.0)
                n_pos = float(cells.get("n_pos") or 0.0)
                n_neg = float(cells.get("n_neg") or 0.0)
                metric_cell = cells.get("metric")
                value = float(metric_cell) if metric_cell is not None else float("nan")
            else:
                values = list(row)
                n_pos = float(values[1]) if len(values) > 1 and values[1] is not None else 0.0
                n_neg = float(values[2]) if len(values) > 2 and values[2] is not None else 0.0
                n_other = float(values[3]) if len(values) > 3 and values[3] is not None else 0.0
                value = (
                    float(values[4]) if len(values) > 4 and values[4] is not None else float("nan")
                )
            if n_other > 0.0:
                raise IllegalArgumentException(
                    "BinaryClassificationEvaluator.areaUnderROC: labels must be binary "
                    "0/1 (found non-binary label values). Non-0/1 labels contaminate "
                    "Mann-Whitney midranks and can yield AUC outside [0, 1]."
                )
            if value != value:
                if (
                    n_pos <= 0.0
                    and n_neg <= 0.0
                    and ("array_element" in score_sql or "array_position" in score_sql)
                ):
                    raise IllegalArgumentException(
                        "BinaryClassificationEvaluator.areaUnderROC: no usable scores after "
                        "rawPrediction extract (dense array_element index 1 or sparse "
                        "size/indices/values at index 1). Binary vectors must have length "
                        "≥ 2 as [neg, pos] (dense list or sparse struct)."
                    )
                raise IllegalArgumentException(
                    "BinaryClassificationEvaluator.areaUnderROC: degenerate labels "
                    "(need at least one positive and one negative example)"
                )
            return value
        finally:
            with contextlib.suppress(Exception):
                frame._session.drop_temp_view(scored)
            with contextlib.suppress(Exception):
                frame._session.drop_temp_view(view)

    def _evaluate_area_under_pr(self, frame: Any) -> float:
        """Evaluate areaUnderPR with score-group average precision.

        Refuse empty, non-binary, or degenerate labels. Project scores to scalars. Process tied
        scores as groups because DataFusion cannot safely order complex array or struct
        expressions. Input order cannot change the result. Unusable vectors refuse.
        """
        label_name, score_sql, _ = self._resolve_score_sql(
            frame, verb="BinaryClassificationEvaluator.areaUnderPR"
        )
        label = _quote_ident(label_name)
        view = scratch_view_name(frame._session, "__repark_aupr_")
        scored = scratch_view_name(frame._session, "__repark_aupr_sc_")
        frame.createOrReplaceTempView(view)
        try:
            _require_nonempty_eval(frame, view, verb="BinaryClassificationEvaluator.areaUnderPR")
            scored_sql = (
                f"SELECT CAST({label} AS DOUBLE) AS label, ({score_sql}) AS score FROM {view}"
            )
            scored_frame = frame._spawn(frame._session.sql(scored_sql))
            scored_frame.createOrReplaceTempView(scored)
            sql = f"""
            WITH ordered AS (
              SELECT label, score
              FROM {scored}
              WHERE label IS NOT NULL AND score IS NOT NULL
            ),
            stats_labels AS (
              SELECT
                SUM(CASE WHEN label = 1.0 THEN 1.0 ELSE 0.0 END) AS n_pos,
                SUM(CASE WHEN label = 0.0 THEN 1.0 ELSE 0.0 END) AS n_neg,
                SUM(
                  CASE
                    WHEN label <> 0.0 AND label <> 1.0 THEN 1.0
                    ELSE 0.0
                  END
                ) AS n_other
              FROM ordered
            ),
            by_score AS (
              SELECT
                score,
                SUM(CASE WHEN label = 1.0 THEN 1.0 ELSE 0.0 END) AS n_pos_s,
                SUM(CASE WHEN label = 0.0 THEN 1.0 ELSE 0.0 END) AS n_neg_s
              FROM ordered
              GROUP BY score
            ),
            cum AS (
              SELECT
                n_pos_s,
                SUM(n_pos_s) OVER (
                  ORDER BY score DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cum_tp,
                SUM(n_neg_s) OVER (
                  ORDER BY score DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cum_fp
              FROM by_score
            ),
            ap AS (
              SELECT
                SUM(
                  CASE
                    WHEN n_pos_s > 0.0 THEN
                      n_pos_s * (cum_tp / NULLIF(cum_tp + cum_fp, 0.0))
                    ELSE 0.0
                  END
                ) AS sum_precision
              FROM cum
            )
            SELECT
              stats_labels.n_pos,
              stats_labels.n_neg,
              stats_labels.n_other,
              (ap.sum_precision / NULLIF(stats_labels.n_pos, 0.0)) AS metric
            FROM stats_labels CROSS JOIN ap
            """
            rows = list(frame._spawn(frame._session.sql(sql)).collect())
            if not rows:
                raise IllegalArgumentException("evaluator query returned no rows")
            row = rows[0]
            cells = row.asDict() if hasattr(row, "asDict") else {}
            if cells:
                n_other = float(cells.get("n_other") or 0.0)
                n_pos = float(cells.get("n_pos") or 0.0)
                n_neg = float(cells.get("n_neg") or 0.0)
                metric_cell = cells.get("metric")
                value = float(metric_cell) if metric_cell is not None else float("nan")
            else:
                values = list(row)
                n_pos = float(values[0]) if len(values) > 0 and values[0] is not None else 0.0
                n_neg = float(values[1]) if len(values) > 1 and values[1] is not None else 0.0
                n_other = float(values[2]) if len(values) > 2 and values[2] is not None else 0.0
                value = (
                    float(values[3]) if len(values) > 3 and values[3] is not None else float("nan")
                )
            if n_other > 0.0:
                raise IllegalArgumentException(
                    "BinaryClassificationEvaluator.areaUnderPR: labels must be binary "
                    "0/1 (found non-binary label values)"
                )
            if (
                n_pos <= 0.0
                and n_neg <= 0.0
                and ("array_element" in score_sql or "array_position" in score_sql)
            ):
                raise IllegalArgumentException(
                    "BinaryClassificationEvaluator.areaUnderPR: no usable scores after "
                    "rawPrediction extract (dense array_element index 1 or sparse "
                    "size/indices/values at index 1). Binary vectors must have length "
                    "≥ 2 as [neg, pos] (dense list or sparse struct)."
                )
            if n_pos <= 0.0 or n_neg <= 0.0 or value != value:
                raise IllegalArgumentException(
                    "BinaryClassificationEvaluator.areaUnderPR: degenerate labels "
                    "(need at least one positive and one negative example)"
                )
            return value
        finally:
            with contextlib.suppress(Exception):
                frame._session.drop_temp_view(scored)
            with contextlib.suppress(Exception):
                frame._session.drop_temp_view(view)


MULTICLASS_F1_SEED = (
    "MulticlassClassificationEvaluator.metricName='f1' requires per-label precision/recall "
    "aggregation; not implemented in v1. Use metricName='accuracy'. Seed → later unit "
    "(macro / weighted F1 plan aggregates)."
)


class MulticlassClassificationEvaluator(HasLabelCol, HasPredictionCol, Evaluator):
    """Evaluate multiclass accuracy only.

    The caller must set ``metricName="accuracy"``. The default ``f1`` is unsupported and
    ``evaluate`` refuses it.
    """

    def __init__(
        self,
        *,
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        metricName: str | None = None,  # noqa: N803
    ) -> None:
        """Initialize evaluator parameters."""
        super().__init__()
        self.metricName: Param[str] = Param(
            self,
            "metricName",
            "metric: accuracy | f1 (only accuracy in v1; f1 is STOP-loud)",
            TypeConverters.toString,
        )
        self._setDefault(metricName="f1")
        if labelCol is not None:
            self.setLabelCol(labelCol)
        if predictionCol is not None:
            self.setPredictionCol(predictionCol)
        if metricName is not None:
            self._set(metricName=metricName)

    def setMetricName(self, value: str) -> MulticlassClassificationEvaluator:
        """Set the metric name."""
        return self._set(metricName=value)

    def getMetricName(self) -> str:
        """Return the metric name."""
        return str(self.getOrDefault(self.metricName))

    def isLargerBetter(self) -> bool:
        """Return ``True`` because classification metrics are larger-is-better."""
        return True

    def evaluate(self, dataset: Any) -> float:
        """Evaluate accuracy or refuse unsupported F1."""
        frame = _require_repark_dataframe(
            dataset, verb="MulticlassClassificationEvaluator.evaluate"
        )
        metric = self.getMetricName().lower()
        if metric == "f1":
            raise UnsupportedOperationException(MULTICLASS_F1_SEED)
        if metric != "accuracy":
            raise IllegalArgumentException(
                f"MulticlassClassificationEvaluator.metricName must be accuracy|f1, got {metric!r}"
            )
        label = _quote_ident(self.getLabelCol())
        pred = _quote_ident(self.getPredictionCol())
        view = scratch_view_name(frame._session, "__repark_meval_")
        frame.createOrReplaceTempView(view)
        try:
            _require_nonempty_eval(frame, view, verb="MulticlassClassificationEvaluator.evaluate")
            sql = (
                f"SELECT avg(CASE WHEN {pred} = {label} THEN 1.0 ELSE 0.0 END) AS metric "
                f"FROM {view}"
            )
            return _collect_scalar(frame, sql)
        finally:
            with contextlib.suppress(Exception):
                frame._session.drop_temp_view(view)


__all__ = [
    "AUC_PR_SEED",
    "AUC_VECTOR_RAW_GAP",
    "MULTICLASS_F1_SEED",
    "BinaryClassificationEvaluator",
    "Evaluator",
    "MulticlassClassificationEvaluator",
    "RegressionEvaluator",
]
