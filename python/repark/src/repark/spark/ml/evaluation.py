"""ML evaluators — pure aggregate plan queries (M3/M5/M6/M7).

``areaUnderROC`` is plan-built Mann-Whitney rank-sum (M5); ``areaUnderPR`` is plan-built
average precision over ranked scores (M6). Vector / array ``rawPrediction`` positive-class
score extraction uses ``array_element`` on the Arrow FixedSizeList / list path (M6) and
sparse-aware index lookup on the ``{size,indices,values}`` struct path (M7).
"""

from __future__ import annotations

import contextlib
import uuid
from typing import Any

from repark.errors import IllegalArgumentException, UnsupportedOperationException

# === r23 QI1: idents ===
from repark.spark._idents import quote_ident as _quote_ident
from repark.spark.ml.base import _require_repark_dataframe
from repark.spark.ml.param import HasLabelCol, HasPredictionCol, Param, Params, TypeConverters

# === r20 M7: evaluators / sparse rawPrediction ===

# Historical M5 seed string — kept exported for residual pins; areaUnderPR is implemented (M6).
AUC_PR_SEED = (
    "areaUnderPR is plan-built average precision (M6): precision-at-each-positive-hit "
    "averaged over n_pos, via window RANK on score DESC. areaUnderROC remains Mann-Whitney."
)

# Historical M5 gap string — dense (M6) + sparse struct (M7) rawPrediction are extracted;
# remaining message covers non-list / non-sparse-struct nested layouts only.
AUC_VECTOR_RAW_GAP = (
    "areaUnderROC/areaUnderPR: rawPrediction score column is not a scalar DOUBLE, not a "
    "dense list/FixedSizeList of length >= 2 (positive-class index 1), and not a sparse "
    "VectorUDT struct {size,indices,values} with size >= 2. Unsupported nested layouts "
    "remain a plan gap — provide a scalar score, a dense [neg, pos] array, or a sparse "
    "struct via rawPredictionCol."
)


def _sparse_positive_class_score_sql(score_quoted: str) -> str:
    """SQL: positive-class score (index 1) from sparse VectorUDT struct column.

    Missing index → ``0.0`` (sparse zero). Null cell / null ``size`` / ``size < 2`` →
    NULL so null rows are filtered (not densified to score 0.0) and short vectors hit
    the short-vector refuse path (not a silent all-zero / degenerate-labels path).
    """
    # array_position is 1-based; element_at is 1-based; missing position → NULL → COALESCE 0.
    # Guard null struct / null size *before* the ELSE extract so a null rawPrediction
    # cannot densify to 0.0 via COALESCE (octo M7 C3 null-sparse).
    return (
        f"CASE WHEN {score_quoted} IS NULL OR {score_quoted}.size IS NULL "
        f"OR {score_quoted}.size < 2 THEN CAST(NULL AS DOUBLE) "
        f"ELSE CAST(COALESCE("
        f"element_at({score_quoted}.values, array_position({score_quoted}.indices, 1)), "
        f"0.0) AS DOUBLE) END"
    )


def _collect_scalar(frame: Any, sql: str) -> float:
    """Run aggregate SQL and return the first cell as float."""
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
    """Refuse empty evaluation frames (avg/sum on zero rows → NULL/NaN)."""
    rows = list(frame._spawn(frame._session.sql(f"SELECT COUNT(*) AS n FROM {view}")).collect())
    if not rows:
        raise IllegalArgumentException(f"{verb}: count query returned no rows")
    values = list(rows[0].asDict().values()) if hasattr(rows[0], "asDict") else list(rows[0])
    count = int(values[0] or 0)
    if count == 0:
        raise IllegalArgumentException(f"{verb}: empty dataset (0 rows) — cannot compute metric")


class Evaluator(Params):
    """Base evaluator (Spark ``Evaluator``)."""

    def evaluate(self, dataset: Any) -> float:
        """Compute metric on ``dataset``."""
        raise NotImplementedError

    def isLargerBetter(self) -> bool:
        """Whether larger metric values are better."""
        return True


class RegressionEvaluator(HasLabelCol, HasPredictionCol, Evaluator):
    """RMSE / MSE / MAE / R2 via aggregate SQL."""

    def __init__(
        self,
        *,
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        metricName: str | None = None,  # noqa: N803
    ) -> None:
        """Optional kwargs."""
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
        """Set metric name."""
        return self._set(metricName=value)

    def getMetricName(self) -> str:
        """Get metric name."""
        return str(self.getOrDefault(self.metricName))

    def isLargerBetter(self) -> bool:
        """R2 is larger-better; error metrics are smaller-better.

        Case-insensitive (matches ``evaluate()`` which lowercases metricName) —
        ``metricName="R2"`` must not take the min path (octo C2-L-001).
        """
        return self.getMetricName().lower() == "r2"

    def evaluate(self, dataset: Any) -> float:
        """Plan-aggregate metric; no Python row loops for learning."""
        frame = _require_repark_dataframe(dataset, verb="RegressionEvaluator.evaluate")
        label = _quote_ident(self.getLabelCol())
        pred = _quote_ident(self.getPredictionCol())
        metric = self.getMetricName().lower()
        view = f"__repark_reval_{uuid.uuid4().hex[:12]}"
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
                # 1 - SS_res / SS_tot
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
    """Binary metrics: accuracy + plan-built areaUnderROC / areaUnderPR (M5/M6)."""

    def __init__(
        self,
        *,
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        metricName: str | None = None,  # noqa: N803
        rawPredictionCol: str | None = None,  # noqa: N803
    ) -> None:
        """Optional kwargs. Default Spark metric is areaUnderROC (M5 rank-sum plan)."""
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
        """Set metric name."""
        return self._set(metricName=value)

    def getMetricName(self) -> str:
        """Get metric name."""
        return str(self.getOrDefault(self.metricName))

    def setRawPredictionCol(self, value: str) -> BinaryClassificationEvaluator:
        """Set raw/score column name for ranking metrics."""
        return self._set(rawPredictionCol=value)

    def getRawPredictionCol(self) -> str:
        """Get raw/score column name."""
        return str(self.getOrDefault(self.rawPredictionCol))

    def evaluate(self, dataset: Any) -> float:
        """Accuracy / areaUnderROC / areaUnderPR via plan aggregates (M3/M5/M6)."""
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
        view = f"__repark_beval_{uuid.uuid4().hex[:12]}"
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
        """Resolve score column name + SQL expression (scalar or positive-class extract).

        Returns ``(label_name, score_sql_expr, score_view_or_none)``. When the score is a
        dense list/array (VectorUDT FixedSizeList path), ``score_sql_expr`` is
        ``array_element(col, 1)`` (0-based positive-class index for binary ``[neg, pos]``).
        Sparse / non-list layouts raise :class:`UnsupportedOperationException`.
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
        # Schema-driven extract; narrow except so we never densify nested types via CAST
        # and never swallow UnsupportedOperationException (octo M7 C3/C4 unwrap).
        score_type = None
        type_name = ""
        try:
            field_types = {field.name: field.dataType for field in frame.schema.fields}
            score_type = field_types.get(score_name)
            type_name = type(score_type).__name__ if score_type is not None else ""
        except (AttributeError, TypeError, ValueError):
            # Schema unavailable — keep scalar CAST; plan will refuse non-numeric loud.
            type_name = ""
        # Dense ML vector / ArrayType → list; extract positive-class index 1 (M6).
        if "Array" in type_name or "List" in type_name or "Vector" in type_name:
            # DataFusion array_element is 0-based; Spark rawPrediction[1] is positive class.
            score_sql = f"CAST(array_element({score_quoted}, 1) AS DOUBLE)"
        elif "Struct" in type_name:
            # M7: sparse VectorUDT {size, indices, values} — index-1 positive class.
            # Confirm sparse-shaped fields when schema exposes them; otherwise still try
            # the sparse extract (createDataFrame sparse path always uses these names).
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
            # Non-vector nested / scalar non-numeric layouts: refuse with the gap string
            # (never CAST→engine "Unsupported CAST from Map…" — octo M7 C3).
            raise UnsupportedOperationException(
                f"{verb}: score column {score_name!r} has type {type_name} which is not a "
                f"scalar DOUBLE, dense list, or sparse VectorUDT. {AUC_VECTOR_RAW_GAP}"
            )
        return label_name, score_sql, None

    def _evaluate_area_under_roc(self, frame: Any) -> float:
        """Mann-Whitney rank-sum AUC via window RANK + aggregate (M5 plan shape).

        Equivalent to trapezoidal ROC for binary labels with a scalar score column.
        Prefers ``rawPredictionCol`` when present on the frame; else ``predictionCol``.
        Dense list/array rawPrediction extracts positive-class score at index 1 (M6);
        sparse struct extract is M7. Scores are projected to a scalar column before
        ``ORDER BY`` so complex sparse/array expressions do not break DF window sorting.
        """
        label_name, score_sql, _ = self._resolve_score_sql(
            frame, verb="BinaryClassificationEvaluator.areaUnderROC"
        )
        label = _quote_ident(label_name)
        view = f"__repark_auc_{uuid.uuid4().hex[:12]}"
        scored = f"__repark_auc_sc_{uuid.uuid4().hex[:12]}"
        frame.createOrReplaceTempView(view)
        try:
            _require_nonempty_eval(frame, view, verb="BinaryClassificationEvaluator.areaUnderROC")
            # Project extract to a simple DOUBLE column so ROW_NUMBER ORDER BY is scalar
            # (complex get_field/array_position ORDER BY fails DataFusion SanityCheckPlan).
            scored_sql = (
                f"SELECT CAST({label} AS DOUBLE) AS label, ({score_sql}) AS score FROM {view}"
            )
            scored_frame = frame._spawn(frame._session.sql(scored_sql))
            scored_frame.createOrReplaceTempView(scored)
            # Average ranks for ties: rank() leaves gaps; mean of peer row_numbers is
            # the Mann-Whitney midrank. Labels must be 0/1 binary — non-binary labels
            # contaminate midranks without counting in n_pos/n_neg and can yield AUC
            # outside [0, 1] (octo M5 C1-F-AUC-NONBIN).
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
            if value != value:  # NaN — degenerate (no pos or no neg) or all scores null
                if (
                    n_pos <= 0.0
                    and n_neg <= 0.0
                    and ("array_element" in score_sql or "array_position" in score_sql)
                ):
                    # Short / empty dense or size<2 sparse → NULL scores → all rows
                    # filtered; "degenerate labels" misleads (octo M6 C4 / M7 sparse).
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
        """Average precision (areaUnderPR) via plan-built score-group rank statistics (M6).

        Distinct scores are processed high→low as threshold groups (not per-row
        ``ROW_NUMBER``). For each score group that contains positives, contribution is
        ``n_pos_at_score * precision_after_group`` where
        ``precision_after_group = cum_tp / (cum_tp + cum_fp)``; AP = sum / n_pos.

        Grouping by score makes AP **order-independent under ties** (per-row
        ``ROW_NUMBER() … DESC`` followed the physical row order among equal scores and
        could return AP ∈ {0.42, 0.5, 0.83, 1.0} for the same multiset — octo M6 C3).
        Unique-score rankings match classic mean precision-at-hit. Perfect ranking of
        positives above negatives → 1.0. Same binary-label / degenerate guards as
        areaUnderROC.
        """
        label_name, score_sql, _ = self._resolve_score_sql(
            frame, verb="BinaryClassificationEvaluator.areaUnderPR"
        )
        label = _quote_ident(label_name)
        view = f"__repark_aupr_{uuid.uuid4().hex[:12]}"
        scored = f"__repark_aupr_sc_{uuid.uuid4().hex[:12]}"
        frame.createOrReplaceTempView(view)
        try:
            _require_nonempty_eval(frame, view, verb="BinaryClassificationEvaluator.areaUnderPR")
            # Scalar score projection first (same SanityCheckPlan reason as ROC / M7 sparse).
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


# Named seed for multiclass F1 (must not silently return accuracy — octo C1-L-002).
MULTICLASS_F1_SEED = (
    "MulticlassClassificationEvaluator.metricName='f1' requires per-label precision/recall "
    "aggregation; not implemented in v1. Use metricName='accuracy'. Seed → later unit "
    "(macro / weighted F1 plan aggregates)."
)


class MulticlassClassificationEvaluator(HasLabelCol, HasPredictionCol, Evaluator):
    """Accuracy via plan aggregate; default Spark metric ``f1`` is loud-unsupported in v1."""

    def __init__(
        self,
        *,
        labelCol: str | None = None,  # noqa: N803
        predictionCol: str | None = None,  # noqa: N803
        metricName: str | None = None,  # noqa: N803
    ) -> None:
        """Optional kwargs. Default Spark metric is ``f1`` (unsupported here — use accuracy)."""
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
        """Set metric name."""
        return self._set(metricName=value)

    def getMetricName(self) -> str:
        """Get metric name."""
        return str(self.getOrDefault(self.metricName))

    def isLargerBetter(self) -> bool:
        """Accuracy and F1 are larger-better."""
        return True

    def evaluate(self, dataset: Any) -> float:
        """Accuracy via plan aggregate; ``f1`` refuses loud (never silent accuracy)."""
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
        view = f"__repark_meval_{uuid.uuid4().hex[:12]}"
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
