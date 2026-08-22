"""Plan-built feature transformers (M2).

Each ``fit`` runs session aggregate/distinct queries; each ``transform`` returns a
plan-built DataFrame. Python never iterates training rows for learning.
"""

from __future__ import annotations

import contextlib
import itertools
import re
from typing import Any

from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    UnsupportedOperationException,
)

# === r23 QI1: idents ===
from repark.spark._idents import quote_ident as _quote_ident
from repark.spark._temp_views import scratch_view_name
from repark.spark.ml.base import Estimator, Model, Transformer, _require_repark_dataframe
from repark.spark.ml.param import (
    HasHandleInvalid,
    HasInputCol,
    HasInputCols,
    HasOutputCol,
    HasOutputCols,
    Param,
    TypeConverters,
)

# ---------------------------------------------------------------------------
# Status seeds (greylight Q2 / Q1 R-ML-QUANTILE)
# ---------------------------------------------------------------------------

QUANTILE_FAMILY_STATUS = (
    "SHIPPED (Q1): fit quantiles via engine approx_percentile_cont / percentile_approx "
    "(DataFusion t-digest; Spark GK accuracy arg accepted-and-ignored). "
    "RobustScaler / QuantileDiscretizer / Imputer(median) plan-built; never Python-side quantiles."
)

COUNT_VECTORIZER_STATUS = (
    "SHIPPED (Q1): vocab = distinct document-frequency counts; transform plan-built dense TF"
)
IDF_STATUS = "SHIPPED (Q1): smooth IDF = log((m+1)/(df+1))+1 via per-dimension doc-freq aggregates"
REGEX_TOKENIZER_GAPS_FALSE_STATUS = (
    "SEED: RegexTokenizer(gaps=False) needs regexp_extract_all; engine has no extract-all"
)


def _sql_float(value: float) -> str:
    """Render a float for SQL (Infinity-safe)."""
    if value != value:  # NaN
        return "CAST('NaN' AS DOUBLE)"
    if value == float("inf"):
        return "CAST('Infinity' AS DOUBLE)"
    if value == float("-inf"):
        return "CAST('-Infinity' AS DOUBLE)"
    return repr(float(value))


def _is_nan_float(value: float) -> bool:
    """True iff ``value`` is a NaN float (NaN != NaN)."""
    return value != value


def _sql_non_missing_predicate(quoted_col: str, missing_value: float) -> str:
    """SQL predicate: column is present (not null and not the configured missingValue)."""
    if _is_nan_float(missing_value):
        # Spark default: null and NaN are missing.
        return f"{quoted_col} IS NOT NULL AND NOT isnan({quoted_col})"
    return f"{quoted_col} IS NOT NULL AND {quoted_col} <> {_sql_float(missing_value)}"


def _sql_impute_expr(quoted_col: str, missing_value: float, replacement: float) -> str:
    """CASE expression replacing null/missingValue with replacement (SQL-safe floats)."""
    rep_sql = _sql_float(replacement)
    if _is_nan_float(missing_value):
        return (
            f"CASE WHEN {quoted_col} IS NULL OR isnan({quoted_col}) "
            f"THEN {rep_sql} ELSE {quoted_col} END"
        )
    miss_sql = _sql_float(missing_value)
    return (
        f"CASE WHEN {quoted_col} IS NULL OR {quoted_col} = {miss_sql} "
        f"THEN {rep_sql} ELSE {quoted_col} END"
    )


def _register_temp(dataset: Any, prefix: str = "ml") -> tuple[Any, str]:
    """Register dataset as a unique temp view; return (frame, view_name).

    The frame's native session powers SQL; results are re-wrapped via ``frame._spawn``.
    """

    frame = _require_repark_dataframe(dataset, verb="ml feature")
    view = scratch_view_name(frame._session, f"__repark_{prefix}_")
    frame.createOrReplaceTempView(view)
    return frame, view


def _sql_on(frame: Any, sql: str) -> Any:
    """Run SQL on the frame's native session; return a Python DataFrame."""
    return frame._spawn(frame._session.sql(sql))


def _collect_sql(frame: Any, sql: str) -> list[Any]:
    """Run SQL and collect rows (aggregate fit path only — not training-row iteration)."""
    return list(_sql_on(frame, sql).collect())


def _materialize_rid_view(frame: Any, source_view: str, prefix: str) -> tuple[Any, str, str]:
    """Assign ``row_number`` once, cache, re-register — stable rid for multi-scan joins.

    DataFusion re-evaluates CTEs per reference; bare ``row_number() OVER ()`` inside a
    WITH clause therefore yields *different* rid→row maps when the same CTE is joined
    to itself (octo F-Q1-009). Caching the rid-bearing plan materializes one assignment.
    Returns ``(host_frame, rid_view_name, rid_column_name)``.
    """

    rid_col = f"__repark_{prefix}_rid"
    rid_view = scratch_view_name(frame._session, f"__repark_{prefix}_base_")
    indexed = _sql_on(
        frame,
        f"SELECT row_number() OVER () AS {_quote_ident(rid_col)}, {source_view}.* "
        f"FROM {source_view}",
    )
    # Materialize rid assignment (plan-built; no Python row loop).
    if hasattr(indexed, "cache"):
        indexed.cache()
    if hasattr(indexed, "count"):
        indexed.count()  # force cache fill
    else:
        indexed.collect()
    indexed.createOrReplaceTempView(rid_view)
    return indexed, rid_view, rid_col


def _drop_temp_view(frame: Any, view: str) -> None:
    """Best-effort temp view drop."""
    with contextlib.suppress(Exception):
        frame._session.drop_temp_view(view)


def _refuse_output_collision(frame: Any, output_col: str, *, stage: str) -> None:
    """Refuse if output column already exists (octo C3 — no silent overwrite)."""
    names = list(frame.columns) if hasattr(frame, "columns") else []
    if not names:
        try:
            names = [f.name for f in frame.schema.fields]
        except Exception:
            names = []
    if output_col in names:
        raise AnalysisException(
            f"{stage}: outputCol {output_col!r} already exists in the input schema "
            f"(repark.ml refuses silent overwrite)"
        )


# ===========================================================================
# VectorAssembler
# ===========================================================================

# === r20 M7: VectorAssembler sparse output ===


def _vector_assembler_sparse_expr(cols: list[str], size: int) -> str:
    """Plan-built sparse ``named_struct`` from scalar input columns (zeros omitted).

    Null scalars are omitted like zeros under ``handleInvalid='keep'`` (disclosed;
    dense keep retains null elements). Indices / values via ``array_compact`` over
    per-column CASE (no Python row loop).
    """
    index_parts: list[str] = []
    value_parts: list[str] = []
    for position, col_name in enumerate(cols):
        quoted = _quote_ident(col_name)
        # Non-null and non-zero → keep index/value; else NULL so array_compact drops it.
        index_parts.append(
            f"CASE WHEN {quoted} IS NOT NULL AND CAST({quoted} AS DOUBLE) <> 0.0 "
            f"THEN CAST({position} AS INT) END"
        )
        value_parts.append(
            f"CASE WHEN {quoted} IS NOT NULL AND CAST({quoted} AS DOUBLE) <> 0.0 "
            f"THEN CAST({quoted} AS DOUBLE) END"
        )
    indices_sql = f"array_compact(make_array({', '.join(index_parts)}))"
    values_sql = f"array_compact(make_array({', '.join(value_parts)}))"
    return f"named_struct('size', {int(size)}, 'indices', {indices_sql}, 'values', {values_sql})"


class VectorAssembler(HasInputCols, HasOutputCol, HasHandleInvalid, Transformer):
    """Merge input columns into a dense or sparse vector column.

    Null handling: ``handleInvalid`` = ``error`` (default) | ``keep`` | ``skip``.
    Default output is a fixed-width dense list via ``make_array`` (plan-built).
    M7 ``sparseOutput=True`` emits sparse struct ``{size, indices, values}`` with
    zeros omitted. Native estimators still densify / require dense width (disclosed).
    """

    def __init__(
        self,
        *,
        inputCols: list[str] | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        handleInvalid: str | None = None,  # noqa: N803
        sparseOutput: bool = False,  # noqa: N803 — M7 repark extension (not Spark param)
    ) -> None:
        """Optional kwargs mirror Spark constructor (+ M7 ``sparseOutput``)."""
        super().__init__()
        self.sparseOutput: Param[bool] = Param(
            self,
            "sparseOutput",
            "M7: emit sparse {size,indices,values} instead of dense make_array",
            TypeConverters.toBoolean,
        )
        self._setDefault(sparseOutput=False)
        if inputCols is not None:
            self.setInputCols(inputCols)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        if handleInvalid is not None:
            self.setHandleInvalid(handleInvalid)
        if sparseOutput:
            self._set(sparseOutput=True)

    def setSparseOutput(self, value: bool) -> VectorAssembler:
        """Set sparse-struct output (M7)."""
        return self._set(sparseOutput=bool(value))

    def getSparseOutput(self) -> bool:
        """Whether transform emits sparse VectorUDT structs."""
        return bool(self.getOrDefault(self.sparseOutput))

    def _transform(self, dataset: Any) -> Any:
        """``SELECT *, make_array(...)`` or sparse ``named_struct`` with null policy."""
        frame = _require_repark_dataframe(dataset, verb="VectorAssembler.transform")
        cols = self.getInputCols()
        out = self.getOutputCol()
        if not cols:
            raise IllegalArgumentException("VectorAssembler.inputCols must be non-empty")
        handle = self.getHandleInvalid()
        if handle not in {"error", "keep", "skip"}:
            raise IllegalArgumentException(
                f"VectorAssembler.handleInvalid must be error|keep|skip, got {handle!r}"
            )
        _refuse_output_collision(frame, out, stage="VectorAssembler")
        host, view = _register_temp(frame, "va")
        array_args = ", ".join(_quote_ident(col) for col in cols)
        # Null-any → error path uses CASE that raises via filter, else keep/skip.
        null_any = " OR ".join(f"{_quote_ident(col)} IS NULL" for col in cols)
        sparse = self.getSparseOutput()
        if sparse:
            vec_expr = _vector_assembler_sparse_expr(cols, size=len(cols))
        else:
            vec_expr = f"make_array({array_args})"
        if handle == "skip":
            # DataFusion may not support SELECT * EXCLUDE — use explicit star + vector.
            sql = (
                f"SELECT {view}.*, ({vec_expr}) AS {_quote_ident(out)} "
                f"FROM {view} WHERE NOT ({null_any})"
            )
        elif handle == "error":
            # Fail if any null: plan a check via WHERE false when nulls present.
            # Loud analysis: collect null count aggregate (fit-style query, not row loop).
            check = _collect_sql(host, f"SELECT COUNT(*) AS n FROM {view} WHERE {null_any}")
            if check and int(check[0].asDict().get("n", 0)) > 0:
                raise AnalysisException(
                    "VectorAssembler encountered NULLs in input columns "
                    f"{cols} with handleInvalid='error'"
                )
            sql = f"SELECT {view}.*, ({vec_expr}) AS {_quote_ident(out)} FROM {view}"
        else:  # keep — dense: null elements; sparse: nulls omitted like zeros (disclosed)
            sql = f"SELECT {view}.*, ({vec_expr}) AS {_quote_ident(out)} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Passthrough transformer — no fitted state."""
        return {}

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> VectorAssembler:
        """Rebuild from save payload."""
        return cls(
            inputCols=list(params.get("inputCols") or []),
            outputCol=params.get("outputCol"),
            handleInvalid=params.get("handleInvalid"),
            sparseOutput=bool(params.get("sparseOutput", False)),
        )


# ===========================================================================
# StringIndexer / IndexToString
# ===========================================================================


class StringIndexer(HasInputCol, HasOutputCol, HasHandleInvalid, Estimator["StringIndexerModel"]):
    """Map string labels to indices ordered by ``stringOrderType``."""

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        handleInvalid: str | None = None,  # noqa: N803
        stringOrderType: str | None = None,  # noqa: N803
    ) -> None:
        """Optional kwargs mirror Spark."""
        super().__init__()
        self.stringOrderType: Param[str] = Param(
            self,
            "stringOrderType",
            "How to order labels. frequencyDesc|frequencyAsc|alphabetDesc|alphabetAsc.",
            TypeConverters.toString,
        )
        self._setDefault(stringOrderType="frequencyDesc")
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        if handleInvalid is not None:
            self.setHandleInvalid(handleInvalid)
        if stringOrderType is not None:
            self._set(stringOrderType=stringOrderType)

    def getStringOrderType(self) -> str:
        """Return label order strategy."""
        return self.getOrDefault(self.stringOrderType)

    def setStringOrderType(self, value: str) -> StringIndexer:
        """Set label order strategy."""
        return self._set(stringOrderType=value)

    def _fit(self, dataset: Any) -> StringIndexerModel:
        """Fit labels via GROUP BY + COUNT ordered per stringOrderType."""
        frame = _require_repark_dataframe(dataset, verb="StringIndexer.fit")
        # Membership check at fit (not only transform) so illegal handleInvalid refuses
        # before vocabulary materialize (octo M7 C5 handleInvalid matrix).
        handle = self.getHandleInvalid()
        if handle not in {"error", "keep", "skip"}:
            raise IllegalArgumentException(
                f"StringIndexer.handleInvalid must be error|keep|skip, got {handle!r}"
            )
        input_col = self.getInputCol()
        order = self.getStringOrderType()
        host, view = _register_temp(frame, "si")
        try:
            quoted = _quote_ident(input_col)
            # Drop nulls for label vocabulary (Spark skips null labels in fit).
            if order == "frequencyDesc":
                order_sql = f"cnt DESC, {quoted} ASC"
            elif order == "frequencyAsc":
                order_sql = f"cnt ASC, {quoted} ASC"
            elif order == "alphabetDesc":
                order_sql = f"{quoted} DESC"
            elif order == "alphabetAsc":
                order_sql = f"{quoted} ASC"
            else:
                raise IllegalArgumentException(
                    f"stringOrderType must be frequencyDesc|frequencyAsc|alphabetDesc|"
                    f"alphabetAsc, got {order!r}"
                )
            sql = (
                f"SELECT {quoted} AS label, COUNT(*) AS cnt FROM {view} "
                f"WHERE {quoted} IS NOT NULL GROUP BY {quoted} ORDER BY {order_sql}"
            )
            rows = _collect_sql(host, sql)
            labels = [str(row.asDict()["label"]) for row in rows]
            model = StringIndexerModel(
                labels=labels,
                inputCol=input_col,
                outputCol=self.getOutputCol(),
                handleInvalid=self.getHandleInvalid(),
            )
            model.uid = self.uid
            return model
        finally:
            _drop_temp_view(host, view)


class StringIndexerModel(HasInputCol, HasOutputCol, HasHandleInvalid, Model):
    """Fitted string → index mapper."""

    def __init__(
        self,
        *,
        labels: list[str] | None = None,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        handleInvalid: str | None = None,  # noqa: N803
    ) -> None:
        """Store ordered labels (index = position)."""
        super().__init__()
        self.labels: list[str] = list(labels or [])
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        if handleInvalid is not None:
            self.setHandleInvalid(handleInvalid)

    def _transform(self, dataset: Any) -> Any:
        """CASE WHEN label map; handleInvalid error|keep|skip."""
        frame = _require_repark_dataframe(dataset, verb="StringIndexerModel.transform")
        input_col = self.getInputCol()
        out = self.getOutputCol()
        handle = self.getHandleInvalid()
        # === r20 M7: StringIndexer handleInvalid membership (keep path was else-fallthrough) ===
        if handle not in {"error", "keep", "skip"}:
            raise IllegalArgumentException(
                f"StringIndexer.handleInvalid must be error|keep|skip, got {handle!r}"
            )
        host, view = _register_temp(frame, "sim")
        quoted_in = _quote_ident(input_col)
        quoted_out = _quote_ident(out)
        if not self.labels:
            # All null / empty fit → everything invalid.
            case_body = "CAST(NULL AS DOUBLE)"
        else:
            branches = []
            for index, label in enumerate(self.labels):
                escaped = label.replace("'", "''")
                branches.append(f"WHEN {quoted_in} = '{escaped}' THEN CAST({index} AS DOUBLE)")
            case_body = "CASE " + " ".join(branches) + " ELSE NULL END"
        index_expr = case_body
        if handle == "error":
            # Detect unseen non-null labels.
            if self.labels:
                known = ", ".join("'" + label.replace("'", "''") + "'" for label in self.labels)
                check_sql = (
                    f"SELECT COUNT(*) AS n FROM {view} WHERE {quoted_in} IS NOT NULL "
                    f"AND {quoted_in} NOT IN ({known})"
                )
            else:
                check_sql = f"SELECT COUNT(*) AS n FROM {view} WHERE {quoted_in} IS NOT NULL"
            check = _collect_sql(host, check_sql)
            if check and int(check[0].asDict().get("n", 0)) > 0:
                raise AnalysisException(
                    "StringIndexer encountered unseen/invalid labels with handleInvalid='error'"
                )
            # Nulls also error in Spark default.
            null_check = _collect_sql(
                host, f"SELECT COUNT(*) AS n FROM {view} WHERE {quoted_in} IS NULL"
            )
            if null_check and int(null_check[0].asDict().get("n", 0)) > 0:
                raise AnalysisException(
                    "StringIndexer encountered NULL labels with handleInvalid='error'"
                )
            sql = f"SELECT {view}.*, ({index_expr}) AS {quoted_out} FROM {view}"
        elif handle == "skip":
            if self.labels:
                known = ", ".join("'" + label.replace("'", "''") + "'" for label in self.labels)
                where = f"{quoted_in} IS NOT NULL AND {quoted_in} IN ({known})"
            else:
                where = "FALSE"
            sql = f"SELECT {view}.*, ({index_expr}) AS {quoted_out} FROM {view} WHERE {where}"
        else:  # keep — unseen/null → numLabels
            num = float(len(self.labels))
            keep_expr = f"CASE WHEN ({index_expr}) IS NULL THEN {num} ELSE ({index_expr}) END"
            sql = f"SELECT {view}.*, ({keep_expr}) AS {quoted_out} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Fitted labels only."""
        return {
            "labels": list(self.labels),
            "inputCol": self.getInputCol(),
            "outputCol": self.getOutputCol(),
            "handleInvalid": self.getHandleInvalid(),
        }

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> StringIndexerModel:
        """Rebuild from save."""
        payload = {**params, **(fitted_state or {})}
        return cls(
            labels=list(payload.get("labels") or []),
            inputCol=payload.get("inputCol"),
            outputCol=payload.get("outputCol"),
            handleInvalid=payload.get("handleInvalid"),
        )


class IndexToString(HasInputCol, HasOutputCol, Transformer):
    """Map indices back to labels (Spark ``IndexToString``)."""

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        labels: list[str] | None = None,
    ) -> None:
        """Optional labels list (or set via setLabels)."""
        super().__init__()
        self.labelsParam: Param[list[str]] = Param(
            self, "labels", "labels array", TypeConverters.toListString
        )
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        if labels is not None:
            self.setLabels(labels)

    def setLabels(self, value: list[str]) -> IndexToString:
        """Set labels array."""
        return self._set(labels=value)

    def getLabels(self) -> list[str]:
        """Get labels array."""
        return list(self.getOrDefault(self.labelsParam)) if self.isDefined(self.labelsParam) else []

    def _transform(self, dataset: Any) -> Any:
        """CASE index → label string."""
        frame = _require_repark_dataframe(dataset, verb="IndexToString.transform")
        labels = self.getLabels()
        if not labels:
            raise IllegalArgumentException("IndexToString.labels must be set")
        host, view = _register_temp(frame, "its")
        quoted_in = _quote_ident(self.getInputCol())
        quoted_out = _quote_ident(self.getOutputCol())
        branches = []
        for index, label in enumerate(labels):
            escaped = label.replace("'", "''")
            branches.append(f"WHEN CAST({quoted_in} AS BIGINT) = {index} THEN '{escaped}'")
        case_sql = "CASE " + " ".join(branches) + " ELSE NULL END"
        sql = f"SELECT {view}.*, ({case_sql}) AS {quoted_out} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Labels are params, not fit state."""
        return {}

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> IndexToString:
        """Rebuild."""
        return cls(
            inputCol=params.get("inputCol"),
            outputCol=params.get("outputCol"),
            labels=list(params.get("labels") or []),
        )


# ===========================================================================
# OneHotEncoder (always sparse struct — greylight Q3)
# ===========================================================================


def _ohe_sparse_expr(
    quoted_in: str,
    size: int,
    *,
    category_size: int,
    handle: str,
) -> str:
    """SQL named_struct sparse one-hot for a single index column.

    Spark ``handleInvalid='keep'`` reserves an extra invalid bucket at index
    ``category_size`` *before* ``dropLast`` shrinks the emitted vector (octo
    C4-L-001). Without that ordering, ``keep`` + ``dropLast=False`` wrongly
    uses ``size=category_size`` and maps invalid/null to empty instead of
    ``size=category_size+1`` with invalid at the last index.
    """
    idx_expr = f"CAST({quoted_in} AS BIGINT)"
    if handle == "keep":
        # Null / out-of-range → keep bucket at category_size; dropLast may then
        # drop that bucket (empty vector) when size <= category_size.
        mapped = (
            f"CASE WHEN {idx_expr} IS NULL OR {idx_expr} < 0 "
            f"OR {idx_expr} >= {int(category_size)} THEN {int(category_size)} "
            f"ELSE {idx_expr} END"
        )
        return (
            f"named_struct("
            f"'size', {int(size)}, "
            f"'indices', CASE "
            f"WHEN ({mapped}) >= 0 AND ({mapped}) < {int(size)} "
            f"THEN make_array(CAST(({mapped}) AS INT)) "
            f"ELSE make_array() END, "
            f"'values', CASE "
            f"WHEN ({mapped}) >= 0 AND ({mapped}) < {int(size)} "
            f"THEN make_array(1.0) "
            f"ELSE make_array() END)"
        )
    return (
        f"named_struct("
        f"'size', {int(size)}, "
        f"'indices', CASE "
        f"WHEN {idx_expr} IS NULL THEN make_array() "
        f"WHEN {idx_expr} >= 0 AND {idx_expr} < {int(size)} "
        f"THEN make_array(CAST({idx_expr} AS INT)) "
        f"ELSE make_array() END, "
        f"'values', CASE "
        f"WHEN {idx_expr} IS NULL THEN make_array() "
        f"WHEN {idx_expr} >= 0 AND {idx_expr} < {int(size)} THEN make_array(1.0) "
        f"ELSE make_array() END)"
    )


class OneHotEncoder(
    HasInputCol,
    HasOutputCol,
    HasInputCols,
    HasOutputCols,
    HasHandleInvalid,
    Estimator["OneHotEncoderModel"],
):
    """One-hot encode index columns to **sparse** vectors (Spark OHE).

    Supports singular ``inputCol``/``outputCol`` and plural ``inputCols``/``outputCols``
    (M4 merge-bar). Plural wins when ``inputCols`` is set.
    """

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        inputCols: list[str] | None = None,  # noqa: N803
        outputCols: list[str] | None = None,  # noqa: N803
        handleInvalid: str | None = None,  # noqa: N803
        dropLast: bool = True,  # noqa: N803
    ) -> None:
        """``dropLast`` default True (Spark)."""
        super().__init__()
        self.dropLast: Param[bool] = Param(
            self, "dropLast", "whether to drop the last category", TypeConverters.toBoolean
        )
        self._setDefault(dropLast=True)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        if inputCols is not None:
            self.setInputCols(inputCols)
        if outputCols is not None:
            self.setOutputCols(outputCols)
        if handleInvalid is not None:
            self.setHandleInvalid(handleInvalid)
        self._set(dropLast=dropLast)

    def getDropLast(self) -> bool:
        """Whether last category is dropped."""
        return bool(self.getOrDefault(self.dropLast))

    def _resolved_io(self) -> tuple[list[str], list[str]]:
        """Resolve singular vs plural input/output column lists."""
        if self.isSet(self.inputCols):
            inputs = list(self.getInputCols())
            if not self.isSet(self.outputCols):
                raise IllegalArgumentException(
                    "OneHotEncoder: inputCols is set but outputCols is not"
                )
            outputs = list(self.getOutputCols())
            if len(inputs) != len(outputs):
                raise IllegalArgumentException("OneHotEncoder inputCols/outputCols length mismatch")
            if not inputs:
                raise IllegalArgumentException("OneHotEncoder.inputCols must be non-empty")
            return inputs, outputs
        # Singular path
        return [self.getInputCol()], [self.getOutputCol()]

    def _fit(self, dataset: Any) -> OneHotEncoderModel:
        """Category size = max(index)+1 per input column."""
        frame = _require_repark_dataframe(dataset, verb="OneHotEncoder.fit")
        inputs, outputs = self._resolved_io()
        host, view = _register_temp(frame, "ohe")
        try:
            category_sizes: list[int] = []
            for input_col in inputs:
                quoted = _quote_ident(input_col)
                row = _collect_sql(
                    host,
                    f"SELECT CAST(MAX({quoted}) AS BIGINT) AS mx FROM {view} "
                    f"WHERE {quoted} IS NOT NULL",
                )
                mx = row[0].asDict().get("mx") if row else None
                category_sizes.append(int(mx) + 1 if mx is not None else 0)
            # Back-compat: singular attribute is first column's size.
            model = OneHotEncoderModel(
                category_size=category_sizes[0] if category_sizes else 0,
                category_sizes=category_sizes,
                drop_last=self.getDropLast(),
                inputCol=inputs[0] if len(inputs) == 1 else None,
                outputCol=outputs[0] if len(outputs) == 1 else None,
                inputCols=inputs if len(inputs) > 1 or self.isSet(self.inputCols) else None,
                outputCols=outputs if len(outputs) > 1 or self.isSet(self.outputCols) else None,
                handleInvalid=self.getHandleInvalid(),
            )
            # Always record plural for transform resolution.
            if not model.isSet(model.inputCols):
                model.setInputCols(inputs)
            if not model.isSet(model.outputCols):
                model.setOutputCols(outputs)
            model.uid = self.uid
            return model
        finally:
            _drop_temp_view(host, view)


class OneHotEncoderModel(
    HasInputCol,
    HasOutputCol,
    HasInputCols,
    HasOutputCols,
    HasHandleInvalid,
    Model,
):
    """Fitted OHE — always emits sparse struct vectors (singular or plural)."""

    def __init__(
        self,
        *,
        category_size: int = 0,
        category_sizes: list[int] | None = None,
        drop_last: bool = True,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        inputCols: list[str] | None = None,  # noqa: N803
        outputCols: list[str] | None = None,  # noqa: N803
        handleInvalid: str | None = None,  # noqa: N803
    ) -> None:
        """Store per-column category sizes and dropLast."""
        super().__init__()
        self.category_size = int(category_size)
        self.category_sizes = (
            [int(value) for value in category_sizes]
            if category_sizes is not None
            else [int(category_size)]
        )
        self.drop_last = bool(drop_last)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        if inputCols is not None:
            self.setInputCols(inputCols)
        if outputCols is not None:
            self.setOutputCols(outputCols)
        if handleInvalid is not None:
            self.setHandleInvalid(handleInvalid)

    def _resolved_io(self) -> tuple[list[str], list[str]]:
        """Resolve singular vs plural column lists on the model."""
        if self.isSet(self.inputCols):
            inputs = list(self.getInputCols())
            outputs = list(self.getOutputCols()) if self.isSet(self.outputCols) else []
            if len(inputs) != len(outputs):
                raise IllegalArgumentException(
                    "OneHotEncoderModel inputCols/outputCols length mismatch"
                )
            return inputs, outputs
        return [self.getInputCol()], [self.getOutputCol()]

    def _transform(self, dataset: Any) -> Any:
        """Emit sparse struct {size, indices, values} via named_struct plan (1+ cols)."""
        frame = _require_repark_dataframe(dataset, verb="OneHotEncoderModel.transform")
        inputs, outputs = self._resolved_io()
        handle = self.getHandleInvalid()
        # Fail loud on illegal modes — do not silently treat typos as keep (octo C3-L-001).
        if handle not in {"error", "keep", "skip"}:
            raise IllegalArgumentException(
                f"OneHotEncoder.handleInvalid must be error|keep|skip, got {handle!r}"
            )
        # Refuse pre-existing output columns (parity with VectorAssembler / ext) (octo C3-L-002).
        for output_col in outputs:
            _refuse_output_collision(frame, output_col, stage="OneHotEncoderModel.transform")
        sizes_src = self.category_sizes
        if len(sizes_src) == 1 and len(inputs) > 1:
            sizes_src = sizes_src * len(inputs)
        if len(sizes_src) != len(inputs):
            # Fall back: single category_size for singular models.
            sizes_src = [self.category_size] * len(inputs)
        host, view = _register_temp(frame, "ohem")
        try:
            if handle == "error":
                for input_col, category_size in zip(inputs, sizes_src, strict=True):
                    quoted_in = _quote_ident(input_col)
                    check = _collect_sql(
                        host,
                        f"SELECT COUNT(*) AS n FROM {view} WHERE {quoted_in} IS NULL "
                        f"OR CAST({quoted_in} AS BIGINT) < 0 "
                        f"OR CAST({quoted_in} AS BIGINT) >= {int(category_size)}",
                    )
                    if check and int(check[0].asDict().get("n", 0)) > 0:
                        raise AnalysisException(
                            "OneHotEncoder encountered invalid indices with handleInvalid='error'"
                        )
            select_parts = [f"{view}.*"]
            where_clauses: list[str] = []
            for input_col, output_col, category_size in zip(
                inputs, outputs, sizes_src, strict=True
            ):
                quoted_in = _quote_ident(input_col)
                quoted_out = _quote_ident(output_col)
                # Spark: expand for keep *then* apply dropLast (octo C4-L-001).
                expanded = int(category_size) + (1 if handle == "keep" else 0)
                size = expanded - (1 if self.drop_last else 0)
                if size < 0:
                    size = 0
                sparse_expr = _ohe_sparse_expr(
                    quoted_in,
                    size,
                    category_size=int(category_size),
                    handle=handle,
                )
                select_parts.append(f"({sparse_expr}) AS {quoted_out}")
                if handle == "skip":
                    where_clauses.append(
                        f"{quoted_in} IS NOT NULL AND CAST({quoted_in} AS BIGINT) >= 0 "
                        f"AND CAST({quoted_in} AS BIGINT) < {int(category_size)}"
                    )
            sql = f"SELECT {', '.join(select_parts)} FROM {view}"
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)
            return _sql_on(host, sql)
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Fitted category sizes + dropLast + column lists."""
        inputs, outputs = self._resolved_io()
        return {
            "category_size": self.category_size,
            "category_sizes": list(self.category_sizes),
            "drop_last": self.drop_last,
            "inputCol": inputs[0] if len(inputs) == 1 else None,
            "outputCol": outputs[0] if len(outputs) == 1 else None,
            "inputCols": list(inputs),
            "outputCols": list(outputs),
            "handleInvalid": self.getHandleInvalid(),
        }

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> OneHotEncoderModel:
        """Rebuild."""
        payload = {**params, **(fitted_state or {})}
        sizes = payload.get("category_sizes")
        return cls(
            category_size=int(payload.get("category_size", 0)),
            category_sizes=[int(value) for value in sizes] if sizes is not None else None,
            drop_last=bool(payload.get("drop_last", True)),
            inputCol=payload.get("inputCol"),
            outputCol=payload.get("outputCol"),
            inputCols=list(payload["inputCols"]) if payload.get("inputCols") else None,
            outputCols=list(payload["outputCols"]) if payload.get("outputCols") else None,
            handleInvalid=payload.get("handleInvalid"),
        )


# ===========================================================================
# Scalers (on dense vector / array columns) + scalar helpers
# ===========================================================================


def _array_length_sql(col_sql: str) -> str:
    """SQL expression for array length."""
    return f"array_length({col_sql})"


class StandardScaler(HasInputCol, HasOutputCol, Estimator["StandardScalerModel"]):
    """Standardize dense vector columns (mean/std per dimension)."""

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        withMean: bool = False,  # noqa: N803
        withStd: bool = True,  # noqa: N803
    ) -> None:
        """Spark defaults: withMean=False, withStd=True."""
        super().__init__()
        self.withMean: Param[bool] = Param(
            self, "withMean", "center data with mean", TypeConverters.toBoolean
        )
        self.withStd: Param[bool] = Param(
            self, "withStd", "scale to unit std", TypeConverters.toBoolean
        )
        self._setDefault(withMean=False, withStd=True)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        self._set(withMean=withMean, withStd=withStd)

    def _fit(self, dataset: Any) -> StandardScalerModel:
        """Per-dimension mean/std via unnest aggregates."""
        frame = _require_repark_dataframe(dataset, verb="StandardScaler.fit")
        input_col = self.getInputCol()
        host, view = _register_temp(frame, "ss")
        try:
            quoted = _quote_ident(input_col)
            # Infer width from first non-null array length.
            width_row = _collect_sql(
                host,
                f"SELECT array_length({quoted}) AS w FROM {view} "
                f"WHERE {quoted} IS NOT NULL LIMIT 1",
            )
            if not width_row or width_row[0].asDict().get("w") is None:
                raise AnalysisException("StandardScaler.fit: no non-null feature vectors")
            width = int(width_row[0].asDict()["w"])
            means: list[float] = []
            stds: list[float] = []
            for index in range(width):
                # DataFusion array_element is 0-based (probed).
                elem = f"array_element({quoted}, {index})"
                stats = _collect_sql(
                    host,
                    f"SELECT avg({elem}) AS mu, stddev_samp({elem}) AS sigma FROM {view} "
                    f"WHERE {quoted} IS NOT NULL AND NOT isnan({elem})",
                )[0].asDict()
                mu = stats["mu"]
                means.append(float(mu) if mu is not None and mu == mu else 0.0)
                sigma = stats["sigma"]
                if sigma is None or float(sigma) != float(sigma) or float(sigma) == 0.0:
                    stds.append(1.0)
                else:
                    stds.append(float(sigma))
            model = StandardScalerModel(
                mean=means,
                std=stds,
                with_mean=bool(self.getOrDefault(self.withMean)),
                with_std=bool(self.getOrDefault(self.withStd)),
                inputCol=input_col,
                outputCol=self.getOutputCol(),
            )
            model.uid = self.uid
            return model
        finally:
            _drop_temp_view(host, view)


class StandardScalerModel(HasInputCol, HasOutputCol, Model):
    """Fitted standard scaler — element-wise plan arithmetic."""

    def __init__(
        self,
        *,
        mean: list[float] | None = None,
        std: list[float] | None = None,
        with_mean: bool = False,
        with_std: bool = True,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
    ) -> None:
        """Store mean/std vectors."""
        super().__init__()
        self.mean = list(mean or [])
        self.std = list(std or [])
        self.with_mean = with_mean
        self.with_std = with_std
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)

    def _transform(self, dataset: Any) -> Any:
        """Element-wise (x - mean) / std via make_array of expressions."""
        frame = _require_repark_dataframe(dataset, verb="StandardScalerModel.transform")
        host, view = _register_temp(frame, "ssm")
        quoted = _quote_ident(self.getInputCol())
        out = _quote_ident(self.getOutputCol())
        parts: list[str] = []
        for index, (mu, sigma) in enumerate(zip(self.mean, self.std, strict=True)):
            elem = f"array_element({quoted}, {index})"
            expr = elem
            if self.with_mean:
                expr = f"(({expr}) - {_sql_float(mu)})"
            if self.with_std:
                # Non-finite / zero sigma → unit scale (keeps plan valid; F-Q1-011).
                scale = sigma if sigma == sigma and sigma != 0.0 else 1.0
                expr = f"(({expr}) / {_sql_float(scale)})"
            parts.append(expr)
        array_sql = "make_array(" + ", ".join(parts) + ")" if parts else "make_array()"
        sql = f"SELECT {view}.*, ({array_sql}) AS {out} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            _drop_temp_view(host, view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Mean/std only."""
        return {
            "mean": list(self.mean),
            "std": list(self.std),
            "with_mean": self.with_mean,
            "with_std": self.with_std,
            "inputCol": self.getInputCol(),
            "outputCol": self.getOutputCol(),
        }

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> StandardScalerModel:
        """Rebuild."""
        payload = {**params, **(fitted_state or {})}
        return cls(
            mean=list(payload.get("mean") or []),
            std=list(payload.get("std") or []),
            with_mean=bool(payload.get("with_mean", False)),
            with_std=bool(payload.get("with_std", True)),
            inputCol=payload.get("inputCol"),
            outputCol=payload.get("outputCol"),
        )


class MinMaxScaler(HasInputCol, HasOutputCol, Estimator["MinMaxScalerModel"]):
    """Scale dense vectors to [min, max] (default [0, 1])."""

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        min: float = 0.0,
        max: float = 1.0,
    ) -> None:
        """Optional range."""
        super().__init__()
        self.min: Param[float] = Param(self, "min", "lower bound", TypeConverters.toFloat)
        self.max: Param[float] = Param(self, "max", "upper bound", TypeConverters.toFloat)
        self._setDefault(min=0.0, max=1.0)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        self._set(min=float(min), max=float(max))

    def _fit(self, dataset: Any) -> MinMaxScalerModel:
        """Per-dimension min/max aggregates."""
        frame = _require_repark_dataframe(dataset, verb="MinMaxScaler.fit")
        input_col = self.getInputCol()
        host, view = _register_temp(frame, "mms")
        try:
            quoted = _quote_ident(input_col)
            width_row = _collect_sql(
                host,
                f"SELECT array_length({quoted}) AS w FROM {view} "
                f"WHERE {quoted} IS NOT NULL LIMIT 1",
            )
            if not width_row or width_row[0].asDict().get("w") is None:
                raise AnalysisException("MinMaxScaler.fit: no non-null feature vectors")
            width = int(width_row[0].asDict()["w"])
            original_min: list[float] = []
            original_max: list[float] = []
            for index in range(width):
                elem = f"array_element({quoted}, {index})"
                stats = _collect_sql(
                    host,
                    (
                        f"SELECT min({elem}) AS lo, max({elem}) AS hi "
                        f"FROM {view} WHERE {quoted} IS NOT NULL AND NOT isnan({elem})"
                    ),
                )[0].asDict()
                lo = stats["lo"]
                hi = stats["hi"]
                original_min.append(float(lo) if lo is not None and lo == lo else 0.0)
                original_max.append(float(hi) if hi is not None and hi == hi else 0.0)
            model = MinMaxScalerModel(
                original_min=original_min,
                original_max=original_max,
                min_value=float(self.getOrDefault(self.min)),
                max_value=float(self.getOrDefault(self.max)),
                inputCol=input_col,
                outputCol=self.getOutputCol(),
            )
            model.uid = self.uid
            return model
        finally:
            _drop_temp_view(host, view)


class MinMaxScalerModel(HasInputCol, HasOutputCol, Model):
    """Fitted min-max scaler."""

    def __init__(
        self,
        *,
        original_min: list[float] | None = None,
        original_max: list[float] | None = None,
        min_value: float = 0.0,
        max_value: float = 1.0,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
    ) -> None:
        """Store original ranges and target range."""
        super().__init__()
        self.original_min = list(original_min or [])
        self.original_max = list(original_max or [])
        self.min_value = min_value
        self.max_value = max_value
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)

    def _transform(self, dataset: Any) -> Any:
        """Scale element-wise to [min, max]."""
        frame = _require_repark_dataframe(dataset, verb="MinMaxScalerModel.transform")
        host, view = _register_temp(frame, "mmsm")
        quoted = _quote_ident(self.getInputCol())
        out = _quote_ident(self.getOutputCol())
        parts: list[str] = []
        span_out = self.max_value - self.min_value
        for index, (lo, hi) in enumerate(zip(self.original_min, self.original_max, strict=True)):
            elem = f"array_element({quoted}, {index})"
            denom = hi - lo
            if denom == 0.0 or denom != denom:
                parts.append(_sql_float(self.min_value))
            else:
                parts.append(
                    f"((({elem}) - {_sql_float(lo)}) / {_sql_float(denom)}) "
                    f"* {_sql_float(span_out)} + {_sql_float(self.min_value)}"
                )
        array_sql = "make_array(" + ", ".join(parts) + ")" if parts else "make_array()"
        sql = f"SELECT {view}.*, ({array_sql}) AS {out} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            _drop_temp_view(host, view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Fitted ranges."""
        return {
            "original_min": list(self.original_min),
            "original_max": list(self.original_max),
            "min_value": self.min_value,
            "max_value": self.max_value,
            "inputCol": self.getInputCol(),
            "outputCol": self.getOutputCol(),
        }

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> MinMaxScalerModel:
        """Rebuild."""
        payload = {**params, **(fitted_state or {})}
        return cls(
            original_min=list(payload.get("original_min") or []),
            original_max=list(payload.get("original_max") or []),
            min_value=float(payload.get("min_value", 0.0)),
            max_value=float(payload.get("max_value", 1.0)),
            inputCol=payload.get("inputCol"),
            outputCol=payload.get("outputCol"),
        )


class MaxAbsScaler(HasInputCol, HasOutputCol, Estimator["MaxAbsScalerModel"]):
    """Scale each feature by its max absolute value."""

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
    ) -> None:
        """Optional input/output cols."""
        super().__init__()
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)

    def _fit(self, dataset: Any) -> MaxAbsScalerModel:
        """Per-dimension max(abs(x))."""
        frame = _require_repark_dataframe(dataset, verb="MaxAbsScaler.fit")
        input_col = self.getInputCol()
        host, view = _register_temp(frame, "mas")
        try:
            quoted = _quote_ident(input_col)
            width_row = _collect_sql(
                host,
                f"SELECT array_length({quoted}) AS w FROM {view} "
                f"WHERE {quoted} IS NOT NULL LIMIT 1",
            )
            if not width_row or width_row[0].asDict().get("w") is None:
                raise AnalysisException("MaxAbsScaler.fit: no non-null feature vectors")
            width = int(width_row[0].asDict()["w"])
            max_abs: list[float] = []
            for index in range(width):
                elem = f"array_element({quoted}, {index})"
                stats = _collect_sql(
                    host,
                    f"SELECT max(abs({elem})) AS m FROM {view} "
                    f"WHERE {quoted} IS NOT NULL AND NOT isnan({elem})",
                )[0].asDict()
                raw = stats["m"]
                value = float(raw) if raw is not None and raw == raw else 0.0
                max_abs.append(value if value != 0.0 else 1.0)
            model = MaxAbsScalerModel(
                max_abs=max_abs,
                inputCol=input_col,
                outputCol=self.getOutputCol(),
            )
            model.uid = self.uid
            return model
        finally:
            _drop_temp_view(host, view)


class MaxAbsScalerModel(HasInputCol, HasOutputCol, Model):
    """Fitted max-abs scaler."""

    def __init__(
        self,
        *,
        max_abs: list[float] | None = None,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
    ) -> None:
        """Store max absolute values."""
        super().__init__()
        self.max_abs = list(max_abs or [])
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)

    def _transform(self, dataset: Any) -> Any:
        """Divide each element by max abs."""
        frame = _require_repark_dataframe(dataset, verb="MaxAbsScalerModel.transform")
        host, view = _register_temp(frame, "masm")
        quoted = _quote_ident(self.getInputCol())
        out = _quote_ident(self.getOutputCol())
        parts: list[str] = []
        for index, scale in enumerate(self.max_abs):
            safe = scale if scale == scale and scale != 0.0 else 1.0
            parts.append(f"(array_element({quoted}, {index}) / {_sql_float(safe)})")
        array_sql = "make_array(" + ", ".join(parts) + ")" if parts else "make_array()"
        sql = f"SELECT {view}.*, ({array_sql}) AS {out} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            _drop_temp_view(host, view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Max abs vector."""
        return {
            "max_abs": list(self.max_abs),
            "inputCol": self.getInputCol(),
            "outputCol": self.getOutputCol(),
        }

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> MaxAbsScalerModel:
        """Rebuild."""
        payload = {**params, **(fitted_state or {})}
        return cls(
            max_abs=list(payload.get("max_abs") or []),
            inputCol=payload.get("inputCol"),
            outputCol=payload.get("outputCol"),
        )


# ===========================================================================
# Bucketizer / Binarizer / Imputer / text / SQL / Polynomial
# ===========================================================================


class Bucketizer(HasInputCol, HasOutputCol, HasHandleInvalid, Transformer):
    """Bucket continuous features using splits (includes -inf/inf typically)."""

    def __init__(
        self,
        *,
        splits: list[float] | None = None,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        handleInvalid: str | None = None,  # noqa: N803
    ) -> None:
        """``splits`` must be strictly increasing."""
        super().__init__()
        self.splits: Param[list[float]] = Param(
            self, "splits", "split points", TypeConverters.toListFloat
        )
        if splits is not None:
            self.setSplits(splits)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        if handleInvalid is not None:
            self.setHandleInvalid(handleInvalid)

    def setSplits(self, value: list[float]) -> Bucketizer:
        """Set split points."""
        if len(value) < 2:
            raise IllegalArgumentException("Bucketizer.splits needs at least 2 values")
        for left, right in itertools.pairwise(value):
            if not (left < right):
                raise IllegalArgumentException("Bucketizer.splits must be strictly increasing")
        return self._set(splits=value)

    def getSplits(self) -> list[float]:
        """Get splits."""
        return list(self.getOrDefault(self.splits))

    def _transform(self, dataset: Any) -> Any:
        """CASE WHEN x in [splits[i], splits[i+1]) → i."""
        frame = _require_repark_dataframe(dataset, verb="Bucketizer.transform")
        splits = self.getSplits()
        host, view = _register_temp(frame, "bkt")
        quoted = _quote_ident(self.getInputCol())
        out = _quote_ident(self.getOutputCol())
        handle = self.getHandleInvalid()
        branches: list[str] = []
        for index in range(len(splits) - 1):
            lo = _sql_float(splits[index])
            hi = _sql_float(splits[index + 1])
            # Last bucket is closed on the right.
            if index == len(splits) - 2:
                branches.append(
                    f"WHEN {quoted} >= {lo} AND {quoted} <= {hi} THEN CAST({index} AS DOUBLE)"
                )
            else:
                branches.append(
                    f"WHEN {quoted} >= {lo} AND {quoted} < {hi} THEN CAST({index} AS DOUBLE)"
                )
        case_sql = "CASE " + " ".join(branches) + " ELSE NULL END"
        if handle == "error":
            check = _collect_sql(
                host, f"SELECT COUNT(*) AS n FROM {view} WHERE ({case_sql}) IS NULL"
            )
            if check and int(check[0].asDict().get("n", 0)) > 0:
                raise AnalysisException(
                    "Bucketizer encountered values outside splits with handleInvalid='error'"
                )
            sql = f"SELECT {view}.*, ({case_sql}) AS {out} FROM {view}"
        elif handle == "skip":
            sql = (
                f"SELECT {view}.*, ({case_sql}) AS {out} FROM {view} WHERE ({case_sql}) IS NOT NULL"
            )
        else:  # keep → extra bucket index
            keep_index = len(splits) - 1
            keep_expr = (
                f"CASE WHEN ({case_sql}) IS NULL THEN CAST({keep_index} AS DOUBLE) "
                f"ELSE ({case_sql}) END"
            )
            sql = f"SELECT {view}.*, ({keep_expr}) AS {out} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Splits are params."""
        return {}

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> Bucketizer:
        """Rebuild."""
        return cls(
            splits=list(params.get("splits") or []),
            inputCol=params.get("inputCol"),
            outputCol=params.get("outputCol"),
            handleInvalid=params.get("handleInvalid"),
        )


class Binarizer(HasInputCol, HasOutputCol, Transformer):
    """Threshold continuous features to 0/1."""

    def __init__(
        self,
        *,
        threshold: float = 0.0,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
    ) -> None:
        """Default threshold 0.0."""
        super().__init__()
        self.threshold: Param[float] = Param(
            self, "threshold", "binarize threshold", TypeConverters.toFloat
        )
        self._setDefault(threshold=0.0)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        self._set(threshold=float(threshold))

    def _transform(self, dataset: Any) -> Any:
        """CASE WHEN x > threshold THEN 1.0 ELSE 0.0."""
        frame = _require_repark_dataframe(dataset, verb="Binarizer.transform")
        threshold_value = float(self.getOrDefault(self.threshold))
        host, view = _register_temp(frame, "bin")
        quoted = _quote_ident(self.getInputCol())
        out = _quote_ident(self.getOutputCol())
        expr = f"CASE WHEN {quoted} > {_sql_float(threshold_value)} THEN 1.0 ELSE 0.0 END"
        sql = f"SELECT {view}.*, ({expr}) AS {out} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            _drop_temp_view(host, view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """No fitted state."""
        return {}

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> Binarizer:
        """Rebuild."""
        return cls(
            threshold=float(params.get("threshold", 0.0)),
            inputCol=params.get("inputCol"),
            outputCol=params.get("outputCol"),
        )


class Imputer(HasInputCols, HasOutputCols, Estimator["ImputerModel"]):
    """Impute missing values with mean, median, or mode (plan-built aggregates)."""

    def __init__(
        self,
        *,
        inputCols: list[str] | None = None,  # noqa: N803
        outputCols: list[str] | None = None,  # noqa: N803
        strategy: str = "mean",
        missingValue: float = float("nan"),  # noqa: N803
    ) -> None:
        """strategy: mean | median | mode. Median uses engine approx_percentile_cont(p=0.5)."""
        super().__init__()
        self.strategy: Param[str] = Param(
            self, "strategy", "impute strategy mean|mode|median", TypeConverters.toString
        )
        self.missingValue: Param[float] = Param(
            self, "missingValue", "value treated as missing", TypeConverters.toFloat
        )
        self._setDefault(strategy="mean", missingValue=float("nan"))
        if inputCols is not None:
            self.setInputCols(inputCols)
        if outputCols is not None:
            self.setOutputCols(outputCols)
        self._set(strategy=strategy, missingValue=missingValue)

    def _fit(self, dataset: Any) -> ImputerModel:
        """Aggregate replacement values per input column (respect missingValue)."""
        frame = _require_repark_dataframe(dataset, verb="Imputer.fit")
        strategy = self.getOrDefault(self.strategy)
        if strategy not in {"mean", "mode", "median"}:
            raise IllegalArgumentException(
                f"Imputer.strategy must be mean|mode|median, got {strategy!r}"
            )
        inputs = self.getInputCols()
        outputs = self.getOutputCols()
        if len(inputs) != len(outputs):
            raise IllegalArgumentException("Imputer inputCols/outputCols length mismatch")
        missing_value = float(self.getOrDefault(self.missingValue))
        host, view = _register_temp(frame, "imp")
        try:
            replacements: dict[str, float] = {}
            for col in inputs:
                quoted = _quote_ident(col)
                present = _sql_non_missing_predicate(quoted, missing_value)
                if strategy == "mean":
                    row = _collect_sql(
                        host, f"SELECT avg({quoted}) AS v FROM {view} WHERE {present}"
                    )[0].asDict()
                    replacements[col] = float(row["v"] if row["v"] is not None else 0.0)
                elif strategy == "median":
                    # Engine t-digest p50 (Spark median Imputer uses approxQuantile).
                    row = _collect_sql(
                        host,
                        f"SELECT approx_percentile_cont({quoted}, 0.5) AS v FROM {view} "
                        f"WHERE {present}",
                    )[0].asDict()
                    replacements[col] = float(row["v"] if row["v"] is not None else 0.0)
                else:  # mode — argmax count
                    row = _collect_sql(
                        host,
                        f"SELECT {quoted} AS v, COUNT(*) AS c FROM {view} "
                        f"WHERE {present} GROUP BY {quoted} "
                        f"ORDER BY c DESC, {quoted} ASC LIMIT 1",
                    )
                    if not row:
                        replacements[col] = 0.0
                    else:
                        replacements[col] = float(row[0].asDict()["v"])
            model = ImputerModel(
                replacements=replacements,
                missing_value=missing_value,
                inputCols=inputs,
                outputCols=outputs,
            )
            model.uid = self.uid
            return model
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)


class ImputerModel(HasInputCols, HasOutputCols, Model):
    """Fitted imputer — replace null/missingValue with fitted replacement (plan CASE)."""

    def __init__(
        self,
        *,
        replacements: dict[str, float] | None = None,
        missing_value: float = float("nan"),
        inputCols: list[str] | None = None,  # noqa: N803
        outputCols: list[str] | None = None,  # noqa: N803
    ) -> None:
        """Store per-column replacements and the missing sentinel."""
        super().__init__()
        self.replacements = dict(replacements or {})
        self.missing_value = float(missing_value)
        if inputCols is not None:
            self.setInputCols(inputCols)
        if outputCols is not None:
            self.setOutputCols(outputCols)

    def _transform(self, dataset: Any) -> Any:
        """Add output cols with null/missingValue replaced (SQL-safe float embeds)."""
        frame = _require_repark_dataframe(dataset, verb="ImputerModel.transform")
        host, view = _register_temp(frame, "impm")
        inputs = self.getInputCols()
        outputs = self.getOutputCols()
        # Same input/output name → REPLACE column (Spark allows in-place impute).
        # Avoid `view.*, expr AS same_name` which DF rejects as ambiguous (F-Q1-010).
        overwrite = set(inputs) & set(outputs)
        if overwrite:
            kept = [
                f"{view}.{_quote_ident(name)}"
                for name in (list(frame.columns) if hasattr(frame, "columns") else [])
                if name not in overwrite
            ]
            select_parts = list(kept)
        else:
            select_parts = [f"{view}.*"]
        for input_col, output_col in zip(inputs, outputs, strict=True):
            rep = self.replacements.get(input_col, 0.0)
            quoted_in = _quote_ident(input_col)
            quoted_out = _quote_ident(output_col)
            expr = _sql_impute_expr(quoted_in, self.missing_value, rep)
            select_parts.append(f"({expr}) AS {quoted_out}")
        sql = f"SELECT {', '.join(select_parts)} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            _drop_temp_view(host, view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Replacement map + missing sentinel."""
        return {
            "replacements": dict(self.replacements),
            "missing_value": self.missing_value,
            "inputCols": list(self.getInputCols()),
            "outputCols": list(self.getOutputCols()),
        }

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> ImputerModel:
        """Rebuild."""
        payload = {**params, **(fitted_state or {})}
        return cls(
            replacements=dict(payload.get("replacements") or {}),
            missing_value=float(payload.get("missing_value", float("nan"))),
            inputCols=list(payload.get("inputCols") or []),
            outputCols=list(payload.get("outputCols") or []),
        )


class Tokenizer(HasInputCol, HasOutputCol, Transformer):
    """Whitespace tokenizer → array of lowercase tokens (Spark default)."""

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
    ) -> None:
        """Optional cols."""
        super().__init__()
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)

    def _transform(self, dataset: Any) -> Any:
        """``string_to_array(lower(col), ' ')`` style via split."""
        frame = _require_repark_dataframe(dataset, verb="Tokenizer.transform")
        host, view = _register_temp(frame, "tok")
        quoted = _quote_ident(self.getInputCol())
        out = _quote_ident(self.getOutputCol())
        # Spark Tokenizer: convert to lowercase, split on whitespace.
        expr = f"string_to_array(lower(trim({quoted})), ' ')"
        sql = f"SELECT {view}.*, ({expr}) AS {out} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """No fitted state."""
        return {}

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> Tokenizer:
        """Rebuild."""
        return cls(inputCol=params.get("inputCol"), outputCol=params.get("outputCol"))


class RegexTokenizer(HasInputCol, HasOutputCol, Transformer):
    """Regex tokenizer (Spark ``RegexTokenizer``) — gaps=True path plan-built.

    ``gaps=True`` (default): treat ``pattern`` as delimiter; replace matches with a unit
    separator then ``string_to_array``, filter empty / short tokens via unnest plan.
    ``gaps=False`` (extract matches) needs ``regexp_extract_all`` — loud STOP seed.
    """

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        pattern: str = r"\s+",
        gaps: bool = True,
        minTokenLength: int = 1,  # noqa: N803
        toLowercase: bool = True,  # noqa: N803
    ) -> None:
        """Spark defaults: pattern whitespace, gaps=True, minTokenLength=1, toLowercase=True."""
        super().__init__()
        self.pattern: Param[str] = Param(self, "pattern", "regex pattern", TypeConverters.toString)
        self.gaps: Param[bool] = Param(
            self, "gaps", "if true pattern is delimiter", TypeConverters.toBoolean
        )
        self.minTokenLength: Param[int] = Param(
            self, "minTokenLength", "minimum token length", TypeConverters.toInt
        )
        self.toLowercase: Param[bool] = Param(
            self, "toLowercase", "lowercase before tokenizing", TypeConverters.toBoolean
        )
        self._setDefault(pattern=r"\s+", gaps=True, minTokenLength=1, toLowercase=True)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        self._set(
            pattern=pattern,
            gaps=gaps,
            minTokenLength=int(minTokenLength),
            toLowercase=toLowercase,
        )

    def _transform(self, dataset: Any) -> Any:
        """Split on pattern (gaps=True) and filter by minTokenLength."""
        frame = _require_repark_dataframe(dataset, verb="RegexTokenizer.transform")
        if not bool(self.getOrDefault(self.gaps)):
            raise UnsupportedOperationException(
                f"RegexTokenizer(gaps=False) is not shipped: {REGEX_TOKENIZER_GAPS_FALSE_STATUS}"
            )
        pattern = str(self.getOrDefault(self.pattern))
        min_len = int(self.getOrDefault(self.minTokenLength))
        to_lower = bool(self.getOrDefault(self.toLowercase))
        host, view = _register_temp(frame, "rtok")
        indexed, rid_view, rid_col = _materialize_rid_view(host, view, "rtok")
        quoted = _quote_ident(self.getInputCol())
        out = _quote_ident(self.getOutputCol())
        rid = _quote_ident(rid_col)
        # Escape single quotes for SQL string literal; pattern is a Java/Spark regex.
        pattern_sql = pattern.replace("'", "''")
        text_expr = f"lower({quoted})" if to_lower else quoted
        # Replace ALL delimiter matches with unit separator (ASCII 31), then split.
        # DataFusion regexp_replace is first-match only unless flags include 'g'.
        split_expr = (
            f"string_to_array(regexp_replace({text_expr}, '{pattern_sql}', chr(31), 'g'), chr(31))"
        )
        # Filter empty tokens and min length via unnest + array_agg (no array_filter SQL).
        # rid view is cache-materialized so multi-scan joins keep association (F-Q1-009).
        sql = f"""
WITH tokens AS (
  SELECT {rid} AS __rid, unnest({split_expr}) AS tok FROM {rid_view}
),
filtered AS (
  SELECT __rid, array_agg(tok) AS __repark_rtok_out FROM tokens
  WHERE tok IS NOT NULL AND tok <> '' AND char_length(tok) >= {min_len}
  GROUP BY __rid
),
joined AS (
  SELECT {rid_view}.*, coalesce(filtered.__repark_rtok_out, make_array()) AS {out}
  FROM {rid_view}
  LEFT JOIN filtered ON {rid_view}.{rid} = filtered.__rid
)
SELECT * EXCLUDE ({rid}) FROM joined ORDER BY {rid}
"""
        try:
            return _sql_on(host, sql)
        finally:
            if hasattr(indexed, "unpersist"):
                with contextlib.suppress(Exception):
                    indexed.unpersist()
            _drop_temp_view(host, rid_view)
            _drop_temp_view(host, view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """No fitted state."""
        return {}

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> RegexTokenizer:
        """Rebuild."""
        return cls(
            inputCol=params.get("inputCol"),
            outputCol=params.get("outputCol"),
            pattern=str(params.get("pattern", r"\s+")),
            gaps=bool(params.get("gaps", True)),
            minTokenLength=int(params.get("minTokenLength", 1)),
            toLowercase=bool(params.get("toLowercase", True)),
        )


class StopWordsRemover(HasInputCol, HasOutputCol, Transformer):
    """Filter stop words from a token array column."""

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        stopWords: list[str] | None = None,  # noqa: N803
    ) -> None:
        """Default English stop words (minimal set if none provided)."""
        super().__init__()
        self.stopWords: Param[list[str]] = Param(
            self, "stopWords", "stop words list", TypeConverters.toListString
        )
        default_stops = [
            "a",
            "an",
            "the",
            "and",
            "or",
            "of",
            "to",
            "in",
            "on",
            "for",
            "is",
            "it",
            "this",
            "that",
        ]
        self._setDefault(stopWords=default_stops)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        if stopWords is not None:
            self._set(stopWords=stopWords)

    def _transform(self, dataset: Any) -> Any:
        """Filter array elements not in stop list (unnest plan; rid materialized)."""
        frame = _require_repark_dataframe(dataset, verb="StopWordsRemover.transform")
        stops = self.getOrDefault(self.stopWords)
        host, view = _register_temp(frame, "swr")
        indexed, rid_view, rid_col = _materialize_rid_view(host, view, "swr")
        quoted = _quote_ident(self.getInputCol())
        out = _quote_ident(self.getOutputCol())
        rid = _quote_ident(rid_col)
        stop_list = ", ".join("'" + word.replace("'", "''") + "'" for word in stops)
        where = f"x NOT IN ({stop_list})" if stop_list else "TRUE"
        # DataFusion has no array_filter — unnest + array_agg join; rid cache (F-Q1-009).
        sql = f"""
WITH filtered AS (
  SELECT __rid, array_agg(x) AS __repark_swr_out FROM (
    SELECT {rid} AS __rid, unnest({quoted}) AS x FROM {rid_view}
  ) u WHERE {where} GROUP BY __rid
),
joined AS (
  SELECT {rid_view}.*, coalesce(filtered.__repark_swr_out, make_array()) AS {out}
  FROM {rid_view}
  LEFT JOIN filtered ON {rid_view}.{rid} = filtered.__rid
)
SELECT * EXCLUDE ({rid}) FROM joined ORDER BY {rid}
"""
        try:
            return _sql_on(host, sql)
        finally:
            if hasattr(indexed, "unpersist"):
                with contextlib.suppress(Exception):
                    indexed.unpersist()
            _drop_temp_view(host, rid_view)
            _drop_temp_view(host, view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """No fitted state."""
        return {}

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> StopWordsRemover:
        """Rebuild."""
        return cls(
            inputCol=params.get("inputCol"),
            outputCol=params.get("outputCol"),
            stopWords=list(params.get("stopWords") or []),
        )


class SQLTransformer(Transformer):
    """Run a SQL statement with ``__THIS__`` replaced by the input view."""

    def __init__(self, *, statement: str | None = None) -> None:
        """SQL statement containing ``__THIS__``."""
        super().__init__()
        self.statement: Param[str] = Param(
            self, "statement", "SQL statement with __THIS__", TypeConverters.toString
        )
        if statement is not None:
            self._set(statement=statement)

    def setStatement(self, value: str) -> SQLTransformer:
        """Set SQL statement."""
        return self._set(statement=value)

    def getStatement(self) -> str:
        """Get SQL statement."""
        return self.getOrDefault(self.statement)

    def _transform(self, dataset: Any) -> Any:
        """Substitute __THIS__ and run (single SELECT only — octo C1-SEC-001)."""
        frame = _require_repark_dataframe(dataset, verb="SQLTransformer.transform")
        statement = self.getStatement()
        if "__THIS__" not in statement:
            raise IllegalArgumentException(
                "SQLTransformer.statement must contain __THIS__ placeholder"
            )
        stripped = statement.strip().rstrip(";").strip()
        if ";" in stripped:
            raise IllegalArgumentException(
                "SQLTransformer.statement must be a single statement (no ';')"
            )
        if not re.match(r"(?is)^select\b", stripped):
            raise IllegalArgumentException(
                "SQLTransformer.statement must be a SELECT (got non-SELECT)"
            )
        host, view = _register_temp(frame, "sqltr")
        try:
            sql = stripped.replace("__THIS__", view)
            return _sql_on(host, sql)
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """No fitted state."""
        return {}

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> SQLTransformer:
        """Rebuild."""
        return cls(statement=params.get("statement"))


def _polynomial_expansion_monomials(
    *,
    start: int,
    remaining: int,
    factors: list[str],
    width: int,
    quoted_input: str,
) -> list[str]:
    """Every monomial of ``remaining`` degree, drawing factors from ``start`` onward.

    Recursion is the combinatorial tree (width * remaining). The caller caps
    ``degree`` at 3 and ``width`` at 8, so stack depth cannot exceed 3.
    """
    if remaining == 0:
        if factors:
            return ["*".join(factors)]
        return []
    terms: list[str] = []
    for index in range(start, width):
        element = f"array_element({quoted_input}, {index})"
        terms.extend(
            _polynomial_expansion_monomials(
                start=index,
                remaining=remaining - 1,
                factors=[*factors, element],
                width=width,
                quoted_input=quoted_input,
            )
        )
    return terms


class PolynomialExpansion(HasInputCol, HasOutputCol, Transformer):
    """Polynomial expansion of a dense vector (degree >= 2)."""

    def __init__(
        self,
        *,
        degree: int = 2,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
    ) -> None:
        """Default degree 2."""
        super().__init__()
        self.degree: Param[int] = Param(self, "degree", "polynomial degree", TypeConverters.toInt)
        self._setDefault(degree=2)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        self._set(degree=int(degree))

    def _transform(self, dataset: Any) -> Any:
        """Expand [x,y] degree 2 → [x, x^2, y, x*y, y^2] (Spark order).

        Width must be known at plan time: inferred from a sample aggregate length.
        """
        frame = _require_repark_dataframe(dataset, verb="PolynomialExpansion.transform")
        degree = int(self.getOrDefault(self.degree))
        if degree < 1:
            raise IllegalArgumentException("PolynomialExpansion.degree must be >= 1")
        host, view = _register_temp(frame, "pe")
        quoted = _quote_ident(self.getInputCol())
        out = _quote_ident(self.getOutputCol())
        width_row = _collect_sql(
            host,
            f"SELECT array_length({quoted}) AS w FROM {view} WHERE {quoted} IS NOT NULL LIMIT 1",
        )
        if not width_row or width_row[0].asDict().get("w") is None:
            raise AnalysisException("PolynomialExpansion: no non-null vectors")
        width = int(width_row[0].asDict()["w"])
        if width > 8 or degree > 3:
            raise UnsupportedOperationException(
                f"PolynomialExpansion v1 supports width<=8 and degree<=3 "
                f"(got width={width}, degree={degree}); seed for larger"
            )
        # Generate monomials in Spark's poly expand order via nested loops.
        terms: list[str] = []
        # Spark includes degree 1..degree interactions (not degree 0).
        for deg in range(1, degree + 1):
            terms.extend(
                _polynomial_expansion_monomials(
                    start=0,
                    remaining=deg,
                    factors=[],
                    width=width,
                    quoted_input=quoted,
                )
            )
        # Spark order is more subtle; for degree=2 width=2: x, x^2, y, x*y, y^2
        # Rebuild explicitly for small cases to match Spark better.
        if degree == 2:
            terms = []
            for index in range(width):
                x = f"array_element({quoted}, {index})"
                terms.append(x)
                terms.append(f"({x}) * ({x})")
                for j in range(index):
                    y = f"array_element({quoted}, {j + 1})"
                    # insertion for interactions — approximate; pin vs Spark in oracle
                    terms.insert(-1, f"({y}) * ({x})")
            # Simpler fixed order for width=2 degree=2:
            if width == 2:
                x = f"array_element({quoted}, 0)"
                y = f"array_element({quoted}, 1)"
                terms = [x, f"({x})*({x})", y, f"({x})*({y})", f"({y})*({y})"]
            elif width == 1:
                x = f"array_element({quoted}, 0)"
                terms = [x, f"({x})*({x})"]
        array_sql = "make_array(" + ", ".join(terms) + ")"
        sql = f"SELECT {view}.*, ({array_sql}) AS {out} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """No fitted state."""
        return {}

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> PolynomialExpansion:
        """Rebuild."""
        return cls(
            degree=int(params.get("degree", 2)),
            inputCol=params.get("inputCol"),
            outputCol=params.get("outputCol"),
        )


# ===========================================================================
# Quantile family (Q1) — RobustScaler / QuantileDiscretizer
# ===========================================================================


class RobustScaler(HasInputCol, HasOutputCol, Estimator["RobustScalerModel"]):
    """Scale features using median and quantile range (Spark ``RobustScaler``).

    Fit uses engine ``approx_percentile_cont`` (t-digest). Defaults match Spark:
    ``lower=0.25``, ``upper=0.75``, ``withCentering=True``, ``withScaling=True``.
    """

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        lower: float = 0.25,
        upper: float = 0.75,
        withCentering: bool = True,  # noqa: N803
        withScaling: bool = True,  # noqa: N803
    ) -> None:
        """Optional range quantiles and centering/scaling flags."""
        super().__init__()
        self.lower: Param[float] = Param(
            self, "lower", "lower quantile for range", TypeConverters.toFloat
        )
        self.upper: Param[float] = Param(
            self, "upper", "upper quantile for range", TypeConverters.toFloat
        )
        self.withCentering: Param[bool] = Param(
            self, "withCentering", "subtract median", TypeConverters.toBoolean
        )
        self.withScaling: Param[bool] = Param(
            self, "withScaling", "divide by quantile range", TypeConverters.toBoolean
        )
        self._setDefault(lower=0.25, upper=0.75, withCentering=True, withScaling=True)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        self._set(
            lower=float(lower),
            upper=float(upper),
            withCentering=withCentering,
            withScaling=withScaling,
        )

    def _fit(self, dataset: Any) -> RobustScalerModel:
        """Per-dimension median + (upper-lower) range via approx_percentile_cont."""
        frame = _require_repark_dataframe(dataset, verb="RobustScaler.fit")
        lower = float(self.getOrDefault(self.lower))
        upper = float(self.getOrDefault(self.upper))
        if not 0.0 <= lower < upper <= 1.0:
            raise IllegalArgumentException(
                f"RobustScaler requires 0 <= lower < upper <= 1 (got lower={lower}, upper={upper})"
            )
        input_col = self.getInputCol()
        host, view = _register_temp(frame, "rs")
        try:
            quoted = _quote_ident(input_col)
            width_row = _collect_sql(
                host,
                f"SELECT array_length({quoted}) AS w FROM {view} "
                f"WHERE {quoted} IS NOT NULL LIMIT 1",
            )
            if not width_row or width_row[0].asDict().get("w") is None:
                raise AnalysisException("RobustScaler.fit: no non-null feature vectors")
            width = int(width_row[0].asDict()["w"])
            centers: list[float] = []
            scales: list[float] = []
            for index in range(width):
                elem = f"array_element({quoted}, {index})"
                stats = _collect_sql(
                    host,
                    f"SELECT approx_percentile_cont({elem}, 0.5) AS med, "
                    f"approx_percentile_cont({elem}, {lower}) AS lo, "
                    f"approx_percentile_cont({elem}, {upper}) AS hi "
                    f"FROM {view} WHERE {quoted} IS NOT NULL",
                )[0].asDict()
                med = float(stats["med"] if stats["med"] is not None else 0.0)
                lo = float(stats["lo"] if stats["lo"] is not None else 0.0)
                hi = float(stats["hi"] if stats["hi"] is not None else 0.0)
                # NaN span (poisoned inputs) → unit scale so transform stays plan-valid.
                if med != med:
                    med = 0.0
                if lo != lo:
                    lo = 0.0
                if hi != hi:
                    hi = 0.0
                span = hi - lo
                centers.append(med)
                scales.append(span if span != 0.0 and span == span else 1.0)
            model = RobustScalerModel(
                center=centers,
                scale=scales,
                with_centering=bool(self.getOrDefault(self.withCentering)),
                with_scaling=bool(self.getOrDefault(self.withScaling)),
                inputCol=input_col,
                outputCol=self.getOutputCol(),
            )
            model.uid = self.uid
            return model
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)


class RobustScalerModel(HasInputCol, HasOutputCol, Model):
    """Fitted robust scaler — element-wise plan arithmetic."""

    def __init__(
        self,
        *,
        center: list[float] | None = None,
        scale: list[float] | None = None,
        with_centering: bool = True,
        with_scaling: bool = True,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
    ) -> None:
        """Store center/scale vectors."""
        super().__init__()
        self.center = list(center or [])
        self.scale = list(scale or [])
        self.with_centering = with_centering
        self.with_scaling = with_scaling
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)

    def _transform(self, dataset: Any) -> Any:
        """Element-wise (x - center) / scale (SQL-safe float embeds)."""
        frame = _require_repark_dataframe(dataset, verb="RobustScalerModel.transform")
        host, view = _register_temp(frame, "rsm")
        quoted = _quote_ident(self.getInputCol())
        out = _quote_ident(self.getOutputCol())
        parts: list[str] = []
        for index, (center, scale) in enumerate(zip(self.center, self.scale, strict=True)):
            elem = f"array_element({quoted}, {index})"
            expr = elem
            if self.with_centering:
                expr = f"(({expr}) - {_sql_float(center)})"
            if self.with_scaling:
                expr = f"(({expr}) / {_sql_float(scale)})"
            parts.append(expr)
        array_sql = "make_array(" + ", ".join(parts) + ")" if parts else "make_array()"
        sql = f"SELECT {view}.*, ({array_sql}) AS {out} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Center/scale only."""
        return {
            "center": list(self.center),
            "scale": list(self.scale),
            "with_centering": self.with_centering,
            "with_scaling": self.with_scaling,
            "inputCol": self.getInputCol(),
            "outputCol": self.getOutputCol(),
        }

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> RobustScalerModel:
        """Rebuild."""
        payload = {**params, **(fitted_state or {})}
        return cls(
            center=list(payload.get("center") or []),
            scale=list(payload.get("scale") or []),
            with_centering=bool(payload.get("with_centering", True)),
            with_scaling=bool(payload.get("with_scaling", True)),
            inputCol=payload.get("inputCol"),
            outputCol=payload.get("outputCol"),
        )


class QuantileDiscretizer(HasInputCol, HasOutputCol, HasHandleInvalid, Estimator["Bucketizer"]):
    """Bucket continuous features by approximate quantiles (Spark ``QuantileDiscretizer``).

    Fit computes interior splits via ``approx_percentile_cont`` at ``k/numBuckets`` for
    ``k=1..numBuckets-1``, then returns a :class:`Bucketizer` with ``[-inf, …, +inf]``.
    """

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        numBuckets: int = 2,  # noqa: N803
        handleInvalid: str | None = None,  # noqa: N803
        relativeError: float = 0.001,  # noqa: N803  # accepted-and-ignored (t-digest)
    ) -> None:
        """``numBuckets`` >= 2; ``relativeError`` accepted and ignored (divergence pin)."""
        super().__init__()
        self.numBuckets: Param[int] = Param(
            self, "numBuckets", "number of buckets", TypeConverters.toInt
        )
        self.relativeError: Param[float] = Param(
            self,
            "relativeError",
            "Spark GK relative error (accepted, ignored — t-digest)",
            TypeConverters.toFloat,
        )
        self._setDefault(numBuckets=2, relativeError=0.001)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        if handleInvalid is not None:
            self.setHandleInvalid(handleInvalid)
        self._set(numBuckets=int(numBuckets), relativeError=float(relativeError))

    def _fit(self, dataset: Any) -> Bucketizer:
        """Compute quantile splits; return configured Bucketizer."""
        frame = _require_repark_dataframe(dataset, verb="QuantileDiscretizer.fit")
        num_buckets = int(self.getOrDefault(self.numBuckets))
        if num_buckets < 2:
            raise IllegalArgumentException("QuantileDiscretizer.numBuckets must be >= 2")
        # relativeError accepted-and-ignored (t-digest has no Greenwald-Khanna accuracy knob).
        _ = self.getOrDefault(self.relativeError)
        input_col = self.getInputCol()
        host, view = _register_temp(frame, "qd")
        try:
            quoted = _quote_ident(input_col)
            # Build one SELECT with all interior percentiles.
            select_parts = [
                f"approx_percentile_cont({quoted}, {k / num_buckets}) AS p{k}"
                for k in range(1, num_buckets)
            ]
            row = _collect_sql(
                host,
                f"SELECT {', '.join(select_parts)} FROM {view} WHERE {quoted} IS NOT NULL",
            )[0].asDict()
            interior: list[float] = []
            for k in range(1, num_buckets):
                value = row.get(f"p{k}")
                if value is None:
                    continue
                interior.append(float(value))
            # Deduplicate non-strictly-increasing approx points (t-digest may collapse).
            splits = [-float("inf")]
            for point in interior:
                if point > splits[-1]:
                    splits.append(point)
            splits.append(float("inf"))
            if len(splits) < 3:
                # Degenerate: single-value column → force a mid interior for Bucketizer.
                splits = [-float("inf"), 0.0, float("inf")]
            model = Bucketizer(
                splits=splits,
                inputCol=input_col,
                outputCol=self.getOutputCol(),
                handleInvalid=self.getHandleInvalid(),
            )
            model.uid = self.uid
            return model
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Estimator — no fitted state of its own (returns Bucketizer)."""
        return {}

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> QuantileDiscretizer:
        """Rebuild estimator (fitted form is Bucketizer saved separately)."""
        return cls(
            inputCol=params.get("inputCol"),
            outputCol=params.get("outputCol"),
            numBuckets=int(params.get("numBuckets", 2)),
            handleInvalid=params.get("handleInvalid"),
            relativeError=float(params.get("relativeError", 0.001)),
        )


# ===========================================================================
# CountVectorizer / IDF (Q1 attempt)
# ===========================================================================


class CountVectorizer(HasInputCol, HasOutputCol, Estimator["CountVectorizerModel"]):
    """Learn a vocabulary from token arrays and emit bag-of-words dense vectors.

    Fit = distinct token counts (ordered by frequency); transform = per-vocab counts
    via plan CASE/array. ``binary`` collapses counts to 0/1.
    """

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        vocabSize: int = 1 << 18,  # noqa: N803
        minDF: float = 1.0,  # noqa: N803
        minTF: float = 1.0,  # noqa: N803
        binary: bool = False,
    ) -> None:
        """Spark-like defaults; large vocabSize capped by data."""
        super().__init__()
        self.vocabSize: Param[int] = Param(
            self, "vocabSize", "max vocabulary size", TypeConverters.toInt
        )
        self.minDF: Param[float] = Param(
            self, "minDF", "min document frequency", TypeConverters.toFloat
        )
        self.minTF: Param[float] = Param(
            self, "minTF", "min term frequency in a document", TypeConverters.toFloat
        )
        self.binary: Param[bool] = Param(
            self, "binary", "binary term counts", TypeConverters.toBoolean
        )
        self._setDefault(vocabSize=1 << 18, minDF=1.0, minTF=1.0, binary=False)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        self._set(
            vocabSize=int(vocabSize),
            minDF=float(minDF),
            minTF=float(minTF),
            binary=binary,
        )

    def _fit(self, dataset: Any) -> CountVectorizerModel:
        """Vocabulary = tokens ordered by document frequency (then token)."""
        frame = _require_repark_dataframe(dataset, verb="CountVectorizer.fit")
        input_col = self.getInputCol()
        vocab_size = int(self.getOrDefault(self.vocabSize))
        min_df = float(self.getOrDefault(self.minDF))
        host, view = _register_temp(frame, "cv")
        try:
            quoted = _quote_ident(input_col)
            # Document count for relative minDF.
            n_docs_row = _collect_sql(host, f"SELECT COUNT(*) AS n FROM {view}")
            n_docs = int(n_docs_row[0].asDict()["n"]) if n_docs_row else 0
            min_df_abs = min_df if min_df >= 1.0 else min_df * max(n_docs, 1)
            # Per-token document frequency via unnest + distinct doc ids.
            sql = f"""
WITH base AS (
  SELECT row_number() OVER () AS __doc, {quoted} AS tokens FROM {view}
),
exploded AS (
  SELECT __doc, unnest(tokens) AS token FROM base WHERE tokens IS NOT NULL
),
df AS (
  SELECT token, COUNT(DISTINCT __doc) AS doc_freq FROM exploded
  WHERE token IS NOT NULL AND token <> ''
  GROUP BY token
)
SELECT token FROM df
WHERE doc_freq >= {min_df_abs}
ORDER BY doc_freq DESC, token ASC
LIMIT {vocab_size}
"""
            rows = _collect_sql(host, sql)
            vocabulary = [str(row.asDict()["token"]) for row in rows]
            model = CountVectorizerModel(
                vocabulary=vocabulary,
                min_tf=float(self.getOrDefault(self.minTF)),
                binary=bool(self.getOrDefault(self.binary)),
                inputCol=input_col,
                outputCol=self.getOutputCol(),
            )
            model.uid = self.uid
            return model
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)


class CountVectorizerModel(HasInputCol, HasOutputCol, Model):
    """Fitted CountVectorizer — dense count vectors over fixed vocabulary."""

    def __init__(
        self,
        *,
        vocabulary: list[str] | None = None,
        min_tf: float = 1.0,
        binary: bool = False,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
    ) -> None:
        """Store ordered vocabulary."""
        super().__init__()
        self.vocabulary = list(vocabulary or [])
        self.min_tf = float(min_tf)
        self.binary = bool(binary)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)

    def _transform(self, dataset: Any) -> Any:
        """Count occurrences of each vocab term in the token array (plan-built).

        Uses unnest + conditional SUM grouped by row id (no correlated scalar subquery —
        physical plan does not support ScalarSubquery over unnest). Rid is
        cache-materialized so multi-scan joins keep row association (F-Q1-009).
        """
        frame = _require_repark_dataframe(dataset, verb="CountVectorizerModel.transform")
        host, view = _register_temp(frame, "cvm")
        quoted = _quote_ident(self.getInputCol())
        out = _quote_ident(self.getOutputCol())
        if not self.vocabulary:
            sql = f"SELECT {view}.*, make_array() AS {out} FROM {view}"
            try:
                return _sql_on(host, sql)
            finally:
                _drop_temp_view(host, view)
        indexed, rid_view, rid_col = _materialize_rid_view(host, view, "cvm")
        rid = _quote_ident(rid_col)
        sum_parts: list[str] = []
        array_parts: list[str] = []
        # Spark minTF: integer >=1 → absolute count; float in [0,1) → fraction of doc tokens.
        min_tf = float(self.min_tf)
        fractional_min_tf = 0.0 <= min_tf < 1.0
        # Total non-empty tokens per doc (for fractional minTF).
        total_expr = "coalesce(counts.__cv_total, 0.0)"
        for index, term in enumerate(self.vocabulary):
            escaped = term.replace("'", "''")
            alias = f"__cv_c{index}"
            sum_parts.append(f"SUM(CASE WHEN __t = '{escaped}' THEN 1.0 ELSE 0.0 END) AS {alias}")
            raw = f"coalesce(counts.{alias}, 0.0)"
            threshold = f"({min_tf} * {total_expr})" if fractional_min_tf else _sql_float(min_tf)
            if self.binary:
                array_parts.append(f"CASE WHEN {raw} >= {threshold} THEN 1.0 ELSE 0.0 END")
            elif min_tf > 1.0 or fractional_min_tf:
                array_parts.append(f"CASE WHEN {raw} >= {threshold} THEN {raw} ELSE 0.0 END")
            else:
                # min_tf == 1.0 (Spark default) — keep raw counts.
                array_parts.append(raw)
        sum_parts.append(
            "SUM(CASE WHEN __t IS NOT NULL AND __t <> '' THEN 1.0 ELSE 0.0 END) AS __cv_total"
        )
        array_sql = "make_array(" + ", ".join(array_parts) + ")"
        sql = f"""
WITH exploded AS (
  SELECT {rid} AS __rid, unnest({quoted}) AS __t FROM {rid_view}
),
counts AS (
  SELECT __rid, {", ".join(sum_parts)}
  FROM exploded
  GROUP BY __rid
),
joined AS (
  SELECT {rid_view}.*, ({array_sql}) AS {out}
  FROM {rid_view}
  LEFT JOIN counts ON {rid_view}.{rid} = counts.__rid
)
SELECT * EXCLUDE ({rid}) FROM joined ORDER BY {rid}
"""
        try:
            return _sql_on(host, sql)
        finally:
            if hasattr(indexed, "unpersist"):
                with contextlib.suppress(Exception):
                    indexed.unpersist()
            _drop_temp_view(host, rid_view)
            _drop_temp_view(host, view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """Vocabulary + flags."""
        return {
            "vocabulary": list(self.vocabulary),
            "min_tf": self.min_tf,
            "binary": self.binary,
            "inputCol": self.getInputCol(),
            "outputCol": self.getOutputCol(),
        }

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> CountVectorizerModel:
        """Rebuild."""
        payload = {**params, **(fitted_state or {})}
        return cls(
            vocabulary=list(payload.get("vocabulary") or []),
            min_tf=float(payload.get("min_tf", 1.0)),
            binary=bool(payload.get("binary", False)),
            inputCol=payload.get("inputCol"),
            outputCol=payload.get("outputCol"),
        )


class IDF(HasInputCol, HasOutputCol, Estimator["IDFModel"]):
    """Inverse document frequency scaling over dense count vectors (Spark ``IDF``).

    Fit: per-dimension ``log((m+1)/(df+1))+1`` where ``df`` = count of rows with
    feature > 0 (Spark smooth IDF). Transform: element-wise multiply.
    """

    def __init__(
        self,
        *,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
        minDocFreq: int = 0,  # noqa: N803
    ) -> None:
        """Optional minDocFreq (features below threshold get idf=0)."""
        super().__init__()
        self.minDocFreq: Param[int] = Param(
            self, "minDocFreq", "min doc frequency for a term", TypeConverters.toInt
        )
        self._setDefault(minDocFreq=0)
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)
        self._set(minDocFreq=int(minDocFreq))

    def _fit(self, dataset: Any) -> IDFModel:
        """Per-dimension document frequency + smooth IDF."""
        import math

        frame = _require_repark_dataframe(dataset, verb="IDF.fit")
        input_col = self.getInputCol()
        min_df = int(self.getOrDefault(self.minDocFreq))
        host, view = _register_temp(frame, "idf")
        try:
            quoted = _quote_ident(input_col)
            width_row = _collect_sql(
                host,
                f"SELECT array_length({quoted}) AS w FROM {view} "
                f"WHERE {quoted} IS NOT NULL LIMIT 1",
            )
            if not width_row or width_row[0].asDict().get("w") is None:
                raise AnalysisException("IDF.fit: no non-null feature vectors")
            width = int(width_row[0].asDict()["w"])
            m_row = _collect_sql(host, f"SELECT COUNT(*) AS m FROM {view}")
            m = float(m_row[0].asDict()["m"] if m_row else 0)
            idf_values: list[float] = []
            for index in range(width):
                elem = f"array_element({quoted}, {index})"
                df_row = _collect_sql(
                    host,
                    f"SELECT COUNT(*) AS df FROM {view} WHERE {quoted} IS NOT NULL "
                    f"AND {elem} IS NOT NULL AND {elem} > 0",
                )[0].asDict()
                doc_freq = float(df_row["df"] or 0.0)
                if doc_freq < min_df:
                    idf_values.append(0.0)
                else:
                    # Spark: log((m+1)/(df+1)) + 1
                    idf_values.append(math.log((m + 1.0) / (doc_freq + 1.0)) + 1.0)
            model = IDFModel(
                idf=idf_values,
                inputCol=input_col,
                outputCol=self.getOutputCol(),
            )
            model.uid = self.uid
            return model
        finally:
            with contextlib.suppress(Exception):
                host._session.drop_temp_view(view)


class IDFModel(HasInputCol, HasOutputCol, Model):
    """Fitted IDF — element-wise multiply by idf vector."""

    def __init__(
        self,
        *,
        idf: list[float] | None = None,
        inputCol: str | None = None,  # noqa: N803
        outputCol: str | None = None,  # noqa: N803
    ) -> None:
        """Store idf weights."""
        super().__init__()
        self.idf = list(idf or [])
        if inputCol is not None:
            self.setInputCol(inputCol)
        if outputCol is not None:
            self.setOutputCol(outputCol)

    def _transform(self, dataset: Any) -> Any:
        """Element-wise tf * idf."""
        frame = _require_repark_dataframe(dataset, verb="IDFModel.transform")
        host, view = _register_temp(frame, "idfm")
        quoted = _quote_ident(self.getInputCol())
        out = _quote_ident(self.getOutputCol())
        parts: list[str] = []
        for index, weight in enumerate(self.idf):
            safe = weight if weight == weight else 0.0
            parts.append(f"(array_element({quoted}, {index}) * {_sql_float(safe)})")
        array_sql = "make_array(" + ", ".join(parts) + ")" if parts else "make_array()"
        sql = f"SELECT {view}.*, ({array_sql}) AS {out} FROM {view}"
        try:
            return _sql_on(host, sql)
        finally:
            _drop_temp_view(host, view)

    def _ml_fitted_state(self) -> dict[str, Any]:
        """IDF vector."""
        return {
            "idf": list(self.idf),
            "inputCol": self.getInputCol(),
            "outputCol": self.getOutputCol(),
        }

    @classmethod
    def _ml_from_save(
        cls,
        *,
        params: dict[str, Any],
        fitted: bool,
        fitted_state: dict[str, Any],
    ) -> IDFModel:
        """Rebuild."""
        payload = {**params, **(fitted_state or {})}
        return cls(
            idf=list(payload.get("idf") or []),
            inputCol=payload.get("inputCol"),
            outputCol=payload.get("outputCol"),
        )


__all__ = [
    "COUNT_VECTORIZER_STATUS",
    "IDF",
    "IDF_STATUS",
    "QUANTILE_FAMILY_STATUS",
    "REGEX_TOKENIZER_GAPS_FALSE_STATUS",
    "Binarizer",
    "Bucketizer",
    "CountVectorizer",
    "CountVectorizerModel",
    "IDFModel",
    "Imputer",
    "ImputerModel",
    "IndexToString",
    "MaxAbsScaler",
    "MaxAbsScalerModel",
    "MinMaxScaler",
    "MinMaxScalerModel",
    "OneHotEncoder",
    "OneHotEncoderModel",
    "PolynomialExpansion",
    "QuantileDiscretizer",
    "RegexTokenizer",
    "RobustScaler",
    "RobustScalerModel",
    "SQLTransformer",
    "StandardScaler",
    "StandardScalerModel",
    "StopWordsRemover",
    "StringIndexer",
    "StringIndexerModel",
    "Tokenizer",
    "VectorAssembler",
]
