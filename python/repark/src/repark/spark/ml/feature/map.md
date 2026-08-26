# map — python/repark/src/repark/spark/ml/feature

## Purpose

Plan-built `pyspark.ml.feature` transformers under `repark.spark.ml.feature`
(Q1 re-home, 2026-08-14; M2 + Q1 quantile unlock). `fit` = session aggregates;
`transform` = SQL/plan expressions only.

## Contents

- **r23 QI1:** `_transformers.py` uses `repark._idents.quote_ident` (always-quote SSOT).
| Path | Role |
|---|---|
| `__init__.py` | Public exports (incl. Q1 quantile + CV/IDF + RegexTokenizer) |
| `_transformers.py` | VectorAssembler (**M7 `sparseOutput`** → sparse struct; default dense `make_array`), StringIndexer/IndexToString (`handleInvalid` ∈ error\|keep\|skip loud), OHE, scalers, Bucketizer, Imputer(mean/median/mode), Tokenizer, RegexTokenizer, StopWordsRemover, SQLTransformer, Binarizer, PolynomialExpansion, RobustScaler, QuantileDiscretizer, CountVectorizer, IDF. **PYC-2:** PolynomialExpansion monomial walker is `_polynomial_expansion_monomials` (depth bound 3) |
| Status constants | `QUANTILE_FAMILY_STATUS` / `COUNT_VECTORIZER_STATUS` / `IDF_STATUS` (SHIPPED Q1); `REGEX_TOKENIZER_GAPS_FALSE_STATUS` (SEED) |
| OneHotEncoder | Singular `inputCol`/`outputCol` **and** plural `inputCols`/`outputCols` (M4 merge-bar); always sparse struct; `handleInvalid` ∈ {error,keep,skip} loud; refuses pre-existing outputCols |

## I want to…

| Task | Go to |
|---|---|
| Add a transformer | `_transformers.py` + oracle in `tests/test_ml_feature_oracle.py` |
| Quantile family | `RobustScaler` / `QuantileDiscretizer` / `Imputer(median)` — fit via `approx_percentile_cont` |
| RegexTokenizer gaps=False | Loud STOP seed until `regexp_extract_all` exists |
| RegexTokenizer pattern → SQL | **SQP-1:** the pattern doubles its backslashes (`\s+` → `'\\s+'`) so the Spark door's escape processing folds it back to `\s+` |

## Pointers

Up: [../map.md](../map.md). Design: [docs/design/python-facade.md](../../../../../../../docs/design/python-facade.md) §4 Q3.

## Debug

- Median Imputer / RobustScaler / QuantileDiscretizer → engine `approx_percentile_cont` (t-digest; bounds oracles).
- `handleInvalid='error'` failures → `AnalysisException` after aggregate null/unseen checks.
- OHE illegal `handleInvalid` (not error|keep|skip) → `IllegalArgumentException` (octo C3-L-001; no silent keep).
- OHE transform refuses pre-existing singular/plural outputCols → `AnalysisException` (octo C3-L-002).
- OHE `handleInvalid=keep` expands `category_size+1` **before** `dropLast` (Spark); keep+dropLast=False → size=category_size+1 with invalid at last index (octo C4-L-001).
- OHE always sparse struct (no dense short-circuit).
- **M7:** VectorAssembler `sparseOutput=True` → `named_struct` sparse via `array_compact` (zeros omitted); default dense. SI `handleInvalid` membership loud at **fit and transform** (octo M7 C5); SI keep × OHE dropLast matrix pinned in oracle.
- **octo c1:** temp views dropped after fit/transform (`drop_temp_view`); SQLTransformer
  SELECT-only + single-statement; StandardScaler n=1 stddev → scale 1.0 pin.
- **octo c1b:** remove stray host/view drop from SQLTransformer._ml_fitted_state.
- **octo c2:** StopWordsRemover unnest+array_agg plan (no array_filter).
- **octo c3:** VectorAssembler refuses existing outputCol.
- **octo c4:** feature fit foreign-frame refuse pin.
- **octo c5:** OHE all-null fit category_size=0 pin.
- **Q1:** RegexTokenizer gaps=True via regexp_replace→string_to_array→unnest filter; gaps=False STOP.
- **Q1:** CountVectorizer transform uses unnest + conditional SUM join (ORDER BY rid); minTF fraction in [0,1).
- **octo c1 (Q1):** Imputer.missingValue (NaN default) applied in fit/transform via `isnan`/sentinel;
  `_sql_float` embeds for replacements/RobustScaler; fit try/finally drops temp views; RegexTokenizer
  ORDER BY rid; SQL 3rd-arg = t-digest centroids (facade dual-path: see
  `functions.percentile_approx` docstring + parent package map entry).
- **octo c2 (Q1):** `_materialize_rid_view` cache-pins `row_number` before unnest joins
  (RegexTokenizer/CountVectorizer/StopWordsRemover) — fixes CTE double-scan association (F-Q1-009);
  Imputer same-col in-place (F-Q1-010); StandardScaler `_sql_float` + NaN-aware fit (F-Q1-011).
- **octo c3:** MinMax/MaxAbs/IDF `_sql_float` + NaN-aware fit (F-Q1-012 class completion).
- **octo c4:** Binarizer threshold via `_sql_float` (F-Q1-013).
- **octo c6–c8:** remaining estimator fit try/finally (StringIndexer/OHE/Standard/MinMax/MaxAbs)
  — no residual S1+ on charter surface (F-Q1-014).
- 2026-08-01 style rider: `_transformers.py` lint fixes (pairwise/ternary/import order) + format.
- M7 format/lint gate clean (ruff format + py-lint).
- **F-3 (2026-08-17):** `_transformers.py` reached 100% public-docstring coverage — the one
  gap was `rec`, the monomial recursion inside `PolynomialExpansion`; it now states what it
  emits and what `start` bounds. Docstring-only, and the file's 2800-line ceiling is unmoved.
- **SQM round 7 (R7-1):** `_register_temp` / `_materialize_rid_view` name their views through
  `repark.spark._temp_views.scratch_view_name`, so the `{view}.*` reads and the `drop_temp_view`
  that follows all use the same home-qualified spelling.
