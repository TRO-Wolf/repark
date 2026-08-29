# map — python/repark/src/repark/spark/ml/feature

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Plan-built `pyspark.ml.feature` transformers under `repark.spark.ml.feature`.
`fit` = session aggregates; `transform` = SQL/plan expressions only.

## Contents

| Path | Role |
|---|---|
| `__init__.py` | Public exports (quantile family + CV/IDF + RegexTokenizer). |
| `_transformers.py` | VectorAssembler (`sparseOutput=True` → sparse `named_struct` via `array_compact`, zeros omitted; default dense), StringIndexer/IndexToString (`handleInvalid` ∈ error\|keep\|skip, loud at fit AND transform), OneHotEncoder (singular and plural `inputCol(s)`/`outputCols`; always sparse struct; refuses pre-existing outputCols; `handleInvalid=keep` expands `category_size+1` BEFORE `dropLast`), scalers, Bucketizer, Imputer (mean/median/mode), Tokenizer, RegexTokenizer, StopWordsRemover, SQLTransformer (SELECT-only, single statement), Binarizer, PolynomialExpansion (monomial walker `_polynomial_expansion_monomials`, depth bound 3), RobustScaler, QuantileDiscretizer, CountVectorizer, IDF. Identifiers go through `repark._idents.quote_ident`; embedded strings (labels, terms, stop words, regex pattern) go through `repark.spark._idents.sql_string_literal`, so a backslash/apostrophe value survives the Spark door — the RegexTokenizer pattern doubles its backslashes (`\s+` → `'\\s+'`) for the same reason. |
| Status constants | `QUANTILE_FAMILY_STATUS` / `COUNT_VECTORIZER_STATUS` / `IDF_STATUS` (shipped); `REGEX_TOKENIZER_GAPS_FALSE_STATUS` (seed). |

## I want to…

| Task | Go to |
|---|---|
| Add a transformer | `_transformers.py` + oracle in `tests/test_ml_feature_oracle.py` |
| Quantile family | `RobustScaler` / `QuantileDiscretizer` / `Imputer(median)` — fit via `approx_percentile_cont` |
| RegexTokenizer gaps=False | Loud STOP seed until `regexp_extract_all` exists |
| RegexTokenizer pattern → SQL | the backslash-doubling rule above |

## Pointers

Up: [../map.md](../map.md). Design: [docs/design/python-facade.md](../../../../../../../docs/design/python-facade.md) §4 Q3.

## Debug

- Median Imputer / RobustScaler / QuantileDiscretizer → engine `approx_percentile_cont` (t-digest; bounds oracles).
- `handleInvalid='error'` failures → `AnalysisException` after aggregate null/unseen checks.
- OHE illegal `handleInvalid` (not error|keep|skip) → `IllegalArgumentException`; no silent keep.
- OHE always sparse struct (no dense short-circuit).
- RegexTokenizer gaps=True: `regexp_replace` → `string_to_array` → `unnest` filter.
- CountVectorizer transform: `unnest` + conditional SUM join ordered by `rid`; `minTF` fraction in [0,1).
- `_materialize_rid_view` cache-pins `row_number` before the unnest joins (RegexTokenizer /
  CountVectorizer / StopWordsRemover), so the CTE cannot double-scan.
- Temp views drop after fit/transform (`drop_temp_view`); every estimator fit runs try/finally.
- SQL 3rd argument of the quantile path = t-digest centroids (facade dual-path:
  `functions.percentile_approx` docstring + parent package map entry).
- Scratch views are named through `repark.spark._temp_views.scratch_view_name`, so `{view}.*`
  reads and the `drop_temp_view` that follows share one home-qualified spelling.
