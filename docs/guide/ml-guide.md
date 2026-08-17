# Machine learning

`repark.spark.ml` mirrors the `pyspark.ml` package layout — `Pipeline`, `Estimator`, `Model`,
`feature`, `regression`, `classification`, `clustering`, `evaluation`, `tuning`, `linalg` — so the
mechanical `pyspark` → `repark.spark` import swap lands on it:

```python
from repark.spark.ml import Pipeline, PipelineModel
from repark.spark.ml.feature import StringIndexer, VectorAssembler
from repark.spark.ml.linalg import Vectors
from repark.spark.ml.regression import LinearRegression
```

`repark.spark.ml` is the path; there is no top-level `repark.ml` module and no `pyspark.ml` alias
shim.

It is a **subset**, not a port of MLlib, and this guide is mostly about where the edge is — because
the edge is where a migration gets hurt. Three things to know before anything else:

- Three estimators train in Rust: linear regression, logistic regression, k-means. Everything else
  either fits with session aggregate queries or delegates to a third-party library.
- Nothing here is tracked in [the divergence registry](../spark-sql-iceberg-parity.md). That file
  records differences from Apache Spark **SQL and Iceberg**; it has no ML rows at all. Every ML
  fact — layout, refusal, gap — lives next to the code it describes.
- Every gap below **raises**. There is no silent approximation in this package: no pseudoinverse
  for a singular system, no silent densify of a sparse feature vector, no quiet substitution of one
  metric for another.

## fit / transform

The flow is PySpark's. `Estimator.fit(dataset)` returns a `Model`; `Model.transform(dataset)`
returns a `DataFrame`. Column names come from the usual params — `featuresCol` (default
`"features"`), `labelCol` (`"label"`), `predictionCol` (`"prediction"`).

```python
from repark import ReparkSession

spark = ReparkSession.builder.appName("ml").getOrCreate()

rows = [(1.0, 1.0, 6.0), (2.0, 1.0, 8.0), (1.0, 3.0, 12.0), (4.0, 2.0, 15.0), (3.0, 5.0, 22.0)]
train = spark.createDataFrame(rows, ["x1", "x2", "label"])

assembler = VectorAssembler(inputCols=["x1", "x2"], outputCol="features")
feats = assembler.transform(train)
feats.show()
```

```text
+-----+-----+-------+------------+
| x1  | x2  | label | features   |
+-----+-----+-------+------------+
| 1.0 | 1.0 | 6.0   | [1.0, 1.0] |
| 2.0 | 1.0 | 8.0   | [2.0, 1.0] |
| 1.0 | 3.0 | 12.0  | [1.0, 3.0] |
| 4.0 | 2.0 | 15.0  | [4.0, 2.0] |
| 3.0 | 5.0 | 22.0  | [3.0, 5.0] |
+-----+-----+-------+------------+
```

```python
model = LinearRegression(featuresCol="features", labelCol="label").fit(feats)
print(model.coefficients, model.intercept)
```

```text
[2.000000000000002, 3.000000000000001] 0.999999999999992
```

(The data is exactly `2·x1 + 3·x2 + 1`, so those are the right numbers to floating-point noise.)

Two properties worth stating because they are contracts, not incidentals:

- **Python never iterates training rows.** `fit` hands the frame's plan to the native binder, which
  streams Arrow into the Rust kernel and returns a parameter dict. A fitted model holds parameters
  only — coefficients, intercept, centres — never cached training data.
- **`transform` is plan-built** and adds exactly one column, `predictionCol`, as a SQL projection
  over the input frame:

```python
scored = model.transform(feats)
print(scored.columns)
scored.select("label", "prediction").show()
```

```text
['x1', 'x2', 'label', 'features', 'prediction']
+-------+--------------------+
| label | prediction         |
+-------+--------------------+
| 6.0   | 5.999999999999995  |
| 8.0   | 7.999999999999997  |
| 12.0  | 11.999999999999998 |
| 15.0  | 15.000000000000002 |
| 22.0  | 22.000000000000004 |
+-------+--------------------+
```

`transform` refuses if the output name would collide with an existing column, and it checks the
feature width against the fitted model before it builds the projection.

## Evaluators

```python
from repark.spark.ml.evaluation import RegressionEvaluator

RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse").evaluate(scored)
```

```text
3.2994363900596664e-15
```

- `RegressionEvaluator` — `rmse`, `mse`, `mae`, `r2`.
- `BinaryClassificationEvaluator` — `accuracy`, `areaUnderROC` (Mann-Whitney rank-sum),
  `areaUnderPR`. It reads a score column you supply; see the note on `probabilityCol` below.
- `MulticlassClassificationEvaluator` — `accuracy` only. Spark's own default, `metricName="f1"`,
  refuses loudly rather than quietly returning accuracy:

```text
UnsupportedOperationException: MulticlassClassificationEvaluator.metricName='f1' requires per-label
precision/recall aggregation; not implemented in v1. Use metricName='accuracy'. Seed → later unit
(macro / weighted F1 plan aggregates).
```

## Vectors: the dense and sparse cell shapes

`repark.spark.ml.linalg` owns the vector layout, and the layout is Arrow-native rather than a Spark
`VectorUDT`. This is the most visible difference from PySpark for anyone who reads cells back into
Python.

```python
dense = spark.createDataFrame([(Vectors.dense([1.0, 2.0, 3.0]),)], ["features"])
print(dense.to_arrow().schema.field("features").type)
print(dense.collect()[0].asDict()["features"])

sparse = spark.createDataFrame([(Vectors.sparse(4, [0, 2], [1.5, 2.5]),)], ["features"])
print(sparse.schema.simpleString())
print(sparse.collect()[0].asDict()["features"])
```

```text
fixed_size_list<item: double>[3]
[1.0, 2.0, 3.0]
struct<features:struct<size:int,indices:array<int>,values:array<double>>>
{'size': 4, 'indices': [0, 2], 'values': [1.5, 2.5]}
```

So a **sparse cell is a dict** with exactly the keys `size`, `indices`, `values` — addressed as
`row["features"]["indices"]`, not through a `SparseVector` method. `Vectors.sparse(...)` still
builds the familiar object on the way *in*; it is the stored shape that is a struct.

Two consequences:

- Dense vectors built by `Vectors.dense` are **fixed width per column**
  (`fixed_size_list<double>[n]`). Mixing dense widths inside one column raises an
  `AnalysisException` naming the fixed-width limitation — it never quietly falls back to a
  variable-length list. (`VectorAssembler`'s dense output is an ordinary `list<double>`, since its
  width is determined by `inputCols`.)
- `VectorAssembler` produces dense output by default; `sparseOutput=True` produces the struct:

```python
sp = VectorAssembler(inputCols=["x1", "x2"], outputCol="features", sparseOutput=True)
print(sp.transform(train).collect()[0].asDict()["features"])
```

```text
{'size': 2, 'indices': [0, 1], 'values': [1.0, 1.0]}
```

## What trains natively

| Estimator | Algorithm | Notes |
|---|---|---|
| `LinearRegression` | OLS via a hand-rolled Cholesky solve | pure OLS |
| `LogisticRegression` | IRLS, reusing the same Cholesky | **binomial only** |
| `KMeans` | Lloyd | `initMode="random"` is **required** |

All three cap at 4096 features, and all three refuse rather than degrade. `elasticNetParam != 0`
(L1 / elastic net) and `standardization=True` are not implemented — scale your features with
`StandardScaler` in the pipeline instead of asking the estimator to do it:

```text
UnsupportedOperationException: repark.ml: elasticNetParam=0.5 is unsupported in M3 (only
elasticNetParam=0 / pure least squares). Seed → M4 for elastic net / coordinate descent
```

A singular design matrix raises rather than silently regularizing or reaching for a pseudoinverse
— worth knowing, because perfectly collinear feature columns are easy to build by accident:

```text
IllegalArgumentException: repark.ml: singular or ill-conditioned design matrix (Cholesky failed at
pivot 2: …). repark refuses pseudoinverse / silent regularization (divergence vs Spark solver path
— see docs/design/python-facade.md §4 Q3)
```

k-means refuses Spark's *default* `initMode`, which is the one case where a ported script fails on
its first line rather than its tenth. `k-means||` is not implemented:

```python
from repark.spark.ml.clustering import KMeans

KMeans(featuresCol="features", k=2).fit(feats)
```

```text
UnsupportedOperationException: repark.ml: KMeans initMode default (k-means||) is not implemented.
Set initMode="random" explicitly (no fake k-means||). Seeded random + assignment parity up to label
permutation when initMode=random
```

```python
KMeans(featuresCol="features", k=2, initMode="random", seed=1).fit(feats).clusterCenters()
```

```text
[[2.333333333333333, 1.3333333333333333], [2.0, 4.0]]
```

And the native estimators refuse **sparse** feature columns outright rather than densifying behind
your back:

```text
IllegalArgumentException: repark.ml: repark.ml fit: features column type Struct([…size…indices…
values…]) unsupported (need List/FixedSizeList of float64, or a scalar numeric column) — sparse
VectorUDT {size,indices,values} is not accepted by native estimators; use dense VectorAssembler
(sparseOutput=False) or densify before fit …
```

## The feature package

`repark.spark.ml.feature` is plan-built end to end: an estimator's `fit` is a session aggregate
query, and every `transform` is a projection. What is there:

`VectorAssembler` · `StringIndexer` · `IndexToString` · `OneHotEncoder` · `StandardScaler` ·
`MinMaxScaler` · `MaxAbsScaler` · `RobustScaler` · `Bucketizer` · `Imputer` · `Tokenizer` ·
`RegexTokenizer` · `StopWordsRemover` · `SQLTransformer` · `Binarizer` · `PolynomialExpansion` ·
`QuantileDiscretizer` · `CountVectorizer` · `IDF` — each with its `…Model` where PySpark has one.

```python
cat = spark.createDataFrame([("a",), ("b",), ("a",)], ["c"])
si = StringIndexer(inputCol="c", outputCol="c_idx")
si.fit(cat).transform(cat).show()
```

```text
+---+-------+
| c | c_idx |
+---+-------+
| a | 0.0   |
| b | 1.0   |
| a | 0.0   |
+---+-------+
```

One declared gap in that list: `RegexTokenizer(gaps=False)` needs an extract-all regex primitive
the engine does not have, and says so.

## Pipelines, and saving one

`Pipeline` / `PipelineModel` behave as in PySpark: stages run left to right, an estimator stage is
fit then applied, a transformer stage passes through. Persistence is a repark format
(`format: "repark-ml"`, version 1), not Spark's:

```
<path>/metadata.json
<path>/stages/<idx>_<uid>/metadata.json
<path>/stages/<idx>_<uid>/fitted/*.parquet
```

```python
pipe = Pipeline(stages=[VectorAssembler(inputCols=["x1", "x2"], outputCol="features"),
                        LinearRegression(featuresCol="features", labelCol="label")])
fitted = pipe.fit(train)
fitted.write().overwrite().save("/tmp/repark-model")

reloaded = PipelineModel.load("/tmp/repark-model")
print([type(stage).__name__ for stage in reloaded.stages])
print(reloaded.transform(train).select("prediction").collect()
      == fitted.transform(train).select("prediction").collect())
```

```text
['VectorAssembler', 'LinearRegressionModel']
True
```

Three properties of that format, each deliberate:

- **Fitted parameters only.** A save never writes training rows — pinned by a test that fits on a
  known secret value, saves, and greps every written file for it.
- **Atomic.** A save writes to a sibling staging directory and renames; it never deletes the target
  before the new tree is complete, so an interrupted overwrite cannot leave you with neither.
- **Allowlisted on load.** Loading instantiates only classes under `repark.spark.ml.*`, and
  explicitly refuses `repark.spark.ml.ext`; a path outside the save root is refused. A model
  directory is not a code-execution vector.

## The delegated backends (`repark[ml-ext]`)

`repark.spark.ml.ext` wraps three third-party libraries behind the same `Estimator` / `Model`
surface. They need the extra:

```
pip install "repark[ml-ext]"
```

| Class | Backend | Save / load |
|---|---|---|
| `XGBoostRegressor` / `XGBoostClassifier` | xgboost | booster bytes — supported |
| `LightGBMRegressor` / `LightGBMClassifier` | lightgbm | model text blob — supported |
| `RandomForestRegressor` / `RandomForestClassifier` | scikit-learn | **refused** |

The scikit-learn refusal is policy, not a gap: persisting those models would mean pickling them,
and `repark.spark.ml.ext` never pickles a fitted model because unpickling executes arbitrary code.
A `PipelineModel` containing *any* ext stage refuses to save rather than writing a hollow directory
that would load as an empty model.

Importing the package always works — the dependency check happens when you construct an estimator,
and it names the extra:

```python
import repark.spark.ml.ext as ext

ext.XGBoostRegressor(featuresCol="f", labelCol="l")
```

```text
ImportError: repark.ml.ext requires the optional extra 'repark[ml-ext]' (xgboost, lightgbm,
scikit-learn, numpy, pandas). Install with: pip install 'repark[ml-ext]'
```

## What is absent, plainly

Measured against `pyspark.ml`:

| Absent | Status |
|---|---|
| `probabilityCol` / model-produced `rawPredictionCol` | no model writes a soft score; classification `transform` adds a hard `prediction` only. `BinaryClassificationEvaluator` reads a score column you supply |
| Multinomial logistic regression | binomial IRLS only |
| Elastic net / L1, in-estimator `standardization` | refuse; use `StandardScaler` upstream |
| `k-means\|\|` init | refuses; `initMode="random"` only |
| `MulticlassClassificationEvaluator(metricName="f1")` | refuses |
| `RegexTokenizer(gaps=False)` | refuses (no extract-all in the engine) |
| Recommendation / collaborative filtering (alternating least squares) | not present at all |
| Trees / forests / GBTs as *native* estimators | only via `repark[ml-ext]` |
| Sparse features into a native estimator | refuses; assemble dense, or densify deliberately |
| `LinearRegressionModel.summary` | the attribute does not exist (`AttributeError`); the placeholder class behind it discloses each missing metric. Use `RegressionEvaluator` |

Because none of this is in the divergence registry, checking a claim here works differently from
the rest of repark: read the module, or call it and read the refusal. Every row above was produced
by calling it.

## See also

- [getting-started.md](getting-started.md) — the `ml-ext` extra and the rest of the optional
  dependency set.
- [dataframe-guide.md](dataframe-guide.md) — the frame operations a feature pipeline is built from.
- [../design/python-facade.md](../design/python-facade.md) §4 — the in-repo design record for the
  ML surface and why the Rust crate exists.
- [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) — the divergence registry, for
  the SQL and Iceberg differences this guide does *not* cover.
