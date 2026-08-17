# Getting started

Install repark, build a session, get data in and out. Written for a data engineer arriving from
PySpark: everything below is the facade surface, so the import line is usually the only edit a
small script needs.

repark is **near-drop-in**, not a clone. Where it differs from Apache Spark on purpose, the
difference is written down in the [divergence registry](../spark-sql-iceberg-parity.md) with the
test that pins it. Read that file before you promise a migration to anyone; this guide links the
rows it touches.

## Install

```
pip install repark
```

The distribution name and the import name are both `repark`. It needs **Python 3.12 or newer**
(`requires-python = ">=3.12"`), and its one hard dependency is `pyarrow` — Arrow is the interchange
format across the Rust boundary.

There is no JVM. Nothing here starts one, looks for `JAVA_HOME`, or reads a `spark-defaults.conf`.

The wheel is a PyO3 **abi3** build (`cp312-abi3`), so a single wheel serves every Python ≥ 3.12 on a
platform instead of one wheel per interpreter minor. Which platforms currently have a published
wheel — and the current release itself — is in [STATUS.md](../../STATUS.md) "Release state"; on a
platform with no published wheel, `pip` falls back to building from source, which needs the Rust
toolchain pinned in [`rust-toolchain.toml`](../../rust-toolchain.toml).

Optional extras, none of them required to run the engine:

| Extra | Pulls in | You want it for |
|---|---|---|
| `pandas` | pandas | `df.toPandas()` / pandas ingestion |
| `polars` | polars | `df.to_polars()` / polars ingestion |
| `numpy` | numpy | `df.to_numpy()` |
| `ml-ext` | xgboost, lightgbm, scikit-learn | the delegated ML backends |

```
pip install "repark[pandas,polars]"
```

## Your first session

```python
from repark import ReparkSession

spark = ReparkSession.builder.appName("etl").getOrCreate()
```

That is the migration edit: `from pyspark.sql import SparkSession` becomes
`from repark import ReparkSession`. `SparkSession` is kept as an alias of `ReparkSession`, so
`from repark import SparkSession` also works and the rest of the script does not move. For a
mechanical port of a larger tree, `sed 's/pyspark/repark.spark/'` works too — the facade is
mirrored at `repark.spark.*` (`repark.spark.functions`, `repark.spark.types`,
`repark.spark.window`, …).

`spark.version` returns `repark-<version>`, **not** a Spark release number — deliberately, so this
engine is identifiable in a log. That is one of the rows in the divergence registry's
[drop-in disclosure table](../spark-sql-iceberg-parity.md#8-drop-in-disclosure-rationale); a
script that *logs* the value is fine, one that *parses* it as a Spark release is not.

Sessions are process-scoped: `getOrCreate()` returns the live session if there is one. Call
`spark.stop()` when you are done — every handle taken from a stopped session refuses loud:

```python
spark.stop()
spark.createDataFrame([(1,)], ["id"])   # anything after stop()
```

```text
RuntimeError: Cannot call methods on a stopped ReparkSession
```

## createDataFrame

The usual PySpark shapes all work — a list of tuples plus column names, a list of dicts, a list of
`Row`, an explicit `StructType`, and pandas / polars frames:

```python
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
df.show()
```

```text
+----+------+
| id | name |
+----+------+
| 1  | a    |
| 2  | b    |
+----+------+
```

```python
from repark import Row
from repark.spark.types import LongType, StringType, StructField, StructType

spark.createDataFrame([{"id": 1, "name": "a"}])                     # dict rows
spark.createDataFrame([Row(id=1, name="a")])                        # Row objects
spark.createDataFrame(
    [(1, "a")],
    StructType([StructField("id", LongType()), StructField("name", StringType())]),
)                                                                   # explicit schema
```

All four infer `struct<id:bigint,name:string>`. Python `int` becomes `bigint` — repark does not
guess a narrower width from the values.

One construction that PySpark accepts and repark refuses: **exact duplicate output names**.

```python
spark.createDataFrame([(1, 2)], ["id", "id"])
```

```text
AnalysisException: unique expression names required; createDataFrame schema has duplicate column names
```

That is registry row [ID-3](../spark-sql-iceberg-parity.md#id-3--exact-duplicate-column-names-are-refused-at-construction)
— repark refuses at construction, Spark accepts and raises later only if you reference the name.

## Reading and writing files

`spark.read` and `df.write` carry the PySpark shape. Parquet needs no options:

```python
df.write.mode("overwrite").parquet("out/events.parquet")
spark.read.parquet("out/events.parquet").orderBy("id").collect()
```

```text
[Row(id=1, name='a'), Row(id=2, name='b')]
```

CSV takes the familiar `header` / `inferSchema` options on both sides:

```python
df.write.mode("overwrite").option("header", True).csv("out/events.csv")
back = spark.read.option("header", True).option("inferSchema", True).csv("out/events.csv")
back.orderBy("id").collect()
```

```text
[Row(id=1, name='a'), Row(id=2, name='b')]
```

`spark.read.json` reads JSON / NDJSON and takes the PySpark `multiLine`, `mode` and `compression`
options; `df.write.json` writes newline-delimited JSON:

```python
df.write.mode("overwrite").json("out/events.json")
spark.read.json("out/events.json").orderBy("id").collect()
```

```text
[Row(id=1, name='a'), Row(id=2, name='b')]
```

`spark.read` also carries `.format(...).option(...).load(...)`, and a repark extension,
`spark.read.smartCsv(...)`, for messy real-world CSV: delimiter auto-detect, preamble skip, header
auto-detect, null-padded ragged rows, and a type-inference ladder that samples the first 10 000
data rows (the full file is always read for *data*; `df.describe_ingest()` reports what inference
actually saw). Reach for plain `.csv()` with an explicit `sep=` when you already know the shape —
auto-detection is a guess, and the tour notebook shows a corpus where it guesses wrong.

## Your first `dynamicFlatten`

`dynamicFlatten()` is a repark extension, not a PySpark API: it walks the schema and unnests every
struct (and, by default, explodes every list) until the frame is flat. Nested dict rows are the
shape it exists for.

```python
nested = [
    {"id": 1, "payload": {"x": 1, "y": "left"}},
    {"id": 2, "payload": {"x": 2, "y": "right"}},
]
frame = spark.createDataFrame(nested)
frame.dynamicFlatten().show()
```

```text
+----+-----------+-----------+
| id | payload_x | payload_y |
+----+-----------+-----------+
| 1  | 1         | left      |
| 2  | 2         | right     |
+----+-----------+-----------+
```

Two things are worth knowing on day one:

- The parent path becomes the prefix (`payload` + `_` + `x` → `payload_x`), so colliding inner
  field names never silently overwrite each other; a collision that survives prefixing raises
  instead.
- It works here **because the nested dict inferred as a struct**. That is repark's default and a
  declared divergence from PySpark, which infers `MapType` — registry row
  [FA-4](../spark-sql-iceberg-parity.md#fa-4--infernesteddictasstruct-defaults-to-true). See
  [session-and-conf.md](session-and-conf.md) for the conf that flips it back.

The walk is schema-only: no rows are read to decide the shape. Details, flags and the list-explode
behavior are in [dataframe-guide.md](dataframe-guide.md).

## Where to go next

- [session-and-conf.md](session-and-conf.md) — the builder, the conf keys you will actually set,
  and which ones are fixed at session build.
- [dataframe-guide.md](dataframe-guide.md) — the lazy model, joins, windows, the actions, and the
  limits worth knowing before you port a job.
- [sql-doors.md](sql-doors.md) — `spark.sql` versus `repark.sql`, and why they are two doors.
- [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) — the honest ledger of every
  difference from Apache Spark, each with the test that pins it.
- [../../examples/notebooks/datasets_tour.ipynb](../../examples/notebooks/datasets_tour.ipynb) —
  a runnable tour over five generated torture datasets (nested structures, inference edges,
  extreme types, credential-named columns, messy CSV). It is committed with outputs cleared; build
  the module with `make develop` and run it against your own kernel.
