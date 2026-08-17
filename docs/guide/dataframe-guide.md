# The DataFrame guide

How the lazy DataFrame behaves, the operations you will reach for first, how data gets in and out,
and the limits worth knowing before you port a job.

This is the facade surface (`repark.spark`, re-exported from `repark`). Start with
[getting-started.md](getting-started.md) if you have not built a session yet, and keep
[the divergence registry](../spark-sql-iceberg-parity.md) open — it is the honest ledger of every
place repark answers differently from Apache Spark, each row carrying the test that pins it.

## The lazy model

A `DataFrame` is a **plan**, not rows. Transformations return a new plan; nothing executes until an
action asks for data.

```python
from repark.spark import functions as F

events = spark.createDataFrame(
    [(1, "eu", 10.0), (2, "us", 20.0), (3, "eu", 30.0)],
    ["id", "region", "amount"],
)
plan = events.filter(F.col("amount") > 15).select("region", "amount")
plan.columns
```

```text
['region', 'amount']
```

`columns` and `schema` came back without reading a row: the schema is resolved by analysis, which
is also why an unresolvable column name raises *there* rather than at collect time. The action is
what runs the query:

```python
plan.orderBy("amount").collect()
```

```text
[Row(region='us', amount=20.0), Row(region='eu', amount=30.0)]
```

`df.explain()` prints the logical and physical plans (DataFusion's, since DataFusion is the engine
underneath). Two consequences of laziness that bite in practice:

- **Row order is not defined without `orderBy`.** Nothing about a single-node engine makes it
  stable across operations.
- **Each action re-runs the plan.** `df.cache()` materializes into an in-process table so repeated
  actions do not; `unpersist()` releases it. `cache` / `persist` accept PySpark's `StorageLevel`
  flags for signature parity but always materialize in-process — no disk spill, no off-heap, no
  replication, and the first call warns to say so.

## Selecting, filtering, aggregating

Everything here is the PySpark spelling.

```python
events.select("id", F.col("amount") * 2).columns
```

```text
['id', '(amount * 2)']
```

```python
events.selectExpr("id", "amount * 2 AS doubled").collect()
```

```text
[Row(id=1, doubled=20.0), Row(id=2, doubled=40.0), Row(id=3, doubled=60.0)]
```

`filter` (and its alias `where`) takes either a SQL string or a `Column`, and the two agree:

```python
events.filter("amount > 15").orderBy("id").collect()
events.filter(events["amount"] > 15).orderBy("id").collect()
```

```text
[Row(id=2, region='us', amount=20.0), Row(id=3, region='eu', amount=30.0)]
```

`withColumn`, `withColumns`, `drop`, `distinct`, `limit`, `union`, `orderBy` / `sort` and the
`groupBy(...).agg(...)` family are all present:

```python
events.groupBy("region").agg(F.sum("amount").alias("total")).orderBy("region").show()
```

```text
+--------+-------+
| region | total |
+--------+-------+
| eu     | 40.0  |
| us     | 20.0  |
+--------+-------+
```

The function library lives in `repark.spark.functions` — the usual `from repark.spark import
functions as F`. (`repark.functions` still resolves as a compatibility shim; prefer the
`repark.spark` spelling in new code.)

## Joins

`how` takes the PySpark vocabulary, including the semi family (`semi` / `leftsemi`,
`anti` / `leftanti`, and the `left_semi` / `left_anti` spellings):

```python
left = spark.createDataFrame([(1, "eu"), (2, "us")], ["id", "region"])
right = spark.createDataFrame([(1, 10.0)], ["id", "amount"])
for how in ("inner", "left", "leftsemi", "leftanti"):
    print(how, left.join(right, on="id", how=how).orderBy("id").collect())
```

```text
inner     [Row(id=1, region='eu', amount=10.0)]
left      [Row(id=1, region='eu', amount=10.0), Row(id=2, region='us', amount=None)]
leftsemi  [Row(id=1, region='eu')]
leftanti  [Row(id=2, region='us')]
```

A semi or anti join contributes no right-hand columns, so referring to one afterwards raises rather
than silently resolving to the left side. And a **conditionless** semi/anti join refuses instead of
falling through to a cross join, because those are different result sets:

```python
left.join(right, how="leftsemi")
```

```text
AnalysisException: join type 'leftsemi' requires an `on` condition. A conditionless leftsemi join
is not a Cartesian product, so repark refuses it rather than returning a cross join's rows. Pass
`on=` a column name, a list of names, or a boolean Column.
```

That refusal is registry row
[G4-3](../spark-sql-iceberg-parity.md#g4-3--conditionless-dataframe-semianti-join-refuses).

## Window functions

`Window` and `WindowSpec` are near-drop-in: `partitionBy`, `orderBy`, `rowsBetween`,
`rangeBetween`, and the `Window.unboundedPreceding` / `currentRow` / `unboundedFollowing`
sentinels.

```python
from repark.spark import Window

steps = spark.createDataFrame(
    [("eu", 1, 10), ("eu", 2, 20), ("us", 1, 5), ("us", 2, 50)],
    ["region", "step", "value"],
)
w = Window.partitionBy("region").orderBy("step")
steps.select(
    "region", "step", "value",
    F.row_number().over(w).alias("rn"),
    F.sum("value").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)).alias("running"),
).orderBy("region", "step").show()
```

```text
+--------+------+-------+----+---------+
| region | step | value | rn | running |
+--------+------+-------+----+---------+
| eu     | 1    | 10    | 1  | 10      |
| eu     | 2    | 20    | 2  | 30      |
| us     | 1    | 5     | 1  | 5       |
| us     | 2    | 50    | 2  | 55      |
+--------+------+-------+----+---------+
```

Ranking and offset functions (`rank`, `dense_rank`, `ntile`, `lag`, `lead`, `nth_value`,
`percent_rank`, `cume_dist`) are all there. Two things the registry records about windows and
worth reading before you depend on them: the SQL door's ranking functions come back with different
**Arrow integer widths** than Spark's (`G5-RANK-TYPE-*`), and a FOLLOWING-to-FOLLOWING frame
includes the current row (`G5b-R4`).

## Actions — getting rows out

| Call | Returns | Peak memory |
|---|---|---|
| `collect()` | `list[Row]` | O(result) |
| `toLocalIterator()` | iterator of `Row` | O(batch) |
| `to_arrow()` | `pyarrow.Table` | O(result) |
| `to_arrow_batches()` | iterator of `pyarrow.RecordBatch` | O(batch) |
| `toPandas()` / `to_pandas()` | `pandas.DataFrame` | O(result) |
| `to_polars()` | `polars.DataFrame` | O(result) |
| `to_numpy()` | `numpy.ndarray` | O(result) |
| `count()` | `int` | — |
| `show()` | prints, returns `None` | O(n rows) |

```python
small = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
small.collect()                                    # [Row(id=1, name='a'), Row(id=2, name='b')]
small.to_arrow().schema.types                      # [DataType(int64), DataType(string)]
[batch.num_rows for batch in small.to_arrow_batches()]   # [2]
list(small.toPandas().columns)                     # ['id', 'name']
small.to_polars().shape                            # (2, 2)
```

`to_arrow` is the zero-copy path — data crosses the Rust boundary through the Arrow C stream, so
Arrow and polars exports do not go through Python objects at all. `collect()` builds `Row` objects
and is the expensive one; it streams the Arrow batches so it does not hold a full table *and* a
full list at once, but the list itself is O(result).

`to_arrow_batches` and `toLocalIterator` are the streaming twins. Reach for them on anything you
would not want resident.

**When you assert on a result, assert on the Arrow types too.** `show()` renders values through a
formatter and will happily agree while the type underneath moved; `collect()` / `to_arrow()` carry
value *and* type. That is the repo's own testing rule, and it is good advice for your pipeline
tests as well.

## Getting data in

### Rows

```python
from repark import Row
from repark.spark.types import LongType, StringType, StructField, StructType

spark.createDataFrame([(1, "a")], ["id", "name"])                     # tuples + names
spark.createDataFrame([{"id": 1, "name": "a"}])                       # dict rows
spark.createDataFrame([Row(id=1, name="a")])                          # Row objects
spark.createDataFrame(
    [(1, "a")],
    StructType([StructField("id", LongType()), StructField("name", StringType())]),
)                                                                     # explicit schema
```

All four give `struct<id:bigint,name:string>`. Python `int` becomes `bigint` — repark does not
narrow to the values. pandas and polars frames are accepted directly too.

**Give an explicit `schema=` when the types matter.** Inference is a convenience, and the two
ingestion paths below are where it surprises people.

### Dict cells: struct or map

```python
rows = [{"id": 1, "tags": {"a": "1"}}, {"id": 2, "tags": {"b": "2", "c": "3"}}]
spark.createDataFrame(rows).schema.simpleString()
```

```text
struct<id:bigint,tags:struct<a:string,b:string,c:string>>
```

A dict-valued **cell** infers as a `StructType`, at any nesting depth. PySpark infers `MapType`
here; repark's default is the opposite, deliberately — registry row
[FA-4](../spark-sql-iceberg-parity.md#fa-4--infernesteddictasstruct-defaults-to-true). Set
`spark.sql.pyspark.inferNestedDictAsStruct.enabled` to `"false"` for byte-identical PySpark
inference:

```text
struct<id:bigint,tags:map<string,string>>
```

The struct is the **union of the fields seen**, with the missing ones null-filled:

```python
u = spark.createDataFrame([{"id": 1, "p": {"a": 1}}, {"id": 2, "p": {"b": 2}}])
u.show()
```

```text
+----+---------------------+
| id | p                   |
+----+---------------------+
| 1  | {'a': 1, 'b': None} |
| 2  | {'a': None, 'b': 2} |
+----+---------------------+
```

Which choice you want is a real decision, not a formality:

- **struct** — fields are addressable and typed; the shape is fixed by what the sample showed. Use
  it for records with a known set of keys. This is the default, and it is what makes
  `dynamicFlatten` do something useful.
- **map** — the key set is open and every value shares one type. Use it for genuinely dynamic keys,
  and set the conf to `"false"` (or pass `schema=`) to get it.

A row-level dict — the dict *is* the row — is unaffected by the conf either way, and an explicit
`schema=` wins over both.

### Addressing a struct field

```python
u.selectExpr("id", "p.a AS a").collect()
u.select("id", F.col("p").getField("a").alias("a")).collect()
u.select("id", u["p"]["a"].alias("a")).collect()
```

```text
[Row(id=1, a=1), Row(id=2, a=None)]
```

The dotted *string* form is not a path here — it is read as a column name:

```python
u.select("id", "p.a")
```

```text
AnalysisException: A column with name `p.a` cannot be resolved; available columns: ['id', 'p']
```

Use `selectExpr`, `getField`, or subscripting.

### Map cells cross the boundary as pairs

```python
m = spark.createDataFrame([{"id": 1, "tags": {"a": "1"}}])   # with the conf set to "false"
m.collect()      # [Row(id=1, tags={'a': '1'})]
m.to_arrow().to_pylist()   # [{'id': 1, 'tags': [('a', '1')]}]
```

`collect()` gives you a `dict` (Spark's shape); the Arrow export keeps Arrow's list-of-pairs. That
split is registry row
[G10-1](../spark-sql-iceberg-parity.md#g10-1--typed-map-topandas-cells-are-list-of-pairs-not-dict).

## `dynamicFlatten` and `explode`

`dynamicFlatten()` is a repark extension: a schema-only walk that unnests every struct and — by
default — explodes every list, repeating until the frame is flat.

```python
nested = [
    {"id": 1, "meta": {"k": "x"}, "vals": [1, 2]},
    {"id": 2, "meta": {"k": "y"}, "vals": [3]},
]
frame = spark.createDataFrame(nested)
frame.schema.simpleString()
```

```text
struct<id:bigint,meta:struct<k:string>,vals:array<bigint>>
```

```python
frame.dynamicFlatten().show()
```

```text
+----+--------+------+
| id | meta_k | vals |
+----+--------+------+
| 1  | x      | 1    |
| 1  | x      | 2    |
| 2  | y      | 3    |
+----+--------+------+
```

The flags, with their defaults:

| Flag | Default | Effect |
|---|---|---|
| `separator` | `"_"` | joins the parent path to the field name (`meta` → `meta_k`) |
| `explode_lists` | `True` | `False` unnests structs and leaves arrays alone |
| `drop_null_lists` | `True` | drops all-null (`array<void>`) columns instead of exploding them |
| `max_depth` | `100` | hard bound; **refuses loud** if work remains, never truncates silently |

```python
frame.dynamicFlatten(explode_lists=False).schema.simpleString()
```

```text
struct<id:bigint,meta_k:string,vals:array<bigint>>
```

The parent-path prefix is the collision guard: `a.x` and `b.x` become `a_x` and `b_x`. If a
prefixed name still collides with a surviving column, it raises rather than overwriting.
`dynamic_flatten` is bound as an alias.

`explode` works as it does in PySpark, and the **string form keeps the column's case** — a
`createDataFrame` field named `Legs` does not fold to `legs` and then fail to resolve:

```python
animals = spark.createDataFrame([{"Name": "cat", "Legs": [1, 2, 3, 4]}])
animals.select("Name", F.explode("Legs").alias("Leg")).show()
```

```text
+------+-----+
| Name | Leg |
+------+-----+
| cat  | 1   |
| cat  | 2   |
| cat  | 3   |
| cat  | 4   |
+------+-----+
```

`explode_outer` behaves the same way. `posexplode` is not implemented and refuses loud rather than
returning something close:

```text
UnsupportedOperationException: posexplode is not supported yet (no first-class
unnest-with-ordinality; explode/explode_outer are available via guarded unnest rewrite)
```

## Limits worth knowing

Each of these is a **declared** difference with a live test pinning it — the registry is the full
list, this is the short one you are most likely to hit while porting.

**No lateral column aliases.** A new column cannot reference another new column created in the same
call:

```python
events.withColumns({"x": F.col("amount") + 1, "y": F.col("x")})
```

raises `AnalysisException` (the message names `x` as an unresolvable field), and so does the
reverse dict order. Spark resolves the forward order laterally. Split it into two `withColumn`
calls. Registry row
[FA-1](../spark-sql-iceberg-parity.md#fa-1--lateral-column-aliases-in-withcolumns).

**Quoted identifiers resolve case-sensitively.** `` `ID` `` (Spark door) and `"ID"` (native door)
do not find a column stored as `id`; unquoted references agree with Spark. Registry row
[ID-1](../spark-sql-iceberg-parity.md#id-1--a-quoted-identifier-resolves-case-sensitively).

**Exact duplicate output names refuse at construction:**

```python
spark.createDataFrame([(1, 2)], ["id", "id"])
```

```text
AnalysisException: unique expression names required; createDataFrame schema has duplicate column names
```

Registry row [ID-3](../spark-sql-iceberg-parity.md#id-3--exact-duplicate-column-names-are-refused-at-construction).

**Interchange round trips widen numeric containers.** A frame exported to polars and re-ingested
through `createDataFrame` comes back wider, because Python re-inference cannot carry the Arrow
width:

```python
src = spark.sql("SELECT CAST(1 AS INT) AS n")
src.to_arrow().schema.types                                  # [DataType(int32)]
spark.createDataFrame(src.to_polars()).to_arrow().schema.types   # [DataType(int64)]
```

Values are preserved; only the container moves. Decimals widen the same way, to `(38, 18)`.
Registry rows [TY-4](../spark-sql-iceberg-parity.md#ty-4--createdataframe-widens-arrow-int32-to-int64)
and [TY-5](../spark-sql-iceberg-parity.md#ty-5--createdataframe-widens-decimal-precision-and-scale).
Pass an explicit `schema=` if the width is load-bearing.

**Errors are `RuntimeError` subclasses.** `PySparkException` and its children subclass
`RuntimeError` here, which they do not in `pyspark.errors` — so `except RuntimeError` catches
strictly more than it would under Spark, never less. Registry row
[FA-3](../spark-sql-iceberg-parity.md#fa-3--python-argument-wrappers-subclass-runtimeerror).

**And the general rule:** decimal width and nullability, float aggregation, timestamp zones, cast
failures and array field naming all have registry rows in
[§4](../spark-sql-iceberg-parity.md#4-type-and-value-semantics-declared) and
[§7](../spark-sql-iceberg-parity.md#7-known-spark-parity-divergences-backlog). If a ported job's
numbers move, look there before you look at your code.

## See also

- [session-and-conf.md](session-and-conf.md) — the conf keys behind the inference and timestamp
  behavior above.
- [sql-doors.md](sql-doors.md) — running SQL against the same data.
- [../../examples/notebooks/datasets_tour.ipynb](../../examples/notebooks/datasets_tour.ipynb) —
  the same surfaces exercised over deliberately hostile datasets.
