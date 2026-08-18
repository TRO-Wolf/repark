# Troubleshooting

The things that surprise people, collected in one place: symptom, why, what to do. Every entry
below was reproduced against a built module — the error text is copied from a real run, not
paraphrased.

A note on where truth lives. Some of these are **declared divergences** with a row in
[the divergence registry](../spark-sql-iceberg-parity.md) and a test that pins them; that row is
authoritative and this page only points at it. Others are **open findings** — reported, pinned as
current behavior, not yet fixed — and this page says so rather than dressing them up as design.

---

## Install and first import

**Symptom.** You want to check that the wheel actually landed.

**What to do.** The import smoke CI runs is one line:

```
python -c "import repark; print('repark', repark.__version__)"
```

```text
repark 0.3.2
```

`repark.__version__` comes from the installed distribution metadata, so it tells you what pip
resolved. `spark.version` is a different string — `repark-<version>` — and is deliberately not a
Spark release number.

**One thing to watch.** PyPI carries a name-reservation package that outversions a locally built
wheel, so when you are testing a wheel you built yourself, install it **by file path**
(`pip install ./dist/repark-….whl`), not by bare name. Which platforms have a published wheel is
release state — [STATUS.md](../../STATUS.md) is the file that says, and today it is manylinux
x86_64 only.

---

## A dict cell became a struct, not a map

**Symptom.** A nested dict in `createDataFrame` infers `StructType` where PySpark infers
`MapType`. No error — just a different schema, which then changes what `dynamicFlatten`,
`toPandas` and struct-field access do.

```python
spark.createDataFrame([{"id": 1, "payload": {"x": 1}}]).schema.simpleString()
```

```text
struct<id:bigint,payload:struct<x:bigint>>
```

**Why.** `spark.sql.pyspark.inferNestedDictAsStruct.enabled` defaults to `"true"` in repark;
Spark's default (SPARK-35929) is `"false"`. This is an owner decision, not an accident, and it is
registry row [FA-4](../spark-sql-iceberg-parity.md#fa-4--infernesteddictasstruct-defaults-to-true).

**What to do.** Set the conf to `"false"` and you get byte-identical PySpark inference back:

```python
spark.conf.set("spark.sql.pyspark.inferNestedDictAsStruct.enabled", "false")
spark.createDataFrame([{"id": 1, "payload": {"x": 1}}]).schema.simpleString()
```

```text
struct<id:bigint,payload:map<string,bigint>>
```

An explicit `schema=` always wins over either default, and a dict that *is* the row (rather than a
cell) is unaffected either way.

---

## `select("p.a")` cannot resolve a struct field

**Symptom.**

```python
nested = spark.createDataFrame([{"id": 1, "p": {"a": 10, "b": 20}}])
nested.select("p.a")
```

```text
AnalysisException: A column with name `p.a` cannot be resolved; available columns: ['id', 'p']
```

**Why.** Bare-string resolution in `select` / `df[...]` matches against **top-level** column names
only — it never splits on `.`. And `col("p.a")` is a *qualified reference* (`"p"."a"`, the
table-alias form MERGE uses), not nested-field access.

**What to do.** Any of these, all executed:

```python
nested.select(nested.p.getField("a").alias("a")).show()
nested.select(nested["p"]["b"].alias("b")).show()
nested.selectExpr("p.a AS a", "p.b AS b").show()
```

```text
+----+----+
| a  | b  |
+----+----+
| 10 | 20 |
+----+----+
```

`nested.p.a` (attribute syntax) works too. This is an **open gap**, not a declared divergence —
there is no registry row; it is tracked as a known-fail in the compatibility census.

---

## A CSV with comma decimals refuses on read

**Symptom.** Inference resolves a sensible decimal type, and then the read itself raises:

```python
euro = Path(tmpdir) / "euro.csv"
euro.write_text("id;price\n1;760,35\n2;12,50\n")
sm = spark.read.smartCsv(str(euro), sep=";")
print(sm.schema.simpleString())
sm.collect()
```

```text
struct<id:int,price:decimal(5,2)>
PySparkException: Arrow error: Cast error: Cannot cast string '760,35' to value of
Decimal128(38, 10) type
```

**Why.** `smartCsv`'s inference ladder normalizes `760,35` while *resolving* the column type — so
it correctly lands on `decimal(5,2)` — but the materializing cast is handed the **raw** cell text,
and the cast kernel has no comma-as-decimal-separator parse. The type promises a value the read
cannot deliver.

**What to do.** There is no conf that enables comma decimals. Take the column as a string with an
explicit schema and convert it yourself (or pre-normalize the file):

```python
from repark.spark.types import IntegerType, StringType, StructField, StructType

schema = StructType([StructField("id", IntegerType()), StructField("price", StringType())])
spark.read.option("header", True).option("sep", ";").schema(schema).csv(str(euro)).show()
```

```text
+----+--------+
| id | price  |
+----+--------+
| 1  | 760,35 |
| 2  | 12,50  |
+----+--------+
```

This is an **open finding**, reported and pinned as current behavior —
`python/repark/tests/test_datasets_facade.py::test_smartcsv_euro_comma_decimal_cast_refuses_loud`
is the pin. It has no registry row.

---

## `explode_outer` on an array of structs

**Symptom (resolved).** `explode_outer` on `array<struct<…>>` used to refuse because the
null/empty guard could not spell a struct element type. It now keeps NULL and empty lists
as one null-element row, matching Spark:

```python
from repark.spark import functions as F

legs = spark.createDataFrame(
    [
        {"id": 1, "Legs": [{"leg_id": 1, "px": 1.5}, {"leg_id": 2, "px": 2.5}]},
        {"id": 2, "Legs": []},
        {"id": 3, "Legs": None},
    ]
)
legs.select("id", F.explode_outer("Legs").alias("leg")).orderBy("id").show()
```

```text
+---+----------------------+
| id|                   leg|
+---+----------------------+
|  1| {'leg_id': 1, 'px... |
|  1| {'leg_id': 2, 'px... |
|  2|                  NULL|
|  3|                  NULL|
+---+----------------------+
```

**Why it works.** The guard is `CASE WHEN array IS NULL OR empty THEN make_array(CAST(NULL
AS struct<…>)) ELSE array END`. Engine SQL accepts that CAST. Void / `array<Null>`
elements have no CAST spelling — the same CASE uses untyped `make_array(NULL)` so
an empty void sibling cannot cartesian-drop typed sibling lists. Map element types, and
struct fields whose names are not simple identifiers, still refuse loud (no safe CAST
spelling). Pin:
`python/repark/tests/test_datasets_facade.py::test_nested_explode_outer_on_array_of_struct_refuses_loud`.

`dynamicFlatten()` defaults to the same keep-both behaviour (`empty_as_null=True`) so a
GA4-shaped frame with empty `items` / `user_properties` does not vanish. Pass
`empty_as_null=False` for the polars ≥2.0 default (NULL kept, empty dropped).

---

## `count()` — and any narrowing `select` — on a deep `dynamicFlatten` plan (FIXED)

**Status: fixed** (2026-08-18, `task/c25-bugfix-ledger.md` → *DEFECT-2*). The section stays as a
fixed entry: the behaviour it used to describe is now pinned *absent*, and the `cache()` line
below is no longer a workaround — it is just caching.

**What used to happen.** After a `dynamicFlatten` that takes two or more explode passes, the
flatten itself and the whole-frame export were fine, but `count()` — and any `select(...)` narrow
enough to drop an inner explode pass's output column — raised out of a DataFusion optimizer rule:

```text
PySparkException: datafusion engine error: Optimizer rule 'push_down_leaf_projections' failed
caused by
Internal error: Assertion failed: expr.is_empty(): Unnest(Unnest { … })
```

…or, on other subsets of the same plan:

```text
Schema error: Schema contains qualified field name datafusion.public.__repark_expl_…."Legs"
and unqualified field name "Legs" which would be ambiguous
```

It was **order-dependent**: renaming the sibling list so it exploded in a different position made
all 15 non-empty projection subsets — and `count()` — succeed on identical data.

**Why it happened, and what changed.** Both errors came from `push_down_leaf_projections`, the
pass-2 half of DataFusion 54.1's leaf-expression extraction. On a plan that stacks `Unnest` nodes
under a projection carrying a `get_field` leaf — the shape every multi-pass flatten and every
repeated `explode` produces — the rule pushes into a node's inputs with
`with_new_exprs(node.expressions(), …)`, and `Unnest` hands out an exec column from
`expressions()` while its own `with_new_exprs` asserts that vector empty; on the other subsets it
merges a pushed pass-through column into a projection that already re-aliases the same name and
lands a qualified and an unqualified spelling of one field in a single schema. Neither is
reachable from repark's plan shape, and both are optimizer-only — with the rule declining, the
identical plan executes and returns the identical rows.

So repark keeps the rule and takes it away from the plans it breaks. The core session installs
DataFusion's own optimizer rule list, in DataFusion's own order, with `push_down_leaf_projections`
wrapped: no `Unnest` in the subtree and the rule runs untouched (its errors included); an `Unnest`
in the subtree and repark tries the rule and keeps the unrewritten plan only if it actually fails.

**Perf (measured, not argued).** A query with no `Unnest` optimizes to the byte-identical plan
stock DataFusion produces, at the same speed — which matters, because turning the rule off wholesale
would have cost up to ~8x in one measured run (ratio is load-sensitive; direction and width-monotonicity reproduce) on a filtered wide-struct parquet scan (500k rows × 60 struct fields,
0.167s → 1.297s), and declining on the mere *presence* of an `Unnest` would have cost **11.8x** on
a wide-struct scan that merely had an unnest alongside (9.1s → 107.5s). What remains is bounded to
the shapes the rule genuinely fails on, which keep the correct, unoptimized plan.

`datafusion.optimizer.enable_leaf_expression_pushdown` is untouched at DataFusion's default and
still yours to set to `false` if you want the optimization off entirely. There is deliberately no
knob that restores the miscompile.

**What works now.** Every projection subset, in either explode order:

```python
rows = [{"id": 1, "Legs": [{"leg_id": 1, "Fills": [{"f": 1.0}]}], "Tags": ["a", "b"]}]
deep = spark.createDataFrame(rows).dynamicFlatten()
print(deep.columns)                     # ['Legs_leg_id', 'Legs_Fills_f', 'Tags', 'id']
deep.count()                            # 2  — equals deep.to_arrow().num_rows
deep.select("id").to_arrow().num_rows   # 2
deep.agg(F.count(F.lit(1))).to_arrow().to_pylist()   # [{'count(1)': 2}]
```

`cache()` still works exactly as it always did — as caching, not as an escape hatch:

```python
cached = deep.cache()
cached.count()                                          # 2
cached.select("Legs_Fills_f").to_arrow().to_pylist()    # [{'Legs_Fills_f': 1.0}, …]
```

Pinned by
`python/repark/tests/test_dynamic_flatten.py::test_multi_pass_flatten_every_projection_subset_is_green`
(the full 15-subset matrix, both explode orders),
`…::test_multi_pass_flatten_count_and_agg_are_green`,
`python/repark/tests/test_explode_rewrite.py::test_two_pass_explode_chain_survives_a_narrowing_projection`
(the same shape without `dynamicFlatten`),
`python/repark/tests/test_datasets_facade.py::test_nested_dynamic_flatten_count_action_is_green`,
and engine-side by
`crates/repark-core/src/session/df_guards.rs` → the five
`session::df_guard_tests::*leaf_pushdown*` pins (the flag stays enabled, the wrapper is installed, a
no-`Unnest` plan matches stock DataFusion byte for byte, an `Unnest` plan the rule can rewrite
still gets the optimization, and an explicit conf can still disable it).

---

## `smartCsv` picked the wrong delimiter

**Symptom.** No error at all — a silently wrong parse. The column names are data, and a row has
gone missing:

```python
p = Path(tmpdir) / "ragged.csv"
p.write_text('id,name,note\n1,"a;b",x\n2,"c;d",y,extra\n3,"e;f",z,extra,more\n')
auto = spark.read.smartCsv(str(p))
print(auto.columns)
auto.show()
```

```text
['1,"a', 'b",x']
+------+-----------------+
| 1,"a | b",x            |
+------+-----------------+
| 2,"c | d",y,extra      |
| 3,"e | f",z,extra,more |
+------+-----------------+
```

**Why.** Delimiter detection scores the candidates `, ; \t |` purely by **field-count agreement**
across lines. Here the comma-delimited rows are ragged (3, 4, 5 fields), while a rival `;` — which
only appears inside quoted values — splits every line into a consistent 2, because a quote is only
honoured when it *starts* a field. `;` wins the agreement contest, and the header detector then
votes the real header line out as preamble.

This is a **known limit**, not a closed class. Three B4 redesigns (field-count-first, identifier
header-join, one-splitter + structural join) each closed the named corpus and regressed an
unnamed one, including value corruption on the declared-`sep` path. Detect and parse stay on
origin/main `csv.reader` semantics.

`describe_ingest()` tells you exactly what happened, which is the fastest way to confirm it:

```python
auto.describe_ingest()["delimiter"], auto.describe_ingest()["skipped_lines"]
```

```text
(';', 1)
```

**What to do.** Declare the delimiter whenever you know it. On European-locale `;` files —
and any file whose values embed a rival — pass `sep=';'` (or the known delimiter). `sep=`
short-circuits detection entirely and must be a **single character** other than newline, CR,
or quote (empty / multi-char / those three raise `IllegalArgumentException`).
`option("sep", "")` does **not** fall through to `option("delimiter", ...)` —
the empty string is a present value and refuses loud:

```python
spark.read.option("sep", "").option("delimiter", ";").smartCsv(str(p))
# IllegalArgumentException: smartCsv sep must be a single character ...
```

When you *do* know the delimiter, pass it (kwarg or option) and the ragged
comma file above parses as comma:

```python
spark.read.smartCsv(str(p), sep=",").show()
```

```text
+----+------+------+-------+------+
| id | name | note | _c3   | _c4  |
+----+------+------+-------+------+
| 1  | a;b  | x    | NULL  | NULL |
| 2  | c;d  | y    | extra | NULL |
| 3  | e;f  | z    | extra | more |
+----+------+------+-------+------+
```

`.option("sep", ...)` / `.option("delimiter", ...)` on the reader resolve to the same parameter.
Auto-detection is a **guess**; treat it as a convenience for exploration, not a production
contract. An **open finding**, pinned by
`python/repark/tests/test_datasets_facade.py::test_smartcsv_delimiter_autodetect_picks_a_rival_delimiter`.

---

## A raw `ParserError` from `repark.sql`

**Symptom.**

```python
import repark

repark.sql("CREATE TABLE t (id BIGINT) USING iceberg TBLPROPERTIES ('a'='b')")
```

```text
ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1,
Column: 28")
```

**Why.** You are on the wrong door. `repark.sql` is the **native** door — a stock DataFusion
session with no Spark extension and its own catalog — so Spark dialect (`USING`, `TBLPROPERTIES`,
`PARTITIONED BY`, backticks, `LATERAL VIEW`, `CALL … system …`) does not parse there. The engine's
ANSI door has a wrong-door sniff that would upgrade this error into a useful one, but that door is
Rust-reachable and the Python callable does not have it yet.

**What to do.** Move the string to `spark.sql(...)`. That is the migration door and the only one
that sees your session's temp views, catalogs and conf. [sql-doors.md](sql-doors.md) is the full
story.

---

## Timestamps came out in UTC

**Symptom.**

```python
spark.conf.get("spark.sql.session.timeZone")
```

```text
UTC
```

…on a session that never set a zone — where Spark would have used the JVM's local zone.

**Why.** repark reads nothing from the host environment
([ADR-0004](../adr/0004-server-prep-disciplines.md)), so the default is the fixed constant `UTC`.
Registry row [TZ-2](../spark-sql-iceberg-parity.md#tz-2--the-session-timezone-default-is-utc).

**And the trap next to it.** A *runtime* `conf.set` of the session zone is accepted for source
compatibility, but neither validated nor applied:

```python
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
spark.conf.get("spark.sql.session.timeZone")
```

```text
UserWarning: config 'spark.sql.session.timeZone' is accepted for source compatibility but NOT
applied at runtime, and its value is NOT validated: the session timezone is resolved AND validated
exactly once, at getOrCreate (default 'UTC') …
UTC
```

The warning fires **once per process**, so a second session in the same interpreter gets a silent
no-op. Registry row
[TZ-3](../spark-sql-iceberg-parity.md#tz-3--a-runtime-confset-of-the-session-zone-is-accepted-neither-validated-nor-applied).

**What to do.** Set the zone on the **builder**, where it is validated:

```python
spark = (
    ReparkSession.builder.appName("etl")
    .config("spark.sql.session.timeZone", "America/New_York")
    .getOrCreate()
)
```

An unknown zone there raises `IllegalArgumentException` instead of being swallowed.

---

## Where to look next

| You are hitting… | Read |
|---|---|
| A difference from Apache Spark, and want the authoritative statement | [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) |
| A conf key that appeared to do nothing | [session-and-conf.md](session-and-conf.md) |
| An error from the wrong `sql()` | [sql-doors.md](sql-doors.md) |
| A nested-data shape you cannot flatten | [dataframe-guide.md](dataframe-guide.md) |
| An Iceberg statement that refuses | [iceberg-guide.md](iceberg-guide.md) |
| Something in the TA library | [ta-guide.md](ta-guide.md) |
| Release / platform / wheel state | [../../STATUS.md](../../STATUS.md) |
