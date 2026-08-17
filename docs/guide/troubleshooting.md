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

## `explode_outer` refuses on an array of structs

**Symptom.**

```python
from repark.spark import functions as F

legs = spark.createDataFrame(
    [{"id": 1, "Legs": [{"leg_id": 1, "px": 1.5}, {"leg_id": 2, "px": 2.5}]}]
)
legs.select(F.explode_outer("Legs").alias("leg"))
```

```text
AnalysisException: explode_outer cannot resolve SQL element type for array column
'__repark_arr_…' (engine type 'array<struct<leg_id:bigint,px:double>>'); cast the array or use a
supported element type
```

**Why.** `explode_outer` has to preserve a row when the array is NULL or empty, so it builds a
guard of the shape `CASE WHEN … THEN [CAST(NULL AS <element type>)] ELSE array END` — which needs a
**SQL spelling** for the element type. There is no spelling for a struct element, and the code
refuses rather than defaulting to something plausible (defaulting would corrupt the guard for
VARCHAR and TIMESTAMP elements).

**What to do.** Plain `explode` has no such guard and unnests fine — at the cost of dropping rows
whose array is NULL or empty:

```python
legs.select(F.explode("Legs").alias("leg")).show()
```

```text
+----------------------+
| leg                  |
+----------------------+
| {'leg_id': 1, 'px... |
| {'leg_id': 2, 'px... |
+----------------------+
```

If you need the outer behaviour, `explode` the array and union the null/empty rows back yourself.
An **open finding**, pinned by
`python/repark/tests/test_datasets_facade.py::test_nested_explode_outer_on_array_of_struct_refuses_loud`.

---

## `count()` fails on a deep `dynamicFlatten` plan

**Symptom.** The flatten itself is fine, and `to_arrow()` returns the right rows — only `count()`
reds, inside a DataFusion optimizer rule:

```python
rows = [{"id": 1, "Legs": [{"leg_id": 1, "Fills": [{"f": 1.0}]}], "Tags": ["a", "b"]}]
deep = spark.createDataFrame(rows).dynamicFlatten()
print(deep.columns)
deep.count()
```

```text
['Legs_leg_id', 'Legs_Fills_f', 'Tags', 'id']
PySparkException: datafusion engine error: Optimizer rule 'push_down_leaf_projections' failed
caused by
Internal error: Assertion failed: expr.is_empty(): Unnest(Unnest { … })      ← plan dump elided
```

**Why.** Two array levels plus a sibling top-level array means several explode passes, and the
plan `dynamicFlatten` builds leaves a qualified field beside its unqualified twin.
`push_down_leaf_projections` trips over that pair — and it is `count()` that triggers the rule; the
export path never reaches it.

**What to do.** Count through the export path, which returns the correct rows:

```python
deep.to_arrow().num_rows
```

```text
2
```

…or flatten shallowly (a single explode pass counts fine on the same data). An **open finding**,
pinned by
`python/repark/tests/test_datasets_facade.py::test_nested_dynamic_flatten_count_action_refuses_loud`,
and called out in the tour notebook where it bites.

---

## `smartCsv` picked the wrong delimiter

**Symptom.** No error at all — a silently wrong parse. The column names are data, and a row has
gone missing.

That used to happen on files like this, where a rival delimiter lived *inside quoted cells* and
the rows were ragged. Auto-detect now counts quote-aware and keeps the comma:

```python
p = Path(tmpdir) / "ragged.csv"
p.write_text('id,name,note\n1,"a;b",x\n2,"c;d",y,extra\n3,"e;f",z,extra,more\n')
auto = spark.read.smartCsv(str(p))
print(auto.columns)
auto.show()
```

```text
['id', 'name', 'note', '_c3', '_c4']
+----+------+------+-------+------+
| id | name | note | _c3   | _c4  |
+----+------+------+-------+------+
| 1  | a;b  | x    | NULL  | NULL |
| 2  | c;d  | y    | extra | NULL |
| 3  | e;f  | z    | extra | more |
+----+------+------+-------+------+
```

```python
auto.describe_ingest()["delimiter"], auto.describe_ingest()["skipped_lines"]
```

```text
(',', 0)
```

**Why it used to fail.** Detection scored the four candidates (comma, semicolon, tab, pipe) by raw field-count agreement, and
`csv.reader` per line only honours a quote that *starts* a field. `;` inside `"a;b"` therefore
split every line into a tidy 2 and beat the ragged comma table; the header was voted out as
preamble. Detection now (1) ignores delimiters inside quotes and (2) prefers a candidate that
splits an identifier header into the modal width, then ranks agreement.

**What to do.** Declare the delimiter whenever you know it. `sep=` short-circuits detection
entirely and must be a **single character** (empty / multi-char raises
`IllegalArgumentException`):

```python
spark.read.smartCsv(str(p), sep=",").show()
```

`.option("sep", ...)` / `.option("delimiter", ...)` on the reader resolve to the same parameter.
Auto-detection is still a **guess** — a header that is not identifier-like, or a file with no
header, falls back to quote-aware agreement and can still pick a rival that lives unquoted in
every value. Treat auto-detect as a convenience for exploration, not a production contract.
Pinned by
`python/repark/tests/test_datasets_facade.py::test_smartcsv_delimiter_autodetect_picks_a_rival_delimiter`
and the (a)–(e) unit pins in `python/repark/tests/test_t4_csv_smart.py`.

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
