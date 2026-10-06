# ATTR-ID-1 release note (2026-10-03)

A user-visible tightening that ships with the ATTR-ID-1 stack, not v1.5.2. Self-join
references Spark refuses now refuse in RePark too, with Spark's error.

## Ambiguous self-join references now raise, as in Spark

```python
s.join(d).select(d.v)
```

with `s = d.select("id")` now raises `_LEGACY_ERROR_TEMP_1182`, as Spark Classic does,
where RePark used to answer the left `v`. The same holds after any join whose two sides
share lineage: a `select`, `filter`, `groupBy`, `orderBy`, `withColumn` or aggregate over
a column bound before the join raises when Spark cannot tell which side it points to.
The error is Spark's, word for word, including the `ambiguousAttrs` names.

Spark's switch `spark.sql.analyzer.failAmbiguousSelfJoin=false` turns the 1182 check
off, and references then resolve by id. It is not a return to the old answers: a
reference to a column the join renewed raises
`MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION`, as Spark does, and the
pattern above is one of them.

A free name over duplicate column names tightens too: `filter("v > 1")`, `selectExpr`,
`F.col("v")` and `summary`/`describe` over a frame with two `v` columns now raise
`AMBIGUOUS_REFERENCE`, as Spark does, instead of answering one side's rows.

## Each read of a SQL temp view mints fresh attribute ids, like Spark

```python
left = spark.table("sv"); right = spark.table("sv")
left.join(right, left["id"] == right["id"]).select(left["v"], right["v"])
```

with `sv` created by `CREATE TEMP VIEW sv AS SELECT * FROM tv` now answers
`[(10, 10), (20, 20)]`, as Spark 4.1.2 does, where the stack refused
`_LEGACY_ERROR_TEMP_1182`. A DataFrame view (`createOrReplaceTempView`) keeps
carrying the registered frame's ids on every read, as Spark does.

One shape still differs from Spark, and answers exactly as main does today:

```python
a = spark.table("sv"); a.join(d, a["id"] == d["id"]).select(d["v"])
```

Here `d` is the DataFrame behind `tv`. Spark refuses this with
`_LEGACY_ERROR_TEMP_1182` ("Column v#… are ambiguous"); this release answers it
from `d`'s side. The same holds for `.select(a["v"], d["v"])`, `d["id"]` and
`.filter(d["v"] > 10)`. Card VIEW-LINEAGE-SELFJOIN-1 owns the fix.

## The fix in user code

Alias the frames and reference the alias, as Spark's own message suggests:

```python
s.alias("a").join(d.alias("b")).select(F.col("b.v"))
```

The join stays the same cross join; only the reference changes.

## Qualified self-joins: written files and exports

A self-join whose two sides carry the same column names answers under this
release, where main could not plan it. The internal twin names
(`__repark_l_*`, `__repark_r_*`) never escape the engine (fold SM-2,
2026-10-06):

- Writes of duplicate display names refuse with Spark's exact
  `[COLUMN_ALREADY_EXISTS] The column `<name>` already exists. Choose another
  name or rename the existing column. SQLSTATE: 42711`, before any file is
  created. This covers parquet, json, orc, `saveAsTable` and `writeTo`.
  The csv refusal is a deliberate divergence (row FA-5 in
  `docs/spark-sql-iceberg-parity.md` §5): Spark writes the duplicate header,
  which the engine cannot plan over; `insertInto` keeps writing positionally,
  as Spark does.
- Arrow exports (`pa.table(frame)`) and `toArrow` carry the display names
  (`id, s, v, id, s, v`), exactly as Spark's `arrow_dup` answers.
  `toPandas` carries the display names the same way.
- `mapInPandas` and `mapInArrow` over a frame with duplicate display names
  refuse with Spark's exact `[AMBIGUOUS_REFERENCE]`, naming the first
  duplicate with its qualified candidates (SQLSTATE 42704), instead of
  running with internal names. Scalar and grouped-aggregation pandas UDF
  input Series are positional (`_0`, `_1`, …), as on Spark.
- Temp views over duplicate display names refuse with
  `[COLUMN_ALREADY_EXISTS]` (SQLSTATE 42711) instead of registering twin
  engine names. This is a deliberate divergence (row FA-6 in
  `docs/spark-sql-iceberg-parity.md` §5): Spark registers the view, which
  the engine cannot plan over yet.

## Pointers

- The rule and its dated residues: row EX-DF-21 in `docs/spark-sql-iceberg-parity.md`.
- Measured on Spark 4.1.2 (2026-10-03): the pattern raises 1182 naming `v`; the aliased
  cross join answers four rows; with the switch off the pattern raises `MISSING_ATTRIBUTES`.
- Up: [map.md](map.md)
