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

## The fix in user code

Alias the frames and reference the alias, as Spark's own message suggests:

```python
s.alias("a").join(d.alias("b")).select(F.col("b.v"))
```

The join stays the same cross join; only the reference changes.

## Pointers

- The rule and its dated residues: row EX-DF-21 in `docs/spark-sql-iceberg-parity.md`.
- Measured on Spark 4.1.2 (2026-10-03): the pattern raises 1182 naming `v`; the aliased
  cross join answers four rows; with the switch off the pattern raises `MISSING_ATTRIBUTES`.
- Up: [map.md](map.md)
