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

The escape hatch is Spark's: `spark.sql.analyzer.failAmbiguousSelfJoin=false` turns the
check off, and every reference binds to the left side by id, as RePark used to.

A free name over duplicate column names tightens too: `filter("v > 1")`, `selectExpr`,
`F.col("v")` and `summary`/`describe` over a frame with two `v` columns now raise
`AMBIGUOUS_REFERENCE`, as Spark does, instead of answering one side's rows.

## The fix in user code

Alias the frames and reference the alias, as Spark's own message suggests:

```python
s.alias("a").join(d.alias("b"), "id").select("b.v")
```

## Pointers

- The rule and its dated residues: row EX-DF-21 in `docs/spark-sql-iceberg-parity.md`.
- Up: [map.md](map.md)
