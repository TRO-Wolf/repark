# USING-PER-SIDE-KEYS-1 release note (2026-10-08)

A follow-up to the v1.5.3 notes' section "Qualified self-joins", whose last bullet named this
unit. It ships with the release that carries branch `fix/using-per-side-keys-1`, not v1.5.3.
Every comparison below is versus v1.5.2 and v1.5.3 (they answer alike here). Ledger:
[using-per-side-keys-1-ledger.md](../../ledgers/staging/using-per-side-keys-1-ledger.md).

## A wrong answer fixed: chained `USING` joins after a `right` or `full` join

```python
l.join(r, "id", "full").join(q, "id", "full")
```

and the SQL form `l FULL JOIN r USING (id) FULL JOIN q USING (id)` matched the second join on the
left key, so a row that only the right side carried never met its match. With ids `1,2,3` /
`2,3,4` / `2,3,4`, earlier releases answer five rows, two of them `(NULL, NULL, z, NULL)` and
`(NULL, NULL, NULL, r)`; Spark and this release answer four, the last being `(4, NULL, z, r)`.
`full` or `right` followed by an `inner` join lost that row altogether. Every release measured
back to 1.0.0 has the defect; if you chain `USING` joins after a `right` or `full` join, re-run
the query.

## `right` and `full` `USING` joins show the merged key

`select("*")`, the unqualified key, `groupBy`, `withColumn`, `distinct` and writes over a `right`
or `full` `USING` join now carry the right key on `right` and `coalesce(left, right)` on `full`,
as Spark does, where earlier releases carried the left key (`NULL` on rows only the right side
has). The same holds for `SELECT *` and the unqualified key on the SQL door.

## Per-side keys answer

On the DataFrame door `l.id` and `r.id` answer their own side's key after a `left`, `right` or
`full` `USING` join, in `select`, `selectExpr`, `filter`, `sort`, a join condition and `l.*` /
`r.*`, written as a string, `col("r.id")`, `frame["r.id"]` or a reference held from before the
join (`r["id"]`), on aliased and unaliased frames. v1.5.3 refused the right side on aliased frames
and answered the left key silently on unaliased ones.

On the SQL door `WHERE id > 2` over a `USING` join answers on every join type; it refused with
`AMBIGUOUS_REFERENCE`.

## Known limits

- A per-side key is reachable on the joined frame, after any chain of `filter` and `sort` over
  it, and after `cache()` or `persist()`. After a narrowing `select`, an `.alias`, a
  `localCheckpoint()`, in `groupBy` / `withColumn`, or written with backticks in a text
  predicate it refuses with the same message as v1.5.3; Spark answers past a `select`.
- Sorting by the side key that is not shown (`l.id` after a `right` or `full` join) after
  `distinct`, `union` or a second `USING` join refuses; v1.5.3 answered there, sorting by the
  left key it showed.
- A key whose two sides have different types shows the right key's type on `right` and, with
  `spark.sql.ansi.enabled=true` on the DataFrame door, Spark's common type on `full` for the
  measured pairs. With ANSI off, for other pairs, and on the SQL door, a mixed-type `full` key
  shows the left key, as before.
- On the SQL door a statement that names both the unqualified key and a side key in one
  select, or has `l.*` / `r.*`, `QUALIFY`, a grouped side key, or a comma-joined relation beside
  the join, answers exactly as v1.5.3 does. `SELECT *` over four or more `USING`-chained
  relations and `SELECT id, l.id, r.id` refuse, as before. The key keeps its position in the
  left relation; Spark moves it to the front.
- A stale right-side key in `sort` after a `left_semi` / `left_anti` join refuses with
  `MISSING_ATTRIBUTES`, as Spark and v1.5.2 do; v1.5.3 answered the left rows.

The limits are registry rows `USING-SIDE-KEY-REACH-1`, `USING-MIXED-KEY-TYPE-1` and
`USING-SQL-STAR-SHAPES-1` in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).
