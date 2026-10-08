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

- A per-side key is reachable on the joined frame and after a `filter` or `sort` of it. After a
  narrowing `select`, an `.alias`, in `groupBy` / `withColumn`, or written with backticks in a text
  predicate it refuses with the same message as v1.5.3; Spark answers past a `select`.
- Mixed `INT` / `STRING` keys keep the left key's type; Spark shows `STRING` on `right` and
  `BIGINT` on `full`.
- On the SQL door, a `USING` join followed by an `ON` join in the same `FROM`, a `NATURAL` join,
  and `SELECT *, r.id` keep the left key in the star; `SELECT id, l.id, r.id` refuses.

The three limits are registry rows `USING-SIDE-KEY-REACH-1`, `USING-MIXED-KEY-TYPE-1` and
`USING-SQL-STAR-SHAPES-1` in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).
