# WITHCOLUMN-DUP-DISPLAY-1 — withColumn over duplicate-display frames raises a bare AMBIGUOUS_REFERENCE

**Filed: 2026-10-02 from ATTR-ID-1 S3e** (source:
`/tmp/oc-worker/direct/wo/attr-id-1/s3e/final-handback.json`,
`out_of_scope_observed[4]` — "owner: engine/bounds follow-up card";
verified present 2026-10-02). **Severity:** refusal shape, not yet rated
(owner). **Status:** pre-existing, main-identical via untouched
`_iter_bound_columns`. **Target:** mid-term.

## The divergence it records

`withColumn` over duplicate-display frames raises a bare
AMBIGUOUS_REFERENCE via `_iter_bound_columns`, on paths S3e did not touch
(main-identical).

## Repro

Not yet measured: the source names the shape but carries no
Spark-versus-RePark error dump. Repro to run on the day, against live
Spark 4.1.2:

```python
both = left.join(right, "k")
both.withColumn("w", F.lit(1)).collect()
```

with the joined frame carrying duplicate display names; compare Spark's
answer or refusal shape with RePark's bare AMBIGUOUS_REFERENCE.

## Scope

The engine/bounds follow-up: `withColumn` over duplicate-display frames
answers or refuses exactly as Spark does.

## Out of scope

- The S3c withColumn binding cutover (shipped; its pins stay green).
- Duplicate-display binding elsewhere (select/filter/sort bind by
  attribute id under S3b/S3e; only the `withColumn` leg moves).

## Clauses (draft)

- **C-001** The repro answers or refuses exactly as Spark.
- **C-002** `withColumn` over distinct-display frames is unchanged.
- **C-003** The S3c withColumn pins stay green.

## Pointers

- Up: [map.md](map.md) · Evidence (evidence location):
  `/tmp/oc-worker/direct/wo/attr-id-1/s3e/final-handback.json`
