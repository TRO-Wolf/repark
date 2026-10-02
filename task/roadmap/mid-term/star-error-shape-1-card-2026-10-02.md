# STAR-ERROR-SHAPE-1 — Spark's star error classes where RePark gives engine errors

**Filed: 2026-10-02 from ATTR-ID-1 S3e** (source:
`/tmp/oc-worker/direct/wo/attr-id-1/s3e/final-handback.json`,
`out_of_scope_observed[2]` — "owner: error-shaping follow-up card";
verified present 2026-10-02). **Severity:** error-contract divergence,
not yet rated (owner). **Status:** pre-existing, kept on the main path.
**Target:** mid-term.

## The divergence it records

Spark raises `INVALID_USAGE_OF_STAR_OR_REGEX` for `F.col("q.*")` in
`filter` / `withColumn` / `orderBy`, `CANNOT_RESOLVE_STAR_EXPAND` for
unknown stars, and shapes `getitem` `q.*`; RePark gives engine errors on
the main path.

## Repro

Not yet measured: the source names the classes but carries no
Spark-versus-RePark error dump. Repro to run on the day, against live
Spark 4.1.2:

```python
df.filter(F.col("q.*"))
df.withColumn("w", F.col("q.*"))
df.orderBy(F.col("q.*"))
df.select("missing.*")
df["q.*"]
```

Each leg must raise (or shape) Spark's class with Spark's message.

## Scope

The three star shapes: `INVALID_USAGE_OF_STAR_OR_REGEX` on the
filter/withColumn/orderBy legs, `CANNOT_RESOLVE_STAR_EXPAND` for unknown
stars, and shaped `getitem` `q.*`.

## Out of scope

- Valid star expansion (unchanged).
- UNRESOLVED suggestion lists
  ([unresolved-suggest-qualified-1-card-2026-10-02.md](unresolved-suggest-qualified-1-card-2026-10-02.md)).

## Clauses (draft)

- **C-001** Each repro leg raises Spark's class and message shape.
- **C-002** Valid stars expand exactly as today.
- **C-003** One test row per door per shape.

## Pointers

- Up: [map.md](map.md) · Evidence (evidence location):
  `/tmp/oc-worker/direct/wo/attr-id-1/s3e/final-handback.json`
